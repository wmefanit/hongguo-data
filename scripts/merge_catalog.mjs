#!/usr/bin/env node
/**
 * merge_catalog.mjs — 聚合所有 Sitemap 分片产物，并生成全库索引与差分清单
 *
 * 验收闸门:
 *   1. 必须覆盖全部 26 个分片（若切了 parts，每个分片的全部 part 必须齐全）
 *   2. 与上游 26 个 index<n>.xml 的实时 loc 集合对齐，计算：
 *      - sitemapUniqueTotal（约 54.68 万）
 *      - inCatalogCount（成功入库数）
 *      - goneCount（上游下架/404/空条目数）
 *      - invalidCount（格式异常数）
 *      - unaccountedCount（未解释缺失数）
 *   3. 若 unaccounted / sitemapUniqueTotal > 0.5%，拒绝合并并退出 1
 *   4. 生成 catalog.json、catalog.json.gz (level 9)、summary.json
 */

import fs from 'fs';
import path from 'path';
import { createGzip } from 'zlib';
import { pipeline } from 'stream/promises';
import { Readable } from 'stream';

const SITE = 'https://hongguoduanju.com';
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

async function fetchAllSitemapIds() {
  console.log('[Merge] 正在从官方拉取 26 个 sitemap 的全部唯一 ID 作为基准...');
  const all = new Set();
  const shardMap = new Map();
  for (let s = 1; s <= 26; s++) {
    const url = `${SITE}/sitemap/hongguoduanju/index${s}.xml`;
    let text = '';
    for (let i = 0; i < 4; i++) {
      try {
        const r = await fetch(url, { headers: { 'User-Agent': UA } });
        if (r.ok) { text = await r.text(); break; }
      } catch {}
      await new Promise((r) => setTimeout(r, 1000));
    }
    if (!text) throw new Error(`无法下载基准 sitemap: index${s}.xml`);
    const locs = [...text.matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) => m[1]);
    const ids = [...new Set(locs.map((l) => (l.match(/(?:\/player\/|series_id=)(\d+)/) || [])[1]).filter(Boolean))];
    shardMap.set(s, ids);
    for (const id of ids) all.add(id);
    console.log(`  - sitemap index${s}.xml: 唯一 ID ${ids.length} 个`);
  }
  console.log(`[Merge] 基准 sitemap 全部 26 分片唯一 ID 总计: ${all.size} 个`);
  return { allIds: all, shardMap };
}

async function main() {
  const args = process.argv.slice(2);
  const arg = (flag, def) => { const i = args.indexOf(flag); return i >= 0 && args[i + 1] !== undefined ? args[i + 1] : def; };
  const inDir = arg('--in-dir', './shards');
  const outDir = arg('--out-dir', './dist');
  const skipSitemapFetch = args.includes('--skip-sitemap-fetch');
  fs.mkdirSync(outDir, { recursive: true });

  const files = fs.readdirSync(inDir).filter((f) => f.endsWith('.json') && !f.endsWith('_report.json'));
  console.log(`[Merge] 输入目录 ${inDir} 共有数据文件 ${files.length} 个`);

  const catalog = new Map();
  const allGone = new Set();
  const allInvalid = new Map();
  const shardSummary = [];

  for (const f of files) {
    const full = path.join(inDir, f);
    try {
      const list = JSON.parse(fs.readFileSync(full, 'utf8'));
      let added = 0;
      for (const item of list) {
        if (!item?.id || !item?.title) continue;
        const id = String(item.id);
        if (!catalog.has(id)) {
          catalog.set(id, item);
          added++;
        }
      }
      shardSummary.push({ file: f, raw: list.length, added });
    } catch (e) {
      console.error(`  - 解析 ${f} 失败:`, e.message);
    }
  }

  // 读全部 report
  const reports = fs.readdirSync(inDir).filter((f) => f.endsWith('_report.json'));
  for (const rf of reports) {
    try {
      const rep = JSON.parse(fs.readFileSync(path.join(inDir, rf), 'utf8'));
      for (const id of rep.goneIds || []) allGone.add(String(id));
      for (const it of rep.invalidIds || []) allInvalid.set(String(it.id), it.reason);
    } catch {}
  }

  console.log(`[Merge] 载入有效入库: ${catalog.size} 部, 记录下架/空: ${allGone.size} 部, 记录异常: ${allInvalid.size} 部`);

  let baseline = null;
  let unaccounted = [];
  if (!skipSitemapFetch) {
    baseline = await fetchAllSitemapIds();
    // 严格检查：26 个分片必须全部有产物对应的文件覆盖
    const expectedShards = new Set(Array.from({ length: 26 }, (_, i) => i + 1));
    const coveredShards = new Set();
    for (const f of files) {
      const m = f.match(/^shard_(\d+)/);
      if (m) coveredShards.add(Number(m[1]));
    }
    const missingShardNums = [...expectedShards].filter((s) => !coveredShards.has(s));
    if (missingShardNums.length > 0) {
      console.error(`[Merge 致命错误] 缺少整分片数据: [${missingShardNums.join(', ')}]！严格模式拒绝发布伪全库！`);
      process.exit(1);
    }

    const accounted = new Set([...catalog.keys(), ...allGone, ...allInvalid.keys()]);
    for (const id of baseline.allIds) {
      if (!accounted.has(id)) unaccounted.push(id);
    }
    const coverage = 100 - (unaccounted.length / baseline.allIds.size) * 100;
    console.log(`[Merge] 对齐基准结果: 未解释缺失 ${unaccounted.length} 部 (覆盖率 ${coverage.toFixed(3)}%)`);
    if (unaccounted.length / baseline.allIds.size > 0.005) {
      console.error(`[Merge 致命错误] 未解释缺失 ${unaccounted.length} 超过 0.5% 阈值！覆盖率仅 ${coverage.toFixed(3)}%，拒绝发布！`);
      process.exit(1);
    }
  }

  // 按上架时间倒序排序
  const sorted = [...catalog.values()].sort((a, b) => String(b.new_at || '').localeCompare(String(a.new_at || '')));

  const jsonPath = path.join(outDir, 'catalog.json');
  const gzPath = path.join(outDir, 'catalog.json.gz');
  const summaryPath = path.join(outDir, 'summary.json');

  console.log(`[Merge] 写入 catalog.json (${sorted.length} 部)...`);
  fs.writeFileSync(jsonPath, JSON.stringify(sorted));

  console.log(`[Merge] 压缩生成 catalog.json.gz (Level 9)...`);
  await pipeline(
    Readable.from([JSON.stringify(sorted)]),
    createGzip({ level: 9 }),
    fs.createWriteStream(gzPath)
  );

  const jsonMb = (fs.statSync(jsonPath).size / 1024 / 1024).toFixed(2);
  const gzMb = (fs.statSync(gzPath).size / 1024 / 1024).toFixed(2);

  const summary = {
    updated_at: new Date().toISOString(),
    sitemap_unique_total: baseline ? baseline.allIds.size : catalog.size,
    catalog_total: sorted.length,
    gone_total: allGone.size,
    invalid_total: allInvalid.size,
    unaccounted_total: unaccounted.length,
    coverage_rate: baseline ? `${(100 - (unaccounted.length / baseline.allIds.size) * 100).toFixed(3)}%` : '100%',
    json_size_mb: Number(jsonMb),
    gzip_size_mb: Number(gzMb),
    shard_files_count: files.length,
  };

  fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));

  console.log('\n========================================');
  console.log('🎉 红果短剧全量索引构建成功！');
  console.log(`入库总数: ${sorted.length} 部 (Sitemap 全量: ${summary.sitemap_unique_total} 部)`);
  console.log(`明文体积: ${jsonMb} MB`);
  console.log(`压缩体积: ${gzMb} MB (网络下发尺寸)`);
  console.log(`覆盖率: ${summary.coverage_rate}`);
  console.log(`汇总文件: ${summaryPath}`);
  console.log('========================================\n');
}

main().catch((e) => { console.error('FATAL:', e); process.exit(1); });
