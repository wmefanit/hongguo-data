#!/usr/bin/env node
// merge_catalog.mjs — 聚合各分片产物为全库索引，并对上游 Sitemap 做严格对齐校验
// 验收闸门：
// 1. 26 个分片全部有产物，缺片退出 1
// 2. 与官方 26 个 index<n>.xml 实时唯一 ID 对齐，缺口 > 0.5% 退出 1
// 3. 字段非法 > 0.1% 退出 1
// 4. gzip 产物回读校验条目一致性

import fs from 'node:fs';
import path from 'node:path';
import { createGzip, gunzipSync } from 'node:zlib';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';

const SITE = 'https://hongguoduanju.com';
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';
const FAIL_RE = /^(net:|bad_payload|loader_fail)/;
const SHARD_FILE_RE = /^shard_(\d+)(?:\.part(\d+))?\.[0-9a-f]{8}\.json$/;

const arg = (flag, def) => {
  const i = process.argv.indexOf(flag);
  return i >= 0 && process.argv[i + 1] !== undefined ? process.argv[i + 1] : def;
};

async function fetchText(url, tries = 4) {
  for (let i = 0; i < tries; i++) {
    try {
      const r = await fetch(url, { headers: { 'User-Agent': UA }, signal: AbortSignal.timeout(30000) });
      if (r.ok) return await r.text();
    } catch {}
    await new Promise((res) => setTimeout(res, 1000 * (i + 1)));
  }
  return null;
}

async function fetchBaselineSitemapIds() {
  console.log('[Merge] 拉取官方 26 个 sitemap 作为基准 ID 全集...');
  const all = new Set();
  for (let s = 1; s <= 26; s++) {
    const text = await fetchText(`${SITE}/sitemap/hongguoduanju/index${s}.xml`);
    if (!text) throw new Error(`基准 sitemap 下载失败: index${s}.xml`);
    const locs = [...text.matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) => m[1]);
    const ids = new Set(locs.map((l) => (l.match(/(?:\/player\/|series_id=)(\d+)/) || [])[1]).filter(Boolean));
    for (const id of ids) all.add(id);
    console.log(`  - index${s}.xml: 唯一 ID ${ids.size}`);
  }
  console.log(`[Merge] 基准唯一 ID 总计: ${all.size}`);
  return all;
}

async function main() {
  const inDir = arg('--in-dir', './shards');
  const outDir = arg('--out-dir', './dist');
  const expectedParts = Number(arg('--expected-parts', '4'));
  const skipBaseline = process.argv.includes('--skip-sitemap-fetch');
  fs.mkdirSync(outDir, { recursive: true });

  const allFiles = fs.readdirSync(inDir);
  const dataFiles = allFiles.filter((f) => SHARD_FILE_RE.test(f));
  if (dataFiles.length === 0) {
    console.error(`[Merge 致命错误] ${inDir} 中没有任何匹配 ${SHARD_FILE_RE} 的分片数据文件！拒绝生成空全库。`);
    process.exit(1);
  }

  // 分片/part 齐备性
  const presence = new Map(); // shard -> Set(part)
  for (const f of dataFiles) {
    const m = f.match(SHARD_FILE_RE);
    const shard = Number(m[1]);
    const part = m[2] === undefined ? 0 : Number(m[2]);
    if (!presence.has(shard)) presence.set(shard, new Set());
    presence.get(shard).add(part);
  }
  const missingShards = [];
  const missingParts = [];
  for (let s = 1; s <= 26; s++) {
    const parts = presence.get(s);
    if (!parts) { missingShards.push(s); continue; }
    for (let p = 0; p < expectedParts; p++) if (!parts.has(p)) missingParts.push(`${s}.${p}`);
  }

  if (!skipBaseline) {
    if (missingShards.length) {
      console.error(`[Merge 致命错误] 缺少整分片: [${missingShards.join(', ')}]，拒绝发布伪全库！`);
      process.exit(1);
    }
    if (missingParts.length) {
      console.error(`[Merge 致命错误] 缺少 part: [${missingParts.join(', ')}]（期望每分片 ${expectedParts} 片），拒绝发布！`);
      process.exit(1);
    }
  } else if (missingShards.length || missingParts.length) {
    console.warn(`[Merge 警告] 实验模式跳过齐备性硬校验；缺分片=[${missingShards.join(',')}] 缺part=${missingParts.length} 个`);
  }

  // 合并条目
  const catalog = new Map();
  let malformed = 0;
  for (const f of dataFiles) {
    let list;
    try { list = JSON.parse(fs.readFileSync(path.join(inDir, f), 'utf8')); } catch (e) { console.error(`  解析失败 ${f}: ${e.message}`); malformed += 1; continue; }
    for (const item of list) {
      if (!item?.id || !item?.title || !(Number(item.eps) > 0)) { malformed += 1; continue; }
      const id = String(item.id);
      if (!catalog.has(id)) catalog.set(id, item);
    }
  }

  // 汇总失败/下架清单
  const gone = new Set();
  const invalid = new Map();
  for (const f of allFiles.filter((x) => x.endsWith('_report.json'))) {
    try {
      const rep = JSON.parse(fs.readFileSync(path.join(inDir, f), 'utf8'));
      for (const id of rep.goneIds || []) gone.add(String(id));
      for (const it of rep.invalidIds || []) invalid.set(String(it.id), it.reason);
    } catch {}
  }
  console.log(`[Merge] 有效入库 ${catalog.size} 部；正常下架/空条目 ${gone.size}；异常 ${invalid.size}；字段不合法 ${malformed}`);

  // 基准对齐
  let baselineTotal = 0;
  let unaccounted = [];
  let failedIds = [];
  if (!skipBaseline) {
    const baseline = await fetchBaselineSitemapIds();
    baselineTotal = baseline.size;
    failedIds = [...invalid.entries()].filter(([, r]) => FAIL_RE.test(String(r))).map(([id]) => id);
    const accounted = new Set([...catalog.keys(), ...gone, ...invalid.keys()]);
    for (const id of baseline) if (!accounted.has(id)) unaccounted.push(id);
    const gaps = unaccounted.length + failedIds.length;
    const coverage = (catalog.size / (catalog.size + gaps)) * 100;
    console.log(`[Merge] 基准对齐: 入库=${catalog.size} 下架=${gone.size} 失败=${failedIds.length} 未解释缺失=${unaccounted.length} 真实覆盖率=${coverage.toFixed(3)}%`);
    if (gaps / baseline.size > 0.005) {
      console.error(`[Merge 致命错误] 缺口 ${gaps}（缺失 ${unaccounted.length} + 失败 ${failedIds.length}）超过 0.5% 阈值，严格拒绝发布！`);
      process.exit(1);
    }
    if (malformed / (catalog.size + malformed) > 0.001) {
      console.error(`[Merge 致命错误] 字段不合法条目 ${malformed} 超过 0.1%，拒绝发布！`);
      process.exit(1);
    }
  } else {
    console.warn('[Merge 警告] 已跳过官方基准校验（实验/局部模式），覆盖率仅供参考；此产物不得作为全库发布。');
  }

  const sorted = [...catalog.values()].sort((a, b) => String(b.new_at || '').localeCompare(String(a.new_at || '')));
  const jsonPath = path.join(outDir, 'catalog.json');
  const gzPath = path.join(outDir, 'catalog.json.gz');
  const summaryPath = path.join(outDir, 'summary.json');

  const payload = JSON.stringify(sorted);
  fs.writeFileSync(jsonPath, payload);
  await pipeline(Readable.from([payload]), createGzip({ level: 9 }), fs.createWriteStream(gzPath));

  // gzip 回读校验
  const roundTrip = JSON.parse(gunzipSync(fs.readFileSync(gzPath)).toString('utf8'));
  if (roundTrip.length !== sorted.length) {
    console.error(`[Merge 致命错误] gzip 回读条目数不一致: ${roundTrip.length} != ${sorted.length}`);
    process.exit(1);
  }

  const jsonMb = (fs.statSync(jsonPath).size / 1024 / 1024).toFixed(2);
  const gzMb = (fs.statSync(gzPath).size / 1024 / 1024).toFixed(2);
  const gaps = unaccounted.length + failedIds.length;
  const summary = {
    updated_at: new Date().toISOString(),
    published: false,
    sitemap_unique_total: baselineTotal || null,
    catalog_total: sorted.length,
    gone_total: gone.size,
    failed_total: failedIds.length,
    invalid_total: invalid.size,
    unaccounted_total: unaccounted.length,
    malformed_total: malformed,
    coverage_rate: baselineTotal ? `${(catalog.size / (catalog.size + gaps) * 100).toFixed(3)}%` : null,
    json_size_mb: Number(jsonMb),
    gzip_size_mb: Number(gzMb),
    shard_files_count: dataFiles.length,
    missing_shards: missingShards,
    missing_parts: missingParts,
  };
  fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));

  console.log('\n========================================');
  console.log('🎉 合并完成');
  console.log(`入库总数: ${sorted.length} 部 / Sitemap 基准: ${baselineTotal || '未校验'}`);
  console.log(`明文 ${jsonMb} MB → gzip ${gzMb} MB`);
  console.log(`覆盖率: ${summary.coverage_rate || '未校验'} | 缺口: ${gaps}`);
  console.log(`汇总: ${summaryPath}`);
  console.log('========================================\n');
}

main().catch((e) => { console.error('FATAL:', e); process.exit(1); });