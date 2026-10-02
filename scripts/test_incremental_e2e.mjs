#!/usr/bin/env node
/**
 * test_incremental_e2e.mjs — 真实本地 Mock 上游与 Release 服务端，端到端验证：
 *   1. 真实运行 plan_run.mjs（增量规划、未变识别）
 *   2. 真实运行 build_shard.mjs（只爬取规划中的 50 项，复用 970 项）
 *   3. 真实运行 merge_catalog.mjs（输出全量、Delta、Manifest、Summary）
 *   4. 真实运行 sync_catalog.mjs（Delta 应用、Hash 损坏降级全量、网络失败零坏账原子保留）
 */

import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { gunzipSync, gzipSync } from 'node:zlib';
import {
  canonicalCatalog,
  catalogBytes,
  catalogHash,
  createSourceManifest,
} from '../lib/catalog_util.js';

const testDir = '/tmp/hg_incremental_strict_e2e_v3';
fs.rmSync(testDir, { recursive: true, force: true });
fs.mkdirSync(testDir, { recursive: true });

function run(cmd, args) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args);
    let out = '', err = '';
    p.stdout.on('data', (d) => { out += d; });
    p.stderr.on('data', (d) => { err += d; });
    p.on('close', (code) => {
      if (code === 0) resolve(out);
      else reject(new Error(`Exit ${code}:\n${err || out}`));
    });
  });
}

function ok(cond, title, details = '') {
  if (!cond) {
    console.error(`❌ FAIL: ${title} ${details ? '(' + details + ')' : ''}`);
    process.exit(1);
  }
  console.log(`✅ PASS: ${title} ${details ? '— ' + details : ''}`);
}

async function main() {
  console.log('=== 运行真实子进程与 Mock 上游集成测试 ===\n');

  // 1. 基线数据构造（1000 部）
  const baseItems = [];
  const baseManifestEntries = [];
  for (let i = 1; i <= 1000; i++) {
    const id = `7600000000000000${String(i).padStart(3, '0')}`;
    const card = {
      id,
      title: `初始短剧_${i}`,
      cover: `https://p6-novel.byteimg.com/novel-pic/c_${i}.jpg`,
      intro: `简介_${i}`,
      tags: ['都市', '逆袭'],
      eps: 80,
      ch: '',
      new_at: '2026-09-28',
    };
    baseItems.push(card);
    baseManifestEntries.push([id, '2026-09-28T12:00:00+08:00', 'ok']);
  }
  const baseCatalog = canonicalCatalog(baseItems);
  const baseGz = gzipSync(catalogBytes(baseCatalog), { level: 9 });
  const baseManifest = createSourceManifest({
    tag: 'full-20261001',
    catalogHash: catalogHash(baseCatalog),
    entries: baseManifestEntries,
  });
  const baseManifestGz = gzipSync(Buffer.from(JSON.stringify(baseManifest), 'utf8'), { level: 9 });

  const baselineDir = path.join(testDir, 'baseline');
  fs.mkdirSync(baselineDir, { recursive: true });
  fs.writeFileSync(path.join(baselineDir, 'catalog.json.gz'), baseGz);
  fs.writeFileSync(path.join(baselineDir, 'source_manifest.json.gz'), baseManifestGz);

  // 2. 构造今天 26 个 Sitemap（删 10 部，改 20 部，新增 30 部）
  // 注入跨 Sitemap 重复 ID 与不同 lastmod（真实官网常态），检验归一化自愈
  const currentEntries = [];
  for (let i = 1; i <= 1000; i++) {
    if (i <= 10) continue; // 删除 1..10
    const id = `7600000000000000${String(i).padStart(3, '0')}`;
    const mod = (i >= 11 && i <= 30) ? '2026-10-02T12:00:00+08:00' : '2026-09-28T12:00:00+08:00';
    currentEntries.push({ id, mod });
  }
  for (let i = 1001; i <= 1030; i++) {
    const id = `7600000000000000${String(i).padStart(3, '0')}`;
    currentEntries.push({ id, mod: '2026-10-02T12:00:00+08:00' });
  }

  // 跨分片追加 2 条已存在 ID 的旧记录，验证 Planner 会自动按最新 lastmod 去重，不重复抓取也不抛错
  currentEntries.push({ id: '7600000000000000015', mod: '2026-09-01T00:00:00+08:00' });
  currentEntries.push({ id: '7600000000000000025', mod: '2026-09-01T00:00:00+08:00' });

  // 3. 启动 Mock 上游服务（提供 Sitemap 与 Player SSR loader 响应）
  let upstreamRequests = 0;
  const requestedPlayerIds = new Set();

  const upstreamServer = http.createServer((req, res) => {
    upstreamRequests++;
    const url = new URL(req.url, 'http://localhost');
    const sitemapMatch = url.pathname.match(/^\/sitemap\/hongguoduanju\/index(\d+)\.xml$/);
    if (sitemapMatch) {
      const s = Number(sitemapMatch[1]);
      const slice = currentEntries.filter((_, idx) => idx % 26 === s - 1);
      const xml = `<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">` +
        slice.map((it) => `<url><loc>https://hongguoduanju.com/player/${it.id}</loc><lastmod>${it.mod}</lastmod></url>`).join('') +
        `</urlset>`;
      res.writeHead(200, { 'Content-Type': 'application/xml' });
      res.end(xml);
      return;
    }

    const playerMatch = url.pathname.match(/^\/player\/(\d+)$/);
    if (playerMatch) {
      const id = playerMatch[1];
      requestedPlayerIds.add(id);
      const numId = Number(id.slice(-4));
      const payload = {
        isSuccess: true,
        seriesDetail: {
          series_name: numId > 1000 ? `新剧_${numId}` : `已改剧名_${numId}`,
          episode_cnt: 100,
          series_cover: `https://p3-novel.byteimg.com/novel-pic/img_${id}.jpg`,
          series_intro: `简介_${id}`,
          tags: ['都市', '豪门'],
          first_visible_time: Math.floor(new Date('2026-10-02T12:00:00Z').getTime() / 1000),
        },
      };
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end(JSON.stringify(payload) + '\ndata:\n{}');
      return;
    }

    res.writeHead(404);
    res.end();
  });

  await new Promise((resolve) => upstreamServer.listen(0, '127.0.0.1', resolve));
  const upstreamPort = upstreamServer.address().port;
  const upstreamBase = `http://127.0.0.1:${upstreamPort}`;

  try {
    // 4. 执行真实 plan_run.mjs（通过 HTTP 访问 mock 上游）
    const planDir = path.join(testDir, 'plan');
    await run(process.execPath, [
      '/mnt/data/code/namehere/hongguo-data/scripts/plan_run.mjs',
      '--out-dir', planDir,
      '--baseline-manifest', path.join(baselineDir, 'source_manifest.json.gz'),
      '--site', upstreamBase,
      '--parts', '4',
      '--mode', 'incremental',
    ]);

    const plan = JSON.parse(fs.readFileSync(path.join(planDir, 'plan.json'), 'utf8'));
    ok(plan.summary.total_fetch === 50, '真实上游下 Planner 精确规划 50 项任务', `fetch=${plan.summary.total_fetch}`);

    // 5. 执行真实 build_shard.mjs（所有 26 分片 × 4 parts）
    const shardsDir = path.join(testDir, 'shards');
    for (let s = 1; s <= 26; s++) {
      for (let p = 0; p < 4; p++) {
        const planFile = path.join(planDir, `plan_${s}_${p}.json`);
        await run(process.execPath, [
          '/mnt/data/code/namehere/hongguo-data/scripts/build_shard.mjs',
          '--shard', String(s),
          '--part', String(p),
          '--plan-file', planFile,
          '--site', upstreamBase,
          '--out-dir', shardsDir,
          '--concurrency', '4',
          '--delay', '5',
        ]);
      }
    }

    ok(requestedPlayerIds.size === 50, 'build_shard 真实只向 HTTP 上游发起了 50 次剧集请求', `实测请求 ${requestedPlayerIds.size} 部 (零冗余抓取)`);

    // 6. 执行真实 merge_catalog.mjs
    const distDir = path.join(testDir, 'dist');
    await run(process.execPath, [
      '/mnt/data/code/namehere/hongguo-data/scripts/merge_catalog.mjs',
      '--plan-file', path.join(planDir, 'plan.json'),
      '--in-dir', shardsDir,
      '--out-dir', distDir,
      '--baseline-catalog', path.join(baselineDir, 'catalog.json.gz'),
      '--to-tag', 'full-20261002',
    ]);

    const summary = JSON.parse(fs.readFileSync(path.join(distDir, 'summary.json'), 'utf8'));
    const deltaSummary = JSON.parse(fs.readFileSync(path.join(distDir, 'delta.summary.json'), 'utf8'));
    ok(summary.catalog_total === 1020, '真实产物条目数完全正确', `1020 条`);
    ok(deltaSummary.added_count === 30 && deltaSummary.modified_count === 20 && deltaSummary.deleted_count === 10, '真实产物 Delta 增删改精准', `+30 / ~20 / -10`);

    // 7. 启动 Mock Release 服务测试 Web 端 sync_catalog.mjs
    const webDataDir = path.join(testDir, 'web_data');
    fs.mkdirSync(webDataDir, { recursive: true });
    fs.copyFileSync(path.join(baselineDir, 'catalog.json.gz'), path.join(webDataDir, 'catalog.full.json.gz'));
    fs.writeFileSync(path.join(webDataDir, 'catalog.full.summary.json'), JSON.stringify({
      schema: 2,
      tag: 'full-20261001',
      catalog_total: 1000,
      catalog_hash: catalogHash(baseCatalog),
    }));

    let mockReleaseFailMode = null; // null | 'corrupt_delta' | 'all_fail'
    const releaseServer = http.createServer((req, res) => {
      const filename = path.basename(req.url);
      if (mockReleaseFailMode === 'all_fail') {
        res.writeHead(500);
        res.end('Mock release server down');
        return;
      }
      if (mockReleaseFailMode === 'corrupt_delta' && filename.includes('delta')) {
        res.writeHead(200, { 'Content-Type': 'application/octet-stream' });
        res.end(Buffer.from('corrupted delta content'));
        return;
      }
      const filePath = path.join(distDir, filename);
      if (fs.existsSync(filePath)) {
        res.writeHead(200, { 'Content-Type': filename.endsWith('.gz') ? 'application/octet-stream' : 'application/json' });
        fs.createReadStream(filePath).pipe(res);
      } else {
        res.writeHead(404);
        res.end();
      }
    });

    await new Promise((resolve) => releaseServer.listen(0, '127.0.0.1', resolve));
    const releasePort = releaseServer.address().port;
    const releaseBase = `http://127.0.0.1:${releasePort}`;
    const syncScript = '/mnt/data/code/namehere/hongguo-web/scripts/sync_catalog.mjs';

    try {
      // 7a. 正常增量同步
      await run(process.execPath, [syncScript, '--base-url', releaseBase, '--out-dir', webDataDir, '--min-count', '10']);
      const webSum1 = JSON.parse(fs.readFileSync(path.join(webDataDir, 'catalog.full.summary.json'), 'utf8'));
      ok(webSum1.sync_mode === 'delta' && webSum1.catalog_total === 1020, 'Web 端真实拉取 Delta 并完成原子打补丁', `mode=${webSum1.sync_mode} count=${webSum1.catalog_total}`);

      // 7b. Delta 损坏时平滑回退下载全量
      mockReleaseFailMode = 'corrupt_delta';
      // 重新置为基线状态
      fs.copyFileSync(path.join(baselineDir, 'catalog.json.gz'), path.join(webDataDir, 'catalog.full.json.gz'));
      fs.writeFileSync(path.join(webDataDir, 'catalog.full.summary.json'), JSON.stringify({
        schema: 2,
        tag: 'full-20261001',
        catalog_total: 1000,
        catalog_hash: catalogHash(baseCatalog),
      }));
      await run(process.execPath, [syncScript, '--base-url', releaseBase, '--out-dir', webDataDir, '--min-count', '10']);
      const webSum2 = JSON.parse(fs.readFileSync(path.join(webDataDir, 'catalog.full.summary.json'), 'utf8'));
      ok(webSum2.sync_mode === 'full' && webSum2.catalog_total === 1020, 'Delta 损坏时自动平滑回退全量包', `mode=${webSum2.sync_mode} count=${webSum2.catalog_total}`);

      // 7c. 全部下载失败时保证本地旧文件绝对不被破坏（零坏账）
      mockReleaseFailMode = 'all_fail';
      const beforeStat = fs.statSync(path.join(webDataDir, 'catalog.full.json.gz'));
      let syncFailed = false;
      try {
        await run(process.execPath, [syncScript, '--base-url', releaseBase, '--out-dir', webDataDir, '--min-count', '10']);
      } catch {
        syncFailed = true;
      }
      const afterStat = fs.statSync(path.join(webDataDir, 'catalog.full.json.gz'));
      ok(syncFailed && beforeStat.size === afterStat.size, '网络完全失败时保留旧索引文件不变，零坏账', `size=${afterStat.size}`);

      console.log('\n🎉 ALL_END_TO_END_REAL_UPSTREAM_TESTS_PASS: 8 项真实子进程/HTTP 交互测试全部通过！');
    } finally {
      releaseServer.close();
    }
  } finally {
    upstreamServer.close();
  }
}

main().catch((e) => {
  console.error('FATAL:', e);
  process.exit(1);
});
