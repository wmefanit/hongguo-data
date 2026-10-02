#!/usr/bin/env node
/**
 * plan_run.mjs — 抓取前置规划器
 * 读取 26 个 Sitemap，比对上一版 source manifest，只输出待抓取的新增/变更 ID。
 */

import fs from 'node:fs';
import path from 'node:path';
import { gunzipSync } from 'node:zlib';
import {
  CATALOG_SCHEMA,
  parseSitemapEntries,
  partForId,
  validateSourceManifest,
  sourceHash,
  normalizeManifestEntries,
} from '../lib/catalog_util.js';

const SITE = 'https://hongguoduanju.com';
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

function arg(flag, fallback) {
  const i = process.argv.indexOf(flag);
  return i >= 0 && process.argv[i + 1] !== undefined ? process.argv[i + 1] : fallback;
}

async function fetchText(url, tries = 4) {
  for (let i = 0; i < tries; i++) {
    try {
      const resp = await fetch(url, { headers: { 'User-Agent': UA }, signal: AbortSignal.timeout(30000) });
      if (resp.ok) return await resp.text();
    } catch {}
    await new Promise((r) => setTimeout(r, 1000 * (i + 1)));
  }
  throw new Error(`下载失败: ${url}`);
}

async function loadSitemaps(sitemapDir, site) {
  const sitemaps = [];
  for (let s = 1; s <= 26; s++) {
    const text = sitemapDir
      ? fs.readFileSync(path.join(sitemapDir, `index${s}.xml`), 'utf8')
      : await fetchText(`${site}/sitemap/hongguoduanju/index${s}.xml`);
    sitemaps.push({ shard: s, entries: parseSitemapEntries(text) });
  }
  return sitemaps;
}

function loadBaseline(baselineManifestPath) {
  if (!baselineManifestPath || !fs.existsSync(baselineManifestPath)) return null;
  const raw = baselineManifestPath.endsWith('.gz')
    ? gunzipSync(fs.readFileSync(baselineManifestPath)).toString('utf8')
    : fs.readFileSync(baselineManifestPath, 'utf8');
  return validateSourceManifest(JSON.parse(raw));
}

export function buildPlan({ sitemaps, baselineManifest, parts = 4, mode = 'incremental' }) {
  const isFull = mode === 'full' || !baselineManifest;
  const baselineEntries = new Map(baselineManifest ? baselineManifest.entries.map(([id, mod, st]) => [id, { lastmod: mod, status: st }]) : []);
  const allCurrent = new Map();
  const currentManifestEntries = [];
  const shards = [];

  for (const item of sitemaps) {
    const shard = item.shard;
    const shardEntries = item.entries;
    const partBuckets = Array.from({ length: parts }, () => []);
    let fetchCount = 0;
    let reuseCount = 0;

    for (const [id, lastmod] of shardEntries) {
      if (allCurrent.has(id)) throw new Error(`跨分片重复 ID: ${id}`);
      allCurrent.set(id, { shard, lastmod });
      currentManifestEntries.push([id, lastmod, 'ok']);
      const base = baselineEntries.get(id);
      const needFetch = isFull || !base || base.lastmod !== lastmod || base.status !== 'ok';
      const part = partForId(id, parts);
      if (needFetch) {
        partBuckets[part].push({ id, lastmod });
        fetchCount++;
      } else {
        reuseCount++;
      }
    }

    shards.push({
      shard,
      total: shardEntries.size,
      fetch_count: fetchCount,
      reuse_count: reuseCount,
      parts: partBuckets.map((items, part) => ({
        part,
        fetch_count: items.length,
        items,
      })),
    });
  }

  const manifestEntriesSorted = normalizeManifestEntries(currentManifestEntries);
  const currentSourceHash = sourceHash(manifestEntriesSorted);
  const sitemapEntriesSorted = normalizeManifestEntries(currentManifestEntries).map(([id, mod]) => [id, mod]);
  const unchanged = Boolean(baselineManifest && baselineManifest.source_hash === currentSourceHash && !isFull);
  const totalFetch = shards.reduce((sum, s) => sum + s.fetch_count, 0);
  const totalReuse = shards.reduce((sum, s) => sum + s.reuse_count, 0);

  return {
    schema: CATALOG_SCHEMA,
    mode: isFull ? 'full' : 'incremental',
    unchanged,
    parts,
    from_tag: baselineManifest?.tag || null,
    from_catalog_hash: baselineManifest?.catalog_hash || null,
    from_source_hash: baselineManifest?.source_hash || null,
    target_source_hash_if_all_ok: currentSourceHash,
    summary: {
      sitemap_unique_total: allCurrent.size,
      total_fetch: totalFetch,
      total_reuse: totalReuse,
    },
    sitemap_entries: sitemapEntriesSorted,
    shards,
  };
}

async function main() {
  const outDir = path.resolve(arg('--out-dir', './plan'));
  const baselineManifestPath = arg('--baseline-manifest', '');
  const sitemapDir = arg('--sitemap-dir', '');
  const site = arg('--site', SITE).replace(/\/$/, '');
  const parts = Number(arg('--parts', '4'));
  const mode = arg('--mode', 'incremental');
  fs.mkdirSync(outDir, { recursive: true });

  console.log(`[Plan] 开始规划: mode=${mode}, baseline=${baselineManifestPath || 'none'}, source=${sitemapDir || site}`);
  const sitemaps = await loadSitemaps(sitemapDir, site);
  const baselineManifest = loadBaseline(baselineManifestPath);
  const plan = buildPlan({ sitemaps, baselineManifest, parts, mode });

  fs.writeFileSync(path.join(outDir, 'plan.json'), JSON.stringify(plan, null, 2));
  for (const s of plan.shards) {
    for (const p of s.parts) {
      fs.writeFileSync(path.join(outDir, `plan_${s.shard}_${p.part}.json`), JSON.stringify(p, null, 2));
    }
  }
  console.log(`[Plan] 规划完成: unique=${plan.summary.sitemap_unique_total}, fetch=${plan.summary.total_fetch}, reuse=${plan.summary.total_reuse}, unchanged=${plan.unchanged}`);
}

if (process.argv[1] && import.meta.url === new URL(process.argv[1], 'file:').href) {
  main().catch((e) => {
    console.error('FATAL:', e);
    process.exit(1);
  });
}
