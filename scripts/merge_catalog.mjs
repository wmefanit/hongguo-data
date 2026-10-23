#!/usr/bin/env node
/**
 * merge_catalog.mjs — 聚合抓取产物与基线，输出全量索引、Delta 增量包、Source Manifest 与 Summary
 */

import fs from 'node:fs';
import path from 'node:path';
import { createGzip, gunzipSync } from 'node:zlib';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import {
  CATALOG_SCHEMA,
  sha256Hex,
  canonicalCatalog,
  catalogBytes,
  catalogHash,
  createSourceManifest,
  diffCatalogs,
  normalizeCard,
} from '../lib/catalog_util.js';

function arg(flag, fallback) {
  const i = process.argv.indexOf(flag);
  return i >= 0 && process.argv[i + 1] !== undefined ? process.argv[i + 1] : fallback;
}

async function writeGzipFile(filePath, buffer) {
  await pipeline(Readable.from([buffer]), createGzip({ level: 9 }), fs.createWriteStream(filePath));
}

function loadBaseline(baselineCatalogPath) {
  if (!baselineCatalogPath || !fs.existsSync(baselineCatalogPath)) return null;
  const raw = baselineCatalogPath.endsWith('.gz')
    ? gunzipSync(fs.readFileSync(baselineCatalogPath)).toString('utf8')
    : fs.readFileSync(baselineCatalogPath, 'utf8');
  return canonicalCatalog(JSON.parse(raw));
}

async function main() {
  const planFile = path.resolve(arg('--plan-file', './plan/plan.json'));
  const shardsDir = path.resolve(arg('--in-dir', './shards'));
  const outDir = path.resolve(arg('--out-dir', './dist'));
  const baselineCatalogPath = arg('--baseline-catalog', '');
  const toTag = arg('--to-tag', `full-${new Date().toISOString().slice(0, 10).replace(/-/g, '')}`);
  fs.mkdirSync(outDir, { recursive: true });

  if (!fs.existsSync(planFile)) throw new Error(`找不到 plan 文件: ${planFile}`);
  const plan = JSON.parse(fs.readFileSync(planFile, 'utf8'));
  const baselineCatalog = loadBaseline(baselineCatalogPath);
  const baselineMap = new Map((baselineCatalog || []).map((card) => [card.id, card]));

  const crawledCatalog = new Map();
  const gone = new Set();
  const invalid = new Map();
  const plannedFetch = new Set();

  // 1. 读取各分片结果，并保证每个计划任务恰有一个最终归因。
  for (const shard of plan.shards) {
    for (const part of shard.parts) {
      for (const item of part.items || []) {
        if (plannedFetch.has(item.id)) throw new Error(`plan 内重复抓取 ID: ${item.id}`);
        plannedFetch.add(item.id);
      }

      const resultFile = path.join(shardsDir, `shard_${shard.shard}.part${part.part}.json`);
      const reportFile = path.join(shardsDir, `shard_${shard.shard}.part${part.part}_report.json`);
      const hasResult = fs.existsSync(resultFile);
      const hasReport = fs.existsSync(reportFile);
      // 分片 job 崩溃/超时/产物上传失败属「数据缺口」，不是「合并逻辑错误」。
      // 直接 throw 会让 1 个坏分片阻断其余 100+ 个成功分片的发布，故降级为 invalid，
      // 由下方全局缺口阈值（0.5%）统一决定是否值得发布。
      if (part.fetch_count > 0 && (!hasResult || !hasReport)) {
        console.warn(`[Merge] 警告: 缺少分片抓取产物 shard=${shard.shard} part=${part.part} (result=${hasResult} report=${hasReport})，未归因项将标记 invalid 待下轮重抓`);
      }

      if (fs.existsSync(resultFile)) {
        const list = JSON.parse(fs.readFileSync(resultFile, 'utf8'));
        for (const raw of list) {
          const card = normalizeCard(raw);
          if (!card || !plannedFetch.has(card.id) || crawledCatalog.has(card.id)) {
            throw new Error(`非法/重复/未计划的抓取结果: ${raw?.id}`);
          }
          crawledCatalog.set(card.id, card);
        }
      }
      if (fs.existsSync(reportFile)) {
        const rep = JSON.parse(fs.readFileSync(reportFile, 'utf8'));
        for (const rawId of rep.goneIds || []) {
          const id = String(rawId);
          if (!plannedFetch.has(id) || crawledCatalog.has(id) || invalid.has(id) || gone.has(id)) throw new Error(`非法 gone 结果: ${id}`);
          gone.add(id);
        }
        for (const it of rep.invalidIds || []) {
          const id = String(it.id);
          if (!plannedFetch.has(id) || crawledCatalog.has(id) || gone.has(id) || invalid.has(id)) throw new Error(`非法 invalid 结果: ${id}`);
          invalid.set(id, it.reason);
        }
      }

      // 产物缺失（分片 job 崩溃/超时/上传失败）时，该 part 内所有尚无归因的条目
      // 统一标记为 invalid：本轮不发布这些条目，下一轮 plan 见 status!=='ok' 会自动重抓。
      if (part.fetch_count > 0 && (!hasResult || !hasReport)) {
        for (const item of part.items || []) {
          const id = String(item.id);
          if (!crawledCatalog.has(id) && !gone.has(id) && !invalid.has(id)) {
            invalid.set(id, 'net:missing_artifact');
          }
        }
      }
    }
  }

  // 分片早停/崩溃/被杀会导致部分 ID 无归因。这是「数据缺口」而非「合并错误」：
  // 标记 invalid 后本轮不发布这些条目，下一轮 plan 见 status!=='ok' 自动重抓。
  // 全局质量仍由下方 0.5% 缺口阈值把关，不放宽标准。
  let unaccounted = 0;
  for (const id of plannedFetch) {
    if (!crawledCatalog.has(id) && !gone.has(id) && !invalid.has(id)) {
      invalid.set(id, 'net:unaccounted');
      unaccounted++;
    }
  }
  if (unaccounted > 0) {
    console.warn(`[Merge] 警告: ${unaccounted} 条计划抓取项无归因（分片早停/崩溃），已标记 invalid 待下轮重抓`);
  }

  // 2. 以当前 sitemap 作为唯一真相组装最终全量：
  // 未变项仅复用基线；明确 404/410 才删除；变更项临时失败时保留旧卡并标记 invalid，供下一轮重试。
  const finalCatalogMap = new Map();
  const manifestEntries = [];
  let reusedCount = 0;
  let fetchedCount = 0;
  let retainedOnFailure = 0;
  let gapCount = 0;

  for (const [id, lastmod] of plan.sitemap_entries || []) {
    let card = null;
    let status = 'ok';

    if (crawledCatalog.has(id)) {
      card = crawledCatalog.get(id);
      fetchedCount++;
    } else if (gone.has(id)) {
      status = 'gone';
    } else if (invalid.has(id)) {
      status = 'invalid';
      if (baselineMap.has(id)) {
        card = baselineMap.get(id); // 暂保留旧元数据，下一轮由 invalid 状态强制重抓。
        retainedOnFailure++;
      } else {
        gapCount++;
      }
    } else if (plan.mode !== 'full' && baselineMap.has(id)) {
      card = baselineMap.get(id);
      reusedCount++;
    } else {
      throw new Error(`Sitemap ID 未获得可用归因: ${id}`);
    }

    if (card) finalCatalogMap.set(id, card);
    manifestEntries.push([id, lastmod, status]);
  }

  const sitemapTotal = (plan.sitemap_entries || []).length;
  console.log(`[Merge] 条目归因: Sitemap总数=${sitemapTotal}, 新抓取=${fetchedCount}, 基线复用=${reusedCount}, 失败暂留=${retainedOnFailure}, 下架=${gone.size}, 异常缺口=${gapCount}`);

  if (sitemapTotal > 0 && gapCount / sitemapTotal > 0.005) {
    throw new Error(`[Merge 致命错误] 异常缺口 ${gapCount} 超过 0.5% 阈值，拒绝发布`);
  }

  const finalCatalog = canonicalCatalog([...finalCatalogMap.values()]);
  const finalBytes = catalogBytes(finalCatalog);
  const finalHash = catalogHash(finalCatalog);

  const sourceManifest = createSourceManifest({
    tag: toTag,
    catalogHash: finalHash,
    entries: manifestEntries,
  });

  // 输出全量 catalog.json / catalog.json.gz
  const catalogJsonPath = path.join(outDir, 'catalog.json');
  const catalogGzPath = path.join(outDir, 'catalog.json.gz');
  fs.writeFileSync(catalogJsonPath, finalBytes);
  await writeGzipFile(catalogGzPath, finalBytes);

  // 输出 source_manifest.json.gz
  const manifestBytes = Buffer.from(JSON.stringify(sourceManifest), 'utf8');
  fs.writeFileSync(path.join(outDir, 'source_manifest.json'), manifestBytes);
  await writeGzipFile(path.join(outDir, 'source_manifest.json.gz'), manifestBytes);

  // 输出 Delta（只要有上一版基线就产出，无论本次抓取是否为全量审计/强制全量，确保下游永远能走增量）
  let deltaSummary = null;
  if (baselineCatalog) {
    const baseHash = catalogHash(baselineCatalog);
    const delta = diffCatalogs(baselineCatalog, finalCatalog, {
      fromTag: plan.from_tag,
      toTag,
      fromCatalogHash: baseHash,
      toCatalogHash: finalHash,
    });
    const deltaBytes = Buffer.from(JSON.stringify(delta), 'utf8');
    fs.writeFileSync(path.join(outDir, 'catalog.delta.json'), deltaBytes);
    await writeGzipFile(path.join(outDir, 'catalog.delta.json.gz'), deltaBytes);

    const deltaGz = fs.readFileSync(path.join(outDir, 'catalog.delta.json.gz'));
    deltaSummary = {
      schema: CATALOG_SCHEMA,
      type: 'catalog-delta-summary',
      from_tag: delta.from_tag,
      to_tag: delta.to_tag,
      from_catalog_hash: delta.from_catalog_hash,
      to_catalog_hash: delta.to_catalog_hash,
      from_count: delta.from_count,
      to_count: delta.to_count,
      added_count: delta.added.length,
      modified_count: delta.modified.length,
      deleted_count: delta.deleted.length,
      delta_gzip_size_kb: Number((deltaGz.length / 1024).toFixed(2)),
      delta_sha256: sha256Hex(deltaGz),
      created_at: new Date().toISOString(),
    };
    fs.writeFileSync(path.join(outDir, 'delta.summary.json'), JSON.stringify(deltaSummary, null, 2));
  }

  const jsonMb = (finalBytes.length / 1024 / 1024).toFixed(2);
  const gzMb = (fs.statSync(catalogGzPath).size / 1024 / 1024).toFixed(2);
  const summary = {
    schema: CATALOG_SCHEMA,
    tag: toTag,
    updated_at: new Date().toISOString(),
    published: false,
    sitemap_unique_total: sitemapTotal,
    catalog_total: finalCatalog.length,
    catalog_hash: finalHash,
    source_hash: sourceManifest.source_hash,
    json_size_mb: Number(jsonMb),
    gzip_size_mb: Number(gzMb),
    has_delta: Boolean(deltaSummary),
    delta: deltaSummary,
  };
  fs.writeFileSync(path.join(outDir, 'summary.json'), JSON.stringify(summary, null, 2));

  console.log(`[Merge] 合并完成: count=${finalCatalog.length} hash=${finalHash.slice(0, 16)}... (明文 ${jsonMb} MB -> gzip ${gzMb} MB)`);
  if (deltaSummary) {
    console.log(`[Merge] Delta 补丁就绪: +${deltaSummary.added_count} / ~${deltaSummary.modified_count} / -${deltaSummary.deleted_count} (${deltaSummary.delta_gzip_size_kb} KB)`);
  }
}

if (process.argv[1] && import.meta.url === new URL(process.argv[1], 'file:').href) {
  main().catch((e) => {
    console.error('FATAL:', e);
    process.exit(1);
  });
}
