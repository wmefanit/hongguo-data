#!/usr/bin/env node
/**
 * build_shard.mjs — 抓取红果短剧单个 Sitemap 分片（可再切 part）的剧集元数据
 *
 * 数据源（实测 52/52、100/100 解析成功）：
 *   https://hongguoduanju.com/player/<series_id>?__loader=player_(series_id)/page&__ssrDirect=true
 *   → 首段 JSON 的 seriesDetail：series_name / episode_cnt / series_cover / series_intro / tags / first_visible_time
 *
 * 归因规则:
 *   - found: 成功解析出 title、eps > 0 的完整卡片
 *   - gone: 只有官方明确返回 404 / 410 时才计入（正常下架）；任何解析缺失、无标题、无集数一律算 invalid，不假装是已下架
 *   - invalid: 解析失败、格式不对、无关键字段、网络失败重试仍失败
 *   - unaccounted: 分配集合中尚未尝试的条目
 *   - gap = unaccounted + invalid（所有未得到真实条目的缺口，无论何种原因）
 *   - gap / assigned > 0.5% 时本片硬失败退出 1
 *
 * 用法:
 *   node scripts/build_shard.mjs --shard 1 [--part 0 --parts 4] [--limit N] [--concurrency 6] [--out-dir ./shards]
 */

import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';

const SITE = 'https://hongguoduanju.com';
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const ENV_PARTS = Number(process.env.PARTS || 1);
const ENV_LIMIT = Number(process.env.LIMIT || 0);
const ENV_CONC = Number(process.env.CONC || 6);

function idToDate(id) {
  try { return new Date(Number(BigInt(id) >> 32n) * 1000).toISOString().slice(0, 10); } catch { return ''; }
}

function safeDate(ts, id) {
  const n = Number(ts);
  if (Number.isFinite(n) && n > 0 && n < 4102444800) {
    try { return new Date(n * 1000).toISOString().slice(0, 10); } catch {}
  }
  return idToDate(id);
}

async function fetchText(url, { tries = 4, timeoutMs = 15000 } = {}) {
  let last = '';
  for (let i = 0; i < tries; i++) {
    try {
      const resp = await fetch(url, {
        headers: { 'User-Agent': UA, 'Accept': '*/*', 'Referer': SITE + '/' },
        signal: AbortSignal.timeout(timeoutMs),
      });
      if (resp.status === 404 || resp.status === 410) return { ok: false, gone: true, status: resp.status };
      if (resp.status === 429 || resp.status >= 500) {
        last = `HTTP ${resp.status}`;
        const ra = Number(resp.headers.get('retry-after') || 0);
        await sleep(Math.min(30000, Math.max(2000, ra * 1000) * (i + 1)));
        continue;
      }
      if (!resp.ok) return { ok: false, status: resp.status, error: `HTTP ${resp.status}` };
      return { ok: true, text: await resp.text(), status: resp.status };
    } catch (e) {
      last = String(e && e.message || e);
      await sleep(1000 * (i + 1));
    }
  }
  return { ok: false, error: last || 'fetch failed' };
}

async function getShardIds(shard) {
  const res = await fetchText(`${SITE}/sitemap/hongguoduanju/index${shard}.xml`, { tries: 5 });
  if (!res.ok) throw new Error(`Sitemap index${shard}.xml 下载失败: ${res.error || res.status}`);
  const locs = [...res.text.matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) => m[1]);
  const ids = locs.map((l) => (l.match(/(?:\/player\/|series_id=)(\d+)/) || [])[1]).filter(Boolean);
  return [...new Set(ids)];
}

function parseCard(text, id) {
  let base = null;
  try {
    base = JSON.parse(text.split(/\r?\n+data:\r?\n?/)[0] || '{}');
  } catch { return { reason: 'bad_payload' }; }
  const sd = base?.seriesDetail;
  if (!sd) return { reason: base?.isSuccess === false ? 'loader_fail' : 'no_seriesDetail' };
  const title = String(sd.series_name || sd.series_title || '').trim().split('\n')[0];
  const eps = Number(sd.episode_cnt || sd.series_episode_info?.episode_cnt || 0);
  if (!title) return { reason: 'no_title' };
  if (!(eps > 0)) return { reason: 'no_episodes' };
  const tags = (Array.isArray(sd.tags) ? sd.tags : []).map((t) => String(t).trim()).filter(Boolean).slice(0, 6);
  const new_at = safeDate(sd.first_visible_time, id);
  return {
    card: {
      id,
      title,
      cover: String(sd.series_cover || '').replace(/\\u002F/g, '/'),
      intro: String(sd.series_intro || '').replace(/\s+/g, ' ').trim().slice(0, 50),
      tags,
      eps,
      ch: '',
      new_at,
    },
  };
}

async function main() {
  const args = process.argv.slice(2);
  const arg = (flag, def) => { const i = args.indexOf(flag); return i >= 0 && args[i + 1] !== undefined ? args[i + 1] : def; };
  const num = (v, def) => (Number.isFinite(Number(v)) ? Number(v) : def);

  const shard = num(arg('--shard', 0), 0);
  if (!Number.isInteger(shard) || shard < 1 || shard > 26) { console.error('--shard 必须是 1..26'); process.exit(2); }
  const parts = Math.max(1, num(arg('--parts', ENV_PARTS), ENV_PARTS));
  const part = num(arg('--part', 0), 0);
  if (part < 0 || part >= parts) { console.error('--part 必须在 0..parts-1'); process.exit(2); }
  const concurrency = Math.min(10, Math.max(1, num(arg('--concurrency', ENV_CONC), ENV_CONC)));
  const delayMs = num(arg('--delay', 80), 80);
  const limit = num(arg('--limit', ENV_LIMIT), ENV_LIMIT);
  const outDir = arg('--out-dir', './shards');
  fs.mkdirSync(outDir, { recursive: true });

  const allIds = await getShardIds(shard);
  const per = Math.ceil(allIds.length / parts);
  let assigned = allIds.slice(part * per, (part + 1) * per);
  if (limit > 0) assigned = assigned.slice(0, limit);

  // 指纹隔离：assigned 变化（如 limit 改了）旧断点自动作废
  const assignedHash = createHash('sha1').update(assigned.join(',')).digest('hex').slice(0, 8);
  const suffix = parts > 1 ? `shard_${shard}.part${part}.${assignedHash}` : `shard_${shard}.${assignedHash}`;
  const outFile = path.join(outDir, `${suffix}.json`);
  const reportFile = path.join(outDir, `${suffix}_report.json`);

  // 清除本 shard/part 属于旧指纹的文件，避免目录里残留历史条目混入合并
  const stalePrefix = parts > 1 ? `shard_${shard}.part${part}.` : `shard_${shard}.`;
  for (const name of fs.readdirSync(outDir)) {
    if (name.startsWith(stalePrefix) && !name.startsWith(suffix)) {
      if (name.endsWith('.json') || name.endsWith('_report.json')) {
        try { fs.unlinkSync(path.join(outDir, name)); } catch {}
      }
    }
  }

  console.log(`[shard ${shard} part ${part}/${parts}] sitemap 唯一ID=${allIds.length} 本片分配=${assigned.length} 指纹=${assignedHash} 并发=${concurrency}`);

  const found = new Map();
  const gone = new Set();
  const invalid = new Map();
  if (fs.existsSync(outFile)) {
    try {
      for (const it of JSON.parse(fs.readFileSync(outFile, 'utf8'))) if (it?.id) found.set(String(it.id), it);
    } catch {}
  }
  if (fs.existsSync(reportFile)) {
    try {
      const prev = JSON.parse(fs.readFileSync(reportFile, 'utf8'));
      for (const id of prev.goneIds || []) gone.add(String(id));
      for (const item of prev.invalidIds || []) invalid.set(String(item.id), item.reason);
    } catch {}
  }

  const attempted = new Set([...found.keys(), ...gone, ...invalid.keys()]);
  let pending = assigned.filter((id) => !attempted.has(id));
  console.log(`[shard ${shard} part ${part}] 已尝试=${attempted.size} 待抓=${pending.length} (found=${found.size} gone=${gone.size} invalid=${invalid.size})`);

  let cursor = 0, lastSave = Date.now();
  const started = Date.now();
  const tag = `[shard ${shard} part ${part}]`;

  const save = () => {
    fs.writeFileSync(outFile, JSON.stringify([...found.values()]));
    fs.writeFileSync(reportFile, JSON.stringify({
      shard, part, parts,
      assigned: assigned.length,
      found: found.size,
      gone: gone.size,
      invalid: invalid.size,
      unaccounted: assigned.length - found.size - gone.size - invalid.size,
      goneIds: [...gone],
      invalidIds: [...invalid].map(([id, reason]) => ({ id, reason })),
      elapsedSec: Math.round((Date.now() - started) / 1000),
      updatedAt: new Date().toISOString(),
    }));
  };

  const worker = async () => {
    for (;;) {
      const i = cursor++;
      if (i >= pending.length) return;
      const id = pending[i];
      const res = await fetchText(`${SITE}/player/${id}?__loader=player_(series_id)/page&__ssrDirect=true`, { tries: 3, timeoutMs: 15000 });
      if (res.ok) {
        const parsed = parseCard(res.text, id);
        if (parsed.card) found.set(id, parsed.card);
        else invalid.set(id, parsed.reason);
      } else if (res.gone) {
        gone.add(id);
      } else {
        invalid.set(id, `net:${res.error || res.status}`);
      }
      if ((i + 1) % 200 === 0 || Date.now() - lastSave > 30000) {
        save();
        const rate = ((i + 1) / ((Date.now() - started) / 1000)).toFixed(1);
        console.log(`${tag} ${i + 1}/${pending.length} found=${found.size} gone=${gone.size} invalid=${invalid.size} ${rate} req/s`);
        lastSave = Date.now();
      }
      await sleep(delayMs);
    }
  };

  await Promise.all(Array.from({ length: concurrency }, () => worker()));

  // 网络类失败做一次集中重试
  const netFailed = [...invalid].filter(([, r]) => String(r).startsWith('net:'));
  if (netFailed.length) {
    console.log(`${tag} 集中重试网络失败 ${netFailed.length} 条`);
    for (const [id] of netFailed) invalid.delete(id);
    pending = netFailed.map(([id]) => id);
    cursor = 0;
    await Promise.all(Array.from({ length: 3 }, () => worker()));
    for (const [id, r] of netFailed) if (!found.has(id) && !gone.has(id)) invalid.set(id, r);
  }

  save();
  const report = JSON.parse(fs.readFileSync(reportFile, 'utf8'));
  const failed = invalid.size;
  const gap = report.unaccounted + failed;
  console.log(`${tag} 完成: ${JSON.stringify({ assigned: report.assigned, found: report.found, gone: report.gone, invalid: report.invalid, unaccounted: report.unaccounted, gap, elapsedSec: report.elapsedSec })}`);
  if (assigned.length > 0 && gap / assigned.length > 0.005) {
    console.error(`${tag} 失败：缺口 ${gap}/${assigned.length}（未解释 ${report.unaccounted} + 失败/异常 ${failed}）超过 0.5% 阈值！`);
    process.exit(1);
  }
}

main().catch((e) => { console.error('FATAL:', e); process.exit(1); });
