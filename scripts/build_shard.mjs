#!/usr/bin/env node
/**
 * build_shard.mjs — 抓取规划器指定的任务项（零冗余抓取）
 */

import fs from 'node:fs';
import path from 'node:path';
import { normalizeCover, safeDate, normalizeCard } from '../lib/catalog_util.js';

const SITE = 'https://hongguoduanju.com';
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function arg(flag, fallback) {
  const i = process.argv.indexOf(flag);
  return i >= 0 && process.argv[i + 1] !== undefined ? process.argv[i + 1] : fallback;
}

function num(v, def) {
  return Number.isFinite(Number(v)) ? Number(v) : def;
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
      cover: normalizeCover(sd.series_cover),
      intro: String(sd.series_intro || '').replace(/\s+/g, ' ').trim().slice(0, 50),
      tags,
      eps,
      ch: '',
      new_at,
    },
  };
}

async function main() {
  const shard = num(arg('--shard', 0), 0);
  const part = num(arg('--part', 0), 0);
  const planFile = arg('--plan-file', `./plan/plan_${shard}_${part}.json`);
  const concurrency = Math.min(10, Math.max(1, num(arg('--concurrency', 6), 6)));
  const delayMs = num(arg('--delay', 80), 80);
  const limit = num(arg('--limit', 0), 0);
  const site = arg('--site', SITE).replace(/\/$/, '');
  const outDir = path.resolve(arg('--out-dir', './shards'));
  fs.mkdirSync(outDir, { recursive: true });

  if (!fs.existsSync(planFile)) {
    throw new Error(`找不到 plan 文件: ${planFile}`);
  }
  const plan = JSON.parse(fs.readFileSync(planFile, 'utf8'));
  let items = plan.items || [];
  if (limit > 0) items = items.slice(0, limit);

  const outFile = path.join(outDir, `shard_${shard}.part${part}.json`);
  const reportFile = path.join(outDir, `shard_${shard}.part${part}_report.json`);
  const found = new Map();
  const gone = new Set();
  const invalid = new Map();

  // 断点恢复（仅当前任务集内的 ID）
  const allowed = new Set(items.map((it) => it.id));
  if (fs.existsSync(outFile)) {
    try {
      for (const raw of JSON.parse(fs.readFileSync(outFile, 'utf8'))) {
        const card = normalizeCard(raw);
        if (card && allowed.has(card.id)) found.set(card.id, card);
      }
    } catch {}
  }
  if (fs.existsSync(reportFile)) {
    try {
      const rep = JSON.parse(fs.readFileSync(reportFile, 'utf8'));
      for (const id of rep.goneIds || []) if (allowed.has(String(id))) gone.add(String(id));
      for (const it of rep.invalidIds || []) if (allowed.has(String(it.id))) invalid.set(String(it.id), it.reason);
    } catch {}
  }

  const attempted = new Set([...found.keys(), ...gone, ...invalid.keys()]);
  const pending = items.filter((it) => !attempted.has(it.id));
  console.log(`[shard ${shard} part ${part}] 分配=${items.length} 断点已恢复=${attempted.size} 需抓取=${pending.length} 并发=${concurrency}`);

  let cursor = 0;
  const started = Date.now();
  let lastSave = Date.now();

  const save = () => {
    fs.writeFileSync(outFile, JSON.stringify([...found.values()]));
    fs.writeFileSync(reportFile, JSON.stringify({
      shard,
      part,
      assigned: items.length,
      found: found.size,
      gone: gone.size,
      invalid: invalid.size,
      unaccounted: items.length - found.size - gone.size - invalid.size,
      goneIds: [...gone],
      invalidIds: [...invalid].map(([id, reason]) => ({ id, reason })),
      elapsedSec: Math.round((Date.now() - started) / 1000),
      updatedAt: new Date().toISOString(),
    }, null, 2));
  };

  const worker = async () => {
    for (;;) {
      const i = cursor++;
      if (i >= pending.length) return;
      const { id } = pending[i];
      const res = await fetchText(`${site}/player/${id}?__loader=player_(series_id)/page&__ssrDirect=true`, { tries: 3, timeoutMs: 15000 });
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
        lastSave = Date.now();
      }
      await sleep(delayMs);
    }
  };

  if (pending.length > 0) {
    await Promise.all(Array.from({ length: concurrency }, () => worker()));
    // 网络错误集中重试一次
    const netFailed = [...invalid].filter(([, r]) => String(r).startsWith('net:'));
    if (netFailed.length > 0) {
      console.log(`[shard ${shard} part ${part}] 重试网络失败 ${netFailed.length} 条`);
      for (const [id] of netFailed) invalid.delete(id);
      const retryItems = netFailed.map(([id]) => ({ id }));
      cursor = 0;
      await Promise.all(Array.from({ length: 3 }, async () => {
        for (;;) {
          const idx = cursor++;
          if (idx >= retryItems.length) return;
          const { id } = retryItems[idx];
          const res = await fetchText(`${site}/player/${id}?__loader=player_(series_id)/page&__ssrDirect=true`, { tries: 3, timeoutMs: 15000 });
          if (res.ok) {
            const parsed = parseCard(res.text, id);
            if (parsed.card) found.set(id, parsed.card);
            else invalid.set(id, parsed.reason);
          } else if (res.gone) {
            gone.add(id);
          } else {
            invalid.set(id, `net:${res.error || res.status}`);
          }
          await sleep(delayMs);
        }
      }));
    }
  }

  save();
  const gap = items.length - found.size - gone.size;
  console.log(`[shard ${shard} part ${part}] 完成: assigned=${items.length} found=${found.size} gone=${gone.size} invalid=${invalid.size} gap=${gap}`);
  if (items.length > 0 && gap / items.length > 0.005) {
    console.error(`[shard ${shard} part ${part}] 失败: 缺口比例超过 0.5%`);
    process.exit(1);
  }
}

if (process.argv[1] && import.meta.url === new URL(process.argv[1], 'file:').href) {
  main().catch((e) => {
    console.error('FATAL:', e);
    process.exit(1);
  });
}
