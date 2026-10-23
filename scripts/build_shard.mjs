#!/usr/bin/env node
/**
 * build_shard.mjs — 抓取规划器指定的任务项（零冗余抓取）
 */

import fs from 'node:fs';
import path from 'node:path';
import { normalizeCover, safeDate, normalizeCard } from '../lib/catalog_util.js';
import { fetchText as httpFetchText, createBreaker } from '../lib/http.mjs';

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

// 语义层可重试：WAF 常以 200 返回 HTML 挑战页或空壳 JSON，
// 若当成「数据不存在」永久判死，就再也抓不回来了。这类必须重试。
const RETRYABLE_SEMANTIC = new Set(['bad_payload', 'loader_fail', 'no_seriesDetail']);

let breaker = null;
let deadlineAt = 0;
let backoffBaseMs = 2000;
const expired = () => deadlineAt > 0 && Date.now() > deadlineAt;

function fetchText(url, opts = {}) {
  return httpFetchText(url, {
    headers: { 'User-Agent': UA, 'Accept': '*/*', 'Referer': SITE + '/' },
    breaker,
    expired,
    backoffBase: backoffBaseMs,
    ...opts,
  });
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
  const maxRuntimeMin = num(arg('--max-runtime', 40), 40);
  backoffBaseMs = num(arg('--backoff-base', 2000), 2000);
  fs.mkdirSync(outDir, { recursive: true });

  // 全局熔断：被 WAF 限流时所有 worker 一起退避，避免并发猛撞把封禁窗口越拉越长。
  breaker = createBreaker({
    window: num(arg('--breaker-window', 20), 20),
    ratio: num(arg('--breaker-ratio', 0.5), 0.5),
    baseMs: num(arg('--breaker-base', 15000), 15000),
    maxMs: num(arg('--breaker-max', 120000), 120000),
    log: (m) => console.log(`[shard ${shard} part ${part}] ${m}`),
  });
  // 自带 deadline：到点收工落盘，把已抓数据完整交出，不给 CI job 超时杀进程的机会。
  deadlineAt = maxRuntimeMin > 0 ? Date.now() + maxRuntimeMin * 60000 : 0;

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
  let deadlineHit = false;

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
      deadlineHit,
      goneIds: [...gone],
      invalidIds: [...invalid].map(([id, reason]) => ({ id, reason })),
      elapsedSec: Math.round((Date.now() - started) / 1000),
      updatedAt: new Date().toISOString(),
    }, null, 2));
  };

  const worker = async () => {
    for (;;) {
      if (expired()) { deadlineHit = true; return; }
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
    // 失败集中重试（最多 2 轮）：既含网络/限流失败，也含 WAF 挑战页等语义失败。
    for (let round = 1; round <= 2; round++) {
      if (expired()) { deadlineHit = true; break; }
      const retryable = [...invalid].filter(([, r]) => {
        const s = String(r);
        return s.startsWith('net:') || RETRYABLE_SEMANTIC.has(s);
      });
      if (retryable.length === 0) break;
      console.log(`[shard ${shard} part ${part}] 重试失败项第 ${round} 轮: ${retryable.length} 条`);
      for (const [id] of retryable) invalid.delete(id);
      const retryItems = retryable.map(([id]) => ({ id }));
      cursor = 0;
      await Promise.all(Array.from({ length: 2 }, async () => {
        for (;;) {
          if (expired()) { deadlineHit = true; return; }
          const idx = cursor++;
          if (idx >= retryItems.length) return;
          const { id } = retryItems[idx];
          const res = await fetchText(`${site}/player/${id}?__loader=player_(series_id)/page&__ssrDirect=true`, { tries: 4, timeoutMs: 20000 });
          if (res.ok) {
            const parsed = parseCard(res.text, id);
            if (parsed.card) found.set(id, parsed.card);
            else invalid.set(id, parsed.reason);
          } else if (res.gone) {
            gone.add(id);
          } else {
            invalid.set(id, `net:${res.error || res.status}`);
          }
          await sleep(delayMs * 1.5);
        }
      }));
    }
  }

  save();
  const gap = items.length - found.size - gone.size;
  const gapRatio = items.length > 0 ? gap / items.length : 0;
  console.log(`[shard ${shard} part ${part}] 完成: assigned=${items.length} found=${found.size} gone=${gone.size} invalid=${invalid.size} gap=${gap}${deadlineHit ? ' [已到时限，提前收工]' : ''}`);
  // 数据缺口是「业务结果」，不是「工程故障」：部分失败只告警，由 merge 阶段的全局缺口阈值统一把关。
  // 局部 8% 阈值曾把 19.7% 的分片缺口判死，导致 99 个成功分片的数据全部无法发布。
  if (items.length > 0 && gapRatio > 0.08 && gap > 5) {
    if (found.size === 0 && gone.size === 0 && !deadlineHit) {
      // 一条都没抓到且非时限原因，说明是站点级封禁/网络中断等工程故障，保留红灯供人工介入；
      // 产物仍会照常上传，merge 由全局阈值决定是否发布，不会阻断其余分片。
      console.error(`[shard ${shard} part ${part}] 严重失败: 全部 ${items.length} 条均未取得结果`);
      process.exit(1);
    }
    console.warn(`[shard ${shard} part ${part}] 警告: 缺口比例 ${(gapRatio * 100).toFixed(1)}% (gap=${gap}/${items.length})，已记为 invalid 交由 merge 全局阈值判定`);
  }
}

if (process.argv[1] && import.meta.url === new URL(process.argv[1], 'file:').href) {
  main().catch((e) => {
    console.error('FATAL:', e);
    process.exit(1);
  });
}
