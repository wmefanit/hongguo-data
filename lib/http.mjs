/**
 * lib/http.mjs — 统一抓取容错层
 *
 * 分类原则（反转白名单，不枚举可重试码）：
 *   404 / 410   → gone：内容确实不存在，终态，不重试
 *   2xx         → ok：交给调用方做语义校验
 *   其余一切     → 可重试：未知状态码、WAF 断连(444)、403 挑战、429、5xx、
 *                 超时、DNS 抖动、连接重置……WAF 换码不影响行为
 */

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function backoff(ms, jitter) {
  const j = jitter ? 0.5 + Math.random() : 1;
  return Math.min(90000, Math.max(800, ms * j));
}

/**
 * 全局熔断器：连续失败达阈值即让所有并发 worker 一起冷却，
 * 避免被限流时 N 个 worker 同时猛撞、把封禁窗口越撞越长。
 */
export function createBreaker({ window = 20, minSamples = Math.min(10, window), ratio = 0.5, baseMs = 15000, maxMs = 120000, log = () => {} } = {}) {
  const outcomes = []; // 滑动窗口：true=成功 false=失败
  let level = 0;
  let okStreak = 0;
  let until = 0;

  return {
    async gate() {
      const wait = until - Date.now();
      if (wait > 0) await sleep(wait);
    },
    record(ok) {
      if (ok) {
        okStreak += 1;
        if (okStreak >= window) level = 0; // 持续健康才降级，避免抖动误判
      } else {
        okStreak = 0;
      }
      outcomes.push(ok);
      if (outcomes.length > window) outcomes.shift();

      if (ok || outcomes.length < minSamples) return;
      const fails = outcomes.filter((x) => !x).length;
      const rate = fails / outcomes.length;
      if (rate < ratio || Date.now() < until) return;

      level += 1;
      const cooldown = Math.min(maxMs, baseMs * 2 ** (level - 1));
      until = Date.now() + cooldown;
      outcomes.length = 0;
      log(`[熔断] 近 ${window} 次失败率 ${Math.round(rate * 100)}%，全局冷却 ${Math.round(cooldown / 1000)}s（第 ${level} 级）`);
    },
    onSuccess() { this.record(true); },
    onFailure() { this.record(false); },
    get tripped() {
      return Date.now() < until;
    },
  };
}

/**
 * @returns {{ok:true,text:string,status:number}}
 *        | {{ok:false,gone:true,status:number}}
 *        | {{ok:false,error:string,status?:number}}
 *        | {{ok:false,error:'deadline',deadline:true}}
 */
export async function fetchText(url, {
  tries = 5,
  timeoutMs = 20000,
  headers = {},
  breaker = null,
  expired = null,
  jitter = true,
  backoffBase = 2000,
} = {}) {
  let last = '';
  for (let i = 0; i < tries; i++) {
    if (expired && expired()) return { ok: false, error: 'deadline', deadline: true };
    if (breaker) await breaker.gate();
    if (expired && expired()) return { ok: false, error: 'deadline', deadline: true };

    try {
      const resp = await fetch(url, { headers, signal: AbortSignal.timeout(timeoutMs) });

      if (resp.status === 404 || resp.status === 410) {
        if (breaker) breaker.onSuccess();
        return { ok: false, gone: true, status: resp.status };
      }
      if (resp.ok) {
        if (breaker) breaker.onSuccess();
        return { ok: true, text: await resp.text(), status: resp.status };
      }

      // 其余一切非 2xx 一律可重试
      last = `HTTP ${resp.status}`;
      if (breaker) breaker.onFailure();
      const ra = Number(resp.headers.get('retry-after') || 0);
      await sleep(backoff(ra > 0 ? ra * 1000 : backoffBase * 2 ** i, jitter));
    } catch (e) {
      last = String((e && e.message) || e);
      if (breaker) breaker.onFailure();
      await sleep(backoff(backoffBase * 0.75 * (i + 1), jitter));
    }
  }
  return { ok: false, error: last || 'fetch failed' };
}
