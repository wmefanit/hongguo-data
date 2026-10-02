import { createHash } from 'node:crypto';

export const CATALOG_SCHEMA = 2;
const SOURCE_STATUSES = new Set(['ok', 'gone', 'invalid']);

export function sha256Hex(value) {
  return createHash('sha256').update(value).digest('hex');
}

export function normalizeCover(url) {
  return String(url || '')
    .replace(/^https?:\/\/p\d+-novel\.byteimg\.com\//, 'https://p3-novel.byteimg.com/')
    .replace(/\\u002F/g, '/');
}

export function idToDate(id) {
  try { return new Date(Number(BigInt(id) >> 32n) * 1000).toISOString().slice(0, 10); } catch { return ''; }
}

export function safeDate(ts, id) {
  const n = Number(ts);
  if (Number.isFinite(n) && n > 0 && n < 4102444800) {
    try { return new Date(n * 1000).toISOString().slice(0, 10); } catch {}
  }
  return idToDate(id);
}

export function parseSitemapEntries(xmlText) {
  const entries = new Map();
  for (const match of xmlText.matchAll(/<url>([\s\S]*?)<\/url>/g)) {
    const loc = (match[1].match(/<loc>([^<]+)<\/loc>/) || [])[1] || '';
    const id = (loc.match(/(?:\/player\/|series_id=)(\d+)/) || [])[1];
    if (!id) continue;
    const lastmod = (match[1].match(/<lastmod>([^<]+)<\/lastmod>/) || [])[1] || '';
    if (!lastmod) throw new Error(`Sitemap 条目 ${id} 缺少 lastmod`);
    const prev = entries.get(id);
    if (!prev || String(lastmod).localeCompare(String(prev)) > 0) {
      entries.set(id, lastmod);
    }
  }
  return entries;
}

export function partForId(id, parts) {
  // FNV-1a：稳定分桶，不受 Sitemap 条目排序增删影响。
  let hash = 2166136261;
  for (let i = 0; i < String(id).length; i++) {
    hash ^= String(id).charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0) % parts;
}

export function normalizeCard(raw) {
  if (!raw || !raw.id || !String(raw.title || '').trim() || !(Number(raw.eps) > 0)) return null;
  return {
    id: String(raw.id),
    title: String(raw.title).trim(),
    cover: normalizeCover(raw.cover),
    intro: String(raw.intro || '').replace(/\s+/g, ' ').trim().slice(0, 50),
    tags: Array.isArray(raw.tags) ? raw.tags.map((tag) => String(tag).trim()).filter(Boolean).slice(0, 6) : [],
    eps: Number(raw.eps),
    ch: String(raw.ch || ''),
    new_at: String(raw.new_at || ''),
  };
}

export function compareCards(a, b) {
  const dateOrder = String(b.new_at || '').localeCompare(String(a.new_at || ''));
  return dateOrder || String(a.id).localeCompare(String(b.id));
}

export function canonicalCatalog(list) {
  if (!Array.isArray(list)) throw new Error('catalog 根节点不是数组');
  const ids = new Set();
  const result = [];
  for (const raw of list) {
    const card = normalizeCard(raw);
    if (!card) throw new Error(`catalog 存在非法条目: ${JSON.stringify(raw).slice(0, 200)}`);
    if (ids.has(card.id)) throw new Error(`catalog 存在重复 ID: ${card.id}`);
    ids.add(card.id);
    result.push(card);
  }
  return result.sort(compareCards);
}

export function catalogBytes(list) {
  return Buffer.from(JSON.stringify(canonicalCatalog(list)), 'utf8');
}

export function catalogHash(list) {
  return sha256Hex(catalogBytes(list));
}

export function normalizeManifestEntries(entries) {
  if (!Array.isArray(entries)) throw new Error('source manifest entries 不是数组');
  const ids = new Set();
  const result = [];
  for (const raw of entries) {
    const [idRaw, lastmodRaw, statusRaw] = Array.isArray(raw) ? raw : [];
    const id = String(idRaw || '');
    const lastmod = String(lastmodRaw || '');
    const status = String(statusRaw || '');
    if (!/^\d+$/.test(id) || !lastmod || !SOURCE_STATUSES.has(status)) {
      throw new Error(`source manifest 存在非法条目: ${JSON.stringify(raw)}`);
    }
    if (ids.has(id)) throw new Error(`source manifest 存在重复 ID: ${id}`);
    ids.add(id);
    result.push([id, lastmod, status]);
  }
  return result.sort((a, b) => a[0].localeCompare(b[0]));
}

export function sourceHash(entries) {
  return sha256Hex(Buffer.from(JSON.stringify(normalizeManifestEntries(entries)), 'utf8'));
}

export function normalizeSitemapEntries(entries) {
  if (!Array.isArray(entries)) throw new Error('Sitemap entries 不是数组');
  const ids = new Set();
  const result = [];
  for (const raw of entries) {
    const [idRaw, lastmodRaw] = Array.isArray(raw) ? raw : [];
    const id = String(idRaw || '');
    const lastmod = String(lastmodRaw || '');
    if (!/^\d+$/.test(id) || !lastmod) throw new Error(`Sitemap entries 存在非法条目: ${JSON.stringify(raw)}`);
    if (ids.has(id)) throw new Error(`Sitemap entries 存在重复 ID: ${id}`);
    ids.add(id);
    result.push([id, lastmod]);
  }
  return result.sort((a, b) => a[0].localeCompare(b[0]));
}

export function sitemapHash(entries) {
  return sha256Hex(Buffer.from(JSON.stringify(normalizeSitemapEntries(entries)), 'utf8'));
}

export function validateSourceManifest(manifest) {
  if (!manifest || manifest.schema !== CATALOG_SCHEMA || manifest.type !== 'source-manifest') {
    throw new Error(`不支持的 source manifest schema/type: ${manifest?.schema}/${manifest?.type}`);
  }
  if (!manifest.catalog_hash || !manifest.source_hash) throw new Error('source manifest 缺少 hash');
  const entries = normalizeManifestEntries(manifest.entries);
  if (sourceHash(entries) !== manifest.source_hash) throw new Error('source manifest source_hash 不匹配');
  return { ...manifest, entries };
}

export function createSourceManifest({ tag, catalogHash: hash, entries }) {
  const normalized = normalizeManifestEntries(entries);
  return {
    schema: CATALOG_SCHEMA,
    type: 'source-manifest',
    tag: tag || null,
    catalog_hash: hash,
    source_hash: sourceHash(normalized),
    entries: normalized,
  };
}

export function diffCatalogs(baseList, targetList, metadata) {
  const base = canonicalCatalog(baseList);
  const target = canonicalCatalog(targetList);
  const baseMap = new Map(base.map((card) => [card.id, card]));
  const targetMap = new Map(target.map((card) => [card.id, card]));
  const added = [];
  const modified = [];
  const deleted = [];

  for (const card of target) {
    const previous = baseMap.get(card.id);
    if (!previous) {
      added.push(card);
    } else if (JSON.stringify(previous) !== JSON.stringify(card)) {
      modified.push(card);
    }
  }
  for (const card of base) if (!targetMap.has(card.id)) deleted.push(card.id);

  return {
    schema: CATALOG_SCHEMA,
    type: 'catalog-delta',
    from_tag: metadata.fromTag || null,
    to_tag: metadata.toTag || null,
    from_catalog_hash: metadata.fromCatalogHash,
    to_catalog_hash: metadata.toCatalogHash,
    from_count: base.length,
    to_count: target.length,
    added,
    modified,
    deleted,
  };
}

export function applyDeltaToCatalog(baseList, delta) {
  if (!delta || delta.schema !== CATALOG_SCHEMA || delta.type !== 'catalog-delta') {
    throw new Error('不支持的 delta schema/type');
  }
  const map = new Map(canonicalCatalog(baseList).map((card) => [card.id, card]));
  const addedIds = new Set();
  const modifiedIds = new Set();
  const deletedIds = new Set();

  for (const raw of delta.added || []) {
    const card = normalizeCard(raw);
    if (!card || addedIds.has(card.id) || map.has(card.id)) throw new Error(`delta added 非法或重复: ${raw?.id}`);
    addedIds.add(card.id);
    map.set(card.id, card);
  }
  for (const raw of delta.modified || []) {
    const card = normalizeCard(raw);
    if (!card || modifiedIds.has(card.id) || !map.has(card.id) || addedIds.has(card.id)) throw new Error(`delta modified 非法: ${raw?.id}`);
    modifiedIds.add(card.id);
    map.set(card.id, card);
  }
  for (const idRaw of delta.deleted || []) {
    const id = String(idRaw);
    if (deletedIds.has(id) || !map.has(id) || addedIds.has(id) || modifiedIds.has(id)) throw new Error(`delta deleted 非法: ${id}`);
    deletedIds.add(id);
    map.delete(id);
  }

  const result = canonicalCatalog([...map.values()]);
  if (result.length !== Number(delta.to_count)) throw new Error(`delta 合并条目数不符: ${result.length} != ${delta.to_count}`);
  return result;
}
