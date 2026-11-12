# hongguo-data

红果短剧全量索引增量构建与发布工程。

## 核心架构与增量流水线

```
上游 Sitemap 总索引 index.xml (动态发现分片，当前 28 片)
   │
   ├──> plan_run.mjs (读取总索引得全部分片 + 云端基线 source_manifest，比对 lastmod)
   │       ├── unchanged=true ──> 提前终止，0 抓取
   │       └── unchanged=false ──> 生成精准 plan_<shard>_<part>.json
   │
   ├──> build_shard.mjs (按索引发现的全部分片矩阵并行，只抓取规划内变动 ID)
   │
   └──> merge_catalog.mjs (基线复用 + 严格归因 + 生成 Delta + SHA256)
           ├── catalog.json.gz (全量索引)
           ├── catalog.delta.json.gz (增量补丁)
           ├── source_manifest.json.gz (源状态真相表)
           └── summary.json / delta.summary.json
```

## 关键命令

- **离线全链路增量与容错验证**：
  ```bash
  node scripts/test_incremental_e2e.mjs
  ```
- **规划抓取任务**：
  ```bash
  node scripts/plan_run.mjs --out-dir ./plan --baseline-manifest ./baseline/source_manifest.json.gz
  ```
- **分片抓取**：
  ```bash
  node scripts/build_shard.mjs --shard 1 --part 0 --plan-file ./plan/plan_1_0.json --out-dir ./shards
  ```
- **合并发布包**：
  ```bash
  node scripts/merge_catalog.mjs --plan-file ./plan/plan.json --in-dir ./shards --out-dir ./dist --baseline-catalog ./baseline/catalog.json.gz
  ```

## 发布不变量（改动发布逻辑前必读）

- **Release tag 一经发布不得复用、不得删除重建**：tag 是增量链的锚点，`delta.from_tag` 指向它。
  同一天多次构建必须产出不同 tag（`full-<日期>-<run_id>-<attempt>`），否则后一次构建会覆盖前一次的
  目录内容，使已发布 delta 的 `from_catalog_hash` 永久失配，下游每次同步都判"链断"→ 退化成 80MB 全量。
- `merge_catalog.mjs` 会校验「计划里的基线哈希 == 实际基线目录哈希」「delta 起始 tag ≠ 目标 tag」，
  对不上直接失败；发布步骤会校验 summary / delta.summary / delta 资产 SHA256 三者一致。
