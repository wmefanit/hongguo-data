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
