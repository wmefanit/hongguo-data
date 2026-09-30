# hongguo-data

红果短剧（hongguoduanju.com）全库元数据矩阵构建与分发仓库。

## 数据规格

- **数据源**：官方 Sitemap 全部 26 个分片（去重唯一 ID **546,830 部**，约 55 万部）
- **抓取端点**：`https://hongguoduanju.com/player/<id>?__loader=player_(series_id)/page&__ssrDirect=true`
- **单条字段**：
  - `id`：剧集唯一 ID（雪花 ID 字符串）
  - `title`：剧名
  - `cover`：封面图片 URL
  - `intro`：前 50 字精简简介
  - `tags`：题材标签数组
  - `eps`：总集数
  - `new_at`：上架日期（优先取官方 `first_visible_time`，回退取 ID 时间戳）
- **输出格式**：
  - `catalog.json.gz`：Level-9 Gzip 压缩的全量数组（~35 MB）
  - `summary.json`：构建统计、覆盖率、时间戳

## 获取最新数据

直接从 Releases 下载 `catalog.json.gz`：

```bash
curl -LO https://github.com/wmefanit/hongguo-data/releases/latest/download/catalog.json.gz
gzip -d catalog.json.gz
```

## 构建方法

```bash
# 抓取单分片（支持再分 part）
node scripts/build_shard.mjs --shard 1 --part 0 --parts 4 --concurrency 6

# 合并所有分片并做严格基准校验
node scripts/merge_catalog.mjs --in-dir ./shards --out-dir ./dist
```
