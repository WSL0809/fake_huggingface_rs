Rust 版 Fake HuggingFace 服务器（Axum）

- 目标：一比一复刻 `main.py` 的 API 与行为，并在 I/O 与范围下载上做流式优化与轻量缓存。
- 技术：Axum + Tokio，按需读取、分块传输，TTL 内存缓存（以空间换时间）。

架构
- `main.rs`：入口，挂载路由。
- `app_state.rs`：保存配置。
- `caches.rs`：缓存结构。

运行
- 依赖：Rust 1.80+（Edition 2024）
- 开发构建：`cd fake_huggingface_rs && cargo build`
- 发布构建：`cd fake_huggingface_rs && cargo build --release`
- 启动：`FAKE_HUB_ROOT=fake_hub ./target/release/fake_huggingface_rs`

环境变量
- `FAKE_HUB_ROOT`：本地“仓库根目录”（默认 `fake_hub`）。数据集位于 `fake_hub/datasets/...`。
- 日志：`LOG_REQUESTS`、`LOG_BODY_MAX`、`LOG_HEADERS=all|minimal`、`LOG_RESP_HEADERS`、`LOG_REDACT`、`LOG_BODY_ALL`。
- 缓存：`CACHE_TTL_MS`（默认 2000ms）、`PATHS_INFO_CACHE_CAP`（默认 512）、`SIBLINGS_CACHE_CAP`（默认 256）、`SHA256_CACHE_CAP`（默认 1024）。

API（与 Python 版对齐）
- 模型信息
  - `GET /api/models/{repo_id}`
  - `GET /api/models/{repo_id}/revision/{revision}`
  - `POST /api/models/{repo_id}/paths-info/{revision}`
- 数据集信息
  - `GET /api/datasets/{repo_id}`
  - `GET /api/datasets/{repo_id}/revision/{revision}`
  - `POST /api/datasets/{repo_id}/paths-info/{revision}`（在 `FAKE_HUB_ROOT/datasets/{repo_id}` 下）
- 文件下载/探测
  - `GET|HEAD /{repo_id}/resolve/{revision}/{filename...}`
  - GET 支持 Range（bytes=...）：返回 206/416；非法 Range 回退 200 全量。
  - HEAD：ETag 仅从 `.paths-info.json` 读取（LFS 文件用 `lfs.oid`，普通文件用 `oid`），不存在则 500；带 LFS 元数据的文件附带 `x-lfs-size`。
- 新增：单文件 SHA-256
  - `GET /{repo_id}/sha256/{revision}/{filename...}`
  - 仅 GET；HEAD 返回 405。
  - 返回：`{"sha256":"<hex>"}`。若文件不存在：404。
  - 忽略 `.paths-info.json`。

paths-info 语义
- 请求体：`{"paths"?: string[], "expand"?: boolean}`。
- 响应以 sidecar（`.paths-info.json`）为优先，返回文件 `size`、`oid`、`lfs.oid` 等；不对 `.paths-info.json` 本身建项。
- 未指定 `paths` 时递归枚举整个仓库；`expand=false` 对目录仅返回占位项。

示例
- 下载：`curl -L http://localhost:8000/tencent/HunyuanImage-2.1/resolve/main/README.md`
- 单文件哈希：`curl http://localhost:8000/tencent/HunyuanImage-2.1/sha256/main/config.json`
- 模型 paths-info：
  - `curl -X POST http://localhost:8000/api/models/tencent/HunyuanImage-2.1/paths-info/main -H 'content-type: application/json' -d '{"paths":["assets/"],"expand":true}'`

实现细节
- 分块大小：256 KiB；Tokio `ReaderStream`/手动读循环，减少系统调用与拷贝。
- 缓存：
  - 兄弟文件与 usedStorage（模型/数据集信息），TTL + 容量上限；
  - paths-info 请求缓存：按 base 路径 + sidecar mtime/size + 请求签名；
  - sidecar 解析缓存：按文件路径 + mtime + size；
  - sha256 结果缓存：按文件路径 + mtime + size。

生成本地仓库骨架
------------------
可使用二进制工具 `fetch_repo` 调用 HuggingFace 的 `paths-info` 接口，按真实元数据在 `fake_hub` 目录下生成目录结构与占位文件：

```bash
cargo run --bin fetch_repo -- user/repo
```

生成时会同时写入 `.paths-info.json` 侧车文件，供服务器在 HEAD 请求中读取 ETag。

参数：
- `--repo-type models|datasets`（默认 `models`）
- `--revision`（默认 `main`）
- `--dest` 目标根目录（默认 `fake_hub`）
