Rust 版 Fake HuggingFace 服务器（Axum）

- 目标：模拟 huggingface网站 download 功能的API 与行为，并在 I/O 与范围下载上做流式优化与轻量缓存。
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
- 远端配置与凭据（给 `fetch_repo` 工具用）：
  - `HF_REMOTE_ENDPOINT`（默认 `https://huggingface.co`）
  - `HF_TOKEN` / `HUGGING_FACE_HUB_TOKEN` / `HUGGINGFACEHUB_API_TOKEN`
  - 代理：`HTTP(S)_PROXY`、`ALL_PROXY`（例如 `all_proxy=socks5h://127.0.0.1:8235`）

API
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
可使用二进制工具 `fetch_repo` 走 HuggingFace 公共 API 的树接口，按真实元数据在 `fake_hub` 目录下生成目录结构与占位文件：

```bash
cargo run --bin fetch_repo -- -t model user/repo
```

生成时会同时写入 `.paths-info.json` 侧车文件，供服务器在 HEAD 请求中读取 ETag。

参数（对齐 Python 原型）：
- `-t, --repo-type model|dataset`（默认 `model`）
- `-r, --revision`（默认 `main`）
- `-e, --endpoint` 远端根地址（默认 `HF_REMOTE_ENDPOINT` 或 `https://huggingface.co`）
- `--token` 访问令牌（也可通过 `HF_TOKEN`、`HUGGING_FACE_HUB_TOKEN`、`HUGGINGFACEHUB_API_TOKEN`）
- `--include`/`--exclude` 多次指定的 glob 过滤（fnmatch 语义）
- `--max-files` 限制文件数
- `--dst` 目标根目录（默认：模型 `fake_hub/<repo>`，数据集 `fake_hub/datasets/<repo>`）
- `--force` 覆盖现有文件
- `--dry-run` 只打印不写入
- `--fill` 按固定大小写入重复内容（代替空文件）
- `--fill-size` 大小（例如 `16MiB`，若未指定则默认 16MiB）
- `--fill-content` 重复内容字符串（默认 0 字节）
- `--no-proxy` 忽略系统代理（默认遵循系统代理）
 - 简单生成模式（无需访问网络）：
   - `--gen-count <N>` 与 `--gen-avg-size <SIZE>`
   - 在仓库根下生成 N 个扁平文件（`file_00001.bin`…），每个大小为 `<SIZE>`；支持 `--fill-content` 自定义填充字节模式。

实现细节：
- 通过 `GET /api/{models|datasets}/{repo}/tree/{rev}?recursive=1&expand=1` 获取文件列表；必要时携带 Bearer Token。
- `repo_id` 每个路径段会做 URL 安全转码；即使传入已编码的 `HunyuanImage%2D2%2E1` 也会先解码再正确编码，避免二次编码。
- 本地实际写入的文件用于计算 `.paths-info.json`（含 sha1 与 sha256），与服务器 HEAD ETag 逻辑一致（LFS 文件使用 `lfs.oid` 形如 `sha256:<hex>`，普通文件使用 `oid`）。
- 默认遵循系统代理（`HTTP(S)_PROXY`/`ALL_PROXY`）；如需显式忽略代理，使用 `--no-proxy`。
- 远端错误会打印状态码与响应体，便于定位鉴权/修订问题。

示例
- 使用系统代理与公共仓库：
  - `export https_proxy=http://127.0.0.1:8234; export http_proxy=http://127.0.0.1:8234; export all_proxy=socks5h://127.0.0.1:8235`
  - `cargo run --bin fetch_repo -- tencent/HunyuanImage-2.1 -t model -r main --max-files 20`
- 只取子目录并填充 1MiB 占位内容：
  - `cargo run --bin fetch_repo -- tencent/HunyuanImage-2.1 --include 'vae/**' --fill --fill-size 1MiB --fill-content X`
- 私有仓库或避免限流（使用 Token）：
  - `export HF_TOKEN=hf_xxx && cargo run --bin fetch_repo -- org/private-repo --token "$HF_TOKEN"`

简单生成模式示例
```bash
cargo run --bin fetch_repo -- user/repo -t model --gen-count 100 --gen-avg-size 16MiB --fill-content X
```
