Rust 版 Fake HuggingFace 服务器（Axum）

- 目标：模拟 huggingface网站 download 功能的API 与行为，并在 I/O 与范围下载上做流式优化与轻量缓存。
- 技术：Axum + Tokio，按需读取、分块传输，TTL 内存缓存（以空间换时间）。

架构
- `src/main.rs`：入口与路由装配、初始化 tracing（打印监听地址，尊重 LOG_REDACT）。
- `src/middleware.rs`：请求日志中间件（可选记录请求体，脱敏敏感头）。
- `src/resolve.rs`：文件 GET/HEAD/Range 与响应构建；ETag 严格来自 sidecar，无回退；单文件 sha256。
- `src/routes_models.rs`：模型相关 API 处理函数。
- `src/routes_datasets.rs`：数据集相关 API 处理函数。
- `src/app_state.rs`：运行时配置与环境变量解析。
- `src/caches.rs`：TTL/容量受限的轻量缓存。
- `src/utils/`：headers 构造、路径安全拼接、sidecar/树信息解析、repo_json 生成、目录遍历等。

运行
- 依赖：Rust 1.80+（Edition 2024）
- 开发构建：`cd fake_huggingface_rs && cargo build`
- 发布构建：`cd fake_huggingface_rs && cargo build --release`
- 启动：`FAKE_HUB_ROOT=fake_hub ./target/release/fake_huggingface_rs`
 - 启动输出：会打印绑定地址、本地与局域网可访问地址，例如：
   - `[fake-hub] Listening on http://0.0.0.0:8000 (local: http://127.0.0.1:8000, lan: http://192.168.1.23:8000)`

环境变量
- `FAKE_HUB_ROOT`：本地“仓库根目录”（默认 `fake_hub`）。数据集位于 `fake_hub/datasets/...`。
- 日志：`LOG_REQUESTS`、`LOG_BODY_MAX`、`LOG_HEADERS=all|minimal`、`LOG_RESP_HEADERS`、`LOG_REDACT`、`LOG_BODY_ALL`、`LOG_JSON_BODY`。
  - 仅当 `LOG_BODY_ALL=1` 或 `LOG_JSON_BODY=1 且 Content-Type: application/json` 时尝试记录请求体；
  - 仅在请求头存在 `Content-Length` 且大小不超过 `4*LOG_BODY_MAX` 时读取（否则跳过以避免 OOM）；
  - 记录的正文内容按 `LOG_BODY_MAX` 截断；敏感头在 `LOG_REDACT=1` 时会脱敏。
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
  - `GET /api/models/{repo_id}/tree/{revision}`（返回数组；支持 `?recursive=1&expand=1`）
- 数据集信息
  - `GET /api/datasets/{repo_id}`
  - `GET /api/datasets/{repo_id}/revision/{revision}`
  - `POST /api/datasets/{repo_id}/paths-info/{revision}`（在 `FAKE_HUB_ROOT/datasets/{repo_id}` 下）
  - `GET /api/datasets/{repo_id}/tree/{revision}`（返回数组；支持 `?recursive=1&expand=1`）
- 文件下载/探测
  - `GET|HEAD /{repo_id}/resolve/{revision}/{filename...}`
  - GET 支持 Range（bytes=...）：返回 206/416；非法 Range 回退 200 全量。
  - HEAD：ETag 仅从 `.paths-info.json` 读取（LFS 文件用 `lfs.oid`，普通文件用 `oid`），不存在则 500（严格，不做回退）；带 LFS 元数据的文件附带 `x-lfs-size`；`416` 时包含 `Content-Length: 0`。
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
 - 树列举（模型）：
   - `curl 'http://localhost:8000/api/models/tencent/HunyuanImage-2.1/tree/main?recursive=1&expand=1'`

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

生成时会同时写入 `.paths-info.json` 侧车文件，供服务器在 HEAD/GET 请求中严格读取 ETag。

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
- `--fill-from-metadata` 若远端返回了文件大小，则按其大小填充（优先于 `--fill-size`）
- `--no-proxy` 忽略系统代理（默认遵循系统代理）
 - 简单生成模式（无需访问网络）：
   - `--gen-count <N>` 与 `--gen-avg-size <SIZE>`
   - 在仓库根下生成 N 个扁平文件（`file_00001.bin`…），每个大小为 `<SIZE>`；文件内容为随机字节；不接受 `--fill-content`。

实现细节：
- 通过 `GET /api/{models|datasets}/{repo}/tree/{rev}?recursive=1&expand=1` 获取文件列表；必要时携带 Bearer Token。
- `repo_id` 每个路径段会做 URL 安全转码；即使传入已编码的 `HunyuanImage%2D2%2E1` 也会先解码再正确编码，避免二次编码。
- 本地实际写入的文件用于计算 `.paths-info.json`（含 sha1 与 sha256），与服务器 ETag 逻辑一致（LFS 使用 `lfs.oid` 形如 `sha256:<hex>`，普通文件使用 `oid`）。
- 生成 `.paths-info.json` 时对文件哈希进行并行计算：按 CPU 并发切片，单线程仅占用 ~1MiB 缓冲，提升大型仓库生成速度。
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
cargo run --bin fetch_repo -- user/repo -t model --gen-count 100 --gen-avg-size 16MiB
# 简单模式文件内容为随机字节；不可与 --fill-content 同用
```

开发与测试
-----------
- 格式/Lint：`cargo fmt --all && cargo clippy --all-targets -- -D warnings`
- 单元测试：`cargo test`（无网络；集成测试可基于 `tower::ServiceExt::oneshot`）
- 关键模块：
  - `middleware.rs`：请求日志/脱敏；
  - `resolve.rs`：文件响应、Range 处理、严格 ETag；
  - `routes_models.rs` / `routes_datasets.rs`：业务 handlers；
  - `utils/`：公用工具与 sidecar 解析。
