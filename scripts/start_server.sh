#!/usr/bin/env bash
set -euo pipefail

# Resolve repository root relative to this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BINARY_PATH="${REPO_ROOT}/target/release/fake_huggingface_rs"
LOG_DIR="${REPO_ROOT}/logs"
LOG_FILE="${LOG_DIR}/server.log"
PID_FILE="${LOG_DIR}/server.pid"

mkdir -p "${LOG_DIR}"

if [[ ! -x "${BINARY_PATH}" ]]; then
    echo "error: ${BINARY_PATH} not found or not executable." >&2
    echo "build it first: cargo build --release" >&2
    exit 1
fi

# Default FAKE_HUB_ROOT to local fake_hub directory when not provided.
export FAKE_HUB_ROOT="${FAKE_HUB_ROOT:-${REPO_ROOT}/fake_hub}"

# Allow overriding log file and RUST_LOG via env vars before invocation.
: "${RUST_LOG:=info}"

cd "${REPO_ROOT}"

nohup env RUST_LOG="${RUST_LOG}" FAKE_HUB_ROOT="${FAKE_HUB_ROOT}" "${BINARY_PATH}" "$@" \
    >>"${LOG_FILE}" 2>&1 &
PID=$!

echo "${PID}" > "${PID_FILE}"
echo "started fake_huggingface_rs (pid ${PID})"
echo "logs: ${LOG_FILE}"
