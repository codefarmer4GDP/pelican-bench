#!/usr/bin/env bash
# pelican-bench.sh — 薄包装：只做环境检查，然后调用 python3 运行 pelican-bench.py
# A thin wrapper: environment checks only, then delegate to python3.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAIN="${SCRIPT_DIR}/pelican-bench.py"

fail() {
    echo "[pelican-bench] $1" >&2
    exit 2
}

# 1) python3 存在
if ! command -v python3 >/dev/null 2>&1; then
	fail "未找到 python3。请安装 Python 3.9+ 后重试。"
fi

# 2) 版本 >= 3.9
PY_VERSION="$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null || echo "0.0")"
if ! python3 - <<'EOF' >/dev/null 2>&1
import sys
sys.exit(0 if sys.version_info >= (3, 9) else 1)
EOF
then
    fail "需要 Python 3.9+，当前为 ${PY_VERSION}。"
fi

# 3) 主脚本存在
if [[ ! -f "${MAIN}" ]]; then
	fail "未找到主脚本 ${MAIN}。"
fi

# 透传全部参数
exec python3 "${MAIN}" "$@"
