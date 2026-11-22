#!/usr/bin/env bash
# 从空目录复跑全部测试：确定性 oracle 对拍 + 真实多进程交错/崩溃演练。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "############ 1/2 确定性演练（独立小模型逐笔对拍） ############"
python3 -I tests/test_deterministic.py

echo
echo "############ 2/2 真实多进程交错 + 崩溃恢复演练 ############"
python3 -I tests/test_concurrent.py

echo
echo "ALL TEST SUITES PASSED"
