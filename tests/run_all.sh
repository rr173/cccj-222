#!/usr/bin/env bash
# 从空目录复跑全部测试：确定性 oracle 对拍 + 真实多进程交错/崩溃演练
# + 字段历史归档/查询演练。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "############ 1/3 确定性演练（独立小模型逐笔对拍） ############"
python3 -I tests/test_deterministic.py

echo
echo "############ 2/3 真实多进程交错 + 崩溃恢复演练 ############"
python3 -I tests/test_concurrent.py

echo
echo "############ 3/3 字段历史归档与查询演练（手算 + 独立参考实现对拍） ############"
python3 -I tests/test_field_history.py

echo
echo "ALL TEST SUITES PASSED"
