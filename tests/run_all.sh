#!/usr/bin/env bash
# 从空目录复跑全部测试：确定性 oracle 对拍 + 真实多进程交错/崩溃演练
# + 字段历史归档/查询演练 + 当前库存物化演练 + 可复现事务包导出演练。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "############ 1/5 确定性演练（独立小模型逐笔对拍） ############"
python3 -I tests/test_deterministic.py

echo
echo "############ 2/5 真实多进程交错 + 崩溃恢复演练 ############"
python3 -I tests/test_concurrent.py

echo
echo "############ 3/5 字段历史归档与查询演练（手算 + 独立参考实现对拍） ############"
python3 -I tests/test_field_history.py

echo
echo "############ 4/5 当前库存物化演练（手算 + 独立全量折叠对拍 + 清理/回滚/增量依据） ############"
python3 -I tests/test_inventory.py

echo
echo "############ 5/5 可复现事务包导出（手算迟到例子 + 独立选样模型 + 清理/重灌/原子性） ############"
python3 -I tests/test_repro_bundle.py

echo
echo "ALL TEST SUITES PASSED"
