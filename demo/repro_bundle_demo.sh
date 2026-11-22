#!/usr/bin/env bash
# 可复现事务包演示：从空库启用归档、摄入、导出、清理后重开再导出、
# 最后把包内每行按顺序摄入到另一个空库。所有服务调用都是独立进程。
# 用法：bash demo/repro_bundle_demo.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CCS="$ROOT/bin/ccs"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/ccs_repro_demo.XXXXXX")"
SRC="$WORK/src.db"
DST="$WORK/dst.db"
OUT="$WORK/out"
mkdir -p "$OUT"
echo "== 工作目录: $WORK =="

# 1. 启用（必须在首次摄入前、空库上）
"$CCS" --db "$SRC" enable-repro
"$CCS" --db "$SRC" begin-source lake-repro

# 2. 自带的小日志：甲/乙/丙/丁/戊 的 X/B/A 依赖链
python3 -I - "$OUT" <<'PY'
import json, os, sys
out = sys.argv[1]
segs = [
    {"txn_id": "甲", "ops": [
        {"source_position": 100, "row_id": "X", "op": "INSERT",
         "before": None, "after": {"v": 1}}]},
    {"txn_id": "乙", "ops": [
        {"source_position": 200, "row_id": "X", "op": "UPDATE",
         "before": {"v": 1}, "after": {"v": 2}},
        {"source_position": 201, "row_id": "B", "op": "INSERT",
         "before": None, "after": {"v": 20}}]},
    {"txn_id": "丙", "ops": [
        {"source_position": 300, "row_id": "X", "op": "UPDATE",
         "before": {"v": 2}, "after": {"v": 3}}]},
    {"txn_id": "丁", "ops": [
        {"source_position": 400, "row_id": "B", "op": "UPDATE",
         "before": {"v": 20}, "after": {"v": 21}},
        {"source_position": 401, "row_id": "A", "op": "INSERT",
         "before": None, "after": {"v": 40}}]},
    {"txn_id": "戊", "ops": [
        {"source_position": 500, "row_id": "A", "op": "UPDATE",
         "before": {"v": 40}, "after": {"v": 41}}]},
]
for s in segs:
    with open(os.path.join(out, s["txn_id"] + ".json"), "w", encoding="utf-8") as fh:
        json.dump(s, fh, ensure_ascii=False)
PY

for t in 甲 乙 丙 丁 戊; do
  "$CCS" --db "$SRC" ingest "$OUT/$t.json" >/dev/null
done
echo "+ 已摄入 5 笔事务，到达水位 5"

# 3. 导出：目标 戊，截点 5（乙可见）-> 应为 甲、乙、丁、戊，不含 丙
"$CCS" --db "$SRC" export-bundle 戊 --cutoff 5 "$OUT/bundle.jsonl"
echo "--- bundle (K=5) ---"
python3 -I - "$OUT/bundle.jsonl" <<'PY'
import json, sys
lines = [json.loads(l) for l in open(sys.argv[1], encoding="utf-8") if l.strip()]
print("header:", lines[0])
print("txns:", [s["txn_id"] for s in lines[1:]])
PY

# 4. 真实订阅清理 -> 重开，仍用原截点导出，字节必须一致
"$CCS" --db "$SRC" register gamma --retention 0 --pull-limit 10 --start-at-zero >/dev/null
TOK=$("$CCS" --db "$SRC" pull gamma | python3 -I -c \
  'import json,sys; print(json.load(sys.stdin)["data"]["token"])')
"$CCS" --db "$SRC" ack gamma "$TOK" >/dev/null
"$CCS" --db "$SRC" cleanup-run >/dev/null
"$CCS" --db "$SRC" export-bundle 戊 --cutoff 5 "$OUT/bundle_after_cleanup.jsonl"
cmp "$OUT/bundle.jsonl" "$OUT/bundle_after_cleanup.jsonl"
echo "+ 清理订阅数据后重开库，原截点导出的包字节一致"

# 5. 空库按输出顺序逐段真实摄入
"$CCS" --db "$DST" begin-source lake-repro >/dev/null
i=0
while IFS= read -r segment; do
  printf '%s\n' "$segment" > "$OUT/seg.json"
  "$CCS" --db "$DST" ingest "$OUT/seg.json" >/dev/null
  i=$((i+1))
done < <(tail -n +2 "$OUT/bundle.jsonl")
echo "+ 空库按顺序摄入 $i 笔事务"
"$CCS" --db "$DST" state | python3 -I -c \
  'import json,sys; d=json.load(sys.stdin)["data"]; print("dst state:", [(t["txn_id"], t["position_range"]) for t in d["transactions"]])'

echo "== 演练完成：$OUT/bundle.jsonl =="
