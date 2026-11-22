#!/usr/bin/env bash
# 端到端演练：从空目录复跑全部场景，输出保存到 demo/output/。
# 用法：bash demo/run_demo.sh
# 所有服务调用都是独立进程、python -I 隔离模式；失败时脚本立即停止并保留现场。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CCS="$ROOT/bin/ccs"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/ccs_demo.XXXXXX")"
DB="$WORK/svc.db"
OUT="$ROOT/demo/output"
SEG="$ROOT/demo/segments"

rm -rf "$OUT"
mkdir -p "$OUT"
echo "== 工作目录: $WORK (输出归档: $OUT) =="

run() {
  # 统一打印命令 + 保存 JSON 输出；服务返回非零（预期的拒绝场景）不杀脚本
  local name="$1"; shift
  echo "+ ccs $*" | tee "$OUT/$name.cmd"
  set +e
  "$CCS" --db "$DB" "$@" > "$OUT/$name.json" 2> "$OUT/$name.err"
  local rc=$?
  set -e
  echo "$rc" > "$OUT/$name.rc"
  echo "  exit=$rc -> $OUT/$name.json"
  return 0
}

# 0. 资料 + 空库
python3 -I "$ROOT/demo/make_fixtures.py" "$SEG" >/dev/null
run 00_init init
run 01_begin begin-source lake-v1

# 1. 注册两个消费者（alpha 要前镜像/小窗口，beta 不要前镜像/大窗口）
run 02_reg_alpha register alpha --retention 3 --pull-limit 2 --need-before --start-at-zero
run 03_reg_beta  register beta  --retention 8 --pull-limit 3

# 2. 按提交顺序摄入（T6 故意迟到）
i=10
for t in T1 T2 T3 T4 T5 T7; do
  run "${i}_ingest_$t" ingest "$SEG/$t.json"; i=$((i+1))
done

# 3. 两个消费者不同速度：alpha 拉两批并确认；beta 只拉一批
run 20_alpha_pull1 pull alpha
TOK_A1=$(python3 -I -c "import json;print(json.load(open('$OUT/20_alpha_pull1.json'))['data']['token'])")
run 21_alpha_pull1_repeat pull alpha --token "$TOK_A1"
run 22_alpha_ack1 ack alpha "$TOK_A1"
run 23_alpha_ack1_dup ack alpha "$TOK_A1"          # 重复确认，返回同一结果
run 24_beta_pull1 pull beta
TOK_B1=$(python3 -I -c "import json;print(json.load(open('$OUT/24_beta_pull1.json'))['data']['token'])")
run 25_beta_ack1 ack beta "$TOK_B1"

# 4. 跳跃确认必须被拒绝（退出码 2）
run 26_jump_ack jump-ack alpha 9

# 5. alpha 继续：T3 含缺前镜像更新，T4 含删除后同身份重新插入
run 27_alpha_pull2 pull alpha
TOK_A2=$(python3 -I -c "import json;print(json.load(open('$OUT/27_alpha_pull2.json'))['data']['token'])")
run 28_alpha_ack2 ack alpha "$TOK_A2"

# 6. alpha 推进到 T7 之后，迟到的 T6（位置 600）才到达
run 29_alpha_pull3 pull alpha
TOK_A3=$(python3 -I -c "import json;print(json.load(open('$OUT/29_alpha_pull3.json'))['data']['token'])")
run 30_alpha_ack3 ack alpha "$TOK_A3"
run 31_ingest_late_T6 ingest "$SEG/T6.json"
run 32_pull_late pull-late alpha
run 33_ack_late ack-late alpha T6
run 34_ack_late_dup ack-late alpha T6

# 7. 改参数 / 换源：旧令牌必须被拒
run 35_alpha_pull4 pull alpha
TOK_A4=$(python3 -I -c "import json;print(json.load(open('$OUT/35_alpha_pull4.json'))['data']['token'])")
run 36_update_params update-params alpha --pull-limit 5
run 37_old_token_after_params ack alpha "$TOK_A4"
run 38_switch_source switch-source lake-v2
run 39_pull_after_switch pull alpha
run 40_switch_back switch-source lake-v1

# 8. 剩余事务摄入；撤销 beta 不影响 alpha
run 41_ingest_T8 ingest "$SEG/T8.json"
run 42_ingest_T9 ingest "$SEG/T9.json"
run 43_revoke_beta revoke beta
run 44_alpha_pull5 pull alpha

# 9. 重放窗口（窗口内成功 / 越界拒绝）
run 45_replay_ok replay alpha 400 502
run 46_replay_bad replay alpha 1 50

# 10. 清理：先看计划（含依据），再执行，再看审计
run 47_cleanup_plan cleanup-plan
run 48_cleanup_run cleanup-run
run 49_cleanup_log cleanup-log

# 11. 整体状态
run 50_state state

# 12. 失败结果展示：上面 26/37/39/46 预期退出码 2
echo
echo "== 预期被拒绝的操作（退出码应为 2） =="
for name in 26_jump_ack 37_old_token_after_params 39_pull_after_switch 46_replay_bad; do
  printf "  %s: rc=%s  code=%s\n" "$name" "$(cat "$OUT/$name.rc")" \
    "$(python3 -I -c "import json;print(json.load(open('$OUT/$name.json'))['error']['code'])" 2>/dev/null || echo PARSE_FAIL)"
done

cp "$DB" "$OUT/svc.db.snapshot"
echo
echo "== 演练完成，数据库快照: $OUT/svc.db.snapshot =="
