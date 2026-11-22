#!/usr/bin/env python3
"""确定性演练：用独立小模型 oracle/fold_oracle.py 逐笔对拍产品实现。

运行：python3 -I tests/test_deterministic.py
不依赖 pytest，只用标准库；每个场景失败都打印 DIFF 并以非零退出码结束。
"""
from __future__ import annotations

import json
import os
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "oracle"))

from ccs import source_store, consumer_state, cleanup  # noqa: E402
import fold_oracle  # noqa: E402  独立模型，绝不 import ccs

DEMO_DIR = os.path.join(ROOT, "demo", "segments")
INGEST_ORDER = ["T1", "T2", "T3", "T4", "T5", "T7", "T6", "T8", "T9"]

FAILS: list[str] = []
PASSES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        PASSES.append(name)
    else:
        FAILS.append(f"{name} :: {detail}")


def load_segments() -> list[dict]:
    segs = []
    for txn_id in INGEST_ORDER:
        with open(os.path.join(DEMO_DIR, f"{txn_id}.json"), encoding="utf-8") as fh:
            segs.append(json.load(fh))
    return segs


def norm_records(records: list[dict]) -> list:
    """归一化净变化记录，便于和 oracle 逐字段对拍（dict 按键排序消除键序差异）。"""
    out = []
    for r in records:
        out.append((
            r["row_id"], r["generation"], r["net_op"],
            r["position_min"], r["position_max"],
            tuple(r["source_positions"]), tuple(r["folded_from_ops"]),
            json.dumps(r["before"], sort_keys=True),
            tuple(sorted(r["before_field_unknown"])), r["before_all_unknown"],
            json.dumps(r["after"], sort_keys=True),
        ))
    return sorted(out)


def main() -> int:
    segments = load_segments()
    seg_by_id = {s["txn_id"]: s for s in segments}
    commit_seq_of = {}

    tmp = tempfile.mkdtemp(prefix="ccs_det_")
    db = os.path.join(tmp, "svc.db")
    conn = source_store.connect(db)
    source_store.begin_source(conn, "src-A")

    # 两个消费者：alpha 要前镜像、窗口小；beta 不要前镜像、窗口大、起点高
    consumer_state.register(conn, "alpha", retention_txns=3, pull_limit=2,
                            need_before=True, start_at_zero=True)
    # beta 在 T3 摄入后注册（稍后做），先占位摄入 T1..T3
    for txn_id in ["T1", "T2", "T3"]:
        r = source_store.ingest_segment(conn, seg_by_id[txn_id])
        commit_seq_of[txn_id] = r["commit_seq"]
    consumer_state.register(conn, "beta", retention_txns=8, pull_limit=3,
                            need_before=False)  # 默认从最新完整事务之后开始
    beta_reg_seq = 3

    for txn_id in ["T4", "T5", "T7", "T6", "T8", "T9"]:
        r = source_store.ingest_segment(conn, seg_by_id[txn_id])
        commit_seq_of[txn_id] = r["commit_seq"]

    # 按 commit_seq 排序的段顺序（oracle 输入）
    ordered_segs = sorted(segments, key=lambda s: commit_seq_of[s["txn_id"]])

    # ---------------------------------------------------------------- #
    # 场景 1-4：逐事务折叠对拍 oracle（独立模型）
    # ---------------------------------------------------------------- #
    for seg in ordered_segs:
        txn_id = seg["txn_id"]
        txn_index = ordered_segs.index(seg)
        expected = fold_oracle.fold_segment(ordered_segs, txn_index)
        actual = consumer_state.fold_txn_from_db(conn, txn_id)
        check(
            f"fold[{txn_id}] matches independent oracle",
            norm_records(actual) == norm_records(expected),
            f"actual={json.dumps(actual, ensure_ascii=False)} "
            f"oracle={json.dumps(expected, ensure_ascii=False)}",
        )

    # 关键语义断言（人读得懂的业务预期）
    t2 = norm_records(consumer_state.fold_txn_from_db(conn, "T2"))
    check("T2 insert->update->delete cancels to zero net change", t2 == [], str(t2))

    t3 = {r["row_id"]: r for r in consumer_state.fold_txn_from_db(conn, "T3")}
    check("T3 r5 folded into one INSERT with unknown field w",
          t3["r5"]["net_op"] == "INSERT"
          and t3["r5"]["before_field_unknown"] == ["w"]
          and t3["r5"]["after"] == {"v": 3, "k": "z", "w": 99},
          json.dumps(t3["r5"], ensure_ascii=False))

    t4 = consumer_state.fold_txn_from_db(conn, "T4")
    gens = {(r["row_id"], r["generation"]): r["net_op"] for r in t4}
    check("T4 delete then reinsert keeps two generations of identity",
          gens.get(("r3", 1)) == "DELETE" and gens.get(("r3", 2)) == "INSERT", str(gens))

    t5 = {r["row_id"]: r for r in consumer_state.fold_txn_from_db(conn, "T5")}
    check("T5 r1 two updates fold to one UPDATE, never merged across txns",
          t5["r1"]["net_op"] == "UPDATE" and t5["r1"]["source_positions"] == [500, 501],
          json.dumps(t5["r1"], ensure_ascii=False))

    # ---------------------------------------------------------------- #
    # 场景 5：两个消费者不同速度；pull 上限控制内存/批量
    # ---------------------------------------------------------------- #
    pull1 = consumer_state.pull(conn, "alpha")
    check("alpha pull_limit=2 reserves exactly 2 txns",
          [d["txn_id"] for d in pull1["deliveries"]] == ["T1", "T2"]
          and pull1["reserved_high_commit_seq"] == 2,
          json.dumps([d["txn_id"] for d in pull1["deliveries"]]))

    # 与 oracle 的消费者级预期对拍
    exp_alpha = fold_oracle.ExpectedConsumer("alpha", 3, 2, True)
    exp_pull1 = exp_alpha.expected_pull(ordered_segs, commit_seq_of)
    actual_norm = [norm_records(d["records"]) for d in pull1["deliveries"]]
    expect_norm = [norm_records(d["records"]) for d in exp_pull1]
    check("alpha pull#1 record-by-record equals oracle", actual_norm == expect_norm,
          f"actual={actual_norm} expect={expect_norm}")

    # beta 起点在 seq3 之后，第一次 pull 应从 T4(seq4) 开始
    bpull1 = consumer_state.pull(conn, "beta")
    beta_first = [d["txn_id"] for d in bpull1["deliveries"]]
    check("beta starts after registration point (no historical T1..T3)",
          beta_first == ["T4", "T5", "T7"], str(beta_first))
    # beta need_before=False：前镜像被抑制，但未知标记结构保留
    for d in bpull1["deliveries"]:
        for r in d["records"]:
            check(f"beta before-image suppressed in {d['txn_id']}/{r['row_id']}",
                  r["before"] is None and r["before_suppressed"] is True
                  and "before_field_unknown" in r,
                  json.dumps(r, ensure_ascii=False))

    # ---------------------------------------------------------------- #
    # 场景 6：重复拉取不推进前沿；重复确认返回同一结果
    # ---------------------------------------------------------------- #
    frontier_before = consumer_state.status(conn, "alpha")["frontier_commit_seq"]
    replay_pull = consumer_state.pull(conn, "alpha", pull1["token"])
    check("repeated pull with same token is idempotent replay",
          replay_pull["idempotent_replay"] is True
          and [d["txn_id"] for d in replay_pull["deliveries"]] == ["T1", "T2"],
          "")
    check("repeated pull does not advance frontier",
          consumer_state.status(conn, "alpha")["frontier_commit_seq"] == frontier_before, "")

    ack1 = consumer_state.ack(conn, "alpha", pull1["token"])
    ack1_dup = consumer_state.ack(conn, "alpha", pull1["token"])
    check("ack advances contiguous frontier 0->2",
          ack1["advanced_from_seq"] == 0 and ack1["advanced_to_seq"] == 2
          and ack1["duplicate_ack"] is False, json.dumps(ack1))
    check("duplicate ack returns same result, no double advance",
          ack1_dup["duplicate_ack"] is True
          and ack1_dup["advanced_to_seq"] == 2
          and consumer_state.status(conn, "alpha")["frontier_commit_seq"] == 2,
          json.dumps(ack1_dup))

    # ---------------------------------------------------------------- #
    # 场景 7：跳跃确认必须拒绝
    # ---------------------------------------------------------------- #
    try:
        consumer_state.jump_ack_probe(conn, "alpha", 6)
        check("jump ack rejected", False, "no exception raised")
    except source_store.StoreError as exc:
        check("jump ack rejected with JUMP_ACK", exc.code == "JUMP_ACK", exc.code)

    # 旧令牌（已消费）再拉取也要拒绝
    try:
        consumer_state.pull(conn, "alpha", pull1["token"])
        check("consumed token rejected on pull", False, "no exception")
    except source_store.StoreError as exc:
        check("consumed token rejected on pull", exc.code == "TOKEN_CONSUMED", exc.code)

    # ---------------------------------------------------------------- #
    # 场景 8：缺前镜像更新只交付未知标记
    # ---------------------------------------------------------------- #
    # alpha 继续拉 T3,T4（pull_limit=2）
    p2 = consumer_state.pull(conn, "alpha")
    t3_delivery = next(d for d in p2["deliveries"] if d["txn_id"] == "T3")
    r5 = next(r for r in t3_delivery["records"] if r["row_id"] == "r5")
    check("missing before-image yields explicit unknown marker, never fabricated old value",
          r5["before_field_unknown"] == ["w"] and r5["before_all_unknown"] is False
          and "w" not in r5["before"], json.dumps(r5, ensure_ascii=False))
    consumer_state.ack(conn, "alpha", p2["token"])

    # ---------------------------------------------------------------- #
    # 场景 9：迟到位置落在已确认范围 -> 旁路补发，不改写快照、不推进前沿
    # ---------------------------------------------------------------- #
    # alpha 确认到 seq4(T4)。T6(seq7, pos600) 在注册后到达且 end_pos=600，
    # 但 alpha 前沿在 401，600 > 401，故摄入时未通知。先把 alpha 推到 T7(seq6,end701)。
    p3 = consumer_state.pull(conn, "alpha")  # T5,T7
    consumer_state.ack(conn, "alpha", p3["token"])
    # T6 已经在摄入序列里（seq7），当时 alpha 前沿低；手工构造一个"确认后迟到"：
    # 直接调用通知函数模拟 T6 在当前前沿下被重新判定（幂等 INSERT OR IGNORE）
    notified = consumer_state.notify_late_after_ingest(conn, "T6")
    check("late T6 notified once frontier passes position 600",
          "alpha" in notified, str(notified))
    late = consumer_state.pull_late(conn, "alpha")
    check("late delivery keeps frontier unchanged",
          late["frontier_commit_seq"] == 6 and len(late["late_deliveries"]) == 1
          and late["late_deliveries"][0]["txn_id"] == "T6"
          and late["late_deliveries"][0]["late"] is True,
          json.dumps(late, ensure_ascii=False))
    late_r1 = late["late_deliveries"][0]["records"][0]
    check("late record carries unknown-marker semantics",
          late_r1["before_all_unknown"] is True
          and set(late_r1["before_field_unknown"]) == {"v", "late"},
          json.dumps(late_r1, ensure_ascii=False))
    ack_late1 = consumer_state.ack_late(conn, "alpha", "T6")
    ack_late2 = consumer_state.ack_late(conn, "alpha", "T6")
    check("late ack idempotent and frontier untouched",
          ack_late1["already_acked"] is False and ack_late2["already_acked"] is True
          and consumer_state.status(conn, "alpha")["frontier_commit_seq"] == 6,
          "")

    # ---------------------------------------------------------------- #
    # 场景 10：令牌绑定源版本/前沿/参数；换源或改参数拒绝旧令牌
    # ---------------------------------------------------------------- #
    p4 = consumer_state.pull(conn, "alpha")  # 新令牌，预留 T6(seq7),T8(seq8)
    consumer_state.update_params(conn, "alpha", pull_limit=5)
    try:
        consumer_state.ack(conn, "alpha", p4["token"])
        check("old token rejected after param change", False, "no exception")
    except source_store.StoreError as exc:
        check("old token rejected after param change",
              exc.code == "TOKEN_PARAMS_STALE", exc.code)

    source_store.switch_source(conn, "src-B")
    try:
        fresh = consumer_state.pull(conn, "alpha")
        check("pull under switched source rejected", False, json.dumps(fresh))
    except source_store.StoreError as exc:
        check("pull under switched source rejected", exc.code == "SOURCE_SWITCHED", exc.code)
    source_store.switch_source(conn, "src-A")  # 切回，供后续清理测试

    # ---------------------------------------------------------------- #
    # 场景 11：撤销一个消费者不影响另一个；重新注册前沿重置
    # ---------------------------------------------------------------- #
    # beta 还在 seq3 起点，先让它确认 T4..T9 中的一批
    bpull2 = consumer_state.pull(conn, "beta")
    consumer_state.ack(conn, "beta", bpull2["token"])
    beta_frontier = consumer_state.status(conn, "beta")["frontier_commit_seq"]
    consumer_state.revoke(conn, "beta")
    # alpha 仍可正常拉取
    alpha_ok = consumer_state.pull(conn, "alpha")
    check("alpha unaffected by beta revoke",
          any(d["txn_id"] == "T6" for d in alpha_ok["deliveries"]),
          json.dumps([d["txn_id"] for d in alpha_ok["deliveries"]]))
    # 重新注册 beta：全新前沿，但保留窗口给得很大，模拟慢消费者钉住历史
    consumer_state.register(conn, "beta", retention_txns=20, pull_limit=3, need_before=False)
    beta_new = consumer_state.status(conn, "beta")
    check("re-registered beta resets to latest frontier, old state discarded",
          beta_new["frontier_commit_seq"] == 9 and beta_new["late_open"] == 0,
          json.dumps(beta_new))

    # ---------------------------------------------------------------- #
    # 场景 12：清理只删全员不需要且不在迟到保留内的事务，并给依据
    # ---------------------------------------------------------------- #
    # alpha: frontier 6, retention 3 -> floor 4
    # beta : 重注册在 seq9，retention 20 -> floor 10..9 裁剪到 reg+1=10（无历史保护）
    plan = cleanup.retention_plan(conn)
    alpha_prot = plan["per_consumer"]["alpha"]["protected_from_seq"]
    check("alpha retention floor computed correctly", alpha_prot == 4, str(alpha_prot))
    deletable_ids = {x["txn_id"] for x in plan["deletable"]}
    check("cleanup candidate is exactly T1..T3 (below slowest floor 4)",
          deletable_ids == {"T1", "T2", "T3"}, str(deletable_ids))
    # T6 迟到通知已 ACK，不再钉住
    check("acked late txn not pinned",
          "T6" not in plan["late_pinned_txn_ids"], str(plan["late_pinned_txn_ids"]))

    # 制造一个未 ACK 迟到通知，验证它能钉住事务（T6 当前受 alpha floor 保护，
    # 这里用另一个位置更老的事务 T4：手工登记通知，验证 pin 优先于窗口裁决）
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "INSERT OR IGNORE INTO late_notice(consumer, txn_id, status) VALUES ('alpha','T4','PENDING')"
    )
    conn.execute("COMMIT")
    plan2 = cleanup.retention_plan(conn)
    check("open late notice pins T4 against cleanup",
          "T4" in plan2["late_pinned_txn_ids"]
          and all(x["txn_id"] != "T4" for x in plan2["deletable"]),
          json.dumps(plan2["late_pinned_txn_ids"]))
    conn.execute("DELETE FROM late_notice WHERE consumer='alpha' AND txn_id='T4'")

    before_txns = {r["txn_id"] for r in conn.execute("SELECT txn_id FROM txn").fetchall()}
    report = cleanup.run_cleanup(conn)
    after_txns = {r["txn_id"] for r in conn.execute("SELECT txn_id FROM txn").fetchall()}
    check("cleanup deletes exactly planned txns with reasons",
          before_txns - after_txns == {"T1", "T2", "T3"}
          and all(x["reason"] for x in report["deleted"]),
          json.dumps(report, ensure_ascii=False))
    log = cleanup.cleanup_log(conn)
    check("cleanup decision persisted in audit log",
          {e["deleted_txn_id"] for e in log} == {"T1", "T2", "T3"}, str(log))
    # alpha 的保留窗口内数据仍可重放
    rp = consumer_state.replay_window(conn, "alpha", 400, 401)
    check("replay still works within retention after cleanup",
          [d["txn_id"] for d in rp["deliveries"]] == ["T4"],
          json.dumps([d["txn_id"] for d in rp["deliveries"]]))

    # ---------------------------------------------------------------- #
    # 场景 13：重放窗口越界拒绝；窗口缩短不立即删数据
    # ---------------------------------------------------------------- #
    try:
        consumer_state.replay_window(conn, "alpha", 100, 200)  # T1 已删且低于 floor
        check("replay below retention rejected", False, "no exception")
    except source_store.StoreError as exc:
        check("replay below retention rejected",
              exc.code == "WINDOW_OUTSIDE_RETENTION", exc.code)
    consumer_state.update_params(conn, "alpha", retention_txns=1)
    # 缩短窗口本身不删除；下次清理的候选变大，但当前数据还在
    still_there = {r["txn_id"] for r in conn.execute("SELECT txn_id FROM txn").fetchall()}
    check("shortening retention does not delete data by itself",
          "T4" in still_there and "T5" in still_there, str(still_there))
    plan3 = cleanup.retention_plan(conn)
    check("shortened window raises alpha cleanup floor",
          plan3["per_consumer"]["alpha"]["protected_from_seq"] == 6,
          json.dumps(plan3["per_consumer"]))

    conn.close()

    print(f"PASS {len(PASSES)} checks")
    if FAILS:
        print(f"\nFAIL {len(FAILS)} checks:")
        for f in FAILS:
            print("  -", f)
        return 1
    print("all deterministic scenarios match the independent oracle.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
