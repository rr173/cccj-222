#!/usr/bin/env python3
"""字段历史归档与查询演练。

运行：python3 -I tests/test_field_history.py

覆盖：
  A. 手算小例子逐区间核对（预期值直接写在测试里，不经过任何产品代码）；
  B. 独立参考实现 oracle/history_oracle.py（栈式配对 + 顺序重放，不调用产品
     归档/查询逻辑）在 行×字段×窗口×截点 矩阵上与产品查询逐区间对拍；
  C. 真实订阅清理删除原始记录后重开数据库：相同查询结果不变；已归档的
     事务标识/位置区间重用被拒绝；归档水位不受清理影响；
  D. 未启用的库保持原订阅行为；非空库拒绝启用；启用后拒绝换源；
  E. CLI 端到端（真实独立进程）：enable-history / field-history / 错误码。
"""
from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "oracle"))

from ccs import source_store, consumer_state, cleanup, field_history  # noqa: E402
import history_oracle  # noqa: E402  独立模型，绝不 import ccs

FAILS: list[str] = []
PASSES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        PASSES.append(name)
    else:
        FAILS.append(f"{name} :: {detail}")


# --------------------------------------------------------------------------- #
# 演练数据：12 笔事务，T6 故意迟到（位置 600 在 T7 之后到达）
# arrival_seq = 下标 + 1：T1..T5,T7 = 1..6，T6 = 7，T8..T12 = 8..12
# --------------------------------------------------------------------------- #
def build_segments() -> list[dict]:
    return [
        {"txn_id": "T1", "ops": [
            {"source_position": 100, "row_id": "r1", "op": "INSERT",
             "before": None, "after": {"v": 1, "tag": "a"}},
            {"source_position": 101, "row_id": "r2", "op": "INSERT",
             "before": None, "after": {"v": 10}},
        ]},
        {"txn_id": "T2", "ops": [
            {"source_position": 200, "row_id": "r1", "op": "UPDATE",
             "before": {"v": 1}, "after": {"v": 2}},
            {"source_position": 201, "row_id": "r1", "op": "UPDATE",
             "before": {"v": 2}, "after": {"v": 3, "extra": None}},
        ]},
        {"txn_id": "T3", "ops": [
            {"source_position": 300, "row_id": "r1", "op": "UPDATE",
             "before": {"v": 3}, "after": {"v": 3}},          # 同值换来源
        ]},
        {"txn_id": "T4", "ops": [
            {"source_position": 400, "row_id": "r1", "op": "DELETE",
             "before": {"v": 3}, "after": None},
            {"source_position": 401, "row_id": "r1", "op": "INSERT",
             "before": None, "after": {"tag": "b"}},          # 删除后重插，不继承 v
        ]},
        {"txn_id": "T5", "ops": [
            {"source_position": 500, "row_id": "r1", "op": "UPDATE",
             "before_missing": True, "after": {"v": None}},   # 显式空值
        ]},
        {"txn_id": "T7", "ops": [
            {"source_position": 700, "row_id": "r1", "op": "UPDATE",
             "before": {"v": None}, "after": {"v": 7}},
        ]},
        {"txn_id": "T6", "ops": [                              # 迟到：位置 600
            {"source_position": 600, "row_id": "r1", "op": "UPDATE",
             "before_missing": True, "after": {"v": 6}},
        ]},
        {"txn_id": "T8", "ops": [
            {"source_position": 800, "row_id": "r9", "op": "UPDATE",
             "before_missing": True, "after": {"v": 9}},      # 孤儿更新：未知生命周期
        ]},
        {"txn_id": "T9", "ops": [
            {"source_position": 900, "row_id": "r2", "op": "DELETE",
             "before": {"v": 10}, "after": None},
            {"source_position": 901, "row_id": "r1", "op": "INSERT",
             "before": None, "after": {"v": 10, "tag": "c"}},
        ]},
        {"txn_id": "T10", "ops": [                             # 事务内完全抵消
            {"source_position": 1000, "row_id": "r5", "op": "INSERT",
             "before": None, "after": {"v": 5}},
            {"source_position": 1001, "row_id": "r5", "op": "UPDATE",
             "before": {"v": 5}, "after": {"v": 6}},
            {"source_position": 1002, "row_id": "r5", "op": "DELETE",
             "before": {"v": 6}, "after": None},
        ]},
        {"txn_id": "T11", "ops": [                             # 删除后同事务更新：不恢复
            {"source_position": 1100, "row_id": "r1", "op": "DELETE",
             "before": {"v": 10}, "after": None},
            {"source_position": 1101, "row_id": "r1", "op": "UPDATE",
             "before_missing": True, "after": {"v": 99}},
        ]},
        {"txn_id": "T12", "ops": [
            {"source_position": 1200, "row_id": "r1", "op": "INSERT",
             "before": None, "after": {"v": 12}},
        ]},
    ]


WATERMARK = 12  # 12 笔事务全部归档后的到达水位


# --------------------------------------------------------------------------- #
# 手算预期构造辅助
# --------------------------------------------------------------------------- #
def iv(s, e, row_state, lifecycle=None, field=None):
    return {"start": s, "end": e, "row_state": row_state,
            "lifecycle": lifecycle, "field": field}


def known(value, txn, pos):
    return {"state": "known", "value": value,
            "source": {"txn_id": txn, "op_pos": pos}}


UNK = {"state": "unknown"}
L100 = {"insert_pos": 100}
L101 = {"insert_pos": 101}
L401 = {"insert_pos": 401}
L901 = {"insert_pos": 901}
L1200 = {"insert_pos": 1200}


def hand_computed_cases() -> list[tuple]:
    """(名称, row, field, start, end, cutoff, 预期区间列表)。"""
    return [
        # 1. 基础窗口：窗首承接此前状态；T3 同值换来源留下分界
        ("same-value-new-source keeps boundary",
         "r1", "v", 150, 350, 12, [
             iv(150, 201, "exists", L100, known(1, "T1", 100)),
             iv(201, 300, "exists", L100, known(3, "T2", 201)),
             iv(300, 350, "exists", L100, known(3, "T3", 300)),
         ]),
        # 2. 事务内中间值不泄漏：T2 的 v=2@200 永不出现
        ("intra-txn intermediate value never leaks",
         "r1", "v", 199, 202, 12, [
             iv(199, 201, "exists", L100, known(1, "T1", 100)),
             iv(201, 202, "exists", L100, known(3, "T2", 201)),
         ]),
        # 3. 窗口切到事务内部：看不到该事务的任何写入
        ("window inside txn sees nothing of it",
         "r1", "v", 200, 201, 12, [
             iv(200, 201, "exists", L100, known(1, "T1", 100)),
         ]),
        # 4. 迟到前截点 K=6：T6 不可见
        ("cutoff before late arrival",
         "r1", "v", 550, 750, 6, [
             iv(550, 700, "exists", L401, known(None, "T5", 500)),
             iv(700, 750, "exists", L401, known(7, "T7", 700)),
         ]),
        # 5. 迟到后截点 K=7：同一窗口答案改变，K=6 的答案不变
        ("cutoff after late arrival rewrites same window",
         "r1", "v", 550, 750, 7, [
             iv(550, 600, "exists", L401, known(None, "T5", 500)),
             iv(600, 700, "exists", L401, known(6, "T6", 600)),
             iv(700, 750, "exists", L401, known(7, "T7", 700)),
         ]),
        # 6. 删除后重插：同一事务内 DELETE+INSERT 在 end_pos=401 原子生效，
        #    中间的已删除状态不可见；重插不继承旧字段 v（未知），
        #    新生命周期以插入操作位置标识
        ("delete then reinsert drops old field and provenance",
         "r1", "v", 350, 450, 12, [
             iv(350, 401, "exists", L100, known(3, "T3", 300)),
             iv(401, 450, "exists", L401, UNK),
         ]),
        # 7. 显式 JSON null 是已知值（与 unknown 区分）
        ("explicit json null is a known value",
         "r1", "v", 450, 550, 12, [
             iv(450, 500, "exists", L401, UNK),
             iv(500, 550, "exists", L401, known(None, "T5", 500)),
         ]),
        # 8. 孤儿更新：行从 unknown 变 exists，生命周期未知，不推测插入
        ("orphan update implies existence with unknown lifecycle",
         "r9", "v", 750, 850, 12, [
             iv(750, 800, "unknown"),
             iv(800, 850, "exists", "unknown", known(9, "T8", 800)),
         ]),
        # 9. 事务内 INSERT..DELETE 完全抵消：不留任何痕迹
        ("cancelled txn leaves no trace",
         "r5", "v", 0, 2000, 12, [
             iv(0, 2000, "unknown"),
         ]),
        # 10. 删除期间同事务的更新不能恢复该行；删除效果在 end_pos=1101 生效；
        #     下一次插入开启新生命周期
        ("update during delete does not resurrect",
         "r1", "v", 1050, 1250, 12, [
             iv(1050, 1101, "exists", L901, known(10, "T9", 901)),
             iv(1101, 1200, "deleted"),
             iv(1200, 1250, "exists", L1200, known(12, "T12", 1200)),
         ]),
        # 11. K=0：尚未见过任何事务
        ("cutoff zero knows nothing",
         "r1", "v", 0, 1300, 0, [
             iv(0, 1300, "unknown"),
         ]),
        # 12. 另一行：删除后保持 deleted（窗口右端之外也不复活）
        ("plain delete persists",
         "r2", "v", 50, 1000, 12, [
             iv(50, 101, "unknown"),
             iv(101, 901, "exists", L101, known(10, "T1", 101)),
             iv(901, 1000, "deleted"),
         ]),
        # 13. 字段维度：extra 只在 T2 写过一次；重插后回到未知；
        #     相邻"同为未知"区间因生命周期不同仍留分界。
        #     T1 的效果在 end_pos=101 才生效，故 [100,101) 仍是 unknown。
        ("field-level unknown spans keep lifecycle boundaries",
         "r1", "extra", 100, 1300, 12, [
             iv(100, 101, "unknown"),
             iv(101, 201, "exists", L100, UNK),
             iv(201, 401, "exists", L100, known(None, "T2", 201)),
             iv(401, 901, "exists", L401, UNK),
             iv(901, 1101, "exists", L901, UNK),
             iv(1101, 1200, "deleted"),
             iv(1200, 1300, "exists", L1200, UNK),
         ]),
        # 14. tag 字段跨多次 v 更新保持不变（无写不分界）
        ("untouched field shows no boundary at other writes",
         "r1", "tag", 100, 1300, 12, [
             iv(100, 101, "unknown"),
             iv(101, 401, "exists", L100, known("a", "T1", 100)),
             iv(401, 901, "exists", L401, known("b", "T4", 401)),
             iv(901, 1101, "exists", L901, known("c", "T9", 901)),
             iv(1101, 1200, "deleted"),
             iv(1200, 1300, "exists", L1200, UNK),
         ]),
        # 15. 同一窗口在 K=5（T7 未到）与 K=6（T7 已到）答案不同
        ("normal arrival also shifts cutoff answers",
         "r1", "v", 650, 750, 5, [
             iv(650, 750, "exists", L401, known(None, "T5", 500)),
         ]),
    ]


def ingest_all(conn, segments):
    arrivals = {}
    for seg in segments:
        r = source_store.ingest_segment(conn, seg)
        arrivals[seg["txn_id"]] = r["arrival_seq"]
    return arrivals


def part_a_hand_computed(conn, segments) -> None:
    print("== Part A: 手算小例子逐区间核对 ==")
    for name, row, field, start, end, cutoff, expect in hand_computed_cases():
        got = field_history.query_field_history(conn, row, field, start, end, cutoff)
        check(f"A: {name}", got["intervals"] == expect,
              f"\n  expect={json.dumps(expect, ensure_ascii=False)}"
              f"\n  actual={json.dumps(got['intervals'], ensure_ascii=False)}")
        check(f"A: {name} :: seamless cover",
              [iv_["start"] for iv_ in got["intervals"]][0] == start
              and [iv_["end"] for iv_ in got["intervals"]][-1] == end
              and all(got["intervals"][i]["end"] == got["intervals"][i + 1]["start"]
                      for i in range(len(got["intervals"]) - 1)),
              json.dumps(got["intervals"]))


def part_b_oracle_matrix(conn, segments) -> None:
    print("== Part B: 独立参考实现矩阵对拍 ==")
    rng = random.Random(20261122)
    rows = ["r1", "r2", "r5", "r9", "rZ"]
    fields = ["v", "tag", "extra", "nope"]
    crafted_windows = [
        (0, 1300), (100, 101), (99, 100), (201, 300), (200, 202), (400, 401),
        (401, 500), (500, 600), (600, 700), (700, 701), (901, 1100),
        (1100, 1200), (1200, 1201), (1299, 1400),
    ]
    random_windows = []
    for _ in range(30):
        s = rng.randint(0, 1299)
        random_windows.append((s, s + rng.randint(1, 400)))

    compared = 0
    for row in rows:
        for field in fields:
            for (s, e) in crafted_windows:
                for k in range(0, WATERMARK + 1):
                    expect = history_oracle.field_history(segments, row, field, s, e, k)
                    got = field_history.query_field_history(
                        conn, row, field, s, e, k)["intervals"]
                    if got != expect:
                        check(f"B: oracle match {row}.{field} [{s},{e}) K={k}",
                              False,
                              f"\n  oracle={json.dumps(expect, ensure_ascii=False)}"
                              f"\n  actual={json.dumps(got, ensure_ascii=False)}")
                        return
                    compared += 1
            for (s, e) in random_windows:
                for k in (0, 3, 5, 6, 7, 9, 12):
                    expect = history_oracle.field_history(segments, row, field, s, e, k)
                    got = field_history.query_field_history(
                        conn, row, field, s, e, k)["intervals"]
                    if got != expect:
                        check(f"B: oracle match {row}.{field} [{s},{e}) K={k}",
                              False,
                              f"\n  oracle={json.dumps(expect, ensure_ascii=False)}"
                              f"\n  actual={json.dumps(got, ensure_ascii=False)}")
                        return
                    compared += 1
    check(f"B: oracle matrix agrees on {compared} queries", True, "")


def part_c_cleanup_reopen(tmp: str, segments) -> None:
    print("== Part C: 真实清理删除原记录 -> 重开数据库 -> 结果不变、范围不可复用 ==")
    db = os.path.join(tmp, "hist_cleanup.db")
    conn = source_store.connect(db)
    field_history.enable_history(conn)
    source_store.begin_source(conn, "hist-src")
    ingest_all(conn, segments)

    # 记录清理前的查询结果（覆盖全部手算窗口 + 每个截点）
    probe_queries = [(r, f, s, e, k)
                     for (s, e) in [(100, 1300), (150, 350), (550, 750),
                                    (350, 450), (1050, 1250), (0, 2000)]
                     for r, f in [("r1", "v"), ("r1", "extra"), ("r2", "v"),
                                  ("r9", "v"), ("r5", "v")]
                     for k in (0, 5, 6, 7, 12)]
    before = {}
    for (r, f, s, e, k) in probe_queries:
        before[(r, f, s, e, k)] = field_history.query_field_history(
            conn, r, f, s, e, k)["intervals"]

    # 真实订阅流：注册 -> 拉取全部 -> 确认 -> 清理（retention=1 只留最后一笔）
    consumer_state.register(conn, "gamma", retention_txns=1, pull_limit=20,
                            start_at_zero=True)
    pull = consumer_state.pull(conn, "gamma")
    check("C: gamma reserved all 12 txns", len(pull["deliveries"]) == 12,
          str(len(pull["deliveries"])))
    consumer_state.ack(conn, "gamma", pull["token"])
    report = cleanup.run_cleanup(conn)
    deleted_ids = {x["txn_id"] for x in report["deleted"]}
    check("C: cleanup deleted T1..T11 (retention keeps only last)",
          deleted_ids == {f"T{i}" for i in range(1, 12)}, str(sorted(deleted_ids)))
    left_txn = {r["txn_id"] for r in conn.execute("SELECT txn_id FROM txn").fetchall()}
    left_rows = conn.execute("SELECT COUNT(*) AS c FROM change_row").fetchone()["c"]
    check("C: subscription data physically gone (only T12 remains)",
          left_txn == {"T12"} and left_rows == 1, f"{left_txn} rows={left_rows}")
    hist_n = conn.execute("SELECT COUNT(*) AS c FROM hist_txn").fetchone()["c"]
    check("C: archive history survives cleanup", hist_n == 12, str(hist_n))
    conn.close()

    # 重开数据库：相同查询必须得到同一结果
    conn = source_store.connect(db)
    mismatches = []
    for key, expect in before.items():
        r, f, s, e, k = key
        got = field_history.query_field_history(conn, r, f, s, e, k)["intervals"]
        if got != expect:
            mismatches.append((key, expect, got))
    check("C: same queries after cleanup+reopen return identical results",
          not mismatches,
          "" if not mismatches else json.dumps(mismatches[0], ensure_ascii=False))
    # 清理后水位不变：K=12 仍合法，K=13 仍越界
    try:
        field_history.query_field_history(conn, "r1", "v", 0, 10, 13)
        check("C: cutoff above archived watermark rejected", False, "no error")
    except source_store.StoreError as exc:
        check("C: cutoff above archived watermark rejected",
              exc.code == "CUTOFF_BEYOND_WATERMARK", exc.code)

    # 已归档的事务标识 / 位置区间不得复用
    try:
        source_store.ingest_segment(conn, {"txn_id": "T1", "ops": [
            {"source_position": 100, "row_id": "rX", "op": "INSERT",
             "before": None, "after": {"v": 0}}]})
        check("C: archived txn_id reuse rejected", False, "no error")
    except source_store.StoreError as exc:
        check("C: archived txn_id reuse rejected", exc.code == "DUP_TXN", exc.code)
    try:
        source_store.ingest_segment(conn, {"txn_id": "T1B", "ops": [
            {"source_position": 101, "row_id": "rX", "op": "INSERT",
             "before": None, "after": {"v": 0}}]})
        check("C: archived position range reuse rejected", False, "no error")
    except source_store.StoreError as exc:
        check("C: archived position range reuse rejected",
              exc.code == "POSITION_OVERLAP", exc.code)
    # 新位置可以继续摄入，归档随之增长，旧截点答案不变
    r = source_store.ingest_segment(conn, {"txn_id": "T13", "ops": [
        {"source_position": 1300, "row_id": "r1", "op": "UPDATE",
         "before": {"v": 12}, "after": {"v": 13}}]})
    check("C: fresh range still ingests after cleanup", r["arrival_seq"] == 13,
          json.dumps(r))
    got = field_history.query_field_history(conn, "r1", "v", 1250, 1350, 13)
    check("C: new txn extends history at new cutoff",
          got["intervals"] == [
              iv(1250, 1300, "exists", L1200, known(12, "T12", 1200)),
              iv(1300, 1350, "exists", L1200, known(13, "T13", 1300)),
          ], json.dumps(got["intervals"], ensure_ascii=False))
    got_old = field_history.query_field_history(conn, "r1", "v", 1250, 1350, 12)
    check("C: original cutoff answer unchanged by later arrival",
          got_old["intervals"] == [
              iv(1250, 1350, "exists", L1200, known(12, "T12", 1200)),
          ], json.dumps(got_old["intervals"], ensure_ascii=False))
    conn.close()


def part_d_disabled_db(tmp: str, segments) -> None:
    print("== Part D: 未启用的库保持原订阅行为；非空库拒绝启用；启用后拒绝换源 ==")
    db = os.path.join(tmp, "disabled.db")
    conn = source_store.connect(db)
    source_store.begin_source(conn, "plain-src")
    source_store.ingest_segment(conn, segments[0])
    # 未启用：查询报 HISTORY_NOT_ENABLED，且库里连归档表都没有
    try:
        field_history.query_field_history(conn, "r1", "v", 0, 200, 1)
        check("D: query on disabled db rejected", False, "no error")
    except source_store.StoreError as exc:
        check("D: query on disabled db rejected",
              exc.code == "HISTORY_NOT_ENABLED", exc.code)
    has_tables = conn.execute(
        "SELECT COUNT(*) AS c FROM sqlite_master WHERE type='table' "
        "AND name LIKE 'hist_%'").fetchone()["c"]
    check("D: disabled db carries no archive tables", has_tables == 0, str(has_tables))
    # 非空库拒绝启用（不回填）
    try:
        field_history.enable_history(conn)
        check("D: enable on non-empty db rejected", False, "no error")
    except source_store.StoreError as exc:
        check("D: enable on non-empty db rejected",
              exc.code == "HISTORY_REQUIRES_EMPTY_DB", exc.code)
    # 原订阅行为不变：清理后同事务标识/位置可以复用（无归档约束）
    consumer_state.register(conn, "solo", retention_txns=0, pull_limit=5,
                            start_at_zero=True)
    p = consumer_state.pull(conn, "solo")
    consumer_state.ack(conn, "solo", p["token"])
    cleanup.run_cleanup(conn)
    gone = conn.execute("SELECT COUNT(*) AS c FROM txn").fetchone()["c"] == 0
    re = source_store.ingest_segment(conn, segments[0])  # 同样的 T1 再进一次
    check("D: disabled db keeps original behavior (reuse after cleanup allowed)",
          gone and re["txn_id"] == "T1", json.dumps(re))
    conn.close()

    # 启用后拒绝换源（单源实例）
    db2 = os.path.join(tmp, "single_source.db")
    conn = source_store.connect(db2)
    field_history.enable_history(conn)
    source_store.begin_source(conn, "only-src")
    try:
        source_store.switch_source(conn, "other-src")
        check("D: switch-source refused while history enabled", False, "no error")
    except source_store.StoreError as exc:
        check("D: switch-source refused while history enabled",
              exc.code == "HISTORY_SINGLE_SOURCE", exc.code)
    again = field_history.enable_history(conn)
    check("D: enable-history is idempotent", again["already_enabled"] is True,
          json.dumps(again))
    conn.close()


def run_cli(db: str, args: list[str]) -> tuple[int, dict]:
    cmd = [os.path.join(ROOT, "bin", "ccs"), "--db", db, *args]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        payload = {"parse_error": proc.stdout[:200], "stderr": proc.stderr[:200]}
    return proc.returncode, payload


def part_e_cli(tmp: str, segments) -> None:
    print("== Part E: CLI 端到端（真实独立进程） ==")
    seg_dir = os.path.join(tmp, "cli_segments")
    os.makedirs(seg_dir, exist_ok=True)
    for seg in segments[:3]:
        with open(os.path.join(seg_dir, f"{seg['txn_id']}.json"), "w",
                  encoding="utf-8") as fh:
            json.dump(seg, fh)

    db = os.path.join(tmp, "cli.db")
    rc, out = run_cli(db, ["enable-history"])
    check("E: cli enable-history", rc == 0 and out["data"]["enabled"] is True,
          json.dumps(out))
    run_cli(db, ["begin-source", "cli-src"])
    for t in ("T1", "T2", "T3"):
        rc, out = run_cli(db, ["ingest", os.path.join(seg_dir, f"{t}.json")])
        if rc != 0:
            check(f"E: cli ingest {t}", False, json.dumps(out))
    rc, out = run_cli(db, ["field-history", "r1", "v", "150", "350", "--cutoff", "2"])
    check("E: cli field-history output",
          rc == 0 and out["data"]["intervals"] == [
              iv(150, 201, "exists", L100, known(1, "T1", 100)),
              iv(201, 350, "exists", L100, known(3, "T2", 201)),
          ] and out["data"]["arrival_watermark"] == 3,
          json.dumps(out, ensure_ascii=False))
    rc, out = run_cli(db, ["field-history", "r1", "v", "150", "350", "--cutoff", "99"])
    check("E: cli cutoff beyond watermark -> exit 2",
          rc == 2 and out["error"]["code"] == "CUTOFF_BEYOND_WATERMARK",
          f"rc={rc} {json.dumps(out)}")
    rc, out = run_cli(db, ["field-history", "r1", "v", "350", "150", "--cutoff", "1"])
    check("E: cli bad window -> exit 2",
          rc == 2 and out["error"]["code"] == "BAD_WINDOW", f"rc={rc} {json.dumps(out)}")
    rc, out = run_cli(db, ["field-history", "r1", "v", "0", "10", "--cutoff", "-1"])
    check("E: cli negative cutoff -> exit 2",
          rc == 2 and out["error"]["code"] == "BAD_CUTOFF", f"rc={rc} {json.dumps(out)}")

    db2 = os.path.join(tmp, "cli_disabled.db")
    run_cli(db2, ["begin-source", "x"])
    rc, out = run_cli(db2, ["field-history", "r1", "v", "0", "10", "--cutoff", "0"])
    check("E: cli query on disabled db -> exit 2",
          rc == 2 and out["error"]["code"] == "HISTORY_NOT_ENABLED",
          f"rc={rc} {json.dumps(out)}")
    run_cli(db2, ["ingest", os.path.join(seg_dir, "T1.json")])
    rc, out = run_cli(db2, ["enable-history"])
    check("E: cli enable on non-empty db -> exit 2",
          rc == 2 and out["error"]["code"] == "HISTORY_REQUIRES_EMPTY_DB",
          f"rc={rc} {json.dumps(out)}")


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="ccs_hist_")
    segments = build_segments()

    # Part A/B 共用一个启用归档的库
    db = os.path.join(tmp, "hist.db")
    conn = source_store.connect(db)
    field_history.enable_history(conn)
    source_store.begin_source(conn, "hist-src")
    arrivals = ingest_all(conn, segments)
    check("arrival_seq assigned in ingest order",
          [arrivals[s["txn_id"]] for s in segments] == list(range(1, 13)),
          json.dumps(arrivals))
    part_a_hand_computed(conn, segments)
    part_b_oracle_matrix(conn, segments)
    conn.close()

    part_c_cleanup_reopen(tmp, segments)
    part_d_disabled_db(tmp, segments)
    part_e_cli(tmp, segments)

    print(f"\nPASS {len(PASSES)} checks")
    if FAILS:
        print(f"FAIL {len(FAILS)} checks:")
        for f in FAILS:
            print("  -", f)
        return 1
    print("field-history archive/query semantics verified against hand-computed "
          "cases and the independent oracle; cleanup/reopen invariants hold.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
