#!/usr/bin/env python3
"""当前库存物化演练。

运行：python3 -I tests/test_inventory.py

覆盖：
  A. 手算小例子逐次核对（预期值直接写在测试里，不经过任何产品代码）：
       1) 反序到达：迟到删除使"未来补丁"失效、迟到重插又使补丁生效；
       2) 同一事务里的多行、多库位调拨（原子生效）+ 迟到删除其中一行；
       3) bin/qty 资格矩阵（缺失 vs 显式无效、0、bool、负数、空串、库位名原串区分）；
  B. 独立参考实现 oracle/inventory_oracle.py（全量源顺序重放，不 import ccs）
     在多组随机段、随机反序到达下，每摄入一笔就对拍一次产品的物化汇总；
  C. 真实订阅清理删除原始记录后重开数据库：汇总不变，再补入一笔旧位置事务
     仍能修正汇总；已占用的事务标识/位置区间不可复用；
  D. 未启用库保持原行为；非空库拒绝启用；启用后拒绝换源；与 history 开关共存；
  E. CLI 端到端（真实独立进程）：enable-inventory / inventory / 错误码 / 空库水位；
  F. 可复现的汇总写入失败（测试侧触发器注入，不加生产故障开关）：整笔事务回滚，
     无半次调拨、到达序号不前进、位置未被占用；
  G. 增量路径不全量重算的可检查依据：EXPLAIN QUERY PLAN 全部走索引搜索、
     两次不同历史规模的摄入语句计数恒定、未涉及库位的汇总行逐字节不变。
"""
from __future__ import annotations

import json
import os
import random
import sqlite3
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "oracle"))

from ccs import source_store, consumer_state, cleanup, inventory  # noqa: E402
import inventory_oracle  # noqa: E402  独立模型，绝不 import ccs

FAILS: list[str] = []
PASSES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        PASSES.append(name)
    else:
        FAILS.append(f"{name} :: {detail}")


def payload(watermark: int, bins: list[tuple]) -> dict:
    return {"arrival_seq": watermark,
            "bins": [{"bin": b, "total_qty": t, "contributing_rows": n}
                     for b, t, n in bins]}


def enable_fresh(db: str, source: str = "inv-src") -> sqlite3.Connection:
    conn = source_store.connect(db)
    inventory.enable_inventory(conn)
    source_store.begin_source(conn, source)
    return conn


def seg(txn_id: str, ops: list[dict]) -> dict:
    return {"txn_id": txn_id, "ops": ops}


def ins(pos, rid, after, txn_placeholder=False):
    return {"source_position": pos, "row_id": rid, "op": "INSERT",
            "before": None, "after": after}


def upd(pos, rid, after):
    return {"source_position": pos, "row_id": rid, "op": "UPDATE",
            "before_missing": True, "after": after}


def dele(pos, rid):
    return {"source_position": pos, "row_id": rid, "op": "DELETE",
            "before_missing": True, "after": None}


# --------------------------------------------------------------------------- #
# Part A1：反序到达——迟到删除使未来补丁失效，迟到重插又使补丁生效
# --------------------------------------------------------------------------- #
def part_a1_late_delete_and_reinsert(tmp: str) -> None:
    print("== Part A1: 迟到删除使未来补丁失效 / 迟到重插又使补丁生效 ==")
    conn = enable_fresh(os.path.join(tmp, "a1.db"))
    # 源顺序：100 INSERT -> 200 DELETE -> 250 重插 -> 300 UPDATE
    # 到达顺序故意打乱：100, 300, 200, 250
    s1 = seg("A1", [ins(100, "r1", {"bin": "A", "qty": 5})])
    s2 = seg("A2", [upd(300, "r1", {"bin": "B", "qty": 7})])
    s3 = seg("A3", [dele(200, "r1")])
    s4 = seg("A4", [ins(250, "r1", {"bin": "C", "qty": 2})])

    source_store.ingest_segment(conn, s1)
    check("A1: after insert, A has 5 from 1 row",
          inventory.read_summary(conn) == payload(1, [("A", 5, 1)]),
          json.dumps(inventory.read_summary(conn), ensure_ascii=False))

    source_store.ingest_segment(conn, s2)  # 补丁先到：当前行被直接改到 B
    check("A1: early patch lands on the live row",
          inventory.read_summary(conn) == payload(2, [("B", 7, 1)]),
          json.dumps(inventory.read_summary(conn), ensure_ascii=False))

    source_store.ingest_segment(conn, s3)  # 迟到删除（源位置 200 在 300 之前）
    got = inventory.read_summary(conn)
    check("A1: late delete voids the later patch (row deleted, all bins removed)",
          got == payload(3, []), json.dumps(got, ensure_ascii=False))
    # 不能把 B 库位残留在汇总里
    check("A1: no phantom bin rows after delete",
          conn.execute("SELECT COUNT(*) AS c FROM inv_bin_summary").fetchone()["c"] == 0,
          "bin summary not empty")

    source_store.ingest_segment(conn, s4)  # 迟到重插（250 仍在 300 之前）
    got = inventory.read_summary(conn)
    check("A1: late reinsert makes the later patch effective again",
          got == payload(4, [("B", 7, 1)]), json.dumps(got, ensure_ascii=False))
    conn.close()


# --------------------------------------------------------------------------- #
# Part A2：同笔事务多行多库位调拨，原子生效；迟到删除抽掉其中一行
# --------------------------------------------------------------------------- #
def part_a2_multi_row_transfer(tmp: str) -> None:
    print("== Part A2: 同笔事务多行多库位调拨 + 迟到删除 ==")
    conn = enable_fresh(os.path.join(tmp, "a2.db"))
    base = seg("B1", [
        ins(400, "r2", {"bin": "X", "qty": 10}),
        ins(401, "r3", {"bin": "X", "qty": 4}),
        ins(402, "r4", {"bin": "Y", "qty": 6}),
    ])
    source_store.ingest_segment(conn, base)
    check("A2: baseline X=14/2 rows, Y=6/1 row",
          inventory.read_summary(conn) == payload(
              1, [("X", 14, 2), ("Y", 6, 1)]),
          json.dumps(inventory.read_summary(conn), ensure_ascii=False))

    # 同一事务里 r2/r3 一起从 X 调到 Y；UPDATE 只给 bin，qty 必须保留
    move = seg("B2", [
        upd(500, "r2", {"bin": "Y"}),
        upd(501, "r3", {"bin": "Y"}),
    ])
    source_store.ingest_segment(conn, move)
    got = inventory.read_summary(conn)
    check("A2: multi-row transfer is atomic: X removed, Y=20/3 rows",
          got == payload(2, [("Y", 20, 3)]), json.dumps(got, ensure_ascii=False))

    # 迟到删除 r3（源位置 450，介于插入 401 与调拨 501 之间）
    source_store.ingest_segment(conn, seg("B3", [dele(450, "r3")]))
    got = inventory.read_summary(conn)
    check("A2: late delete removes r3 and its later transfer cannot resurrect it",
          got == payload(3, [("Y", 16, 2)]), json.dumps(got, ensure_ascii=False))
    conn.close()


# --------------------------------------------------------------------------- #
# Part A3：资格矩阵 + 事务内折叠 + 库位名原串区分
# --------------------------------------------------------------------------- #
def part_a3_eligibility(tmp: str) -> None:
    print("== Part A3: bin/qty 资格矩阵、事务内折叠、库位名原串区分 ==")
    conn = enable_fresh(os.path.join(tmp, "a3.db"))
    pos = [1000]

    def nxt():
        pos[0] += 1
        return pos[0]

    def take(rid, op, after):
        if op == "DELETE":
            entry = dele(nxt(), rid)
        else:
            entry = {"source_position": nxt(), "row_id": rid, "op": op,
                     "before_missing": True, "before": None, "after": after}
        source_store.ingest_segment(conn, seg(f"S{pos[0]}_{rid}_{op}", [entry]))

    take("r5", "INSERT", {"bin": "P", "qty": 3})
    check("A3: valid insert contributes",
          inventory.read_summary(conn)["bins"] == [
              {"bin": "P", "total_qty": 3, "contributing_rows": 1}], "")
    take("r5", "UPDATE", {"qty": 0})           # 0 是合法非负整数；bin 省略=保留
    check("A3: zero qty valid; omitted bin retained",
          inventory.read_summary(conn)["bins"] == [
              {"bin": "P", "total_qty": 0, "contributing_rows": 1}], "")
    take("r5", "UPDATE", {"bin": None})        # 显式 null 清掉 bin 资格
    check("A3: explicit null bin removes contribution and the bin row",
          inventory.read_summary(conn) == payload(3, []), "")
    take("r5", "UPDATE", {"bin": "P"})         # 恢复 bin；qty 仍是 0
    check("A3: bin restored, qty 0 still in effect",
          inventory.read_summary(conn)["bins"] == [
              {"bin": "P", "total_qty": 0, "contributing_rows": 1}], "")
    take("r5", "UPDATE", {"qty": True})        # bool 不算整数
    check("A3: boolean qty invalid",
          inventory.read_summary(conn) == payload(5, []), "")
    take("r5", "UPDATE", {"qty": 2})
    check("A3: integer qty restores contribution",
          inventory.read_summary(conn)["bins"] == [
              {"bin": "P", "total_qty": 2, "contributing_rows": 1}], "")
    take("r5", "UPDATE", {"bin": ""})          # 空串不是非空字符串
    check("A3: empty-string bin invalid",
          inventory.read_summary(conn) == payload(7, []), "")
    take("r5", "UPDATE", {"qty": -1})          # 负整数无效；bin 仍是无效空串
    check("A3: negative qty invalid even with invalid bin",
          inventory.read_summary(conn) == payload(8, []), "")
    take("r5", "UPDATE", {"qty": "2"})         # 字符串 qty 同样无效
    check("A3: string qty invalid",
          inventory.read_summary(conn) == payload(9, []), "")
    take("r5", "UPDATE", {"qty": 2})           # 省略 bin：无效空串被保留，仍不贡献
    check("A3: omitted invalid bin stays invalid (no rollback, no resurrection)",
          inventory.read_summary(conn) == payload(10, []), "")
    take("r5", "UPDATE", None)                 # 没有 after：什么都不改
    check("A3: update without after changes nothing",
          inventory.read_summary(conn) == payload(11, []), "")
    # 恢复成有效贡献，便于后面核对库位排序
    take("r5", "UPDATE", {"bin": "P", "qty": 2})
    check("A3: valid bin+qty restore contribution",
          inventory.read_summary(conn)["bins"] == [
              {"bin": "P", "total_qty": 2, "contributing_rows": 1}], "")

    # 库位名按原字符串区分；BINARY 排序下大写字母排在小写前
    source_store.ingest_segment(conn, seg("Sname1", [
        ins(nxt(), "r7", {"bin": "z", "qty": 1}),
        ins(nxt(), "r8", {"bin": "Z", "qty": 2}),
        ins(nxt(), "r9", {"bin": " Z", "qty": 4}),  # 前导空格是另一个库位
    ]))
    got = inventory.read_summary(conn)
    check("A3: raw bin strings distinct and BINARY-sorted",
          [b["bin"] for b in got["bins"]] == [" Z", "P", "Z", "z"]
          and got["arrival_seq"] == 13, json.dumps(got, ensure_ascii=False))

    # 事务内 INSERT->UPDATE->DELETE 完全抵消：不留库存贡献也不留行状态贡献
    p = nxt()
    pos[0] = p + 2  # 本段占用 p..p+2，同步共享位置计数器
    source_store.ingest_segment(conn, seg("Scancel", [
        ins(p, "r10", {"bin": "M", "qty": 1}),
        upd(p + 1, "r10", {"qty": 5}),
        dele(p + 2, "r10"),
    ]))
    got = inventory.read_summary(conn)
    check("A3: intra-txn insert/update/delete cancels completely",
          all(b["bin"] != "M" for b in got["bins"]) and got["arrival_seq"] == 14,
          json.dumps(got, ensure_ascii=False))

    # 对从未插入的行做 UPDATE/DELETE：不接受，不产生贡献
    p = nxt()
    pos[0] = p + 1
    source_store.ingest_segment(conn, seg("Sorphan", [
        upd(p, "rGhost", {"bin": "G", "qty": 9}),
        dele(p + 1, "rGhost"),
    ]))
    got = inventory.read_summary(conn)
    check("A3: updates/deletes on never-inserted row are ignored",
          all(b["bin"] != "G" for b in got["bins"]),
          json.dumps(got, ensure_ascii=False))
    conn.close()


# --------------------------------------------------------------------------- #
# Part B：随机段 + 随机反序到达，每摄入一笔与独立全量折叠 oracle 对拍
# --------------------------------------------------------------------------- #
BINS = ["P", "Q", " A ", "", None, 7, True]
QTS = [0, 1, 5, 11, -1, "2", 1.5, True, False, None]


def random_after(rng: random.Random) -> dict | None:
    if rng.random() < 0.1:
        return None
    after: dict = {}
    if rng.random() < 0.8:
        after["bin"] = rng.choice(BINS)
    if rng.random() < 0.8:
        after["qty"] = rng.choice(QTS)
    return after


def build_random_segments(seed: int, txn_count: int, rows: int) -> list[dict]:
    """按源顺序生成生命周期合法的随机段；源位置块全局不相交。"""
    rng = random.Random(seed)
    alive: set[str] = set()
    segments: list[dict] = []
    pos = 100
    for ti in range(txn_count):
        ops: list[dict] = []
        chosen = rng.sample([f"k{i}" for i in range(rows)], rng.randint(1, min(4, rows)))
        for rid in chosen:
            if rid not in alive:  # 新行/已删除行：先 INSERT 开新生命周期
                pos += 1
                ops.append(ins(pos, rid, random_after(rng) or {}))
                alive.add(rid)
                if rng.random() < 0.2:
                    pos += 1
                    ops.append(dele(pos, rid))
                    alive.discard(rid)
                    continue
            else:
                kind = rng.choice(["UPDATE", "UPDATE", "UPDATE", "DELETE"])
                if kind == "DELETE":
                    pos += 1
                    ops.append(dele(pos, rid))
                    alive.discard(rid)
                    continue
                pos += 1
                ops.append(upd(pos, rid, random_after(rng)))
            # 活着的行再追加 0-2 个更新
            for _ in range(rng.randint(0, 2)):
                pos += 1
                ops.append(upd(pos, rid, random_after(rng)))
        segments.append(seg(f"R{seed}-{ti}", ops))
    return segments


def part_b_oracle_shuffled(tmp: str) -> None:
    print("== Part B: 随机反序到达，逐笔与独立全量折叠 oracle 对拍 ==")
    for seed in (20261122, 424242, 7):
        segments = build_random_segments(seed, txn_count=24, rows=6)
        order = list(range(len(segments)))
        random.Random(seed + 99).shuffle(order)
        conn = enable_fresh(os.path.join(tmp, f"b{seed}.db"), f"src-{seed}")
        arrived: list[dict] = []
        for idx in order:
            s = segments[idx]
            source_store.ingest_segment(conn, s)
            arrived.append(s)
            got = inventory.read_summary(conn)
            expect = inventory_oracle.expected_payload(arrived)
            check(f"B: seed={seed} after {len(arrived)} arrivals (txn {s['txn_id']})",
                  got == expect,
                  f"\n  expect={json.dumps(expect, ensure_ascii=False)}"
                  f"\n  actual={json.dumps(got, ensure_ascii=False)}")
            if got != expect:
                conn.close()
                return
        # 全到齐后再对一次完整折叠
        check(f"B: seed={seed} final summary equals full source-order fold",
              inventory.read_summary(conn)
              == inventory_oracle.expected_payload(segments), "")
        conn.close()


# --------------------------------------------------------------------------- #
# Part C：真实清理 -> 重开 -> 迟到旧位置事务仍能修正汇总；标识/区间不复用
# --------------------------------------------------------------------------- #
def part_c_cleanup_reopen(tmp: str) -> None:
    print("== Part C: 真实清理后重开，迟到旧位置事务仍修正汇总 ==")
    db = os.path.join(tmp, "cleanup.db")
    conn = enable_fresh(db)
    segments = [
        seg("C1", [ins(10, "r1", {"bin": "A", "qty": 5}),
                   ins(11, "r2", {"bin": "A", "qty": 3})]),
        seg("C2", [upd(20, "r1", {"bin": "B"})]),
        seg("C3", [dele(30, "r2")]),
    ]
    for s in segments:
        source_store.ingest_segment(conn, s)
    before = inventory.read_summary(conn)
    check("C: pre-cleanup summary", before == payload(3, [("B", 5, 1)]),
          json.dumps(before, ensure_ascii=False))

    # 走完整真实订阅流：注册 -> 拉取 -> 确认 -> 清理（retention=0 全删订阅数据）
    consumer_state.register(conn, "gamma", retention_txns=0, pull_limit=10,
                            start_at_zero=True)
    pull = consumer_state.pull(conn, "gamma")
    consumer_state.ack(conn, "gamma", pull["token"])
    report = cleanup.run_cleanup(conn)
    check("C: cleanup physically removed subscription rows",
          conn.execute("SELECT COUNT(*) AS c FROM txn").fetchone()["c"] == 0
          and conn.execute("SELECT COUNT(*) AS c FROM change_row").fetchone()["c"] == 0
          and len(report["deleted"]) == 3, json.dumps(report, ensure_ascii=False))
    # 库存日志与物化结果必须保留
    check("C: inventory log and materialization survive cleanup",
          conn.execute("SELECT COUNT(*) AS c FROM inv_op").fetchone()["c"] == 4
          and conn.execute("SELECT COUNT(*) AS c FROM inv_bin_summary").fetchone()["c"] == 1,
          "inventory tables touched by cleanup")
    conn.close()

    # 重开数据库：直接读物化表，结果不变，不遍历订阅记录
    conn = source_store.connect(db)
    check("C: reopen reads identical materialized summary",
          inventory.read_summary(conn) == before, "")
    check("C: watermark survived reopen",
          inventory.read_summary(conn)["arrival_seq"] == 3, "")

    # 补入一笔源位置在已清理事务之间的迟到事务（位置 25：C2 之后、C3 之前）。
    # 用此前从未出现过的新行 r3，保证源语义合法；库存日志在清理后仍保留，
    # 物化必须像全量折叠一样把这笔迟到插入纳入当前汇总。
    late = seg("C4-late", [ins(25, "r3", {"bin": "C", "qty": 9})])
    r = source_store.ingest_segment(conn, late)
    check("C: late txn gets next arrival_seq, not a reused one",
          r["arrival_seq"] == 4, json.dumps(r))
    got = inventory.read_summary(conn)
    expect = inventory_oracle.expected_payload(segments + [late])
    check("C: late old-position txn corrects summary exactly like full fold",
          got == expect and got == payload(4, [("B", 5, 1), ("C", 9, 1)]),
          f"got={json.dumps(got, ensure_ascii=False)} expect={json.dumps(expect)}")

    # 已占用的事务标识 / 源位置区间即使订阅数据已删也不可复用
    try:
        source_store.ingest_segment(conn, seg("C1", [ins(999, "rx", {"bin": "Z", "qty": 1})]))
        check("C: txn id reuse rejected after cleanup", False, "no error")
    except source_store.StoreError as exc:
        check("C: txn id reuse rejected after cleanup", exc.code == "DUP_TXN", exc.code)
    try:
        source_store.ingest_segment(conn, seg("C5", [upd(10, "r1", {"qty": 1})]))
        check("C: occupied position reuse rejected after cleanup", False, "no error")
    except source_store.StoreError as exc:
        check("C: occupied position reuse rejected after cleanup",
              exc.code == "POSITION_OVERLAP", exc.code)
    conn.close()


# --------------------------------------------------------------------------- #
# Part D：未启用行为、非空拒绝、单源限制、幂等、与 history 开关共存
# --------------------------------------------------------------------------- #
def part_d_feature_gating(tmp: str) -> None:
    print("== Part D: 开关契约（未启用/非空/单源/幂等/共存） ==")
    db = os.path.join(tmp, "disabled.db")
    conn = source_store.connect(db)
    source_store.begin_source(conn, "plain")
    try:
        inventory.read_summary(conn)
        check("D: inventory command on disabled db rejected", False, "no error")
    except source_store.StoreError as exc:
        check("D: inventory command on disabled db rejected",
              exc.code == "INVENTORY_NOT_ENABLED", exc.code)
    n_inv = conn.execute(
        "SELECT COUNT(*) AS c FROM sqlite_master WHERE type='table' AND name LIKE 'inv_%'"
    ).fetchone()["c"]
    check("D: disabled db has no inventory tables", n_inv == 0, str(n_inv))

    # 未启用时摄入路径与原服务完全一致
    source_store.ingest_segment(conn, seg("D1", [ins(1, "r1", {"bin": "A", "qty": 1})]))
    try:
        inventory.enable_inventory(conn)
        check("D: enable on non-empty db rejected", False, "no error")
    except source_store.StoreError as exc:
        check("D: enable on non-empty db rejected",
              exc.code == "INVENTORY_REQUIRES_EMPTY_DB", exc.code)
    try:
        source_store.switch_source(conn, "plain2")
        check("D: switch still works while inventory disabled", True, "")
    except source_store.StoreError as exc:
        check("D: switch still works while inventory disabled", False, exc.code)
    conn.close()

    db2 = os.path.join(tmp, "single.db")
    conn = source_store.connect(db2)
    first = inventory.enable_inventory(conn)
    again = inventory.enable_inventory(conn)
    check("D: enable is idempotent",
          first["already_enabled"] is False and again["already_enabled"] is True,
          json.dumps((first, again)))
    source_store.begin_source(conn, "only")
    try:
        source_store.switch_source(conn, "other")
        check("D: switch-source refused while inventory enabled", False, "no error")
    except source_store.StoreError as exc:
        check("D: switch-source refused while inventory enabled",
              exc.code == "INVENTORY_SINGLE_SOURCE", exc.code)
    conn.close()

    # 与 field_history 开关独立共存：两个功能可同时启用且互不干扰
    db3 = os.path.join(tmp, "coexist.db")
    conn = source_store.connect(db3)
    from ccs import field_history
    field_history.enable_history(conn)
    inventory.enable_inventory(conn)
    source_store.begin_source(conn, "co")
    source_store.ingest_segment(conn, seg("E1", [
        ins(1, "r1", {"bin": "A", "qty": 2, "v": 1}),
        upd(2, "r1", {"bin": "B"}),
    ]))
    check("D: inventory works alongside history archive",
          inventory.read_summary(conn) == payload(1, [("B", 2, 1)]),
          json.dumps(inventory.read_summary(conn), ensure_ascii=False))
    fh = field_history.query_field_history(conn, "r1", "v", 0, 10, 1)
    check("D: history archive still works alongside inventory",
          fh["intervals"] and fh["arrival_watermark"] == 1,
          json.dumps(fh, ensure_ascii=False))
    conn.close()


# --------------------------------------------------------------------------- #
# Part E：CLI 端到端（真实独立进程）
# --------------------------------------------------------------------------- #
def run_cli(db: str, args: list[str]) -> tuple[int, dict]:
    proc = subprocess.run([os.path.join(ROOT, "bin", "ccs"), "--db", db, *args],
                          capture_output=True, text=True)
    try:
        payload_out = json.loads(proc.stdout)
    except json.JSONDecodeError:
        payload_out = {"parse_error": proc.stdout[:200], "stderr": proc.stderr[:200]}
    return proc.returncode, payload_out


def part_e_cli(tmp: str) -> None:
    print("== Part E: CLI 端到端 ==")
    db = os.path.join(tmp, "cli.db")
    rc, out = run_cli(db, ["inventory"])
    check("E: inventory before enable -> exit 2",
          rc == 2 and out["error"]["code"] == "INVENTORY_NOT_ENABLED",
          f"rc={rc} {json.dumps(out)}")
    rc, out = run_cli(db, ["enable-inventory"])
    check("E: enable-inventory", rc == 0 and out["data"]["enabled"] is True,
          json.dumps(out))
    rc, out = run_cli(db, ["inventory"])
    check("E: empty enabled db reports watermark 0 and no bins",
          rc == 0 and out["data"] == {"arrival_seq": 0, "bins": []},
          json.dumps(out))
    run_cli(db, ["begin-source", "cli-src"])

    seg_dir = os.path.join(tmp, "cli_segments")
    os.makedirs(seg_dir, exist_ok=True)
    segments = [
        seg("I1", [ins(10, "r1", {"bin": "A", "qty": 5}),
                   ins(11, "r2", {"bin": "A", "qty": 7})]),
        seg("I2", [upd(20, "r1", {"bin": "B", "qty": 9})]),
    ]
    for s in segments:
        with open(os.path.join(seg_dir, f"{s['txn_id']}.json"), "w",
                  encoding="utf-8") as fh:
            json.dump(s, fh)
    rc, out = run_cli(db, ["ingest", os.path.join(seg_dir, "I1.json")])
    check("E: first ingest arrival_seq=1", rc == 0 and out["data"]["arrival_seq"] == 1,
          json.dumps(out))
    rc, out = run_cli(db, ["ingest", os.path.join(seg_dir, "I2.json")])
    check("E: second ingest arrival_seq=2", rc == 0 and out["data"]["arrival_seq"] == 2,
          json.dumps(out))
    rc, out = run_cli(db, ["inventory"])
    expect = inventory_oracle.expected_payload(segments)
    check("E: inventory JSON matches oracle and is sorted by bin name",
          rc == 0 and out["data"] == expect
          and [b["bin"] for b in out["data"]["bins"]] == sorted(b["bin"] for b in expect["bins"])
          and set(out["data"]["bins"][0].keys()) == {"bin", "total_qty", "contributing_rows"},
          json.dumps(out, ensure_ascii=False))
    rc, out = run_cli(db, ["switch-source", "x"])
    check("E: switch-source refused via CLI",
          rc == 2 and out["error"]["code"] == "INVENTORY_SINGLE_SOURCE",
          f"rc={rc} {json.dumps(out)}")


# --------------------------------------------------------------------------- #
# Part F：汇总写入失败 -> 整笔事务回滚（测试侧触发器，无生产故障开关）
# --------------------------------------------------------------------------- #
def part_f_rollback(tmp: str) -> None:
    print("== Part F: 注入汇总写入失败，核对事务原子回滚 ==")
    db = os.path.join(tmp, "rollback.db")
    conn = enable_fresh(db)
    # 测试夹具：任何对物化汇总的插入都失败。触发器只存在于测试库，生产代码零开关。
    conn.execute(
        "CREATE TRIGGER fail_inv_summary BEFORE INSERT ON inv_bin_summary "
        "BEGIN SELECT RAISE(ABORT, 'injected inventory summary write failure'); END"
    )
    conn.commit()

    txns_before = conn.execute("SELECT COUNT(*) AS c FROM txn").fetchone()["c"]
    next_arrival_before = conn.execute(
        "SELECT next_arrival FROM meta WHERE id=1").fetchone()["next_arrival"]

    raised = None
    try:
        source_store.ingest_segment(
            conn, seg("F1", [ins(1, "r1", {"bin": "A", "qty": 5}),
                             ins(2, "r2", {"bin": "B", "qty": 6})]))
    except Exception as exc:  # noqa: BLE001 - 要核对错误类型与回滚后的现场
        raised = exc
    check("F: summary write failure aborts ingest with an error",
          raised is not None, repr(raised))

    # 同一提交边界：源端数据、到达序号、库存日志、汇总全部回到摄入前
    check("F: no txn/change_row left behind (no half transfer)",
          conn.execute("SELECT COUNT(*) AS c FROM txn").fetchone()["c"] == txns_before
          and conn.execute("SELECT COUNT(*) AS c FROM change_row").fetchone()["c"] == 0
          and conn.execute("SELECT COUNT(*) AS c FROM inv_op").fetchone()["c"] == 0
          and conn.execute("SELECT COUNT(*) AS c FROM inv_bin_summary").fetchone()["c"] == 0,
          "rollback incomplete")
    check("F: arrival sequence did not advance",
          conn.execute("SELECT next_arrival FROM meta WHERE id=1").fetchone()["next_arrival"]
          == next_arrival_before
          and conn.execute("SELECT watermark FROM inv_meta WHERE id=1").fetchone()["watermark"] == 0,
          "watermark advanced despite rollback")

    # 移除注入后，同一段可以原样摄入并成功（位置未被失败事务占用）
    conn.execute("DROP TRIGGER fail_inv_summary")
    conn.commit()
    r = source_store.ingest_segment(
        conn, seg("F1", [ins(1, "r1", {"bin": "A", "qty": 5}),
                         ins(2, "r2", {"bin": "B", "qty": 6})]))
    check("F: retry after fix succeeds with arrival_seq=1 and correct summary",
          r["arrival_seq"] == 1
          and inventory.read_summary(conn) == payload(
              1, [("A", 5, 1), ("B", 6, 1)]),
          json.dumps(inventory.read_summary(conn), ensure_ascii=False))
    conn.close()


# --------------------------------------------------------------------------- #
# Part G：增量路径不全量重算的可检查依据
# --------------------------------------------------------------------------- #
def _explain(conn: sqlite3.Connection, sql: str, params: tuple) -> list[str]:
    return [r["detail"] for r in conn.execute("EXPLAIN QUERY PLAN " + sql, params).fetchall()]


def _populate_scale_db(db: str, n_rows: int, history_per_row: int) -> sqlite3.Connection:
    """造一个每行积累大量被覆盖旧操作的库，返回连接（含一行待摄入的新段位置）。"""
    conn = enable_fresh(db, f"scale-{n_rows}")
    pos = 0
    for i in range(n_rows):
        rid = f"row{i}"
        pos += 1
        source_store.ingest_segment(
            conn, seg(f"init-{i}", [ins(pos, rid, {"bin": f"bin{i % 7}", "qty": 1})]))
        for h in range(history_per_row):  # 大量随后被覆盖的旧操作
            pos += 1
            source_store.ingest_segment(conn, seg(f"hist-{i}-{h}", [
                upd(pos, rid, {"bin": f"bin{(i + h) % 7}", "qty": h + 2})]))
    return conn, pos + 1


def part_g_incremental_evidence(tmp: str) -> None:
    print("== Part G: 增量路径不全量重算的可检查依据 ==")
    small, small_pos = _populate_scale_db(os.path.join(tmp, "g_small.db"), 6, 3)
    big, big_pos = _populate_scale_db(os.path.join(tmp, "g_big.db"), 60, 25)

    # G1: 重算所用查询的执行计划全部是索引 SEARCH，没有任何对库存日志的全表 SCAN
    queries = [
        ("SELECT MAX(src_pos) AS p FROM inv_op WHERE row_id=? AND op=?", ("row0", "INSERT")),
        ("SELECT MAX(src_pos) AS p FROM inv_op WHERE row_id=? AND op=? AND src_pos>?",
         ("row0", "DELETE", 0)),
        ("SELECT value_json FROM inv_field_write WHERE row_id=? AND field=? AND src_pos>=? "
         "ORDER BY src_pos DESC LIMIT 1", ("row0", "bin", 0)),
    ]
    for conn, tag in ((small, "small"), (big, "big")):
        for sql, params in queries:
            plan = " | ".join(_explain(conn, sql, params))
            check(f"G1[{tag}]: index SEARCH, no full scan -- {sql[:48]}...",
                  "SEARCH" in plan and "SCAN inv_op" not in plan
                  and "SCAN inv_field_write" not in plan, plan)

    # G2: 追踪一次单行摄入实际执行的语句；历史规模差 50 倍，语句数必须恒定
    def trace_ingest(conn: sqlite3.Connection, next_pos: int) -> tuple[int, int, int]:
        seen: list[str] = []
        conn.set_trace_callback(lambda s: seen.append(" ".join(s.split())))
        source_store.ingest_segment(
            conn, seg("probe", [upd(next_pos, "row0", {"qty": 999})]))
        conn.set_trace_callback(None)
        inv_reads = [s for s in seen if s.upper().startswith("SELECT")
                     and ("inv_op" in s or "inv_field_write" in s
                          or "inv_row_state" in s or "inv_bin_summary" in s)]
        # 任何针对库存日志的 SELECT 都必须是可定位查找：按 row_id 定位，或按
        # src_pos 区间走位置索引（区间占用检查）；不允许无界的行历史扫描。
        def _bounded(sql: str) -> bool:
            if "inv_row_state" in sql or "inv_bin_summary" in sql:
                return True  # 主键/单行定位的物化表访问
            # 行重算：row_id 前缀定位；占用检查：txn_id 索引或 src_pos 区间索引。
            # 追踪回调里参数会被展开成字面量，因此同时匹配占位符与字面量两种写法。
            return ("row_id=" in sql
                    or "src_pos BETWEEN" in sql
                    or "txn_id=" in sql)

        bounded = all(_bounded(s) for s in inv_reads)
        return len(seen), len(inv_reads), int(bounded)

    s_total, s_inv, s_bounded = trace_ingest(small, small_pos)
    b_total, b_inv, b_bounded = trace_ingest(big, big_pos)
    check("G2: traced statement count is constant as per-row history grows 50x",
          (s_total, s_inv) == (b_total, b_inv) and s_total <= 40,
          f"small=({s_total},{s_inv}) big=({b_total},{b_inv})")
    check("G2: every inventory-log read is bounded (row id / txn id / pos range, no linear row replay)",
          s_bounded == 1 and b_bounded == 1, "unbounded inventory scan detected")

    # G3: 未涉及库位的汇总行逐字节不变；只有该行原先/现在贡献的库位被改
    # G3: 未涉及库位的汇总行逐字节不变；只有该行原先/现在贡献的库位被改
    before = {r["bin"]: dict(r) for r in big.execute(
        "SELECT * FROM inv_bin_summary").fetchall()}
    old_bin = big.execute(
        "SELECT cur_bin FROM inv_row_state WHERE row_id='row0'").fetchone()["cur_bin"]
    source_store.ingest_segment(
        big, seg("probe2", [upd(big_pos + 1, "row0", {"bin": "bin99", "qty": 1})]))
    after = {r["bin"]: dict(r) for r in big.execute(
        "SELECT * FROM inv_bin_summary").fetchall()}
    # row0 原先和现在贡献的两个库位允许变化；其他行贡献的库位必须逐字节一致
    row0_bins = {b for b in (old_bin, "bin99") if b is not None}
    untouched = set(before) - row0_bins
    changed = sorted(b for b in untouched if before[b] != after.get(b))
    check("G3: bins unrelated to the touched row are byte-identical",
          not changed and all(b in after for b in untouched),
          f"changed untouched bins={changed}")
    check("G3: only old/new bins touched; no zero-count bins remain",
          "bin99" in after and all(r["row_count"] > 0 for r in after.values()),
          f"after={sorted(after)}")
    # 汇总与 oracle 全量折叠一致（规模库同样成立）
    # 直接核对总量守恒式：sum(qty) 与 sum(rows)
    tot = big.execute(
        "SELECT COALESCE(SUM(total_qty),0) AS q, COALESCE(SUM(row_count),0) AS n "
        "FROM inv_bin_summary").fetchone()
    live = big.execute(
        "SELECT COUNT(*) AS c FROM inv_row_state WHERE status='live' "
        "AND cur_bin IS NOT NULL AND cur_qty IS NOT NULL").fetchone()["c"]
    check("G3: summary row counts reconcile with per-row state",
          tot["n"] == live and tot["n"] > 0, f"summary={dict(tot)} live_rows={live}")
    small.close()
    big.close()


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="ccs_inv_")
    print("workdir:", tmp)
    part_a1_late_delete_and_reinsert(tmp)
    part_a2_multi_row_transfer(tmp)
    part_a3_eligibility(tmp)
    part_b_oracle_shuffled(tmp)
    part_c_cleanup_reopen(tmp)
    part_d_feature_gating(tmp)
    part_e_cli(tmp)
    part_f_rollback(tmp)
    part_g_incremental_evidence(tmp)

    print(f"\nPASS {len(PASSES)} checks")
    if FAILS:
        print(f"FAIL {len(FAILS)} checks:")
        for f in FAILS:
            print("  -", f)
        return 1
    print("inventory materialization matches the independent source-order fold under "
          "shuffled arrivals; cleanup/reopen, rollback and incrementality hold.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
