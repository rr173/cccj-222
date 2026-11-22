#!/usr/bin/env python3
"""可复现事务包导出演练。

运行：python3 -I tests/test_repro_bundle.py

覆盖：
  A. 手算迟到例子（题目原文）：乙可见时带甲/乙/丁/戊、不带丙；乙迟到时原截点
     只含丁/戊；补到乙后用原截点重新导出仍是原来的包；
  B. 自建小日志 + 独立预期：多条依赖路径汇到同一事务、同一行在不同源位置被
     追查、事务内多次操作不折叠、段内键的省略与显式 null 原样保留，
     对所有目标 × 截点与 oracle/bundle_oracle.py 对拍；
  C. 清理并重开源库后同一截点仍能导出同一份包；已记录标识/区间不可复用；
  D. 空库按输出顺序真实摄入（CLI），事务内原始操作与源位置完全一致；
  E. 无关历史不进入搜索：生成大量无关事务，比较导出读取的行数与执行计划；
  F. 错误与原子性：截点越界、目标不可见、失败不覆盖原文件、成功可替换；
  G. 启用契约：未启用保持原行为、空库才可启用、单源限制、幂等、CLI 端到端。
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

from ccs import source_store, consumer_state, cleanup, field_history, repro  # noqa: E402
import bundle_oracle  # noqa: E402  独立模型，绝不 import ccs

FAILS: list[str] = []
PASSES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        PASSES.append(name)
    else:
        FAILS.append(f"{name} :: {detail}")


def seg(txn_id: str, ops: list[dict]) -> dict:
    return {"txn_id": txn_id, "ops": ops}


def op(pos: int, rid: str, kind: str, after=None, before=None,
       before_missing: bool = False) -> dict:
    entry = {"source_position": pos, "row_id": rid, "op": kind}
    if before_missing:
        entry["before_missing"] = True
    elif before is not None:
        entry["before"] = before
    else:
        entry["before"] = None
    entry["after"] = after
    return entry


def enable_fresh(db: str, source: str = "repro-src") -> sqlite3.Connection:
    conn = source_store.connect(db)
    repro.enable_repro(conn)
    source_store.begin_source(conn, source)
    return conn


def read_bundle(path: str) -> tuple[dict, list[dict]]:
    with open(path, encoding="utf-8") as fh:
        lines = [json.loads(line) for line in fh if line.strip()]
    return lines[0], lines[1:]


def export_ids(db: str, txn_id: str, cutoff: int, path: str) -> tuple[int, dict, list[str]]:
    """用真实独立进程导出，返回 (rc, CLI JSON, 包内 txn_id 顺序)。"""
    proc = subprocess.run(
        [os.path.join(ROOT, "bin", "ccs"), "--db", db, "export-bundle",
         txn_id, "--cutoff", str(cutoff), path],
        capture_output=True, text=True)
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        payload = {"parse_error": proc.stdout[:300], "stderr": proc.stderr[:300]}
    ids: list[str] = []
    if os.path.exists(path) and proc.returncode == 0:
        _, segs = read_bundle(path)
        ids = [s["txn_id"] for s in segs]
    return proc.returncode, payload, ids


# --------------------------------------------------------------------------- #
# Part A：手算迟到例子（题目原文的甲/乙/丙/丁/戊）
# --------------------------------------------------------------------------- #
def late_example_segments() -> list[dict]:
    return [
        seg("甲", [op(100, "X", "INSERT", {"v": 1})]),          # arrival 1
        seg("乙", [op(200, "X", "UPDATE", {"v": 2}, before={"v": 1}),
                   op(201, "B", "INSERT", {"v": 20})]),         # arrival 2（正常先到）
        seg("丙", [op(300, "X", "UPDATE", {"v": 3}, before={"v": 2})]),  # arrival 3
        seg("丁", [op(400, "B", "UPDATE", {"v": 21}, before={"v": 20}),
                   op(401, "A", "INSERT", {"v": 40})]),         # arrival 4
        seg("戊", [op(500, "A", "UPDATE", {"v": 41}, before={"v": 40})]),  # arrival 5
    ]


def part_a_late_example(tmp: str) -> None:
    print("== Part A: 手算迟到例子 ==")
    db = os.path.join(tmp, "late.db")
    conn = enable_fresh(db)
    for s in late_example_segments():
        source_store.ingest_segment(conn, s)
    conn.close()

    out = os.path.join(tmp, "late_b1.jsonl")
    r = repro.export_bundle(source_store.connect(db), "戊", 5, out)
    meta, got = read_bundle(out)
    check("A: visible 乙 -> {甲,乙,丁,戊}, 丙 excluded",
          [s["txn_id"] for s in got] == ["甲", "乙", "丁", "戊"],
          json.dumps([s["txn_id"] for s in got], ensure_ascii=False))
    check("A: bundle ordered by source position, target is 戊",
          [s["ops"][0]["source_position"] for s in got] == [100, 200, 400, 500]
          and meta["target_txn_id"] == "戊" and meta["txn_count"] == 4,
          json.dumps(meta, ensure_ascii=False))
    check("A: header carries source version, cutoff, target",
          meta["source_version"] == "repro-src" and meta["cutoff"] == 5
          and meta["format"] == "ccs-repro-bundle" and meta["format_version"] == 1,
          json.dumps(meta, ensure_ascii=False))

    # 乙是迟到的：先到 甲,丙,丁,戊（乙最后到）
    db2 = os.path.join(tmp, "late2.db")
    conn = enable_fresh(db2)
    segs = late_example_segments()
    order = [(segs[0], None), (segs[2], None), (segs[3], None), (segs[4], None),
             (segs[1], None)]
    # 只摄入 甲/丙/丁/戊：arrival 1..4，K=4 是原截点
    for s, _ in order[:4]:
        source_store.ingest_segment(conn, s)
    conn.close()
    out_before = os.path.join(tmp, "late_before.jsonl")
    repro.export_bundle(source_store.connect(db2), "戊", 4, out_before)
    meta_before, got_before = read_bundle(out_before)
    check("A: with 乙 not yet arrived, original cutoff bundle is {丁,戊}",
          [s["txn_id"] for s in got_before] == ["丁", "戊"],
          json.dumps([s["txn_id"] for s in got_before], ensure_ascii=False))
    bytes_before = open(out_before, "rb").read()

    # 补到乙（arrival 5），仍用原截点 4 重新导出：包必须不变
    conn = source_store.connect(db2)
    source_store.ingest_segment(conn, segs[1])
    conn.close()
    out_after = os.path.join(tmp, "late_after.jsonl")
    repro.export_bundle(source_store.connect(db2), "戊", 4, out_after)
    check("A: after late 乙 arrives, same original cutoff still yields same bundle",
          open(out_after, "rb").read() == bytes_before,
          "bundle changed after late arrival")
    # 新截点 5 才看到乙，于是带上 甲、乙
    out_k5 = os.path.join(tmp, "late_k5.jsonl")
    repro.export_bundle(source_store.connect(db2), "戊", 5, out_k5)
    _, got_k5 = read_bundle(out_k5)
    check("A: newer cutoff sees 乙 and pulls in 甲,乙",
          [s["txn_id"] for s in got_k5] == ["甲", "乙", "丁", "戊"],
          json.dumps([s["txn_id"] for s in got_k5], ensure_ascii=False))
    # 达不到的截点/水位外仍然是错误
    conn = source_store.connect(db2)
    try:
        repro.export_bundle(conn, "戊", 6, os.path.join(tmp, "nope.jsonl"))
        check("A: cutoff beyond watermark rejected", False, "no error")
    except source_store.StoreError as exc:
        check("A: cutoff beyond watermark rejected",
              exc.code == "CUTOFF_BEYOND_WATERMARK", exc.code)
    conn.close()


# --------------------------------------------------------------------------- #
# Part B：自建小日志 + 独立预期（多路径汇合、同位置多次追查、不折叠）
# --------------------------------------------------------------------------- #
def crafted_segments() -> list[dict]:
    """源位置从早到晚：A(10s) B(20s) C(30s) D(40s) E(50s) F(60s) G(70s)。

    依赖图（行触达 + 更早）：
      A: X@10
      B: X@20, Y@21          -> 依赖 A（X）
      C: X@30                -> 依赖 A, B（X；源位置比 B 晚所以 B 也早于 C）
      D: Y@40, Z@41          -> 依赖 B（Y）
      E: X@50, Y@51          -> 依赖 A, B, C（X）与 B, D（Y）
      F: Z@60                -> 依赖 D
      G: W@70, X@71, X@72    -> 依赖 A, B, C, E（X）；F 仅触达 Z 不相关
    导出 G 应得 A,B,C,D,E,G：D 经 E/Y 进入，F 仅经 Z 触达，不进入。
    """
    return [
        seg("A", [op(10, "X", "INSERT", {"v": 1})]),
        seg("B", [op(20, "X", "UPDATE", {"v": 2}, before={"v": 1}),
                  op(21, "Y", "INSERT", {"v": 10})]),
        seg("C", [op(30, "X", "UPDATE", {"v": 3}, before_missing=True)]),
        seg("D", [op(40, "Y", "UPDATE", {"v": 11}, before={"v": 10}),
                  op(41, "Z", "INSERT", {"v": 100})]),
        seg("E", [op(50, "X", "UPDATE", {"v": 4}, before={"v": 3}),
                  op(51, "Y", "UPDATE", {"v": 12}, before={"v": 11})]),
        seg("F", [op(60, "Z", "UPDATE", {"v": 101}, before={"v": 100})]),
        seg("G", [op(70, "W", "INSERT", {"v": 7}),
                  op(71, "X", "UPDATE", {"v": 5}, before={"v": 4}),
                  op(72, "X", "DELETE", None, before={"v": 5})]),
    ]


def part_b_crafted_oracle(tmp: str) -> None:
    print("== Part B: 自建小日志 + 独立预期矩阵 ==")
    db = os.path.join(tmp, "crafted.db")
    conn = enable_fresh(db)
    segments = crafted_segments()
    arrivals = {}
    for s in segments:
        arrivals[s["txn_id"]] = source_store.ingest_segment(conn, s)["arrival_seq"]
    conn.close()
    by_id = {s["txn_id"]: s for s in segments}

    # 手算：导出 G 带 A,B,C,D,E（D 经 E 的 Y 触达；F 只触达 Z 不进入）
    out = os.path.join(tmp, "g.jsonl")
    repro.export_bundle(source_store.connect(db), "G", 7, out)
    meta, got = read_bundle(out)
    check("B: export G pulls A,B,C,D,E but not F",
          [s["txn_id"] for s in got] == ["A", "B", "C", "D", "E", "G"],
          json.dumps([s["txn_id"] for s in got], ensure_ascii=False))
    # 导出 G 时，同一行 X 在 71/72 被追查两次，但事务只出现一次、事务内操作不折叠
    check("B: repeated row in target does not duplicate the transaction",
          [s["txn_id"] for s in got].count("G") == 1, "")
    check("B: intra-txn op order and repeated writes preserved",
          by_id["G"]["ops"] == got[-1]["ops"] and len(got[-1]["ops"]) == 3
          and [o["source_position"] for o in got[-1]["ops"]] == [70, 71, 72],
          json.dumps(got[-1], ensure_ascii=False))
    # 省略键与显式 null 的区别（C 缺前镜像 = 没有 before 键；G DELETE 有 before）
    check("B: omitted before key and explicit null survive byte-for-byte in raw form",
          "before" not in got[2]["ops"][0] and got[2]["ops"][0]["before_missing"] is True
          and got[-1]["ops"][2]["after"] is None,
          json.dumps(got[2], ensure_ascii=False))
    # 导出 E：依赖 A,B,C（X）与 B,D（Y）-> A,B,C,D,E；不带 F/G
    out_e = os.path.join(tmp, "e.jsonl")
    repro.export_bundle(source_store.connect(db), "E", 7, out_e)
    _, got_e = read_bundle(out_e)
    check("B: export E pulls A,B,C,D exactly (multiple paths converge)",
          [s["txn_id"] for s in got_e] == ["A", "B", "C", "D", "E"],
          json.dumps([s["txn_id"] for s in got_e], ensure_ascii=False))
    # 导出 D：B（Y）-> B 依赖 A（X），所以 A 也被纳入；这是"同位置多次追查"的另一路径
    out_d = os.path.join(tmp, "d.jsonl")
    repro.export_bundle(source_store.connect(db), "D", 7, out_d)
    _, got_d = read_bundle(out_d)
    check("B: export D follows Y back to B and then X back to A",
          [s["txn_id"] for s in got_d] == ["A", "B", "D"],
          json.dumps([s["txn_id"] for s in got_d], ensure_ascii=False))
    # 导出 C：只带 A,B,C
    out_c = os.path.join(tmp, "c.jsonl")
    repro.export_bundle(source_store.connect(db), "C", 7, out_c)
    _, got_c = read_bundle(out_c)
    check("B: export C pulls A,B,C only", [s["txn_id"] for s in got_c] == ["A", "B", "C"],
          json.dumps([s["txn_id"] for s in got_c], ensure_ascii=False))

    # 全矩阵对拍：每个目标 × 每个截点
    conn = source_store.connect(db)
    compared = 0
    for tgt in by_id:
        for k in range(0, 8):
            path = os.path.join(tmp, f"m_{tgt}_{k}.jsonl")
            expect = bundle_oracle.select(segments, tgt, k)
            if expect is None:
                try:
                    repro.export_bundle(conn, tgt, k, path)
                    check(f"B: oracle matrix invisible {tgt} K={k}", False, "no error")
                except source_store.StoreError as exc:
                    check(f"B: oracle matrix invisible {tgt} K={k}",
                          exc.code in ("TARGET_NOT_VISIBLE", "UNKNOWN_TXN"), exc.code)
                continue
            repro.export_bundle(conn, tgt, k, path)
            _, got_m = read_bundle(path)
            if got_m != expect:
                check(f"B: oracle matrix {tgt} K={k}", False,
                      f"\n  oracle={json.dumps(expect, ensure_ascii=False)}"
                      f"\n  actual={json.dumps(got_m, ensure_ascii=False)}")
                conn.close()
                return
            compared += 1
    conn.close()
    check(f"B: oracle matrix agrees on {compared} target/cutoff exports", True, "")


def random_segments(seed: int, txn_count: int, rows: int) -> list[dict]:
    """合法的随机段：每个事务按源位置顺序，块之间位置不重叠。"""
    rng = random.Random(seed)
    alive: set[str] = set()
    out: list[dict] = []
    pos = 100
    for ti in range(txn_count):
        ops: list[dict] = []
        chosen = rng.sample([f"r{i}" for i in range(rows)],
                            rng.randint(1, min(4, rows)))
        for rid in chosen:
            if rid not in alive:
                pos += 1
                ops.append(op(pos, rid, "INSERT", {"v": pos}))
                alive.add(rid)
                if rng.random() < 0.2:
                    pos += 1
                    ops.append(op(pos, rid, "DELETE", None, before={"v": pos - 1}))
                    alive.discard(rid)
                    continue
            else:
                if rng.random() < 0.25:
                    pos += 1
                    ops.append(op(pos, rid, "DELETE", None, before={"v": 0}))
                    alive.discard(rid)
                    continue
                pos += 1
                ops.append(op(pos, rid, "UPDATE", {"v": pos}, before={"v": 0}))
            for _ in range(rng.randint(0, 2)):
                pos += 1
                ops.append(op(pos, rid, "UPDATE", {"v": pos}, before_missing=True))
        out.append(seg(f"R{seed}-{ti}", ops))
    return out


def part_b_random_oracle(tmp: str) -> None:
    print("== Part B2: 随机小日志 + 随机到达顺序 × 随机截点对拍 ==")
    for seed in (20261122, 4242, 77):
        segments = random_segments(seed, txn_count=14, rows=5)
        arrivals_so_far: list[dict] = []
        order = list(range(len(segments)))
        random.Random(seed + 1).shuffle(order)
        db = os.path.join(tmp, f"rand{seed}.db")
        conn = enable_fresh(db, f"rand-src-{seed}")
        conn.close()
        checked = 0
        for n, idx in enumerate(order, start=1):
            conn = source_store.connect(db)
            source_store.ingest_segment(conn, segments[idx])
            conn.close()
            arrivals_so_far.append(segments[idx])
            # 随机挑已到达的目标与截点（截点=当前水位或更小）
            tgt = random.Random(seed * 1000 + n).choice(arrivals_so_far)["txn_id"]
            k = random.Random(seed * 7 + n).randint(1, n)
            expect = bundle_oracle.select(arrivals_so_far, tgt, k)
            path = os.path.join(tmp, f"rand_{seed}_{n}.jsonl")
            conn = source_store.connect(db)
            if expect is None:
                try:
                    repro.export_bundle(conn, tgt, k, path)
                    check(f"B2: seed={seed} n={n} invisible target", False, "no error")
                except source_store.StoreError as exc:
                    check(f"B2: seed={seed} n={n} invisible target",
                          exc.code == "TARGET_NOT_VISIBLE", exc.code)
            else:
                repro.export_bundle(conn, tgt, k, path)
                _, got = read_bundle(path)
                if got != expect:
                    check(f"B2: seed={seed} n={n} tgt={tgt} K={k}", False,
                          f"\n  oracle={json.dumps(expect, ensure_ascii=False)}"
                          f"\n  actual={json.dumps(got, ensure_ascii=False)}")
                    conn.close()
                    return
                checked += 1
            conn.close()
        check(f"B2: seed={seed} agrees on {checked} shuffled exports", checked > 0, "")


# --------------------------------------------------------------------------- #
# Part C：真实清理 -> 重开 -> 同一截点导出同一份包；标识/区间不可复用
# --------------------------------------------------------------------------- #
def part_c_cleanup_reopen(tmp: str) -> None:
    print("== Part C: 清理订阅数据并重开后仍可导出同一截点的包 ==")
    db = os.path.join(tmp, "cleanup.db")
    conn = enable_fresh(db)
    segments = crafted_segments()
    for s in segments:
        source_store.ingest_segment(conn, s)
    before = {}
    for tgt, k in (("G", 7), ("E", 5), ("D", 7)):
        p = os.path.join(tmp, f"before_{tgt}_{k}.jsonl")
        repro.export_bundle(conn, tgt, k, p)
        before[(tgt, k)] = open(p, "rb").read()

    consumer_state.register(conn, "gamma", retention_txns=0, pull_limit=20,
                            start_at_zero=True)
    pull = consumer_state.pull(conn, "gamma")
    consumer_state.ack(conn, "gamma", pull["token"])
    report = cleanup.run_cleanup(conn)
    check("C: cleanup physically removed all subscription rows",
          len(report["deleted"]) == 7
          and conn.execute("SELECT COUNT(*) AS c FROM txn").fetchone()["c"] == 0
          and conn.execute("SELECT COUNT(*) AS c FROM change_row").fetchone()["c"] == 0,
          json.dumps(report, ensure_ascii=False))
    n_repro = conn.execute("SELECT COUNT(*) AS c FROM repro_txn").fetchone()["c"]
    check("C: bundle archive survives cleanup", n_repro == 7, str(n_repro))
    conn.close()

    conn = source_store.connect(db)
    for (tgt, k), expect_bytes in before.items():
        p = os.path.join(tmp, f"after_{tgt}_{k}.jsonl")
        repro.export_bundle(conn, tgt, k, p)
        check(f"C: same export after cleanup+reopen ({tgt},K={k})",
              open(p, "rb").read() == expect_bytes, "bytes differ")
    check("C: late target still invisible at old cutoff",
          _expect_error(conn, "G", 3, os.path.join(tmp, "x.jsonl"),
                        "TARGET_NOT_VISIBLE"), "")
    check("C: cutoff beyond archive watermark rejected",
          _expect_error(conn, "G", 8, os.path.join(tmp, "x.jsonl"),
                        "CUTOFF_BEYOND_WATERMARK"), "")

    # 已记录标识 / 区间不可复用
    check("C: reused txn id rejected after cleanup",
          _expect_ingest_error(conn, seg("A", [op(900, "new", "INSERT", {"v": 1})]),
                               "DUP_TXN"), "")
    check("C: reused position range rejected after cleanup",
          _expect_ingest_error(conn, seg("A2", [op(20, "new", "INSERT", {"v": 1})]),
                               "POSITION_OVERLAP"), "")
    # 新位置照常摄入，包可继续增长；旧截点答案不变
    source_store.ingest_segment(conn, seg("H", [op(200, "X", "UPDATE", {"v": 99},
                                                    before={"v": 5})]))
    p = os.path.join(tmp, "after_h.jsonl")
    repro.export_bundle(conn, "H", 8, p)
    _, got_h = read_bundle(p)
    check("C: new txn at new position can be exported with its ancestors",
          [s["txn_id"] for s in got_h] == ["A", "B", "C", "D", "E", "G", "H"],
          json.dumps([s["txn_id"] for s in got_h], ensure_ascii=False))
    conn.close()


def _expect_error(conn, txn_id, cutoff, path, code) -> bool:
    try:
        repro.export_bundle(conn, txn_id, cutoff, path)
        return False
    except source_store.StoreError as exc:
        return exc.code == code


def _expect_ingest_error(conn, segment, code) -> bool:
    try:
        source_store.ingest_segment(conn, segment)
        return False
    except source_store.StoreError as exc:
        return exc.code == code


# --------------------------------------------------------------------------- #
# Part D：空库按输出顺序真实摄入（CLI），原始操作与源位置完全一致
# --------------------------------------------------------------------------- #
def part_d_reingest_into_empty(tmp: str) -> None:
    print("== Part D: 空库按包内顺序真实摄入 ==")
    src_db = os.path.join(tmp, "src_for_reingest.db")
    conn = enable_fresh(src_db)
    for s in crafted_segments():
        source_store.ingest_segment(conn, s)
    conn.close()
    bundle_path = os.path.join(tmp, "reingest.jsonl")
    rc, out, ids = export_ids(src_db, "G", 7, bundle_path)
    check("D: CLI export-bundle succeeds", rc == 0 and out["ok"] is True,
          json.dumps(out, ensure_ascii=False))
    meta, payload = read_bundle(bundle_path)
    check("D: CLI reports txn_count matching file",
          out["data"]["txn_count"] == len(payload) == 6, json.dumps(out))

    # 全新空库：按输出顺序逐段用真实 CLI 摄入
    dst_db = os.path.join(tmp, "dst_empty.db")
    subprocess.run([os.path.join(ROOT, "bin", "ccs"), "--db", dst_db,
                    "begin-source", "repro-src"], capture_output=True, text=True)
    for i, s in enumerate(payload):
        seg_path = os.path.join(tmp, f"reingest_{i}.json")
        with open(seg_path, "w", encoding="utf-8") as fh:
            json.dump(s, fh, ensure_ascii=False)
        proc = subprocess.run([os.path.join(ROOT, "bin", "ccs"), "--db", dst_db,
                               "ingest", seg_path], capture_output=True, text=True)
        if proc.returncode != 0:
            check(f"D: reingest {s['txn_id']}", False, proc.stdout[:400])
    # 对照：目标库的原始操作与来源库归档逐字节一致
    src = source_store.connect(src_db)
    dst = source_store.connect(dst_db)
    mismatch = []
    for s in payload:
        orig = src.execute("SELECT segment_json FROM repro_txn WHERE txn_id=?",
                           (s["txn_id"],)).fetchone()["segment_json"]
        if json.dumps(s, ensure_ascii=False, separators=(",", ":"),
                      sort_keys=False) != orig:
            mismatch.append(s["txn_id"])
    check("D: every ingested segment equals the archived raw segment",
          not mismatch, str(mismatch))
    src_rows = [tuple(r) for r in src.execute(
        "SELECT txn_id, source_position, row_id, op, before_json, after_json "
        "FROM change_row WHERE txn_id IN ('A','B','C','D','E','G') "
        "ORDER BY source_position")]
    dst_rows = [tuple(r) for r in dst.execute(
        "SELECT txn_id, source_position, row_id, op, before_json, after_json "
        "FROM change_row ORDER BY source_position")]
    check("D: empty-db ingestion preserves every op, position and image",
          src_rows == dst_rows and len(dst_rows) == 11, f"{len(src_rows)} vs {len(dst_rows)}")
    check("D: txn order in destination matches source position order",
          [r["txn_id"] for r in dst.execute(
              "SELECT txn_id FROM txn ORDER BY begin_pos").fetchall()]
          == ["A", "B", "C", "D", "E", "G"], "")
    # 目标库已含数据，不能再启用归档（空库限制）
    try:
        repro.enable_repro(dst)
        check("D: enable-repro refused on non-empty destination", False, "no error")
    except source_store.StoreError as exc:
        check("D: enable-repro refused on non-empty destination",
              exc.code == "REPRO_REQUIRES_EMPTY_DB", exc.code)
    src.close()
    dst.close()

    # 全新的空库启用归档后按包摄入，再用同一命令导出同一份包（闭环）
    db3 = os.path.join(tmp, "dst_archive.db")
    dst3 = source_store.connect(db3)
    repro.enable_repro(dst3)
    source_store.begin_source(dst3, "repro-src")
    for s in payload:
        source_store.ingest_segment(dst3, s)
    roundtrip = os.path.join(tmp, "roundtrip.jsonl")
    # 目标库只收到包内 6 笔，到达水位 6；同一目标在本地新水位下重新导出
    repro.export_bundle(dst3, "G", 6, roundtrip)
    rt_meta, rt_segs = read_bundle(roundtrip)
    check("D: re-exporting from the re-ingested archive reproduces the same segments",
          json.dumps(rt_segs, ensure_ascii=False) == json.dumps(payload, ensure_ascii=False)
          and rt_meta["target_txn_id"] == "G" and rt_meta["txn_count"] == 6,
          json.dumps(rt_meta, ensure_ascii=False))
    dst3.close()


# --------------------------------------------------------------------------- #
# Part E：无关历史不进入搜索
# --------------------------------------------------------------------------- #
def part_e_unrelated_history(tmp: str) -> None:
    print("== Part E: 无关历史不进入搜索（按行定位，不遍历整档） ==")
    db = os.path.join(tmp, "scale.db")
    conn = enable_fresh(db, "scale")
    # 500 笔无关事务，每笔插入并更新一个自己的行（大量历史）
    pos = 1000
    for i in range(500):
        pos += 1
        source_store.ingest_segment(conn, seg(f"U{i}", [
            op(pos, f"u{i}", "INSERT", {"v": i})]))
        pos += 1
        source_store.ingest_segment(conn, seg(f"U{i}b", [
            op(pos, f"u{i}", "UPDATE", {"v": i + 1}, before={"v": i})]))
    # 一条与无关历史无交集的小链
    chain = [
        seg("S1", [op(100, "rX", "INSERT", {"v": 1})]),
        seg("S2", [op(200, "rX", "UPDATE", {"v": 2}, before={"v": 1})]),
        seg("S3", [op(300, "rX", "UPDATE", {"v": 3}, before={"v": 2})]),
    ]
    for s in chain:
        source_store.ingest_segment(conn, s)
    # 注意：S 链的位置在无关历史之前，故意让"按位置范围扫描"会碰到更多数据

    reads: list[str] = []
    conn.set_trace_callback(lambda s: reads.append(" ".join(s.split())))
    out = os.path.join(tmp, "scale_bundle.jsonl")
    result = repro.export_bundle(conn, "S3", 1003, out)
    conn.set_trace_callback(None)
    check("E: export selects only the S chain", result["txn_count"] == 3,
          json.dumps(result, ensure_ascii=False))
    # 追踪 repro_row_touch 的 SELECT：必须带 row_id 定位，且不出现全表扫描语句
    touch_reads = [s for s in reads
                   if s.upper().startswith("SELECT") and "repro_row_touch" in s]
    check("E: candidate lookups are row/txn-bound (no full-archive walk)",
          touch_reads and all(("row_id=" in s or "txn_id=" in s) for s in touch_reads)
          and not any("SCAN repro_row_touch" in s for s in reads),
          "unbounded touch scan detected")

    # 候选量证据：无关历史有上千行触达记录；本包只该读目标行 rX 的少量候选。
    candidate_count = 0
    for sel_txn in ("S3", "S2", "S1"):
        rows = conn.execute("SELECT row_id FROM repro_row_touch WHERE txn_id=?",
                            (sel_txn,)).fetchall()
        for r in rows:
            candidate_count += conn.execute(
                "SELECT COUNT(*) FROM repro_row_touch WHERE row_id=? AND begin_pos<? "
                "AND arrival_seq<=?", (r["row_id"], 1000, 1003)).fetchone()[0]
    total_touch = conn.execute("SELECT COUNT(*) FROM repro_row_touch").fetchone()[0]
    check("E: candidates considered are bounded by touched rows, not archive size",
          total_touch >= 1000 and candidate_count <= 10,
          f"total_touch={total_touch} candidates={candidate_count}")

    # 执行计划：候选查询走 (row_id, begin_pos) 索引 SEARCH
    plan = " | ".join(
        r["detail"] for r in conn.execute(
            "EXPLAIN QUERY PLAN SELECT txn_id, begin_pos FROM repro_row_touch "
            "WHERE row_id=? AND begin_pos<? AND arrival_seq<=?",
            ("rX", 300, 1003)).fetchall())
    check("E: candidate query uses index SEARCH on (row_id, begin_pos)",
          "SEARCH" in plan and "SCAN repro_row_touch" not in plan, plan)
    # 一个反证：如果按位置范围扫描，[最老, 目标] 区间会命中上千行；
    # 本导出的候选量由行触达决定，与无关历史规模无关。
    conn.close()

    # 更硬的证据：同一目标在无关历史规模 50 -> 500 -> 2000 的库上导出，
    # 导出过程的 SQLite VM 步数必须恒定（不随整档增长）。
    steps = {}
    for n in (50, 500, 2000):
        sconn, watermark = _build_scale_db(os.path.join(tmp, f"scale_{n}.db"), n)
        calls = [0]

        def bump() -> int:
            calls[0] += 1
            return 0

        sconn.set_progress_handler(bump, 10)
        repro.export_bundle(sconn, "S2", watermark,
                            os.path.join(tmp, f"scale_{n}_out.jsonl"))
        sconn.set_progress_handler(None, 0)
        steps[n] = calls[0]
        sconn.close()
    check("E: export VM work is constant while unrelated history grows 40x",
          len(set(steps.values())) == 1, str(steps))


def _build_scale_db(db: str, n_unrelated: int) -> tuple[sqlite3.Connection, int]:
    """n 笔无关事务（每笔一行）后追加一条 3 笔的小链；返回连接与到达水位。"""
    conn = enable_fresh(db, f"scale-{n_unrelated}")
    pos = 1000
    for i in range(n_unrelated):
        pos += 1
        source_store.ingest_segment(conn, seg(f"U{i}", [
            op(pos, f"u{i}", "INSERT", {"v": i})]))
        pos += 1
        source_store.ingest_segment(conn, seg(f"U{i}b", [
            op(pos, f"u{i}", "UPDATE", {"v": i + 1}, before={"v": i})]))
    chain = [
        seg("S1", [op(100, "rX", "INSERT", {"v": 1})]),
        seg("S2", [op(200, "rX", "UPDATE", {"v": 2}, before={"v": 1})]),
        seg("S3", [op(300, "rX", "UPDATE", {"v": 3}, before={"v": 2})]),
    ]
    for s in chain:
        source_store.ingest_segment(conn, s)
    watermark = conn.execute("SELECT MAX(arrival_seq) FROM repro_txn").fetchone()[0]
    return conn, watermark


# --------------------------------------------------------------------------- #
# Part F：错误与原子性
# --------------------------------------------------------------------------- #
def part_f_errors_atomicity(tmp: str) -> None:
    print("== Part F: 错误码与输出文件原子性 ==")
    db = os.path.join(tmp, "atomic.db")
    conn = enable_fresh(db)
    for s in crafted_segments():
        source_store.ingest_segment(conn, s)

    target = os.path.join(tmp, "keep.jsonl")
    with open(target, "w", encoding="utf-8") as fh:
        fh.write("ORIGINAL CONTENT MUST SURVIVE\n")

    # 失败：未知目标
    check("F: unknown target rejected",
          _expect_error(conn, "NOPE", 7, target, "UNKNOWN_TXN"), "")
    check("F: original file untouched after unknown-target failure",
          open(target, encoding="utf-8").read() == "ORIGINAL CONTENT MUST SURVIVE\n", "")
    # 失败：目标在截点不可见
    check("F: target beyond cutoff rejected",
          _expect_error(conn, "G", 6, target, "TARGET_NOT_VISIBLE"), "")
    check("F: original file untouched after invisible-target failure",
          open(target, encoding="utf-8").read() == "ORIGINAL CONTENT MUST SURVIVE\n", "")
    # 失败：截点越界
    check("F: cutoff beyond watermark rejected",
          _expect_error(conn, "G", 99, target, "CUTOFF_BEYOND_WATERMARK"), "")
    check("F: original file untouched after cutoff failure",
          open(target, encoding="utf-8").read() == "ORIGINAL CONTENT MUST SURVIVE\n", "")
    # 失败：写入阶段（目录不存在）不留下半份包
    missing_dir = os.path.join(tmp, "no_such_dir", "out.jsonl")
    check("F: unwritable output path reported as EXPORT_FAILED",
          _expect_error(conn, "G", 7, missing_dir, "EXPORT_FAILED"), "")
    check("F: no partial file left behind", not os.path.exists(missing_dir), "")
    # 没有临时文件泄漏在目标目录
    leftovers = [n for n in os.listdir(tmp) if n.startswith(".ccs_repro_")]
    check("F: no temp files left in output directory", not leftovers, str(leftovers))

    # 成功：可替换同路径原文件
    repro.export_bundle(conn, "G", 7, target)
    meta, got = read_bundle(target)
    check("F: successful export replaces the existing file atomically",
          meta["target_txn_id"] == "G" and len(got) == 6, json.dumps(meta))

    # 空输出路径
    check("F: empty output path rejected",
          _expect_error(conn, "G", 7, "", "BAD_OUTPUT_PATH"), "")
    # 负截点
    check("F: negative cutoff rejected",
          _expect_error(conn, "G", -1, target, "BAD_CUTOFF"), "")
    # 未启用的库
    db2 = os.path.join(tmp, "disabled_export.db")
    c2 = source_store.connect(db2)
    check("F: export on disabled db rejected",
          _expect_error(c2, "G", 1, target, "REPRO_NOT_ENABLED"), "")
    c2.close()
    conn.close()


# --------------------------------------------------------------------------- #
# Part G：启用契约与 CLI 端到端
# --------------------------------------------------------------------------- #
def run_cli(db: str, args: list[str]) -> tuple[int, dict]:
    proc = subprocess.run([os.path.join(ROOT, "bin", "ccs"), "--db", db, *args],
                          capture_output=True, text=True)
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        payload = {"parse_error": proc.stdout[:200], "stderr": proc.stderr[:200]}
    return proc.returncode, payload


def part_g_gating_cli(tmp: str) -> None:
    print("== Part G: 启用契约与 CLI 端到端 ==")
    # 未启用的库：原订阅行为完全不变，连 repro_* 表都没有
    db = os.path.join(tmp, "plain.db")
    conn = source_store.connect(db)
    source_store.begin_source(conn, "plain")
    source_store.ingest_segment(conn, crafted_segments()[0])
    n = conn.execute("SELECT COUNT(*) AS c FROM sqlite_master WHERE type='table' "
                     "AND name LIKE 'repro_%'").fetchone()["c"]
    check("G: disabled db carries no repro tables", n == 0, str(n))
    try:
        repro.enable_repro(conn)
        check("G: enable on non-empty db rejected", False, "no error")
    except source_store.StoreError as exc:
        check("G: enable on non-empty db rejected",
              exc.code == "REPRO_REQUIRES_EMPTY_DB", exc.code)
    # 未启用时清理后可复用标识/位置（原行为）
    consumer_state.register(conn, "solo", retention_txns=0, pull_limit=5,
                            start_at_zero=True)
    p = consumer_state.pull(conn, "solo")
    consumer_state.ack(conn, "solo", p["token"])
    cleanup.run_cleanup(conn)
    re = source_store.ingest_segment(conn, crafted_segments()[0])
    check("G: disabled db keeps original behavior (reuse after cleanup allowed)",
          re["txn_id"] == "A", json.dumps(re))
    conn.close()

    # 启用幂等 + 单源限制
    db2 = os.path.join(tmp, "single.db")
    conn = source_store.connect(db2)
    first = repro.enable_repro(conn)
    again = repro.enable_repro(conn)
    check("G: enable is idempotent",
          first["already_enabled"] is False and again["already_enabled"] is True,
          json.dumps([first, again]))
    source_store.begin_source(conn, "only")
    try:
        source_store.switch_source(conn, "other")
        check("G: switch-source refused while repro enabled", False, "no error")
    except source_store.StoreError as exc:
        check("G: switch-source refused while repro enabled",
              exc.code == "REPRO_SINGLE_SOURCE", exc.code)
    conn.close()

    # 与 field-history / inventory 共存，互不干扰
    db3 = os.path.join(tmp, "coexist.db")
    conn = source_store.connect(db3)
    field_history.enable_history(conn)
    from ccs import inventory
    inventory.enable_inventory(conn)
    repro.enable_repro(conn)
    source_store.begin_source(conn, "co")
    for s in crafted_segments():
        source_store.ingest_segment(conn, s)
    check("G: three optional features coexist",
          inventory.read_summary(conn)["arrival_seq"] == 7
          and field_history.query_field_history(
              conn, "X", "v", 0, 100, 7)["arrival_watermark"] == 7, "")
    p = os.path.join(tmp, "co.jsonl")
    repro.export_bundle(conn, "G", 7, p)
    _, got = read_bundle(p)
    check("G: repro export works alongside history/inventory",
          [s["txn_id"] for s in got] == ["A", "B", "C", "D", "E", "G"], "")
    conn.close()

    # CLI：enable-repro / export-bundle / 错误码
    db4 = os.path.join(tmp, "cli.db")
    rc, out = run_cli(db4, ["export-bundle", "A", "--cutoff", "1",
                            os.path.join(tmp, "cli_before.jsonl")])
    check("G: CLI export before enable -> exit 2",
          rc == 2 and out["error"]["code"] == "REPRO_NOT_ENABLED", json.dumps(out))
    rc, out = run_cli(db4, ["enable-repro"])
    check("G: CLI enable-repro", rc == 0 and out["data"]["enabled"] is True,
          json.dumps(out))
    run_cli(db4, ["begin-source", "cli-src"])
    seg_dir = os.path.join(tmp, "cli_segments")
    os.makedirs(seg_dir, exist_ok=True)
    for s in crafted_segments():
        with open(os.path.join(seg_dir, f"{s['txn_id']}.json"), "w",
                  encoding="utf-8") as fh:
            json.dump(s, fh, ensure_ascii=False)
    for s in crafted_segments():
        rc, out = run_cli(db4, ["ingest", os.path.join(seg_dir, f"{s['txn_id']}.json")])
        if rc != 0:
            check(f"G: CLI ingest {s['txn_id']}", False, json.dumps(out))
    cli_bundle = os.path.join(tmp, "cli_bundle.jsonl")
    rc, out = run_cli(db4, ["export-bundle", "G", "--cutoff", "7", cli_bundle])
    check("G: CLI export-bundle ok", rc == 0 and out["data"]["txn_count"] == 6,
          json.dumps(out, ensure_ascii=False))
    meta, got = read_bundle(cli_bundle)
    check("G: CLI bundle header and ordering",
          meta["source_version"] == "cli-src" and meta["cutoff"] == 7
          and [s["txn_id"] for s in got] == ["A", "B", "C", "D", "E", "G"],
          json.dumps(meta, ensure_ascii=False))
    rc, out = run_cli(db4, ["export-bundle", "G", "--cutoff", "99",
                            os.path.join(tmp, "cli_bad.jsonl")])
    check("G: CLI cutoff beyond watermark -> exit 2",
          rc == 2 and out["error"]["code"] == "CUTOFF_BEYOND_WATERMARK",
          f"rc={rc} {json.dumps(out)}")
    check("G: CLI failed export leaves no file",
          not os.path.exists(os.path.join(tmp, "cli_bad.jsonl")), "")
    rc, out = run_cli(db4, ["switch-source", "x"])
    check("G: CLI switch-source refused",
          rc == 2 and out["error"]["code"] == "REPRO_SINGLE_SOURCE", json.dumps(out))


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="ccs_repro_")
    print("workdir:", tmp)
    part_a_late_example(tmp)
    part_b_crafted_oracle(tmp)
    part_b_random_oracle(tmp)
    part_c_cleanup_reopen(tmp)
    part_d_reingest_into_empty(tmp)
    part_e_unrelated_history(tmp)
    part_f_errors_atomicity(tmp)
    part_g_gating_cli(tmp)

    print(f"\nPASS {len(PASSES)} checks")
    if FAILS:
        print(f"FAIL {len(FAILS)} checks:")
        for f in FAILS:
            print("  -", f)
        return 1
    print("reproducible bundles match the hand-computed late example and the "
          "independent oracle; cleanup/reopen, empty-db re-ingest, bounded "
          "candidate lookup and atomic publication hold.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
