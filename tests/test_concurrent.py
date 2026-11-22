#!/usr/bin/env python3
"""真实多进程交错演练 + 崩溃恢复演练。

运行：python3 -I tests/test_concurrent.py

覆盖：
  A. 三个真实进程并发：快消费者、慢消费者、清理进程，确认与清理交错；
     结束后核对无漏交付、无越过前沿、无提前删除。
  B. 撤销慢消费者后，快消费者继续拉取、清理可以回收。
  C. ack 决策后提交前崩溃（os._exit(17)），重开后前沿未推进、可重复确认，
     不出现重复推进或不可解释的空洞。
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from ccs import source_store, consumer_state, cleanup  # noqa: E402

DEMO_DIR = os.path.join(ROOT, "demo", "segments")
INGEST_ORDER = ["T1", "T2", "T3", "T4", "T5", "T7", "T6", "T8", "T9"]
FAILS: list[str] = []
PASSES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    (PASSES if cond else FAILS).append(name) if cond else FAILS.append(f"{name} :: {detail}")


def read_jsonl(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def run_worker(role: str, db: str, log: str, rounds: int, name: str | None = None) -> subprocess.Popen:
    cmd = [sys.executable, "-I", os.path.join(ROOT, "tests", "worker.py"),
           role, "--db", db, "--rounds", str(rounds), "--log", log]
    if name:
        cmd += ["--name", name]
    return subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def run_cli_isolated(db: str, cli_args: list[str]) -> subprocess.CompletedProcess:
    """以隔离模式在独立进程里跑 ccs CLI（-I 下显式注入仓库根到 sys.path）。"""
    bootstrap = (
        f"import sys; sys.path.insert(0, {ROOT!r}); "
        "from ccs.cli import main; sys.exit(main())"
    )
    cmd = [sys.executable, "-I", "-c", bootstrap, "--db", db, *cli_args]
    return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)


def ingest_all(db: str, order=INGEST_ORDER) -> None:
    conn = source_store.connect(db)
    try:
        for txn_id in order:
            with open(os.path.join(DEMO_DIR, f"{txn_id}.json"), encoding="utf-8") as fh:
                source_store.ingest_segment(conn, json.load(fh))
    finally:
        conn.close()


def part_a_concurrent(tmp: str) -> None:
    print("== Part A: 三个真实进程并发（快/慢消费者 + 清理） ==")
    db = os.path.join(tmp, "conc.db")
    conn = source_store.connect(db)
    source_store.begin_source(conn, "src-C")
    # fast: pull_limit 3, retention 2（保留少）；slow: pull_limit 1, retention 8（钉住历史）
    consumer_state.register(conn, "fast", retention_txns=2, pull_limit=3,
                            need_before=True, start_at_zero=True)
    consumer_state.register(conn, "slow", retention_txns=8, pull_limit=1,
                            need_before=False, start_at_zero=True)
    conn.close()
    ingest_all(db)

    log_fast = os.path.join(tmp, "fast.jsonl")
    log_slow = os.path.join(tmp, "slow.jsonl")
    log_clean = os.path.join(tmp, "clean.jsonl")

    procs = [
        run_worker("consumer", db, log_fast, 80, "fast"),
        run_worker("cleanup", db, log_clean, 40),
        run_worker("consumer", db, log_slow, 80, "slow"),
    ]
    # 慢消费者故意再慢一点：驱动进程中途再启动一个清理循环制造交错
    time.sleep(0.15)
    extra_clean = run_worker("cleanup", db, log_clean, 20)
    procs.append(extra_clean)

    for p in procs:
        out, err = p.communicate(timeout=60)
        if p.returncode != 0:
            FAILS.append(f"worker exit {p.returncode}: {err.decode()[:400]}")

    fast_events = read_jsonl(log_fast)
    slow_events = read_jsonl(log_slow)
    clean_events = read_jsonl(log_clean)

    # ---- 不变量 1：每个消费者交付的事务序号严格连续、无重复、无跳跃 ----
    for cname, events in (("fast", fast_events), ("slow", slow_events)):
        delivered_seqs: list[int] = []
        for e in events:
            if e["kind"] == "pull":
                delivered_seqs.extend(e["commit_seqs"])
        expected_seqs = list(range(1, len(INGEST_ORDER) + 1))
        check(f"[{cname}] every txn delivered exactly once in commit order",
              delivered_seqs == expected_seqs,
              f"got {delivered_seqs}")
        # ack 前沿单调不减，且每次增量等于当批预留
        acks = [e for e in events if e["kind"] == "ack"]
        prev = 0
        monotonic = True
        for e in acks:
            if e["to"] < prev or e["from"] != prev:
                monotonic = False
            prev = e["to"]
        check(f"[{cname}] ack frontier strictly advances over contiguous reservations",
              monotonic and (not acks or acks[-1]["to"] == len(INGEST_ORDER)),
              f"last ack {acks[-1] if acks else None}")
        check(f"[{cname}] no pull errors",
              all(e["kind"] != "pull_error" for e in events),
              str([e for e in events if e["kind"] == "pull_error"]))

    # ---- 不变量 2：被清理的事务，当时两个消费者都不再需要 ----
    conn = source_store.connect(db)
    try:
        deleted = []
        for e in clean_events:
            deleted.extend(d["txn_id"] for d in e["deleted"])
        all_log = cleanup.cleanup_log(conn)
        check("cleanup audit log complete and ordered",
              [x["deleted_txn_id"] for x in all_log] == deleted,
              f"log={[x['deleted_txn_id'] for x in all_log]} deleted={deleted}")

        # 慢消费者 retention=8 且最终前沿 9：floor = max(1, 9-8+1)=2，
        # 因此只有 seq1 (T1) 可以被清理；任何 T2+ 被删都是提前删除。
        deleted_seqs = sorted(x["commit_seq"] for x in all_log)
        check("slow consumer pins history: only seq 1 may be cleaned",
              deleted_seqs == [1], f"deleted_seqs={deleted_seqs}")

        # ---- 不变量 3：仍在任意消费者保留窗口内的事务一定还在库里 ----
        plan = cleanup.retention_plan(conn)
        surviving = {r["txn_id"] for r in conn.execute("SELECT txn_id FROM txn").fetchall()}
        protected_ids = {x["txn_id"] for x in plan["protected"]}
        check("all protected transactions still physically present",
              protected_ids.issubset(surviving),
              f"missing={protected_ids - surviving}")

        # 慢消费者仍可重放窗口内数据（没有被快消费者/清理误伤）
        rp = consumer_state.replay_window(conn, "slow", 200, 202)
        check("slow consumer replay of retained T2 works after concurrent cleanup",
              [d["txn_id"] for d in rp["deliveries"]] == ["T2"],
              json.dumps([d["txn_id"] for d in rp["deliveries"]]))
    finally:
        conn.close()


def part_b_revoke(tmp: str) -> None:
    print("== Part B: 撤销慢消费者后快消费者继续、清理回收 ==")
    db = os.path.join(tmp, "revoke.db")
    conn = source_store.connect(db)
    source_store.begin_source(conn, "src-R")
    consumer_state.register(conn, "fast", retention_txns=1, pull_limit=2, start_at_zero=True)
    consumer_state.register(conn, "slow", retention_txns=30, pull_limit=1, start_at_zero=True)
    conn.close()
    ingest_all(db)

    log = os.path.join(tmp, "revoke.jsonl")
    p_fast = run_worker("consumer", db, log, 60, "fast")
    p_slow = run_worker("consumer", db, os.path.join(tmp, "revoke_slow.jsonl"), 3, "slow")
    p_slow.communicate(timeout=30)
    p_fast.communicate(timeout=30)

    conn = source_store.connect(db)
    try:
        check("fast reached full frontier despite slow sibling",
              consumer_state.status(conn, "fast")["frontier_commit_seq"] == len(INGEST_ORDER),
              "")
        consumer_state.revoke(conn, "slow")
        plan_before = cleanup.retention_plan(conn)
        check("revoked consumer no longer contributes retention",
              "slow" not in plan_before["active_consumers"],
              str(plan_before["active_consumers"]))
        # fast retention=1, frontier 9 -> floor 9：只有 T9 受保护，其余可清
        report = cleanup.run_cleanup(conn)
        deleted = {x["txn_id"] for x in report["deleted"]}
        check("after revoke, cleanup reclaims everything fast does not need",
              deleted == {f"T{n}" for n in ["1", "2", "3", "4", "5", "7", "6", "8"]},
              str(sorted(deleted)))
        # fast 仍能拉取（无新数据，幂等空拉）和重放保留窗口
        pull = consumer_state.pull(conn, "fast")
        check("fast pull still works after sibling revoke", pull["deliveries"] == [], "")
        rp = consumer_state.replay_window(conn, "fast", 900, 902)
        check("fast retained T9 replayable",
              [d["txn_id"] for d in rp["deliveries"]] == ["T9"], "")
    finally:
        conn.close()


def part_c_crash(tmp: str) -> None:
    print("== Part C: ack 提交前崩溃 -> 重开无重复推进/空洞 ==")
    db = os.path.join(tmp, "crash.db")
    conn = source_store.connect(db)
    source_store.begin_source(conn, "src-X")
    consumer_state.register(conn, "alpha", retention_txns=5, pull_limit=2,
                            need_before=True, start_at_zero=True)
    conn.close()
    ingest_all(db)

    conn = source_store.connect(db)
    pull = consumer_state.pull(conn, "alpha")
    token = pull["token"]
    conn.close()

    # 独立进程：在决策完成、COMMIT 之前 os._exit(17)
    proc = run_cli_isolated(db, ["ack", "alpha", token, "--crash-before-commit"])
    check("crash hook exits with code 17", proc.returncode == 17,
          f"rc={proc.returncode} err={proc.stderr[:200]}")

    # 重开数据库：前沿必须停在 0，令牌未被标记 consumed
    conn = source_store.connect(db)
    try:
        st = consumer_state.status(conn, "alpha")
        check("frontier unchanged after crash (no phantom advance)",
              st["frontier_commit_seq"] == 0 and st["frontier_end_pos"] == 0,
              json.dumps(st))
        trow = conn.execute("SELECT consumed FROM pull_token WHERE token=?", (token,)).fetchone()
        check("reservation token not consumed after crash", trow["consumed"] == 0, str(dict(trow)))
        # 用同一令牌在新进程/新连接里确认成功，且只推进一次
        ack1 = consumer_state.ack(conn, "alpha", token)
        ack2 = consumer_state.ack(conn, "alpha", token)
        check("post-crash ack advances exactly once",
              ack1["duplicate_ack"] is False and ack1["advanced_to_seq"] == 2
              and ack2["duplicate_ack"] is True and ack2["advanced_to_seq"] == 2,
              f"{ack1} {ack2}")
        # 后续拉取连续，无空洞
        pull2 = consumer_state.pull(conn, "alpha")
        check("next pull resumes at contiguous seq 3",
              [d["commit_seq"] for d in pull2["deliveries"]] == [3, 4],
              str([d["commit_seq"] for d in pull2["deliveries"]]))
    finally:
        conn.close()


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="ccs_conc_")
    print("workdir:", tmp)
    part_a_concurrent(tmp)
    part_b_revoke(tmp)
    part_c_crash(tmp)

    print(f"\nPASS {len(PASSES)} checks")
    if FAILS:
        print(f"FAIL {len(FAILS)} checks:")
        for f in FAILS:
            print("  -", f)
        return 1
    print("concurrent/crash invariants hold: no missing delivery, no frontier overshoot, "
          "no premature deletion.")
    print("artifacts kept at:", tmp)
    return 0


if __name__ == "__main__":
    sys.exit(main())
