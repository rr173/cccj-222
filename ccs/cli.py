#!/usr/bin/env python3
"""变更订阅服务的命令行入口。

每个命令都是一次独立进程调用：打开 SQLite -> 单事务决策 -> 提交 -> 退出。
输出统一为 JSON：成功 {"ok": true, "data": ...}，失败 {"ok": false, "error": {...}}。
这样演练脚本可以用真实进程安排拉取/确认/清理交错。
"""
from __future__ import annotations

import argparse
import json
import sys

from ccs import source_store, consumer_state, cleanup
from ccs.source_store import StoreError


def _print(payload: dict, code: int = 0) -> int:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n")
    return code


def _ok(data) -> int:
    return _print({"ok": True, "data": data})


def _fail(exc: StoreError) -> int:
    return _print({"ok": False, "error": exc.to_dict()}, code=2)


def _load_segment(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="ccs", description="本地变更订阅服务 CLI")
    p.add_argument("--db", required=True, help="SQLite 数据库文件路径")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init", help="初始化空库（也会在首次访问时自动建表）")

    sp = sub.add_parser("begin-source")
    sp.add_argument("source_version")

    sw = sub.add_parser("switch-source")
    sw.add_argument("new_source_version")

    ing = sub.add_parser("ingest", help="摄入一个完整事务变更段")
    ing.add_argument("segment_json", help="段 JSON 文件路径")

    reg = sub.add_parser("register")
    reg.add_argument("name")
    reg.add_argument("--retention", type=int, default=10)
    reg.add_argument("--pull-limit", type=int, default=100)
    reg.add_argument("--need-before", action="store_true")
    reg.add_argument("--start-at-zero", action="store_true")

    rvk = sub.add_parser("revoke")
    rvk.add_argument("name")

    upp = sub.add_parser("update-params")
    upp.add_argument("name")
    upp.add_argument("--retention", type=int)
    upp.add_argument("--pull-limit", type=int)
    upp.add_argument("--need-before", type=lambda s: s.lower() in ("1", "true", "yes"))

    pull = sub.add_parser("pull")
    pull.add_argument("name")
    pull.add_argument("--token", default=None)

    ack = sub.add_parser("ack")
    ack.add_argument("name")
    ack.add_argument("token")
    ack.add_argument("--crash-before-commit", action="store_true",
                     help="演练钩子：决策后提交前以退出码 17 杀掉进程")

    jmp = sub.add_parser("jump-ack")
    jmp.add_argument("name")
    jmp.add_argument("target_commit_seq", type=int)

    pl = sub.add_parser("pull-late")
    pl.add_argument("name")

    al = sub.add_parser("ack-late")
    al.add_argument("name")
    al.add_argument("txn_id")

    rw = sub.add_parser("replay")
    rw.add_argument("name")
    rw.add_argument("low_pos", type=int)
    rw.add_argument("high_pos", type=int)
    rw.add_argument("--token", default=None)

    st = sub.add_parser("status")
    st.add_argument("name")

    sub.add_parser("cleanup-plan")
    sub.add_parser("cleanup-run")
    sub.add_parser("cleanup-log")
    sub.add_parser("state", help="整体状态输出")
    return p


def _state(conn) -> dict:
    txns = [
        {"txn_id": t["txn_id"], "commit_seq": t["commit_seq"],
         "position_range": [t["begin_pos"], t["end_pos"]],
         "arrival_seq": t["arrival_seq"]}
        for t in conn.execute(
            "SELECT * FROM txn WHERE state='complete' ORDER BY commit_seq"
        ).fetchall()
    ]
    consumers = []
    for c in conn.execute("SELECT * FROM consumer ORDER BY name").fetchall():
        consumers.append({
            "name": c["name"], "active": bool(c["active"]),
            "source_version": c["source_version"],
            "frontier_commit_seq": c["acked_commit_seq"],
            "frontier_end_pos": c["acked_end_pos"],
            "retention_txns": c["retention_txns"],
            "pull_limit": c["pull_limit"],
            "need_before": bool(c["need_before"]),
            "late_open": consumer_state.count_pending_late(conn, c["name"]),
        })
    return {
        "source_version": source_store.get_source_version(conn),
        "transactions": txns,
        "consumers": consumers,
        "cleanup_plan": cleanup.retention_plan(conn),
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    conn = source_store.connect(args.db)
    try:
        if args.cmd == "init":
            return _ok({"initialized": args.db})
        if args.cmd == "begin-source":
            return _ok(source_store.begin_source(conn, args.source_version))
        if args.cmd == "switch-source":
            return _ok(source_store.switch_source(conn, args.new_source_version))
        if args.cmd == "ingest":
            result = source_store.ingest_segment(conn, _load_segment(args.segment_json))
            # 摄入与迟到通知在同一进程的两个连续事务里；通知本身幂等（INSERT OR IGNORE）
            result["late_notified_consumers"] = consumer_state.notify_late_after_ingest(
                conn, result["txn_id"])
            return _ok(result)
        if args.cmd == "register":
            return _ok(consumer_state.register(
                conn, args.name, retention_txns=args.retention,
                pull_limit=args.pull_limit, need_before=args.need_before,
                start_at_zero=args.start_at_zero))
        if args.cmd == "revoke":
            return _ok(consumer_state.revoke(conn, args.name))
        if args.cmd == "update-params":
            return _ok(consumer_state.update_params(
                conn, args.name, retention_txns=args.retention,
                pull_limit=args.pull_limit, need_before=args.need_before))
        if args.cmd == "pull":
            return _ok(consumer_state.pull(conn, args.name, args.token))
        if args.cmd == "ack":
            return _ok(consumer_state.ack(
                conn, args.name, args.token,
                crash_before_commit=args.crash_before_commit))
        if args.cmd == "jump-ack":
            return _ok(consumer_state.jump_ack_probe(
                conn, args.name, args.target_commit_seq))
        if args.cmd == "pull-late":
            return _ok(consumer_state.pull_late(conn, args.name))
        if args.cmd == "ack-late":
            return _ok(consumer_state.ack_late(conn, args.name, args.txn_id))
        if args.cmd == "replay":
            return _ok(consumer_state.replay_window(
                conn, args.name, args.low_pos, args.high_pos, args.token))
        if args.cmd == "status":
            return _ok(consumer_state.status(conn, args.name))
        if args.cmd == "cleanup-plan":
            return _ok(cleanup.retention_plan(conn))
        if args.cmd == "cleanup-run":
            return _ok(cleanup.run_cleanup(conn))
        if args.cmd == "cleanup-log":
            return _ok({"entries": cleanup.cleanup_log(conn)})
        if args.cmd == "state":
            return _ok(_state(conn))
        raise AssertionError(f"unhandled command {args.cmd}")
    except StoreError as exc:
        return _fail(exc)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
