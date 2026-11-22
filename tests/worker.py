#!/usr/bin/env python3
"""并发演练 worker：每个 worker 是一个独立的真实 OS 进程。

用法：
  python3 -I tests/worker.py consumer --db DB --name N --rounds R --log LOG
  python3 -I tests/worker.py cleanup  --db DB --log LOG --rounds R

consumer worker：循环 pull -> ack（每轮都是全新进程内的独立连接），
  记录每次交付的 txn_id 顺序与前沿，追加 JSONL 日志。
cleanup worker：循环计算清理计划并执行，记录每次删除依据。

所有日志只追加，驱动进程在结束后做统一不变量核对。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from ccs import source_store, consumer_state, cleanup  # noqa: E402


def append_log(path: str, event: dict) -> None:
    event["pid"] = os.getpid()
    event["ts"] = time.time()
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, ensure_ascii=False) + "\n")


def consumer_loop(db: str, name: str, rounds: int, log: str) -> int:
    idle_polls = 0
    for _ in range(rounds):
        conn = source_store.connect(db)
        try:
            try:
                pulled = consumer_state.pull(conn, name)
            except source_store.StoreError as exc:
                append_log(log, {"kind": "pull_error", "name": name,
                                 "code": exc.code, "message": exc.message})
                time.sleep(0.01)
                continue
            deliveries = pulled["deliveries"]
            if not deliveries:
                idle_polls += 1
                append_log(log, {"kind": "idle", "name": name})
                if idle_polls >= 4:
                    return 0
                time.sleep(0.02)
                continue
            idle_polls = 0
            txn_ids = [d["txn_id"] for d in deliveries]
            seqs = [d["commit_seq"] for d in deliveries]
            append_log(log, {
                "kind": "pull", "name": name, "token": pulled["token"],
                "txn_ids": txn_ids, "commit_seqs": seqs,
                "frontier_before": pulled["frontier_commit_seq"],
            })
            # 用同一令牌确认；独立进程崩溃语义由驱动另做演练
            acked = consumer_state.ack(conn, name, pulled["token"])
            append_log(log, {
                "kind": "ack", "name": name,
                "from": acked["advanced_from_seq"], "to": acked["advanced_to_seq"],
                "txn_ids": acked["txn_ids"], "duplicate": acked["duplicate_ack"],
            })
        finally:
            conn.close()
        time.sleep(0.005)
    return 0


def cleanup_loop(db: str, rounds: int, log: str) -> int:
    for i in range(rounds):
        conn = source_store.connect(db)
        try:
            report = cleanup.run_cleanup(conn)
            append_log(log, {
                "kind": "cleanup", "round": i,
                "deleted": [{"txn_id": x["txn_id"], "commit_seq": x["commit_seq"],
                             "reason": x["reason"]} for x in report["deleted"]],
                "basis": report["basis"],
            })
        finally:
            conn.close()
        time.sleep(0.01)
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("role", choices=["consumer", "cleanup"])
    p.add_argument("--db", required=True)
    p.add_argument("--name")
    p.add_argument("--rounds", type=int, default=50)
    p.add_argument("--log", required=True)
    args = p.parse_args()
    if args.role == "consumer":
        if not args.name:
            p.error("consumer needs --name")
        return consumer_loop(args.db, args.name, args.rounds, args.log)
    return cleanup_loop(args.db, args.rounds, args.log)


if __name__ == "__main__":
    sys.exit(main())
