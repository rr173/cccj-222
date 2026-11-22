"""清理模块：唯一允许删除源变更的地方。

删除一条完整事务必须同时满足：
  1. 它对每个活跃消费者都不再需要——即事务序号低于所有消费者的
     "保留高水位"（连续前沿往前数 retention_txns 个事务）；
  2. 没有任何未 ACK 的迟到通知引用它（迟到保留范围）；
  3. 事务本身已经 complete（open 事务永不删除）。

撤销的消费者不再贡献保留需求；重新注册的消费者从新前沿算起。
窗口缩短只改变下一次清理的裁决，已删除数据无法复活，这也是保留窗口的契约。
清理决定与删除动作在同一个 SQLite 事务内持久化，并写 cleanup_log 作为依据。
"""
from __future__ import annotations

import sqlite3

from .source_store import StoreError


def _consumer_protected_floor(conn: sqlite3.Connection, consumer: sqlite3.Row) -> int:
    """单个消费者仍需要的最旧事务序号（含）。

    floor = max(注册起点, 前沿 - retention_txns + 1)，且不小于 1。
    注册起点保证新消费者不会"需要"它注册前就被删掉的数据。
    """
    frontier = consumer["acked_commit_seq"]
    retention = consumer["retention_txns"]
    floor = frontier - retention + 1
    if floor < 1:
        floor = 1
    # 注册之前的事务不属于该消费者的可重放责任范围
    if floor <= consumer["reg_commit_seq"]:
        floor = consumer["reg_commit_seq"] + 1
    return floor


def retention_plan(conn: sqlite3.Connection) -> dict:
    """计算当前的清理裁决，不执行删除。供状态输出和演练断言使用。"""
    active = list(conn.execute(
        "SELECT * FROM consumer WHERE active=1"
    ).fetchall())
    per_consumer = {}
    # 全局安全线 = 所有活跃消费者保留下界的最小值：
    # 最慢的消费者需要最旧的数据，它的需求决定什么还不能删（慢消费者不阻塞别人，
    # 但它需要的历史任何人都不能删）。
    global_floor: int | None = None
    for c in active:
        floor = _consumer_protected_floor(conn, c)
        per_consumer[c["name"]] = {
            "frontier_commit_seq": c["acked_commit_seq"],
            "retention_txns": c["retention_txns"],
            "protected_from_seq": floor,
        }
        if global_floor is None or floor < global_floor:
            global_floor = floor

    # 迟到保留：任何未 ACK 的迟到通知都钉住对应事务
    pinned = {
        r["txn_id"]: r["consumer"]
        for r in conn.execute(
            "SELECT txn_id, consumer FROM late_notice WHERE status != 'ACKED'"
        ).fetchall()
    }

    candidates = []
    protected = []
    for t in conn.execute(
        "SELECT * FROM txn WHERE state='complete' ORDER BY commit_seq"
    ).fetchall():
        reasons = []
        if global_floor is not None and t["commit_seq"] >= global_floor:
            reasons.append(
                f"commit_seq {t['commit_seq']} >= slowest-consumer retention floor {global_floor}"
            )
        if t["txn_id"] in pinned:
            reasons.append(f"late notice open for consumer {pinned[t['txn_id']]!r}")
        if reasons:
            protected.append({"txn_id": t["txn_id"], "commit_seq": t["commit_seq"],
                              "reasons": reasons})
        else:
            candidates.append({"txn_id": t["txn_id"], "commit_seq": t["commit_seq"],
                               "begin_pos": t["begin_pos"], "end_pos": t["end_pos"]})
    return {
        "active_consumers": [c["name"] for c in active],
        "per_consumer": per_consumer,
        "global_protected_floor_seq": global_floor,
        "late_pinned_txn_ids": sorted(pinned),
        "deletable": candidates,
        "protected": protected,
    }


def run_cleanup(conn: sqlite3.Connection) -> dict:
    """执行清理。裁决 + 删除 + 审计在同一个事务内，崩溃不会留下半删状态。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        plan = retention_plan(conn)
        deletable = plan["deletable"]
        deleted = []
        for item in deletable:
            txn_id = item["txn_id"]
            trow = conn.execute("SELECT * FROM txn WHERE txn_id=?", (txn_id,)).fetchone()
            # 再次核对：迟到通知（防御并发下的瞬时变化，事务锁已保证，这里是显式断言）
            late = conn.execute(
                "SELECT 1 FROM late_notice WHERE txn_id=? AND status != 'ACKED' LIMIT 1",
                (txn_id,),
            ).fetchone()
            if late:
                continue
            reason = (
                f"below all active consumer retention floors "
                f"(global floor seq {plan['global_protected_floor_seq']}); "
                "no open late notice"
            )
            conn.execute("DELETE FROM change_row WHERE txn_id=?", (txn_id,))
            conn.execute("DELETE FROM late_notice WHERE txn_id=?", (txn_id,))
            conn.execute("DELETE FROM txn WHERE txn_id=?", (txn_id,))
            conn.execute(
                "INSERT INTO cleanup_log(deleted_txn_id, commit_seq, begin_pos, end_pos, reason) "
                "VALUES (?,?,?,?,?)",
                (txn_id, trow["commit_seq"], trow["begin_pos"], trow["end_pos"], reason),
            )
            deleted.append({"txn_id": txn_id, "commit_seq": item["commit_seq"],
                            "position_range": [item["begin_pos"], item["end_pos"]],
                            "reason": reason})
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return {
        "deleted_count": len(deleted),
        "deleted": deleted,
        "basis": {
            "active_consumers": plan["active_consumers"],
            "per_consumer": plan["per_consumer"],
            "global_protected_floor_seq": plan["global_protected_floor_seq"],
            "late_pinned_txn_ids": plan["late_pinned_txn_ids"],
        },
        "still_protected": plan["protected"],
    }


def cleanup_log(conn: sqlite3.Connection) -> list[dict]:
    return [dict(r) for r in conn.execute(
        "SELECT * FROM cleanup_log ORDER BY id"
    ).fetchall()]
