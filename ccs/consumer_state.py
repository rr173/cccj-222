"""消费者状态模块：注册、拉取预留、确认、迟到补发、重放窗口、撤销。

并发与持久化模型
----------------
每个接口在单个 SQLite 事务（BEGIN IMMEDIATE）内完成"读状态 + 决策 + 写状态"，
进程在响应前退出时，事务要么整体提交、要么整体回滚：
  * 拉取预留先把响应快照写入 pull_token，重复拉取同一令牌返回同一快照，不推进前沿；
  * 确认只接受连续前沿的下一个完整事务，跳跃确认拒绝；重复确认返回同一结果；
  * 迟到事务进入每个消费者独立的 late_notice，补发不推进已确认前沿，
    也不改写任何已确认快照。

令牌绑定三元组：(source_version, 签发时连续前沿, 查询参数哈希)，
换源或改参数后旧令牌一律拒绝。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from typing import Any, Optional

from . import source_store
from .source_store import StoreError
from .netchange import fold_txn_from_db, apply_consumer_view


# --------------------------------------------------------------------------- #
# 参数与令牌
# --------------------------------------------------------------------------- #
def params_hash(retention_txns: int, pull_limit: int, need_before: bool) -> str:
    payload = json.dumps(
        {"retention_txns": retention_txns,
         "pull_limit": pull_limit,
         "need_before": bool(need_before)},
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _get_consumer(conn: sqlite3.Connection, name: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM consumer WHERE name=?", (name,)).fetchone()
    if row is None:
        raise StoreError("UNKNOWN_CONSUMER", f"consumer {name!r} is not registered")
    if not row["active"]:
        raise StoreError("CONSUMER_REVOKED", f"consumer {name!r} has been revoked")
    return row


def _check_token(conn: sqlite3.Connection, consumer: sqlite3.Row, token: str,
                 allow_consumed: bool = False) -> sqlite3.Row:
    """校验令牌：存在、属于该消费者、绑定源版本/前沿/参数。

    allow_consumed=True（ack 路径）时允许已消费令牌，用于重复确认返回同一结果；
    其他路径（带 token 重拉、replay）拒绝已消费令牌。
    """
    trow = conn.execute("SELECT * FROM pull_token WHERE token=?", (token,)).fetchone()
    if trow is None:
        raise StoreError("BAD_TOKEN", "token does not exist")
    if trow["consumer"] != consumer["name"]:
        raise StoreError("TOKEN_OWNER_MISMATCH", "token belongs to another consumer")
    active_source = source_store.get_source_version(conn)
    if trow["source_version"] != active_source or trow["source_version"] != consumer["source_version"]:
        raise StoreError("TOKEN_SOURCE_STALE",
                         "token is bound to an old source version; re-pull with the new source")
    expect_hash = params_hash(consumer["retention_txns"], consumer["pull_limit"],
                              bool(consumer["need_before"]))
    if trow["params_hash"] != expect_hash:
        raise StoreError("TOKEN_PARAMS_STALE",
                         "query parameters changed; old token rejected, please pull again")
    if trow["consumed"]:
        # 已消费令牌：前沿必然已经推进，只允许 ack 路径做幂等重复确认
        if not allow_consumed:
            raise StoreError("TOKEN_CONSUMED", "token already acknowledged; pull a fresh one")
        return trow
    if trow["issued_at_acked_seq"] != consumer["acked_commit_seq"]:
        raise StoreError("TOKEN_FRONTIER_STALE",
                         "token was issued at a different frontier; pull a fresh token")
    return trow


# --------------------------------------------------------------------------- #
# 注册 / 撤销 / 改参
# --------------------------------------------------------------------------- #
def register(conn: sqlite3.Connection, name: str, retention_txns: int = 10,
             pull_limit: int = 100, need_before: bool = False,
             start_at_zero: bool = False) -> dict:
    """注册消费者。新消费者默认从注册时刻的最新完整事务之后开始（只读新数据）。

    start_at_zero=True 时从空前沿开始，可重放全部历史。
    撤销后重新注册是全新状态：前沿重置，但其他消费者的数据不受影响。
    """
    if retention_txns < 0 or pull_limit <= 0:
        raise StoreError("BAD_PARAMS", "retention_txns >= 0 and pull_limit > 0 required")
    source_version = source_store.get_source_version(conn)
    if source_version is None:
        raise StoreError("NO_SOURCE", "call begin_source before registering consumers")

    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = conn.execute(
            "SELECT * FROM consumer WHERE name=?", (name,)
        ).fetchone()
        if existing and existing["active"]:
            raise StoreError("DUP_CONSUMER", f"consumer {name!r} already registered")
        top = source_store.highest_commit(conn, source_version)
        if start_at_zero or top is None:
            start_seq, start_pos = 0, 0
        else:
            start_seq, start_pos = top["commit_seq"], top["end_pos"]
        meta = conn.execute("SELECT next_arrival FROM meta WHERE id=1").fetchone()
        reg_arrival = meta["next_arrival"] - 1  # 当前已分配的最大 arrival_seq

        if existing:  # 撤销后重新注册：清掉旧令牌与旧迟到通知
            conn.execute("DELETE FROM pull_token WHERE consumer=?", (name,))
            conn.execute("DELETE FROM late_notice WHERE consumer=?", (name,))
            conn.execute(
                "UPDATE consumer SET source_version=?, reg_commit_seq=?, reg_arrival_seq=?, "
                "acked_commit_seq=?, acked_end_pos=?, retention_txns=?, pull_limit=?, "
                "need_before=?, active=1, revoked_at=NULL WHERE name=?",
                (source_version, start_seq, reg_arrival, start_seq, start_pos,
                 retention_txns, pull_limit, int(bool(need_before)), name),
            )
        else:
            conn.execute(
                "INSERT INTO consumer(name, source_version, reg_commit_seq, reg_arrival_seq, "
                "acked_commit_seq, acked_end_pos, retention_txns, pull_limit, need_before, active) "
                "VALUES (?,?,?,?,?,?,?,?,?,1)",
                (name, source_version, start_seq, reg_arrival, start_seq, start_pos,
                 retention_txns, pull_limit, int(bool(need_before))),
            )
        conn.execute("COMMIT")
    except StoreError:
        conn.execute("ROLLBACK")
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {
        "consumer": name,
        "source_version": source_version,
        "frontier_commit_seq": start_seq,
        "frontier_end_pos": start_pos,
        "retention_txns": retention_txns,
        "pull_limit": pull_limit,
        "need_before": bool(need_before),
    }


def revoke(conn: sqlite3.Connection, name: str) -> dict:
    """撤销消费者：其保留需求立即消失，但不删除任何源数据（清理由 cleanup 模块裁决）。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        _get_consumer(conn, name)
        conn.execute("DELETE FROM pull_token WHERE consumer=?", (name,))
        conn.execute(
            "UPDATE consumer SET active=0, revoked_at=datetime('now') WHERE name=?",
            (name,),
        )
        conn.execute("COMMIT")
    except StoreError:
        conn.execute("ROLLBACK")
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {"consumer": name, "revoked": True}


def update_params(conn: sqlite3.Connection, name: str, *,
                  retention_txns: Optional[int] = None,
                  pull_limit: Optional[int] = None,
                  need_before: Optional[bool] = None) -> dict:
    """修改查询参数。缩短窗口不会删除任何数据（清理模块另有裁决），旧令牌立即失效。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        c = _get_consumer(conn, name)
        new_ret = c["retention_txns"] if retention_txns is None else retention_txns
        new_limit = c["pull_limit"] if pull_limit is None else pull_limit
        new_before = c["need_before"] if need_before is None else int(bool(need_before))
        if new_ret < 0 or new_limit <= 0:
            raise StoreError("BAD_PARAMS", "retention_txns >= 0 and pull_limit > 0 required")
        # 不删除令牌行：保留下来才能对旧令牌给出明确的 TOKEN_PARAMS_STALE 裁决
        conn.execute(
            "UPDATE consumer SET retention_txns=?, pull_limit=?, need_before=? WHERE name=?",
            (new_ret, new_limit, new_before, name),
        )
        conn.execute("COMMIT")
    except StoreError:
        conn.execute("ROLLBACK")
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {"consumer": name, "retention_txns": new_ret,
            "pull_limit": new_limit, "need_before": bool(new_before),
            "note": "all previous tokens invalidated"}


# --------------------------------------------------------------------------- #
# 迟到通知（摄入后由服务层调用；也在注册后补扫）
# --------------------------------------------------------------------------- #
def scan_late_for_consumer(conn: sqlite3.Connection, consumer: sqlite3.Row) -> int:
    """找出位置落在该消费者已确认区间、但在注册/确认之后才到达的事务，登记补发通知。

    迟到的两个必要条件（缺一不可）：
      a. 事务整体位于已确认前沿之内（end_pos <= acked_end_pos）——不能改写已确认快照，
         只能旁路补发；
      b. 事务在消费者注册之后才到达（arrival_seq > reg_arrival_seq）——
         注册前就存在的历史数据属于正常重放范围，不是迟到。
    """
    sv = consumer["source_version"]
    frontier_pos = consumer["acked_end_pos"]
    rows = conn.execute(
        """
        SELECT t.txn_id FROM txn t
        WHERE t.source_version=? AND t.state='complete'
          AND t.end_pos <= ? AND t.arrival_seq > ?
        ORDER BY t.commit_seq
        """,
        (sv, frontier_pos, consumer["reg_arrival_seq"]),
    ).fetchall()
    added = 0
    for r in rows:
        cur = conn.execute(
            "INSERT OR IGNORE INTO late_notice(consumer, txn_id, status) VALUES (?,?, 'PENDING')",
            (consumer["name"], r["txn_id"]),
        )
        added += cur.rowcount
    return added


def notify_late_after_ingest(conn: sqlite3.Connection, txn_id: str) -> list[str]:
    """摄入新事务后，对所有活跃消费者判断它是否迟到并登记。

    迟到 = 事务在消费者注册之后才到达（arrival_seq > reg_arrival_seq）
    且位置整体落在该消费者已确认前沿之内（end_pos <= acked_end_pos）。
    已确认快照不被改写，只通过 late_notice 旁路补发。
    """
    txn = source_store.get_txn(conn, txn_id)
    if txn is None:
        return []
    out: list[str] = []
    for c in conn.execute("SELECT * FROM consumer WHERE active=1").fetchall():
        if c["source_version"] != txn["source_version"]:
            continue
        if txn["arrival_seq"] <= c["reg_arrival_seq"]:
            continue  # 注册之前就存在的事务，属于正常历史重放，不是迟到
        if txn["end_pos"] <= c["acked_end_pos"]:
            cur = conn.execute(
                "INSERT OR IGNORE INTO late_notice(consumer, txn_id, status) "
                "VALUES (?,?, 'PENDING')",
                (c["name"], txn_id),
            )
            if cur.rowcount:
                out.append(c["name"])
    return out


# --------------------------------------------------------------------------- #
# 拉取（预留）
# --------------------------------------------------------------------------- #
def _build_delivery(conn: sqlite3.Connection, consumer: sqlite3.Row,
                    txn_row: sqlite3.Row) -> dict:
    folded = fold_txn_from_db(conn, txn_row["txn_id"])
    records = [apply_consumer_view(r, bool(consumer["need_before"])) for r in folded]
    return {
        "txn_id": txn_row["txn_id"],
        "commit_seq": txn_row["commit_seq"],
        "txn_boundary": {"begin_pos": txn_row["begin_pos"], "end_pos": txn_row["end_pos"]},
        "source_position_range": [txn_row["begin_pos"], txn_row["end_pos"]],
        "records": records,
        "record_count": len(records),
    }


def pull(conn: sqlite3.Connection, name: str, token: Optional[str] = None) -> dict:
    """拉取下一批净变化。

    * 不带 token：签发新令牌并预留最多 pull_limit 个完整事务（至少 1 个，有数据时）；
    * 带 token：幂等返回预留快照，绝不重新预留、绝不推进前沿；
    * 内存受 pull_limit 约束：只装载本批事务的变更行，积压全部留在磁盘上。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        consumer = _get_consumer(conn, name)
        sv = consumer["source_version"]
        if source_store.get_source_version(conn) != sv:
            raise StoreError("SOURCE_SWITCHED",
                             "active source changed; re-register or re-pull is required")

        if token is not None:
            trow = _check_token(conn, consumer, token)
            conn.execute("COMMIT")
            return {
                "token": trow["token"],
                "idempotent_replay": True,
                "consumer": name,
                "frontier_commit_seq": consumer["acked_commit_seq"],
                "deliveries": json.loads(trow["response_json"]),
            }

        # 迟到通知只在摄入时刻由 notify_late_after_ingest 裁决，这里不补扫，
        # 避免把正常历史重放误标成迟到。
        frontier = consumer["acked_commit_seq"]
        batch = list(conn.execute(
            "SELECT * FROM txn WHERE state='complete' AND source_version=? "
            "AND commit_seq > ? ORDER BY commit_seq LIMIT ?",
            (sv, frontier, consumer["pull_limit"]),
        ).fetchall())

        deliveries = [_build_delivery(conn, consumer, t) for t in batch]
        high_seq = batch[-1]["commit_seq"] if batch else frontier
        high_pos = batch[-1]["end_pos"] if batch else consumer["acked_end_pos"]

        new_token = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO pull_token(token, consumer, source_version, params_hash, "
            "issued_at_acked_seq, high_commit_seq, high_end_pos, response_json) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (new_token, name, sv,
             params_hash(consumer["retention_txns"], consumer["pull_limit"],
                         bool(consumer["need_before"])),
             frontier, high_seq, high_pos, json.dumps(deliveries)),
        )
        conn.execute("COMMIT")
    except StoreError:
        conn.execute("ROLLBACK")
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return {
        "token": new_token,
        "idempotent_replay": False,
        "consumer": name,
        "source_version": sv,
        "frontier_commit_seq": frontier,
        "pull_limit": consumer["pull_limit"],
        "reserved_high_commit_seq": high_seq,
        "reserved_high_end_pos": high_pos,
        "deliveries": deliveries,
        "late_pending": count_pending_late(conn, name),
    }


def count_pending_late(conn: sqlite3.Connection, name: str) -> int:
    return conn.execute(
        "SELECT COUNT(*) AS c FROM late_notice WHERE consumer=? AND status != 'ACKED'",
        (name,),
    ).fetchone()["c"]


# --------------------------------------------------------------------------- #
# 确认（连续前沿）
# --------------------------------------------------------------------------- #
def ack(conn: sqlite3.Connection, name: str, token: str, crash_before_commit: bool = False) -> dict:
    """用拉取令牌确认预留区间。

    连续前沿规则：只允许确认 acked_seq+1 .. 令牌预留高水位 的完整区间，
    令牌必须恰好签发在当前前沿上。跳跃确认（中间事务缺失）拒绝。
    重复确认：同一令牌第二次提交直接返回首次结果，前沿不重复推进。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        consumer = _get_consumer(conn, name)
        # allow_consumed：已消费令牌走到这里即重复确认，返回首次结果且不重复推进
        trow = _check_token(conn, consumer, token, allow_consumed=True)
        first_result = {
            "consumer": name,
            "token": token,
            "advanced_from_seq": consumer["acked_commit_seq"]
                                  if not trow["consumed"] else trow["issued_at_acked_seq"],
            "advanced_to_seq": trow["high_commit_seq"],
            "advanced_to_end_pos": trow["high_end_pos"],
            "txn_ids": [d["txn_id"] for d in json.loads(trow["response_json"])],
        }
        if trow["consumed"]:
            # 重复确认：前沿已经在首次确认时推进，这里原样返回同一结果
            conn.execute("COMMIT")
            first_result["duplicate_ack"] = True
            first_result["late_notices_created"] = 0
            return first_result

        # 连续性保证：令牌只在当前前沿上签发，且预留的是 commit_seq 连续的下一批，
        # 因此 high_commit_seq 与前沿之间不可能存在空洞。这里做一次显式断言式核对。
        if trow["high_commit_seq"] > consumer["acked_commit_seq"]:
            gap_count = conn.execute(
                "SELECT COUNT(*) AS c FROM txn WHERE source_version=? AND state='complete' "
                "AND commit_seq > ? AND commit_seq <= ?",
                (consumer["source_version"], consumer["acked_commit_seq"],
                 trow["high_commit_seq"]),
            ).fetchone()["c"]
            expected = trow["high_commit_seq"] - consumer["acked_commit_seq"]
            if gap_count != expected:
                raise StoreError(
                    "JUMP_ACK",
                    f"refusing ack: reserved window would skip transactions "
                    f"(have {gap_count}, expect {expected})",
                )

        if crash_before_commit:
            # 演练钩子：决策完成、提交前杀进程，验证重启后无重复推进/空洞
            conn.execute("ROLLBACK")
            import os
            os._exit(17)

        conn.execute(
            "UPDATE consumer SET acked_commit_seq=?, acked_end_pos=? WHERE name=?",
            (trow["high_commit_seq"], trow["high_end_pos"], name),
        )
        conn.execute("UPDATE pull_token SET consumed=1 WHERE token=?", (token,))
        # 迟到通知只在事务摄入时刻裁决（notify_late_after_ingest）：
        # 这里不能补扫，否则会把"注册后、确认前"正常交付过的事务误判为迟到。
        conn.execute("COMMIT")
    except StoreError:
        conn.execute("ROLLBACK")
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise

    first_result["duplicate_ack"] = False
    first_result["late_notices_created"] = 0
    return first_result


def jump_ack_probe(conn: sqlite3.Connection, name: str, target_commit_seq: int) -> dict:
    """显式演练接口：尝试跳过中间事务直接确认到 target_commit_seq，必须被拒绝。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        consumer = _get_consumer(conn, name)
        if target_commit_seq <= consumer["acked_commit_seq"]:
            raise StoreError("BAD_ACK", "target is not ahead of the frontier")
        missing = conn.execute(
            "SELECT COUNT(*) AS c FROM txn WHERE source_version=? AND state='complete' "
            "AND commit_seq > ? AND commit_seq < ?",
            (consumer["source_version"], consumer["acked_commit_seq"], target_commit_seq),
        ).fetchone()["c"]
        if missing:
            raise StoreError(
                "JUMP_ACK",
                f"refusing to ack seq {target_commit_seq}: {missing} transaction(s) "
                f"after frontier {consumer['acked_commit_seq']} are not confirmed",
            )
        conn.execute("COMMIT")
        raise StoreError("BAD_ACK", "target equals the next contiguous seq; use normal ack")
    except StoreError:
        conn.execute("ROLLBACK")
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise


# --------------------------------------------------------------------------- #
# 迟到补发拉取与确认
# --------------------------------------------------------------------------- #
def pull_late(conn: sqlite3.Connection, name: str) -> dict:
    """拉取迟到补发。每条通知独立投递、独立确认，不移动连续前沿。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        consumer = _get_consumer(conn, name)
        notices = list(conn.execute(
            "SELECT ln.*, t.begin_pos, t.end_pos, t.commit_seq FROM late_notice ln "
            "JOIN txn t ON t.txn_id=ln.txn_id "
            "WHERE ln.consumer=? AND ln.status='PENDING' ORDER BY t.commit_seq",
            (name,),
        ).fetchall())
        deliveries = []
        for n in notices:
            txn_row = source_store.get_txn(conn, n["txn_id"])
            delivery = _build_delivery(conn, consumer, txn_row)
            delivery["late"] = True
            delivery["late_reason"] = (
                f"txn end_pos {txn_row['end_pos']} <= confirmed frontier "
                f"{consumer['acked_end_pos']}; delivered out-of-band, snapshot not rewritten"
            )
            deliveries.append(delivery)
            conn.execute(
                "UPDATE late_notice SET status='DELIVERED' WHERE id=?", (n["id"],)
            )
        conn.execute("COMMIT")
    except StoreError:
        conn.execute("ROLLBACK")
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {
        "consumer": name,
        "frontier_commit_seq": consumer["acked_commit_seq"],
        "frontier_end_pos": consumer["acked_end_pos"],
        "late_deliveries": deliveries,
        "late_remaining_pending": count_pending_late(conn, name),
    }


def ack_late(conn: sqlite3.Connection, name: str, txn_id: str) -> dict:
    """确认一条迟到补发。幂等：重复确认返回 ACKED，不重复投递。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        _get_consumer(conn, name)
        row = conn.execute(
            "SELECT * FROM late_notice WHERE consumer=? AND txn_id=?",
            (name, txn_id),
        ).fetchone()
        if row is None:
            raise StoreError("UNKNOWN_LATE", f"no late notice for txn {txn_id!r}")
        before = row["status"]
        conn.execute(
            "UPDATE late_notice SET status='ACKED' WHERE consumer=? AND txn_id=?",
            (name, txn_id),
        )
        conn.execute("COMMIT")
    except StoreError:
        conn.execute("ROLLBACK")
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {"consumer": name, "txn_id": txn_id, "status": "ACKED",
            "already_acked": before == "ACKED", "frontier_unchanged": True}


# --------------------------------------------------------------------------- #
# 重放窗口
# --------------------------------------------------------------------------- #
def replay_window(conn: sqlite3.Connection, name: str, low_pos: int, high_pos: int,
                  token: Optional[str] = None) -> dict:
    """按源位置区间重放已存储变更（仍按事务逐笔折叠，不跨事务合并）。

    窗口必须落在消费者保留范围内；带 token 时校验同一套绑定规则。
    迟到事务若位置在窗口内，会带 late=true 标记，但快照语义不变。
    """
    if low_pos < 0 or high_pos < low_pos:
        raise StoreError("BAD_WINDOW", "require 0 <= low_pos <= high_pos")
    conn.execute("BEGIN IMMEDIATE")
    try:
        consumer = _get_consumer(conn, name)
        if token is not None:
            _check_token(conn, consumer, token)
        sv = consumer["source_version"]
        # 保留窗口下界：前沿往前 retention_txns 个事务的 begin_pos
        bound = conn.execute(
            "SELECT MIN(begin_pos) AS lo FROM (SELECT begin_pos FROM txn "
            "WHERE source_version=? AND state='complete' AND commit_seq <= ? "
            "ORDER BY commit_seq DESC LIMIT ?)",
            (sv, consumer["acked_commit_seq"], max(consumer["retention_txns"], 1)),
        ).fetchone()
        retention_low = bound["lo"] if bound and bound["lo"] is not None else 0
        if low_pos < retention_low:
            raise StoreError(
                "WINDOW_OUTSIDE_RETENTION",
                f"low_pos {low_pos} is below retention floor {retention_low}; "
                "shorten the window or extend retention",
            )
        rows = source_store.rows_in_position_window(conn, sv, low_pos, high_pos)
        # 按事务分组，逐笔折叠（绝不跨事务合并）
        txn_ids: list[str] = []
        for r in rows:
            if not txn_ids or txn_ids[-1] != r["txn_id"]:
                txn_ids.append(r["txn_id"])
        late_ids = {
            r["txn_id"] for r in conn.execute(
                "SELECT txn_id FROM late_notice WHERE consumer=?", (name,)
            ).fetchall()
        }
        deliveries = []
        for txn_id in txn_ids:
            txn_row = source_store.get_txn(conn, txn_id)
            d = _build_delivery(conn, consumer, txn_row)
            d["late"] = txn_id in late_ids
            deliveries.append(d)
        conn.execute("COMMIT")
    except StoreError:
        conn.execute("ROLLBACK")
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {
        "consumer": name,
        "window": [low_pos, high_pos],
        "retention_floor_pos": retention_low,
        "deliveries": deliveries,
    }


def status(conn: sqlite3.Connection, name: str) -> dict:
    c = _get_consumer(conn, name)
    return {
        "consumer": name,
        "source_version": c["source_version"],
        "active": bool(c["active"]),
        "frontier_commit_seq": c["acked_commit_seq"],
        "frontier_end_pos": c["acked_end_pos"],
        "retention_txns": c["retention_txns"],
        "pull_limit": c["pull_limit"],
        "need_before": bool(c["need_before"]),
        "late_open": count_pending_late(conn, name),
    }
