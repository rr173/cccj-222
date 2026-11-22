"""源端存储模块：只管源版本、事务、变更行三类数据。

职责边界：
  * 源版本（source_version）的创建与密封；
  * 变更段摄入，校验"每个源位置只属于一个完整事务"以及事务区间不重叠；
  * 事务完成时按 end_pos 分配全序 commit_seq（按源提交顺序可重放的基础）；
  * 身份世代 generation 的确定性计算（删除后重新插入 = 新世代）；
  * 迟到识别所需的 arrival_seq。

本模块不感知消费者，也不做净变化折叠。
"""
from __future__ import annotations

import json
import os
import sqlite3
from typing import Any, Iterable, Optional

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")

VALID_OPS = ("INSERT", "UPDATE", "DELETE")


class StoreError(Exception):
    """带稳定错误码的异常，error[code] 供演练脚本断言。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message

    def to_dict(self) -> dict:
        return {"code": self.code, "message": self.message}


def connect(db_path: str) -> sqlite3.Connection:
    """打开服务数据库。每次 CLI 调用都是一个独立进程、独立连接。"""
    fresh = not os.path.exists(db_path)
    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    if fresh:
        with open(SCHEMA_PATH, "r", encoding="utf-8") as fh:
            conn.executescript(fh.read())
    return conn


# --------------------------------------------------------------------------- #
# 源版本
# --------------------------------------------------------------------------- #
def begin_source(conn: sqlite3.Connection, source_version: str) -> dict:
    """声明当前源版本。空库时创建；换源需要显式 seal/新源，旧令牌随后被拒。"""
    row = conn.execute("SELECT source_version FROM meta WHERE id=1").fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO meta(id, source_version, next_arrival) VALUES (1, ?, 1)",
            (source_version,),
        )
        return {"source_version": source_version, "changed": True}
    if row["source_version"] == source_version:
        return {"source_version": source_version, "changed": False}
    raise StoreError(
        "SOURCE_VERSION_CONFLICT",
        f"active source is {row['source_version']!r}, refusing {source_version!r}; "
        "seal the old source before switching",
    )


def get_source_version(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute("SELECT source_version FROM meta WHERE id=1").fetchone()
    return row["source_version"] if row else None


def switch_source(conn: sqlite3.Connection, new_source_version: str) -> dict:
    """管理员显式换源。已注册消费者绑定旧版本，其旧令牌一律拒绝。"""
    old = get_source_version(conn)
    if old is None:
        return begin_source(conn, new_source_version)
    if old == new_source_version:
        raise StoreError("SAME_SOURCE", f"source already {old!r}")
    conn.execute("UPDATE meta SET source_version=? WHERE id=1", (new_source_version,))
    return {"old_source_version": old, "source_version": new_source_version}


def _next_arrival(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT next_arrival FROM meta WHERE id=1").fetchone()
    seq = row["next_arrival"]
    conn.execute("UPDATE meta SET next_arrival=? WHERE id=1", (seq + 1,))
    return seq


# --------------------------------------------------------------------------- #
# 变更段摄入
# --------------------------------------------------------------------------- #
def _validate_ops(ops: list[dict]) -> None:
    if not ops:
        raise StoreError("EMPTY_SEGMENT", "a transaction segment needs at least one op")
    last_pos = None
    seen_pos: set[int] = set()
    for op in ops:
        for key in ("source_position", "row_id", "op"):
            if key not in op:
                raise StoreError("BAD_OP", f"op missing field {key!r}: {op}")
        pos = op["source_position"]
        if not isinstance(pos, int) or pos < 0:
            raise StoreError("BAD_POSITION", f"source_position must be non-negative int: {pos}")
        if pos in seen_pos:
            raise StoreError("DUP_POSITION_IN_SEGMENT", f"position {pos} repeated in segment")
        if last_pos is not None and pos <= last_pos:
            raise StoreError("UNORDERED_SEGMENT", f"positions must be strictly increasing: {last_pos} -> {pos}")
        seen_pos.add(pos)
        last_pos = pos
        if op["op"] not in VALID_OPS:
            raise StoreError("BAD_OP", f"unknown op {op['op']!r}, want one of {VALID_OPS}")
        if op["op"] == "INSERT" and op.get("after") is None:
            raise StoreError("BAD_OP", f"INSERT at {pos} requires after-image")
        if op["op"] == "DELETE" and op.get("before") is None and op.get("before_missing") is not True:
            # 删除允许缺前镜像（墓碑本身即净变化），但必须显式声明
            raise StoreError("BAD_OP", f"DELETE at {pos} requires before-image or before_missing=true")


def _compute_generation(conn: sqlite3.Connection, source_version: str,
                        row_id: str, source_position: int, op: str) -> int:
    """身份世代：同一 row_id 在该源版本中、该位置之前出现过几次 INSERT。

    第一次插入是世代 1；删除后重新插入得到世代 2，依此类推。
    完全由已持久化数据 + 当前段内顺序确定性推导，崩溃重放结果不变。
    """
    prior = conn.execute(
        """
        SELECT COUNT(*) AS c FROM change_row
        WHERE source_version=? AND row_id=? AND source_position < ? AND op='INSERT'
        """,
        (source_version, row_id, source_position),
    ).fetchone()["c"]
    gen = prior + 1
    if op != "INSERT":
        # UPDATE / DELETE 属于"当前活着的世代"，即最近一次 INSERT 的世代；
        # 若此前没有 INSERT（对一个从未见过的行做更新/删除），世代记为 0，语义上是未知世代。
        gen = prior if prior > 0 else 0
    return gen


def ingest_segment(conn: sqlite3.Connection, segment: dict) -> dict:
    """摄入一个完整事务段。

    segment 结构：
      {
        "txn_id": "T1",
        "ops": [
          {"source_position": 10, "row_id": "r1", "op": "INSERT",
           "before": null, "after": {...}, "before_missing": false},
          ...
        ]
      }
    一个 txn_id 只允许完整提交一次；位置区间不得与任何既有事务重叠。
    """
    txn_id = segment.get("txn_id")
    ops = segment.get("ops")
    if not txn_id:
        raise StoreError("BAD_SEGMENT", "segment.txn_id is required")
    _validate_ops(ops)

    source_version = get_source_version(conn)
    if source_version is None:
        raise StoreError("NO_SOURCE", "call begin_source before ingesting segments")

    begin_pos = ops[0]["source_position"]
    end_pos = ops[-1]["source_position"]

    conn.execute("BEGIN IMMEDIATE")
    try:
        if conn.execute("SELECT 1 FROM txn WHERE txn_id=?", (txn_id,)).fetchone():
            raise StoreError("DUP_TXN", f"transaction {txn_id!r} already ingested")
        # 每个源位置只能属于一个事务：区间重叠 / 包含即拒绝
        clash = conn.execute(
            "SELECT txn_id, begin_pos, end_pos FROM txn "
            "WHERE NOT (end_pos < ? OR begin_pos > ?) LIMIT 1",
            (begin_pos, end_pos),
        ).fetchone()
        if clash:
            raise StoreError(
                "POSITION_OVERLAP",
                f"segment [{begin_pos},{end_pos}] overlaps txn {clash['txn_id']} "
                f"[{clash['begin_pos']},{clash['end_pos']}]",
            )
        # 同位置全局唯一（change_row 主键兜底，这里给出明确错误）
        for op in ops:
            hit = conn.execute(
                "SELECT txn_id FROM change_row WHERE source_version=? AND source_position=?",
                (source_version, op["source_position"]),
            ).fetchone()
            if hit:
                raise StoreError(
                    "DUP_POSITION",
                    f"position {op['source_position']} already belongs to txn {hit['txn_id']}",
                )

        arrival_seq = _next_arrival(conn)
        # 完成时刻按 end_pos 分配全序 commit_seq
        max_seq = conn.execute(
            "SELECT COALESCE(MAX(commit_seq), 0) AS m FROM txn WHERE state='complete'"
        ).fetchone()["m"]
        # 迟到的事务 end_pos 可能小于老事务，commit_seq 仍然单调（提交到达顺序），
        # 重放顺序由 begin_pos/end_pos 决定，迟到补发由消费者模块另行处理。
        commit_seq = max_seq + 1

        conn.execute(
            "INSERT INTO txn(txn_id, source_version, begin_pos, end_pos, state, "
            "commit_seq, arrival_seq) VALUES (?,?,?,?,'complete',?,?)",
            (txn_id, source_version, begin_pos, end_pos, commit_seq, arrival_seq),
        )
        for op in ops:
            gen = _compute_generation(conn, source_version, op["row_id"],
                                      op["source_position"], op["op"])
            before = op.get("before")
            after = op.get("after")
            before_missing = bool(op.get("before_missing")) or before is None
            conn.execute(
                "INSERT INTO change_row(source_version, source_position, txn_id, row_id, "
                "op, before_json, after_json, generation, arrival_seq) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    source_version,
                    op["source_position"],
                    txn_id,
                    op["row_id"],
                    op["op"],
                    json.dumps(before, sort_keys=True) if before is not None else None,
                    json.dumps(after, sort_keys=True) if after is not None else None,
                    gen,
                    arrival_seq,
                ),
            )
        conn.execute("COMMIT")
    except StoreError:
        conn.execute("ROLLBACK")
        raise
    except sqlite3.IntegrityError as exc:
        conn.execute("ROLLBACK")
        raise StoreError("INTEGRITY", str(exc)) from exc
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return {
        "txn_id": txn_id,
        "source_version": source_version,
        "position_range": [begin_pos, end_pos],
        "commit_seq": commit_seq,
        "arrival_seq": arrival_seq,
        "late": _is_late(conn, commit_seq),
        "row_count": len(ops),
    }


def _is_late(conn: sqlite3.Connection, commit_seq: int) -> bool:
    """该事务的 end_pos 是否落在某个更早 commit_seq 事务的位置区间之后。

    即：它在源位置序上"插回"了已经交付过的位置区间，属于迟到数据。
    """
    me = conn.execute("SELECT * FROM txn WHERE commit_seq=?", (commit_seq,)).fetchone()
    earlier = conn.execute(
        "SELECT 1 FROM txn WHERE commit_seq < ? AND end_pos > ? LIMIT 1",
        (commit_seq, me["end_pos"]),
    ).fetchone()
    return earlier is not None


# --------------------------------------------------------------------------- #
# 读取辅助（供 netchange / cleanup 模块调用）
# --------------------------------------------------------------------------- #
def list_committed_txns(conn: sqlite3.Connection, source_version: str) -> list[sqlite3.Row]:
    return list(conn.execute(
        "SELECT * FROM txn WHERE state='complete' AND source_version=? "
        "ORDER BY commit_seq",
        (source_version,),
    ).fetchall())


def get_txn(conn: sqlite3.Connection, txn_id: str) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM txn WHERE txn_id=?", (txn_id,)).fetchone()


def get_txn_by_seq(conn: sqlite3.Connection, source_version: str,
                   commit_seq: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM txn WHERE source_version=? AND commit_seq=?",
        (source_version, commit_seq),
    ).fetchone()


def list_rows_of_txn(conn: sqlite3.Connection, txn_id: str) -> list[sqlite3.Row]:
    """按源位置顺序返回事务内的全部变更。"""
    return list(conn.execute(
        "SELECT * FROM change_row WHERE txn_id=? ORDER BY source_position",
        (txn_id,),
    ).fetchall())


def highest_commit(conn: sqlite3.Connection, source_version: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM txn WHERE state='complete' AND source_version=? "
        "ORDER BY commit_seq DESC LIMIT 1",
        (source_version,),
    ).fetchone()


def rows_in_position_window(conn: sqlite3.Connection, source_version: str,
                            low_pos: int, high_pos: int) -> list[sqlite3.Row]:
    """重放窗口用：取位置区间 [low_pos, high_pos] 内的全部变更，按位置排序。"""
    return list(conn.execute(
        "SELECT * FROM change_row WHERE source_version=? "
        "AND source_position BETWEEN ? AND ? ORDER BY source_position",
        (source_version, low_pos, high_pos),
    ).fetchall())
