"""字段历史归档与审计查询模块（可选功能，默认关闭）。

对账场景：上周出了一版对账单，本周补到一笔源位置更早的迟到事务。审计员需要
针对一个行键的一个字段，重现"出单时"（知识截点 K）看得到的值与来源，再用较新
的截点查同一段源位置，厘清差异从哪里开始。订阅重放交付的是逐笔净变化，本模块
给出的是按源位置解释的字段历史区间。

职责边界：
  * 启用后，ingest 在同一个 SQLite 事务里把每笔事务对每个行的净效果写入归档
    索引（hist_txn / hist_row_event / hist_field_write），与摄入一同成功或失败；
  * 归档只增不改：消费者清理可以释放订阅数据（txn/change_row/late_notice），
    归档历史保留；已归档的事务标识与源位置区间永不复用；
  * 查询 (row_id, field, [start, end), cutoff K)：先以 arrival_seq <= K 决定
    当时知道哪些事务，再按这些事务的源位置解释字段历史；
  * 查询只走落盘的归档索引、只访问目标行的历史：窗首用索引定位已有状态，
    不从该行第一笔事务顺序重放，也不逐位置枚举窗口。

核心语义：
  * 一笔事务的效果在它的 end_pos 一起生效；窗口切进事务内部看不到中间写入；
    同一事务内按操作位置决定最终状态（同一字段最后写者胜）；
  * 首条可见操作之前行是否存在未知；第一条是更新时，只知道它存在以及更新带来
    的字段，不推测一次没看到的插入（生命周期标 "unknown"）；
  * 插入开启新生命周期，以插入操作位置标识；重新插入不继承旧字段或其来源；
  * 更新只覆盖 after 明确包含的字段；没提供 after 时不改变任何字段；绝不拿
    前镜像补值；显式 JSON null 是已知值；
  * 删除使行处于已删除状态，直到下一次插入；期间更新不能恢复该行；
  * 迟到事务只改变较新截点的答案，原截点的答案不变。

本模块不感知消费者状态，不做净变化折叠交付（那是 netchange 的职责）。
只支持"从空库启用"的单源实例：非空库拒绝启用（不回填），启用后拒绝换源。
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional

from .source_store import StoreError

FEATURE_NAME = "field_history"

# 归档结构只在 enable-history 时创建；未启用的库（包括旧库）连表都没有，
# 保持完全原始的订阅行为。
_HISTORY_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS feature_flag (
        name    TEXT PRIMARY KEY,
        enabled INTEGER NOT NULL DEFAULT 0
    )
    """,
    # 已归档事务的身份与位置区间：清理不得删除，标识与区间永不复用
    """
    CREATE TABLE IF NOT EXISTS hist_txn (
        txn_id        TEXT PRIMARY KEY,
        source_version TEXT NOT NULL,
        begin_pos     INTEGER NOT NULL,
        end_pos       INTEGER NOT NULL,
        arrival_seq   INTEGER NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_hist_txn_range ON hist_txn(begin_pos, end_pos)
    """,
    # 行级事件：一笔事务对一行的净效果，在 eff_pos（= 事务 end_pos）原子生效。
    # 一行在一笔事务里最多一个事件，end_pos 全局唯一，故 (row_id, eff_pos) 唯一。
    """
    CREATE TABLE IF NOT EXISTS hist_row_event (
        row_id        TEXT NOT NULL,
        eff_pos       INTEGER NOT NULL,
        arrival_seq   INTEGER NOT NULL,
        txn_id        TEXT NOT NULL,
        event         TEXT NOT NULL CHECK (event IN ('INSERT','UPDATE','DELETE')),
        lifecycle_pos INTEGER,          -- INSERT 事件：插入操作位置；其余为 NULL
        PRIMARY KEY (row_id, eff_pos)
    )
    """,
    # 字段级写入：一笔事务对一行某字段的最终写入（事务内最后写者胜）。
    # 窗首定位用 (row_id, field, eff_pos) 索引直接找"起点之前最后一次写入"。
    """
    CREATE TABLE IF NOT EXISTS hist_field_write (
        row_id      TEXT NOT NULL,
        field       TEXT NOT NULL,
        eff_pos     INTEGER NOT NULL,
        arrival_seq INTEGER NOT NULL,
        txn_id      TEXT NOT NULL,
        op_pos      INTEGER NOT NULL,   -- 实际写入该字段的操作位置
        value_json  TEXT NOT NULL,      -- JSON 编码；显式 null 存 'null'（已知值）
        PRIMARY KEY (row_id, field, eff_pos)
    )
    """,
)


# --------------------------------------------------------------------------- #
# 启用与特性开关
# --------------------------------------------------------------------------- #
def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def history_enabled(conn: sqlite3.Connection) -> bool:
    """功能是否已启用。未启用（含旧库、从未建过归档表的库）时返回 False。"""
    if not _table_exists(conn, "feature_flag"):
        return False
    row = conn.execute(
        "SELECT enabled FROM feature_flag WHERE name=?", (FEATURE_NAME,)
    ).fetchone()
    return bool(row and row["enabled"])


def enable_history(conn: sqlite3.Connection) -> dict:
    """在空库上启用字段历史归档。幂等；非空库拒绝（不回填旧数据）。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        for stmt in _HISTORY_STATEMENTS:
            conn.execute(stmt)
        already = history_enabled(conn)
        if not already:
            has_txn = conn.execute("SELECT 1 FROM txn LIMIT 1").fetchone()
            if has_txn:
                raise StoreError(
                    "HISTORY_REQUIRES_EMPTY_DB",
                    "field history can only be enabled on an empty database; "
                    "backfill of pre-existing data is not supported",
                )
            conn.execute(
                "INSERT INTO feature_flag(name, enabled) VALUES (?, 1)", (FEATURE_NAME,)
            )
        conn.execute("COMMIT")
    except StoreError:
        conn.execute("ROLLBACK")
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {"feature": FEATURE_NAME, "enabled": True, "already_enabled": already}


# --------------------------------------------------------------------------- #
# 事务内净效果归约（归档写入侧）
# --------------------------------------------------------------------------- #
def _new_row_state() -> dict:
    # phase: INHERIT（维持事务前状态）/ EXISTS / DELETED
    return {"phase": "INHERIT", "txn_born": False, "lifecycle_pos": None,
            "fields": {}, "checkpoint": None}


def _apply_op(st: dict, op: dict) -> None:
    kind = op["op"]
    pos = op["source_position"]
    if kind == "INSERT":
        # 记录检查点：若本事务内随后被 DELETE 抵消，恢复到插入前状态
        st["checkpoint"] = (st["phase"], st["txn_born"], st["lifecycle_pos"],
                            dict(st["fields"]))
        st["phase"] = "EXISTS"
        st["txn_born"] = True
        st["lifecycle_pos"] = pos
        # 新生命周期：字段全集重置为插入的 after，不继承任何旧字段
        st["fields"] = {k: (v, pos) for k, v in (op.get("after") or {}).items()}
    elif kind == "DELETE":
        if st["txn_born"]:
            # INSERT .. DELETE 在同一事务内完全抵消：回到插入前状态
            phase, txn_born, lifecycle_pos, fields = st["checkpoint"]
            st["phase"] = phase
            st["txn_born"] = txn_born
            st["lifecycle_pos"] = lifecycle_pos
            st["fields"] = fields
            st["checkpoint"] = None
        else:
            st["phase"] = "DELETED"
            st["lifecycle_pos"] = None
            st["fields"] = {}
            st["checkpoint"] = None
    else:  # UPDATE
        if st["phase"] == "DELETED":
            return  # 删除期间的更新不能恢复该行
        after = op.get("after")
        if after:  # 只覆盖 after 明确包含的字段；没提供 after 时不改变任何字段
            for k, v in after.items():
                st["fields"][k] = (v, pos)


def compute_row_effects(ops: list[dict]) -> dict[str, dict]:
    """把一笔事务内每个行的操作序列归约成"在 end_pos 原子生效"的净效果。

    返回 row_id -> {"event": INSERT|UPDATE|DELETE,
                    "lifecycle_pos": int|None,
                    "fields": {field: (value, op_pos)}}。
    完全抵消（INSERT..DELETE）或没有任何字段写入的行不产生事件。
    """
    states: dict[str, dict] = {}
    order: list[str] = []
    for op in ops:
        rid = op["row_id"]
        st = states.get(rid)
        if st is None:
            st = _new_row_state()
            states[rid] = st
            order.append(rid)
        _apply_op(st, op)

    effects: dict[str, dict] = {}
    for rid in order:
        st = states[rid]
        if st["phase"] == "DELETED":
            effects[rid] = {"event": "DELETE", "lifecycle_pos": None, "fields": {}}
        elif st["phase"] == "EXISTS" and st["txn_born"]:
            effects[rid] = {"event": "INSERT",
                            "lifecycle_pos": st["lifecycle_pos"],
                            "fields": st["fields"]}
        elif st["fields"]:
            # INHERIT + 纯更新增量：行是否存在取决于事务前状态，查询侧解释
            effects[rid] = {"event": "UPDATE", "lifecycle_pos": None,
                            "fields": st["fields"]}
        # 其余（完全抵消 / 无字段写入）不产生事件
    return effects


# --------------------------------------------------------------------------- #
# 归档钩子（由 source_store.ingest_segment 在同一事务内调用）
# --------------------------------------------------------------------------- #
def _check_not_archived(conn: sqlite3.Connection, txn_id: str,
                        begin_pos: int, end_pos: int) -> None:
    """已归档的事务标识与源位置区间不得被重新占用（清理只删订阅数据）。"""
    row = conn.execute(
        "SELECT txn_id FROM hist_txn WHERE txn_id=?", (txn_id,)
    ).fetchone()
    if row:
        raise StoreError(
            "DUP_TXN",
            f"transaction {txn_id!r} is archived; archived identifiers are never reused",
        )
    clash = conn.execute(
        "SELECT txn_id, begin_pos, end_pos FROM hist_txn "
        "WHERE NOT (end_pos < ? OR begin_pos > ?) LIMIT 1",
        (begin_pos, end_pos),
    ).fetchone()
    if clash:
        raise StoreError(
            "POSITION_OVERLAP",
            f"segment [{begin_pos},{end_pos}] overlaps archived txn {clash['txn_id']} "
            f"[{clash['begin_pos']},{clash['end_pos']}]; archived ranges are never reused",
        )


def archive_segment(conn: sqlite3.Connection, segment: dict,
                    source_version: str, arrival_seq: int) -> None:
    """在摄入事务内把该段的净效果写入归档索引。与摄入一同成功或失败。"""
    txn_id = segment["txn_id"]
    ops = segment["ops"]
    begin_pos = ops[0]["source_position"]
    end_pos = ops[-1]["source_position"]
    _check_not_archived(conn, txn_id, begin_pos, end_pos)
    conn.execute(
        "INSERT INTO hist_txn(txn_id, source_version, begin_pos, end_pos, arrival_seq) "
        "VALUES (?,?,?,?,?)",
        (txn_id, source_version, begin_pos, end_pos, arrival_seq),
    )
    for row_id, eff in compute_row_effects(ops).items():
        conn.execute(
            "INSERT INTO hist_row_event(row_id, eff_pos, arrival_seq, txn_id, event, "
            "lifecycle_pos) VALUES (?,?,?,?,?,?)",
            (row_id, end_pos, arrival_seq, txn_id, eff["event"], eff["lifecycle_pos"]),
        )
        for field, (value, op_pos) in eff["fields"].items():
            conn.execute(
                "INSERT INTO hist_field_write(row_id, field, eff_pos, arrival_seq, "
                "txn_id, op_pos, value_json) VALUES (?,?,?,?,?,?,?)",
                (row_id, field, end_pos, arrival_seq, txn_id, op_pos,
                 json.dumps(value, sort_keys=True)),
            )


# --------------------------------------------------------------------------- #
# 查询：窗口 [start, end)，知识截点 K
# --------------------------------------------------------------------------- #
def _initial_state(conn: sqlite3.Connection, row_id: str, field: str,
                   start: int, cutoff: int) -> dict:
    """窗首状态：用归档索引直接定位 start 之前已生效的状态，不做顺序重放。"""
    # 行状态由"决定性事件"（INSERT/DELETE 绝对设定状态）决定；
    # UPDATE 事件的效果依赖此前状态，不能单独作为窗首依据。
    det = conn.execute(
        "SELECT event, lifecycle_pos, eff_pos FROM hist_row_event "
        "WHERE row_id=? AND arrival_seq<=? AND eff_pos<=? "
        "AND event IN ('INSERT','DELETE') ORDER BY eff_pos DESC LIMIT 1",
        (row_id, cutoff, start),
    ).fetchone()
    if det is None:
        any_update = conn.execute(
            "SELECT 1 FROM hist_row_event WHERE row_id=? AND arrival_seq<=? "
            "AND eff_pos<=? AND event='UPDATE' LIMIT 1",
            (row_id, cutoff, start),
        ).fetchone()
        if any_update is None:
            return {"row_state": "unknown", "lifecycle": None, "field": None}
        # 首条可见操作是更新：只知道行存在，生命周期未知
        row_state, lifecycle, write_floor = "exists", "unknown", -1
    elif det["event"] == "INSERT":
        row_state, lifecycle = "exists", det["lifecycle_pos"]
        # 重新插入不继承旧字段：只看本生命周期内的写入（含下界——
        # 开启该生命周期的 INSERT 自己的字段写入也落在它的 eff_pos 上）
        write_floor = det["eff_pos"]
    else:
        return {"row_state": "deleted", "lifecycle": None, "field": None}

    w = conn.execute(
        "SELECT txn_id, op_pos, value_json FROM hist_field_write "
        "WHERE row_id=? AND field=? AND arrival_seq<=? AND eff_pos<=? AND eff_pos>=? "
        "ORDER BY eff_pos DESC LIMIT 1",
        (row_id, field, cutoff, start, write_floor),
    ).fetchone()
    field_state = ("known", w["value_json"], w["txn_id"], w["op_pos"]) if w else ("unknown",)
    return {"row_state": row_state, "lifecycle": lifecycle, "field": field_state}


def _apply_event(cur: dict, ev: sqlite3.Row, write: Optional[sqlite3.Row]) -> dict:
    """在 eff_pos 处应用一个行事件，返回新状态（不修改入参）。"""
    st = dict(cur)
    event = ev["event"]
    if event == "INSERT":
        st["row_state"] = "exists"
        st["lifecycle"] = ev["lifecycle_pos"]
        st["field"] = (("known", write["value_json"], write["txn_id"], write["op_pos"])
                       if write else ("unknown",))
    elif event == "DELETE":
        st["row_state"] = "deleted"
        st["lifecycle"] = None
        st["field"] = None
    else:  # UPDATE
        if st["row_state"] == "deleted":
            pass  # 删除期间的更新不能恢复该行
        elif st["row_state"] == "unknown":
            st["row_state"] = "exists"
            st["lifecycle"] = "unknown"
            st["field"] = (("known", write["value_json"], write["txn_id"], write["op_pos"])
                           if write else ("unknown",))
        elif write is not None:
            st["field"] = ("known", write["value_json"], write["txn_id"], write["op_pos"])
    return st


def _visible_key(st: dict) -> tuple:
    """参与区间合并的可见元组：状态、生命周期、值与来源全相同才合并。"""
    return (st["row_state"], st["lifecycle"], st["field"])


def _format_interval(frm: int, to: int, st: dict) -> dict:
    iv = {"start": frm, "end": to, "row_state": st["row_state"]}
    if st["row_state"] == "exists":
        lifecycle = st["lifecycle"]
        iv["lifecycle"] = {"insert_pos": lifecycle} if isinstance(lifecycle, int) else "unknown"
        f = st["field"]
        if f[0] == "known":
            iv["field"] = {"state": "known", "value": json.loads(f[1]),
                           "source": {"txn_id": f[2], "op_pos": f[3]}}
        else:
            iv["field"] = {"state": "unknown"}
    else:
        iv["lifecycle"] = None
        iv["field"] = None
    return iv


def query_field_history(conn: sqlite3.Connection, row_id: str, field: str,
                        start: int, end: int, cutoff: int) -> dict:
    """查询指定行键/字段在源位置窗口 [start, end) 内、知识截点 K 下的历史。

    先以 arrival_seq <= K 决定当时知道哪些事务，再按源位置解释字段历史；
    输出按位置排序、无缝覆盖窗口的半开区间。
    """
    if not history_enabled(conn):
        raise StoreError("HISTORY_NOT_ENABLED",
                         "field history archive is not enabled on this database")
    if cutoff < 0:
        raise StoreError("BAD_CUTOFF", f"cutoff must be >= 0, got {cutoff}")
    if not (0 <= start < end):
        raise StoreError("BAD_WINDOW", f"require 0 <= start < end, got [{start}, {end})")
    watermark = conn.execute(
        "SELECT COALESCE(MAX(arrival_seq), 0) AS w FROM hist_txn"
    ).fetchone()["w"]
    if cutoff > watermark:
        raise StoreError(
            "CUTOFF_BEYOND_WATERMARK",
            f"cutoff {cutoff} exceeds archived arrival watermark {watermark}",
        )

    state = _initial_state(conn, row_id, field, start, cutoff)
    events = list(conn.execute(
        "SELECT eff_pos, event, lifecycle_pos, txn_id FROM hist_row_event "
        "WHERE row_id=? AND arrival_seq<=? AND eff_pos>? AND eff_pos<? "
        "ORDER BY eff_pos",
        (row_id, cutoff, start, end),
    ).fetchall())
    writes = {
        r["eff_pos"]: r
        for r in conn.execute(
            "SELECT eff_pos, txn_id, op_pos, value_json FROM hist_field_write "
            "WHERE row_id=? AND field=? AND arrival_seq<=? AND eff_pos>? AND eff_pos<?",
            (row_id, field, cutoff, start, end),
        ).fetchall()
    }

    intervals: list[dict] = []
    cur = state
    cur_from = start
    for ev in events:
        nxt = _apply_event(cur, ev, writes.get(ev["eff_pos"]))
        if _visible_key(nxt) != _visible_key(cur):
            intervals.append(_format_interval(cur_from, ev["eff_pos"], cur))
            cur_from = ev["eff_pos"]
        cur = nxt
    intervals.append(_format_interval(cur_from, end, cur))

    return {
        "row_id": row_id,
        "field": field,
        "window": [start, end],
        "cutoff": cutoff,
        "arrival_watermark": watermark,
        "intervals": intervals,
    }
