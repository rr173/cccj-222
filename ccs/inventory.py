"""当前库存物化模块（可选功能，默认关闭）。

配货服务需要一张"现在每个库位有多少件、由多少条库存行贡献"的现存货表，
服务重启后直接读表即可，不能遍历订阅记录（txn/change_row），也不能逐行
重放字段历史。本模块把该结果物化成 inv_bin_summary，并在每次摄入的同一个
SQLite 事务里增量维护：源端摄入与库存汇总要么一起生效，要么一起回滚。

与 field_history 相同的启用契约
--------------------------------
  * 只能在还没有任何事务的空库上启用（INVENTORY_REQUIRES_EMPTY_DB），不回填；
  * 启用后只支持单源实例，switch-source 被拒绝（INVENTORY_SINGLE_SOURCE）；
  * 未启用的库不建任何 inv_* 表，走完全原始的订阅路径，零额外开销；
  * 订阅清理（cleanup）只释放 txn/change_row/late_notice，本模块的索引与
    物化结果一律保留，因此迟到旧位置事务到来时仍有足够信息修正汇总，
    已占用的事务标识与源位置区间永不复用。

库存语义（独立于订阅/字段历史规则，只看 after 里的 bin 和 qty）
----------------------------------------------------------------
  * 按**源位置**解释迄今成功摄入的全部事务，按**操作位置**解释同一事务内的
    库存行；一笔事务的效果在它的 end_pos 原子生效；
  * INSERT 建立新行并重置 bin/qty 两个字段（不继承上一生命周期）；
    DELETE 清空该行；UPDATE 只改 after 中明确出现的字段；尚未插入或已删除
    的行不接受 UPDATE（迟到的更新不会复活一行，也不会泄漏进未来的生命周期）；
  * 只有"非空字符串 bin 且非负整数 qty（布尔值不算整数）"同时成立的行才
    贡献一个库位。字段缺失与显式无效值都不贡献；UPDATE 省略字段=保留原值，
    显式写 null 或其他无效值=清掉该字段的贡献资格；
  * 库位名按原字符串区分（SQLite BINARY 排序）；一条有效行在一个库位上算
    一条贡献行；某库位贡献行数降到 0 时移除该库位。

增量维护方式（禁止全量重算）
----------------------------
迟到事务可能插在任意源位置之间，不能把新操作当作对当前行的补丁。本模块：
  * inv_op / inv_field_write 是只增的、可定位的索引日志（清理不删）；
  * 每次摄入只收集**本段涉及的 row_id**，对每个这样的行用索引定位其当前
    贡献：最后一次 INSERT、其后是否有 DELETE、本生命周期内 bin/qty 各自的
    最后一次显式写入——全部是 (row_id, ...) 前缀上的索引搜索/有界区间扫描，
    不线性重放该行的全部历史，也不扫描其他行；
  * 只调整该行"原先贡献的库位"和"现在贡献的库位"，其他库位一行不碰。
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional

from .source_store import StoreError

FEATURE_NAME = "inventory"

# 库存结构只在 enable-inventory 时创建；未启用的库（含旧库）连表都没有。
_INVENTORY_STATEMENTS = (
    # 与 field_history 共用 feature_flag（各自独立开关，可分别启用）
    """
    CREATE TABLE IF NOT EXISTS feature_flag (
        name    TEXT PRIMARY KEY,
        enabled INTEGER NOT NULL DEFAULT 0
    )
    """,
    # 库存水位：已提交的最新 arrival_seq；空库为 0。与汇总在同一事务更新，
    # 只读命令在同一个读事务里取水位和全部库位，输出天然来自同一快照。
    """
    CREATE TABLE IF NOT EXISTS inv_meta (
        id        INTEGER PRIMARY KEY CHECK (id = 1),
        watermark INTEGER NOT NULL DEFAULT 0
    )
    """,
    # 只增的行级操作日志：每个库存行的每次 INSERT/UPDATE/DELETE 各一行，
    # 订阅清理不删。bin_json/qty_json 为 NULL 表示该 op 的 after 里没有这个
    # 字段；显式 JSON null 存 'null'，与"缺失"严格区分。
    """
    CREATE TABLE IF NOT EXISTS inv_op (
        row_id      TEXT NOT NULL,
        src_pos     INTEGER NOT NULL,
        txn_id      TEXT NOT NULL,
        op          TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
        bin_json    TEXT,
        qty_json    TEXT,
        arrival_seq INTEGER NOT NULL,
        PRIMARY KEY (row_id, src_pos)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_inv_op_txn ON inv_op(txn_id)",
    "CREATE INDEX IF NOT EXISTS ix_inv_op_pos ON inv_op(src_pos)",
    # 生命周期定位索引：(row_id, op, src_pos) 上 MAX(src_pos) 是直接索引搜索，
    # 不需要扫完该行的全部历史操作。
    "CREATE INDEX IF NOT EXISTS ix_inv_op_lifecycle ON inv_op(row_id, op, src_pos)",
    # 只增的字段显式写入日志：只记录 after 中**明确出现**的 bin/qty 写入
    # （含显式 null 与无效值，资格判定在读侧做）。主键即"按行+字段定位最后
    # 一次写入"的索引，省略字段的 UPDATE 在这里不留痕，天然实现"省略=保留"。
    """
    CREATE TABLE IF NOT EXISTS inv_field_write (
        row_id     TEXT NOT NULL,
        field      TEXT NOT NULL CHECK (field IN ('bin','qty')),
        src_pos    INTEGER NOT NULL,
        value_json TEXT NOT NULL,
        PRIMARY KEY (row_id, field, src_pos)
    )
    """,
    # 每行当前状态的可定位摘要：重算时先读它拿到"原先的贡献"，再决定动哪些库位。
    # cur_bin/cur_qty 为 NULL 表示该行当前不贡献（已删除或字段无效/缺失）。
    """
    CREATE TABLE IF NOT EXISTS inv_row_state (
        row_id  TEXT PRIMARY KEY,
        status  TEXT NOT NULL CHECK (status IN ('live','deleted')),
        cur_bin TEXT,
        cur_qty INTEGER
    )
    """,
    # 物化结果：重启后配货服务直接读这张表。贡献行数降到 0 的库位整行删除。
    """
    CREATE TABLE IF NOT EXISTS inv_bin_summary (
        bin       TEXT PRIMARY KEY,
        total_qty INTEGER NOT NULL CHECK (total_qty >= 0),
        row_count INTEGER NOT NULL CHECK (row_count >= 0)
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


def inventory_enabled(conn: sqlite3.Connection) -> bool:
    """功能是否已启用。未启用（含旧库、从未建过库存表的库）时返回 False。"""
    if not _table_exists(conn, "feature_flag"):
        return False
    row = conn.execute(
        "SELECT enabled FROM feature_flag WHERE name=?", (FEATURE_NAME,)
    ).fetchone()
    return bool(row and row["enabled"])


def enable_inventory(conn: sqlite3.Connection) -> dict:
    """在空库上启用当前库存物化。幂等；非空库拒绝（不回填旧数据）。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        for stmt in _INVENTORY_STATEMENTS:
            conn.execute(stmt)
        already = inventory_enabled(conn)
        if not already:
            has_txn = conn.execute("SELECT 1 FROM txn LIMIT 1").fetchone()
            if has_txn:
                raise StoreError(
                    "INVENTORY_REQUIRES_EMPTY_DB",
                    "inventory materialization can only be enabled on an empty "
                    "database; backfill of pre-existing data is not supported",
                )
            conn.execute(
                "INSERT INTO feature_flag(name, enabled) VALUES (?, 1)",
                (FEATURE_NAME,),
            )
            conn.execute(
                "INSERT INTO inv_meta(id, watermark) VALUES (1, 0) "
                "ON CONFLICT(id) DO NOTHING"
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
# 资格判定：产品侧唯一的真值来源，oracle 另有一份独立实现用于对拍
# --------------------------------------------------------------------------- #
def _valid_bin(value: Any) -> bool:
    """非空字符串；布尔值不是字符串，自然被排除。"""
    return isinstance(value, str) and len(value) > 0


def _valid_qty(value: Any) -> bool:
    """非负整数；bool 是 int 的子类，必须显式排除。"""
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


# --------------------------------------------------------------------------- #
# 摄入钩子：由 source_store.ingest_segment 在同一个 SQLite 事务内调用
# --------------------------------------------------------------------------- #
def _check_not_recorded(conn: sqlite3.Connection, txn_id: str,
                        begin_pos: int, end_pos: int) -> None:
    """库存日志保留全部已摄入操作（清理不删）：事务标识与源位置永不复用。"""
    row = conn.execute(
        "SELECT txn_id FROM inv_op WHERE txn_id=? LIMIT 1", (txn_id,)
    ).fetchone()
    if row:
        raise StoreError(
            "DUP_TXN",
            f"transaction {txn_id!r} is already recorded in the inventory log; "
            "identifiers are never reused",
        )
    clash = conn.execute(
        "SELECT txn_id, src_pos FROM inv_op WHERE src_pos BETWEEN ? AND ? LIMIT 1",
        (begin_pos, end_pos),
    ).fetchone()
    if clash:
        raise StoreError(
            "POSITION_OVERLAP",
            f"segment [{begin_pos},{end_pos}] overlaps recorded inventory op at "
            f"position {clash['src_pos']} (txn {clash['txn_id']}); "
            "recorded ranges are never reused",
        )


def _record_op(conn: sqlite3.Connection, op: dict, txn_id: str,
               arrival_seq: int) -> None:
    after = op.get("after")
    fields = after if isinstance(after, dict) else None
    bin_present = fields is not None and "bin" in fields
    qty_present = fields is not None and "qty" in fields
    conn.execute(
        "INSERT INTO inv_op(row_id, src_pos, txn_id, op, bin_json, qty_json, "
        "arrival_seq) VALUES (?,?,?,?,?,?,?)",
        (
            op["row_id"], op["source_position"], txn_id, op["op"],
            json.dumps(fields["bin"], sort_keys=True) if bin_present else None,
            json.dumps(fields["qty"], sort_keys=True) if qty_present else None,
            arrival_seq,
        ),
    )
    # 只记录明确出现的字段写入；省略的字段不写 = 重算时保留上一次写入。
    if bin_present:
        conn.execute(
            "INSERT INTO inv_field_write(row_id, field, src_pos, value_json) "
            "VALUES (?, 'bin', ?, ?)",
            (op["row_id"], op["source_position"],
             json.dumps(fields["bin"], sort_keys=True)),
        )
    if qty_present:
        conn.execute(
            "INSERT INTO inv_field_write(row_id, field, src_pos, value_json) "
            "VALUES (?, 'qty', ?, ?)",
            (op["row_id"], op["source_position"],
             json.dumps(fields["qty"], sort_keys=True)),
        )


def _last_event_pos(conn: sqlite3.Connection, row_id: str, op: str,
                    above: Optional[int] = None) -> Optional[int]:
    """索引定位该行最后一次指定事件的位置；above 给定时只看更晚的位置。"""
    if above is None:
        row = conn.execute(
            "SELECT MAX(src_pos) AS p FROM inv_op WHERE row_id=? AND op=?",
            (row_id, op),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT MAX(src_pos) AS p FROM inv_op "
            "WHERE row_id=? AND op=? AND src_pos>?",
            (row_id, op, above),
        ).fetchone()
    return row["p"]


def _latest_field_value(conn: sqlite3.Connection, row_id: str, field: str,
                        floor_pos: int) -> tuple[bool, Any]:
    """定位本生命周期内（src_pos >= floor_pos）该字段最后一次显式写入。

    返回 (是否出现过显式写入, 值)。没有任何写入时字段缺失（INSERT 未提供且
    之后的 UPDATE 一直省略）。
    """
    row = conn.execute(
        "SELECT value_json FROM inv_field_write "
        "WHERE row_id=? AND field=? AND src_pos>=? "
        "ORDER BY src_pos DESC LIMIT 1",
        (row_id, field, floor_pos),
    ).fetchone()
    if row is None:
        return False, None
    return True, json.loads(row["value_json"])


def _recompute_row(conn: sqlite3.Connection, row_id: str) -> None:
    """用索引重新定位一行的当前贡献，并只调整其原先/现在贡献的库位。

    不重放该行全部历史：生命周期靠两个 MAX 索引搜索定位，字段值靠主键索引
    取最后一次写入，其余历史操作一概不读。
    """
    old = conn.execute(
        "SELECT * FROM inv_row_state WHERE row_id=?", (row_id,)
    ).fetchone()
    old_contrib: Optional[tuple[str, int]] = None
    if old is not None and old["status"] == "live" \
            and old["cur_bin"] is not None and old["cur_qty"] is not None:
        old_contrib = (old["cur_bin"], old["cur_qty"])

    insert_pos = _last_event_pos(conn, row_id, "INSERT")
    if insert_pos is None:
        # 从未插入：孤儿 UPDATE/DELETE 不接受，行不存在，不贡献。
        new_status, new_bin, new_qty = "deleted", None, None
        new_contrib: Optional[tuple[str, int]] = None
    elif _last_event_pos(conn, row_id, "DELETE", above=insert_pos) is not None:
        # 最后一次 INSERT 之后又被删除：当前已删除，之后的迟到 UPDATE 不能复活。
        new_status, new_bin, new_qty = "deleted", None, None
        new_contrib = None
    else:
        new_status = "live"
        bin_ok, bin_value = _latest_field_value(conn, row_id, "bin", insert_pos)
        qty_ok, qty_value = _latest_field_value(conn, row_id, "qty", insert_pos)
        new_bin = bin_value if bin_ok and _valid_bin(bin_value) else None
        new_qty = qty_value if qty_ok and _valid_qty(qty_value) else None
        new_contrib = (new_bin, new_qty) if new_bin is not None and new_qty is not None else None

    conn.execute(
        "INSERT INTO inv_row_state(row_id, status, cur_bin, cur_qty) VALUES (?,?,?,?) "
        "ON CONFLICT(row_id) DO UPDATE SET status=excluded.status, "
        "cur_bin=excluded.cur_bin, cur_qty=excluded.cur_qty",
        (row_id, new_status, new_bin, new_qty),
    )

    if old_contrib is not None and old_contrib != new_contrib:
        old_bin, old_qty = old_contrib
        conn.execute(
            "UPDATE inv_bin_summary SET total_qty=total_qty-?, row_count=row_count-1 "
            "WHERE bin=?",
            (old_qty, old_bin),
        )
        # 只有贡献行数降到零才移除该库位
        conn.execute(
            "DELETE FROM inv_bin_summary WHERE bin=? AND row_count=0", (old_bin,)
        )
    if new_contrib is not None and new_contrib != old_contrib:
        new_bin, new_qty = new_contrib
        conn.execute(
            "INSERT INTO inv_bin_summary(bin, total_qty, row_count) VALUES (?,?,1) "
            "ON CONFLICT(bin) DO UPDATE SET "
            "total_qty=inv_bin_summary.total_qty+excluded.total_qty, "
            "row_count=inv_bin_summary.row_count+1",
            (new_bin, new_qty),
        )


def apply_segment(conn: sqlite3.Connection, segment: dict,
                  source_version: str, arrival_seq: int) -> None:
    """在摄入事务内记录本段并增量维护库存物化结果。与摄入一同成功或失败。"""
    txn_id = segment["txn_id"]
    ops = segment["ops"]
    begin_pos = ops[0]["source_position"]
    end_pos = ops[-1]["source_position"]
    _check_not_recorded(conn, txn_id, begin_pos, end_pos)

    affected: dict[str, None] = {}
    for op in ops:
        _record_op(conn, op, txn_id, arrival_seq)
        affected[op["row_id"]] = None

    # 只重算段中涉及的行；每行的当前贡献全部从索引定位，不碰其他行。
    for row_id in affected:
        _recompute_row(conn, row_id)

    conn.execute("UPDATE inv_meta SET watermark=? WHERE id=1", (arrival_seq,))


# --------------------------------------------------------------------------- #
# 只读命令：单个读事务取水位与全部库位，保证来自同一提交快照
# --------------------------------------------------------------------------- #
def read_summary(conn: sqlite3.Connection) -> dict:
    """返回 {"arrival_seq": N, "bins": [{bin, total_qty, contributing_rows}]}。

    空库（启用后尚未摄入）水位为 0、库位列表为空。只读物化表，不遍历
    订阅记录也不查询字段历史。
    """
    if not inventory_enabled(conn):
        raise StoreError(
            "INVENTORY_NOT_ENABLED",
            "inventory materialization is not enabled on this database",
        )
    conn.execute("BEGIN")
    try:
        watermark = conn.execute(
            "SELECT watermark FROM inv_meta WHERE id=1"
        ).fetchone()["watermark"]
        bins = [
            {"bin": r["bin"], "total_qty": r["total_qty"],
             "contributing_rows": r["row_count"]}
            for r in conn.execute(
                "SELECT bin, total_qty, row_count FROM inv_bin_summary ORDER BY bin"
            ).fetchall()
        ]
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {"arrival_seq": watermark, "bins": bins}
