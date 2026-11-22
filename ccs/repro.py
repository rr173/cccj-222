"""可复现事务包导出模块（可选功能，默认关闭）。

下游排查时，常常只拿到某笔事务的 ``txn_id`` 和当时的到达截点 ``K``，需要把
那笔事务与"它真正依赖的上游变更"带到一台空环境里复跑。本模块提供：

  * 启用时（仅限首次摄入前的单源空库）创建只增的事务包归档；
  * ``ingest`` 在**同一个 SQLite 事务**里记录原始事务段、事务身份/位置/到达
    序号，以及"事务 -> 涉及的行"定位索引；未启用的库不建表、行为完全不变；
  * 查询时从目标事务出发，按规则闭包取样，按源位置排序写出 JSON Lines：
      1. 只承认 ``arrival_seq <= K`` 的已提交事务，目标也必须在其中；
      2. 对每笔已选事务涉及的每个 row_id，找出源位置严格早于**这笔已选事务**
         且也操作过该行的全部可见事务，整笔纳入；
      3. 新纳入的事务继续产生同样的要求，直到不再新增；
      4. 用事务的源位置（区间起点）判断早晚，不用到达序号；插入不截断向前
         查找，同一事务内的多次操作原样保留，绝不折叠成净变化。

输出格式
--------
UTF-8 JSON Lines，首行是元数据：

    {"format": "ccs-repro-bundle", "format_version": 1,
     "source_version": "...", "cutoff": K, "target_txn_id": "...", "txn_count": N}

其后每行是一个**原始段对象**（与 ``ccs ingest`` 接受的 JSON 完全同构），
按源位置从早到晚排列、每笔恰好一次。段内操作次序、键的省略与显式 ``null``
的区别全部保留：内容直接来自摄入时保存的原始段，不从字段历史/库存表反推。

扫描与内存
----------
候选定位只走 ``(row_id, begin_pos)`` 索引：从已选事务的行出发找更早触达，
与这些行无关的历史不会被扫描。闭包工作集与"已选事务"放在独立的磁盘
SQLite 暂存库中，输出边生成边写临时文件，不把整个包或候选集合囤在内存里。
成功时原子替换目标路径；任何失败都不留下半份包，也不改动同路径原文件。

启用契约（与 field_history / inventory 一致）
--------------------------------------------
  * 只能在还没有任何事务的空库上启用（REPRO_REQUIRES_EMPTY_DB），不回填旧库；
  * 启用后只支持单源实例，switch-source 被拒绝（REPRO_SINGLE_SOURCE）；
  * 未启用的库连 repro_* 表都不创建；
  * 订阅清理只删 txn/change_row/late_notice，本模块的归档保留：清理后仍可用
    同一截点导出同一份包；已记录的事务标识与源位置区间永不复用
    （DUP_TXN / POSITION_OVERLAP）。
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from typing import Optional

from .source_store import StoreError

FEATURE_NAME = "repro_bundle"
BUNDLE_FORMAT = "ccs-repro-bundle"
BUNDLE_FORMAT_VERSION = 1

# 归档结构只在 enable-repro 时创建；未启用的库（包括旧库）连表都没有。
_REPRO_STATEMENTS = (
    # 与 field_history / inventory 共用 feature_flag（各自独立开关）
    """
    CREATE TABLE IF NOT EXISTS feature_flag (
        name    TEXT PRIMARY KEY,
        enabled INTEGER NOT NULL DEFAULT 0
    )
    """,
    # 已归档事务：身份、源位置区间、到达序号，以及摄入时的原始段 JSON。
    # 清理不得删除；标识与区间永不复用。segment_json 是唯一的内容来源。
    """
    CREATE TABLE IF NOT EXISTS repro_txn (
        txn_id        TEXT PRIMARY KEY,
        source_version TEXT NOT NULL,
        begin_pos     INTEGER NOT NULL,
        end_pos       INTEGER NOT NULL,
        arrival_seq   INTEGER NOT NULL,
        segment_json  TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_repro_txn_pos ON repro_txn(begin_pos, end_pos)",
    "CREATE INDEX IF NOT EXISTS ix_repro_txn_arrival ON repro_txn(arrival_seq)",
    # 行触达索引：(事务, 行) 一条。begin_pos/arrival_seq 冗余在这里，候选查询
    # 只走 (row_id, begin_pos) 前缀，再按 arrival_seq <= K 过滤，不需要回表。
    """
    CREATE TABLE IF NOT EXISTS repro_row_touch (
        row_id      TEXT NOT NULL,
        txn_id      TEXT NOT NULL,
        begin_pos   INTEGER NOT NULL,
        arrival_seq INTEGER NOT NULL,
        PRIMARY KEY (row_id, txn_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_repro_touch_row_begin "
    "ON repro_row_touch(row_id, begin_pos)",
    "CREATE INDEX IF NOT EXISTS ix_repro_touch_txn ON repro_row_touch(txn_id)",
)


# --------------------------------------------------------------------------- #
# 启用与特性开关
# --------------------------------------------------------------------------- #
def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def repro_enabled(conn: sqlite3.Connection) -> bool:
    """功能是否已启用。未启用（含旧库）时返回 False，摄入零额外开销。"""
    if not _table_exists(conn, "feature_flag"):
        return False
    row = conn.execute(
        "SELECT enabled FROM feature_flag WHERE name=?", (FEATURE_NAME,)
    ).fetchone()
    return bool(row and row["enabled"])


def enable_repro(conn: sqlite3.Connection) -> dict:
    """在空库上启用事务包归档。幂等；非空库拒绝（不回填旧数据）。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        for stmt in _REPRO_STATEMENTS:
            conn.execute(stmt)
        already = repro_enabled(conn)
        if not already:
            has_txn = conn.execute("SELECT 1 FROM txn LIMIT 1").fetchone()
            if has_txn:
                raise StoreError(
                    "REPRO_REQUIRES_EMPTY_DB",
                    "reproducible bundles can only be enabled on an empty database; "
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
# 归档钩子（由 source_store.ingest_segment 在同一事务内调用）
# --------------------------------------------------------------------------- #
def _check_not_recorded(conn: sqlite3.Connection, txn_id: str,
                        begin_pos: int, end_pos: int) -> None:
    """已归档的事务标识与源位置区间不得被重新占用（清理只删订阅数据）。"""
    row = conn.execute(
        "SELECT txn_id FROM repro_txn WHERE txn_id=?", (txn_id,)
    ).fetchone()
    if row:
        raise StoreError(
            "DUP_TXN",
            f"transaction {txn_id!r} is already recorded for reproducible bundles; "
            "recorded identifiers are never reused",
        )
    clash = conn.execute(
        "SELECT txn_id, begin_pos, end_pos FROM repro_txn "
        "WHERE NOT (end_pos < ? OR begin_pos > ?) LIMIT 1",
        (begin_pos, end_pos),
    ).fetchone()
    if clash:
        raise StoreError(
            "POSITION_OVERLAP",
            f"segment [{begin_pos},{end_pos}] overlaps recorded bundle txn "
            f"{clash['txn_id']} [{clash['begin_pos']},{clash['end_pos']}]; "
            "recorded ranges are never reused",
        )


def archive_segment(conn: sqlite3.Connection, segment: dict,
                    source_version: str, arrival_seq: int) -> None:
    """在摄入事务内保存原始段与行触达索引。与摄入一同成功或失败。

    原始段用 JSON 紧凑序列化后原样保存：键的缺失与显式 null 是两种不同
    语义，必须由原始对象本身保留，不能从 change_row / 字段历史反推。
    """
    txn_id = segment["txn_id"]
    ops = segment["ops"]
    begin_pos = ops[0]["source_position"]
    end_pos = ops[-1]["source_position"]
    _check_not_recorded(conn, txn_id, begin_pos, end_pos)

    segment_json = json.dumps(segment, ensure_ascii=False, separators=(",", ":"),
                              sort_keys=False)
    conn.execute(
        "INSERT INTO repro_txn(txn_id, source_version, begin_pos, end_pos, "
        "arrival_seq, segment_json) VALUES (?,?,?,?,?,?)",
        (txn_id, source_version, begin_pos, end_pos, arrival_seq, segment_json),
    )
    for op in ops:
        # 同一事务对同一行的多次操作只留一条触达记录（候选定位只需"是否触达"）。
        conn.execute(
            "INSERT OR IGNORE INTO repro_row_touch(row_id, txn_id, begin_pos, "
            "arrival_seq) VALUES (?,?,?,?)",
            (op["row_id"], txn_id, begin_pos, arrival_seq),
        )


# --------------------------------------------------------------------------- #
# 导出：按行定位的闭包 + 磁盘暂存 + 原子发布
# --------------------------------------------------------------------------- #
_CANDIDATE_SQL = (
    "SELECT txn_id, begin_pos FROM repro_row_touch "
    "WHERE row_id=? AND begin_pos<? AND arrival_seq<=?"
)


class _Scratch:
    """独立的磁盘 SQLite 暂存库：保存闭包已选集合与待处理前沿。"""

    def __init__(self) -> None:
        fd, path = tempfile.mkstemp(prefix="ccs_repro_", suffix=".db")
        os.close(fd)
        self.path = path
        self.conn = sqlite3.connect(path, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        try:
            # 暂存数据可丢弃，不需要崩溃恢复；关闭日志减少小写入开销。
            self.conn.execute("PRAGMA journal_mode=OFF")
            self.conn.execute("PRAGMA synchronous=OFF")
            self.conn.execute("PRAGMA temp_store=FILE")
            self.conn.execute(
                "CREATE TABLE selected ("
                "txn_id TEXT PRIMARY KEY, begin_pos INTEGER NOT NULL)"
            )
            self.conn.execute("CREATE INDEX ix_selected_pos ON selected(begin_pos)")
            self.conn.execute(
                "CREATE TABLE work ("
                "txn_id TEXT PRIMARY KEY, begin_pos INTEGER NOT NULL)"
            )
        except Exception:
            self.close()
            raise

    def add(self, txn_id: str, begin_pos: int) -> bool:
        """把事务加入已选集合；若它是新加入的，同时放入待处理前沿。"""
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO selected(txn_id, begin_pos) VALUES (?,?)",
            (txn_id, begin_pos),
        )
        if cur.rowcount:
            self.conn.execute(
                "INSERT OR IGNORE INTO work(txn_id, begin_pos) VALUES (?,?)",
                (txn_id, begin_pos),
            )
            return True
        return False

    def pop(self) -> Optional[tuple[str, int]]:
        row = self.conn.execute(
            "SELECT txn_id, begin_pos FROM work LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        self.conn.execute("DELETE FROM work WHERE txn_id=?", (row[0],))
        return row[0], row[1]

    def ordered(self):
        return self.conn.execute(
            "SELECT txn_id, begin_pos FROM selected ORDER BY begin_pos, txn_id"
        )

    def count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM selected").fetchone()[0]

    def close(self) -> None:
        try:
            self.conn.close()
        finally:
            try:
                os.unlink(self.path)
            except OSError:
                pass


def _close_closure(conn: sqlite3.Connection, scratch: _Scratch,
                   cutoff: int) -> None:
    """从已选前沿出发，按 (row_id, begin_pos) 索引把更早的可见触达整笔纳入。

    每笔已选事务只处理一次：它对每个涉及行的"更早触达集合"只由自身源位置
    和截点决定，不随之后选入什么而改变。
    """
    while True:
        item = scratch.pop()
        if item is None:
            return
        txn_id, begin_pos = item
        # 当前事务涉及的行（每行一条，走 ix_repro_touch_txn）。
        rows = conn.execute(
            "SELECT row_id FROM repro_row_touch WHERE txn_id=?", (txn_id,)
        )
        for row in rows:
            # 只从该行定位更早的触达；无关行不进入搜索。
            candidates = conn.execute(
                _CANDIDATE_SQL, (row["row_id"], begin_pos, cutoff)
            )
            for cand in candidates:
                scratch.add(cand["txn_id"], cand["begin_pos"])


def _atomic_write_bundle(conn: sqlite3.Connection, scratch: _Scratch,
                         out_path: str, meta: dict) -> tuple[int, int]:
    """把元数据行与已选事务的原始段流式写临时文件，成功后再原子替换。

    返回 (事务数, 字节数)。临时文件与目标同目录，失败只清理临时文件，
    绝不触碰目标路径上的原内容。
    """
    out_path = os.fspath(out_path)
    out_dir = os.path.dirname(os.path.abspath(out_path))
    tmp_fd, tmp_path = tempfile.mkstemp(prefix=".ccs_repro_", suffix=".tmp",
                                        dir=out_dir)
    published = False
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(json.dumps(meta, ensure_ascii=False, separators=(",", ":"),
                                sort_keys=False))
            fh.write("\n")
            n = 0
            for sel in scratch.ordered():
                row = conn.execute(
                    "SELECT segment_json FROM repro_txn WHERE txn_id=?",
                    (sel["txn_id"],),
                ).fetchone()
                if row is None:  # 归档与暂存不一致：宁可失败也不写半份包
                    raise StoreError(
                        "EXPORT_FAILED",
                        f"selected transaction {sel['txn_id']!r} disappeared "
                        "from the bundle archive during export",
                    )
                fh.write(row["segment_json"])
                fh.write("\n")
                n += 1
            fh.flush()
            os.fsync(fh.fileno())
            size = fh.tell()
        # 读快照在发布前结束；发布前失败不会覆盖原文件。
        conn.execute("COMMIT")
        os.chmod(tmp_path, 0o644)
        os.replace(tmp_path, out_path)
        published = True
        return n, size
    finally:
        if not published:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


def export_bundle(conn: sqlite3.Connection, txn_id: str, cutoff: int,
                  out_path: str) -> dict:
    """导出目标事务的可复现事务包。失败时目标路径保持原样（或不存在）。"""
    if not repro_enabled(conn):
        raise StoreError(
            "REPRO_NOT_ENABLED",
            "reproducible bundle archive is not enabled on this database",
        )
    if cutoff < 0:
        raise StoreError("BAD_CUTOFF", f"cutoff must be >= 0, got {cutoff}")
    if not out_path:
        raise StoreError("BAD_OUTPUT_PATH", "an output file path is required")

    in_txn = False
    scratch: Optional[_Scratch] = None
    try:
        # 单个读事务取水位、目标与全部闭包/输出数据，得到一致快照。
        conn.execute("BEGIN")
        in_txn = True
        watermark = conn.execute(
            "SELECT COALESCE(MAX(arrival_seq), 0) AS w FROM repro_txn"
        ).fetchone()["w"]
        if cutoff > watermark:
            raise StoreError(
                "CUTOFF_BEYOND_WATERMARK",
                f"cutoff {cutoff} exceeds committed arrival watermark {watermark}",
            )
        target = conn.execute(
            "SELECT txn_id, source_version, begin_pos, arrival_seq "
            "FROM repro_txn WHERE txn_id=?",
            (txn_id,),
        ).fetchone()
        if target is None:
            raise StoreError(
                "UNKNOWN_TXN",
                f"transaction {txn_id!r} is not present in the bundle archive",
            )
        if target["arrival_seq"] > cutoff:
            raise StoreError(
                "TARGET_NOT_VISIBLE",
                f"target transaction {txn_id!r} has arrival_seq "
                f"{target['arrival_seq']} > cutoff {cutoff}",
            )

        scratch = _Scratch()
        scratch.add(target["txn_id"], target["begin_pos"])
        _close_closure(conn, scratch, cutoff)

        meta = {
            "format": BUNDLE_FORMAT,
            "format_version": BUNDLE_FORMAT_VERSION,
            "source_version": target["source_version"],
            "cutoff": cutoff,
            "target_txn_id": target["txn_id"],
            "txn_count": scratch.count(),
        }
        count, size = _atomic_write_bundle(conn, scratch, out_path, meta)
        in_txn = False  # _atomic_write_bundle 已提交读事务
    except StoreError:
        if in_txn:
            _rollback_quiet(conn)
        raise
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        if in_txn:
            _rollback_quiet(conn)
        raise StoreError("EXPORT_FAILED", f"failed to write bundle: {exc}") from exc
    except Exception:
        if in_txn:
            _rollback_quiet(conn)
        raise
    finally:
        if scratch is not None:
            scratch.close()

    return {
        "path": os.fspath(out_path),
        "source_version": meta["source_version"],
        "cutoff": cutoff,
        "target_txn_id": txn_id,
        "txn_count": count,
        "bytes": size,
        "watermark": watermark,
    }


def _rollback_quiet(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass
