"""净变化计算模块：把一笔事务内对同一行的多次修改折叠成净变化。

硬规则：
  1. 一次 fold_txn 调用只处理一个事务，不同事务永不合并；
  2. 同一行在事务内可以多次修改，按 source_position 顺序归约；
  3. 身份世代 generation 由源端存储确定性给出：
       - 插入开启新世代；删除终结当前世代；删除后重新插入是两个净变化记录；
       - INSERT -> UPDATE -> DELETE 完全抵消时，该行在本事务无净变化；
  4. 前镜像缺失时只交付"字段未知"标记，绝不伪造旧值。

本模块是纯函数式的：输入 sqlite 行（或等价 dict 列表），输出普通 dict，
不读写任何消费者状态，方便与 oracle/fold_oracle.py 的独立实现对拍。
"""
from __future__ import annotations

import json
from typing import Any, Optional

from .source_store import list_rows_of_txn


def _loads(maybe_json: Optional[str]) -> Optional[dict]:
    return json.loads(maybe_json) if maybe_json is not None else None


def _merge_missing_fields(known: dict, raw_before: Optional[dict]) -> tuple[dict, list[str], bool]:
    """归约 UPDATE 的前镜像：取最早出现的已知字段；记录从未见过的字段。

    返回 (before_known, unknown_fields, all_unknown)。
    raw_before 为 None（源端没给前镜像）时不产生任何伪造字段。
    """
    before = dict(known)
    unknown: set[str] = set()
    if raw_before is None:
        return before, sorted(unknown), len(before) == 0
    for key, value in raw_before.items():
        if key not in before:
            if value is None:
                unknown.add(key)
            else:
                before[key] = value
    return before, sorted(unknown), False


def fold_txn(rows: list[Any]) -> list[dict]:
    """把单笔事务的变更行折叠成净变化记录列表。

    rows: 按 source_position 排序的 sqlite3.Row 或 dict，每行包含
          source_position, row_id, op, before_json/after_json, generation。
    输出按 (row_id, generation, min_position) 排序，保证可重放顺序稳定。
    """
    # live[row_id] 保存"当前活着的世代"的折叠状态；删除后清空，下一个 INSERT 另起。
    live: dict[str, dict] = {}
    # 已终结世代（删除墓碑）保存在这里，与之后的重新插入区分开。
    tombstones: list[dict] = []

    for raw in rows:
        r = dict(raw)
        row_id = r["row_id"]
        op = r["op"]
        gen = r["generation"]
        pos = r["source_position"]
        before = _loads(r.get("before_json"))
        after = _loads(r.get("after_json"))
        before_missing = r.get("before_json") is None

        if op == "INSERT":
            if row_id in live:
                # 没有先 DELETE 就 INSERT 同一身份：源端语义非法
                raise ValueError(
                    f"row {row_id!r} re-INSERTed at pos {pos} without intervening DELETE"
                )
            live[row_id] = {
                "row_id": row_id,
                "generation": gen,
                "net_op": "INSERT",
                "min_pos": pos,
                "max_pos": pos,
                "before_known": {},
                "before_unknown_fields": [],
                "before_all_unknown": False,
                "after": dict(after or {}),
                "positions": [pos],
                "ops": ["INSERT"],
            }
            continue

        if row_id not in live:
            # 对未见过 / 已删除的行做 UPDATE/DELETE：
            # 只能作为独立墓碑/未知世代记录，不能虚构一个 INSERT。
            if op == "DELETE":
                tombstones.append(_orphan_delete(row_id, gen, pos, before, before_missing))
            else:
                live[row_id] = _orphan_update(row_id, gen, pos, before, after, before_missing)
            continue

        acc = live[row_id]
        acc["max_pos"] = pos
        acc["positions"].append(pos)
        acc["ops"].append(op)

        if op == "UPDATE":
            if after:
                acc["after"].update(after)
            known, unknown, all_unknown = _merge_missing_fields(
                acc["before_known"], None if before_missing else before
            )
            acc["before_known"] = known
            # 累计未知字段（多次缺镜像更新，未知集合只增不减）
            merged_unknown = set(acc["before_unknown_fields"]) | set(unknown)
            if before_missing:
                # 源端明确没有前镜像：after 里出现、但已知前镜像里没有的字段全部未知
                for key in (after or {}):
                    if key not in known:
                        merged_unknown.add(key)
            acc["before_unknown_fields"] = sorted(merged_unknown)
            acc["before_all_unknown"] = len(known) == 0 and len(merged_unknown) > 0
            if acc["net_op"] == "INSERT":
                # INSERT 后再 UPDATE，净操作仍是 INSERT（新世代的建立）
                acc["net_op"] = "INSERT"
            else:
                acc["net_op"] = "UPDATE"

        elif op == "DELETE":
            known, unknown, _ = _merge_missing_fields(
                acc["before_known"], None if before_missing else before
            )
            acc["before_known"] = known
            merged_unknown = set(acc["before_unknown_fields"]) | set(unknown)
            if before_missing:
                for key in list(acc["after"].keys()):
                    if key not in known:
                        merged_unknown.add(key)
            acc["before_unknown_fields"] = sorted(merged_unknown)
            acc["before_all_unknown"] = len(known) == 0 and len(merged_unknown) > 0
            if acc["net_op"] == "INSERT":
                # INSERT -> ... -> DELETE：本事务内完全抵消，不产生任何净变化
                del live[row_id]
            else:
                # UPDATE -> DELETE（活世代来自事务之前）：净变化为删除墓碑
                acc["net_op"] = "DELETE"
                acc["after"] = None
                tombstones.append(acc)
                del live[row_id]

    records = list(live.values()) + tombstones
    return [_finalize(rec) for rec in sorted(
        records, key=lambda x: (x["row_id"], x["generation"], x["min_pos"])
    )]


def _orphan_delete(row_id, gen, pos, before, before_missing) -> dict:
    unknown = sorted((before or {}).keys()) if not before_missing else []
    return {
        "row_id": row_id,
        "generation": gen,
        "net_op": "DELETE",
        "min_pos": pos,
        "max_pos": pos,
        "before_known": dict(before or {}) if not before_missing else {},
        "before_unknown_fields": unknown if before_missing else [],
        "before_all_unknown": before_missing,
        "after": None,
        "positions": [pos],
        "ops": ["DELETE"],
    }


def _orphan_update(row_id, gen, pos, before, after, before_missing) -> dict:
    known = dict(before or {}) if not before_missing else {}
    unknown: set[str] = set()
    if before_missing:
        unknown.update((after or {}).keys())
    return {
        "row_id": row_id,
        "generation": gen,
        "net_op": "UPDATE",
        "min_pos": pos,
        "max_pos": pos,
        "before_known": known,
        "before_unknown_fields": sorted(unknown),
        "before_all_unknown": before_missing,
        "after": dict(after or {}),
        "positions": [pos],
        "ops": ["UPDATE"],
    }


def _finalize(rec: dict) -> dict:
    return {
        "row_id": rec["row_id"],
        "generation": rec["generation"],
        "net_op": rec["net_op"],
        "position_min": rec["min_pos"],
        "position_max": rec["max_pos"],
        "source_positions": rec["positions"],
        "folded_from_ops": rec["ops"],
        "before": rec["before_known"],
        "before_field_unknown": rec["before_unknown_fields"],
        "before_all_unknown": rec["before_all_unknown"],
        "after": rec["after"],
    }


def fold_txn_from_db(conn, txn_id: str) -> list[dict]:
    """供服务层调用：从数据库读取一笔事务并折叠。"""
    return fold_txn(list_rows_of_txn(conn, txn_id))


def apply_consumer_view(record: dict, need_before: bool) -> dict:
    """按消费者参数整形净变化记录（折叠本身对所有消费者一致，这里只做视图裁剪）。

    need_before=False 时不下发前镜像字段，但仍保留未知标记结构。
    缺前镜像时 before 永远是空 dict + before_all_unknown/未知字段列表，
    消费者据此只能得到"明确未知"状态。
    """
    view = dict(record)
    if not need_before:
        view["before"] = None
        view["before_suppressed"] = True
    else:
        view["before_suppressed"] = False
    return view
