"""字段历史独立预期模型（oracle）。不 import ccs，与产品实现完全分离。

与产品 ccs/field_history.py 刻意不同的另一套写法：
  * 事务内净效果用"插入/删除配对抵消"的栈式归约（产品用检查点状态机模拟）；
  * 窗口求值用从位置 0 开始的**顺序重放**（产品被禁止这样做，必须用索引定位
    窗首）——两边路线完全不同，结果一致才说明索引定位与区间合并没有抄错。

输入是与摄入 JSON 同构的普通 dict（按到达顺序排列，arrival_seq = 下标 + 1），
输出是与产品查询相同的区间 dict 列表，供 tests/test_field_history.py 对拍。
"""
from __future__ import annotations

import json
from typing import Optional


# --------------------------------------------------------------------------- #
# 单笔事务对一行的净效果（在事务 end_pos 原子生效）。独立写法：配对抵消。
# --------------------------------------------------------------------------- #
def _txn_row_effect(ops: list[dict], row_id: str) -> Optional[tuple]:
    """返回 (kind, lifecycle_pos, fields) 或 None（完全抵消/无字段写入）。

    kind: INSERT / UPDATE / DELETE；fields: {field: (value, op_pos)}。
    """
    inserts: list[dict] = []   # 未配对的 INSERT（栈）
    real_delete = False        # 是否存在未被插入抵消的 DELETE
    updates: list[dict] = []
    for op in ops:
        if op["row_id"] != row_id:
            continue
        if op["op"] == "INSERT":
            inserts.append(op)
        elif op["op"] == "DELETE":
            if inserts:
                inserts.pop()  # 与最近一个未配对插入相互抵消（其中的更新一并消失）
            else:
                real_delete = True
        else:
            # 未配对插入生命期内的更新随抵消消失；已删除行上的更新不能恢复该行
            if not inserts and not real_delete:
                updates.append(op)

    if inserts:
        # 最后一个未配对插入开启新生命周期；其后的删除不可能存在（否则已配对），
        # 字段 = 该插入的 after + 其后各次更新（同字段后写覆盖先写）。
        last = inserts[-1]
        fields: dict[str, tuple] = {}
        for op in ops:
            if op["row_id"] != row_id:
                continue
            if op["source_position"] < last["source_position"]:
                continue
            if op["op"] in ("INSERT", "UPDATE") and op.get("after"):
                for k, v in op["after"].items():
                    fields[k] = (v, op["source_position"])
        return ("INSERT", last["source_position"], fields)
    if real_delete:
        # 删除终结一切：之前的更新被抹去，之后的更新不能恢复该行
        return ("DELETE", None, {})
    fields = {}
    for op in updates:
        if op.get("after"):
            for k, v in op["after"].items():
                fields[k] = (v, op["source_position"])
    if fields:
        return ("UPDATE", None, fields)
    return None


# --------------------------------------------------------------------------- #
# 顺序重放求值：从位置 0 开始按 eff_pos 逐个应用事件，裁剪到窗口输出。
# --------------------------------------------------------------------------- #
def _apply(row_state, lifecycle, fstate, kind, lifecycle_pos, fields, txn_id, field):
    if kind == "INSERT":
        if field in fields:
            v, pos = fields[field]
            return ("exists", lifecycle_pos, ("known", v, txn_id, pos))
        return ("exists", lifecycle_pos, ("unknown",))
    if kind == "DELETE":
        return ("deleted", None, None)
    # UPDATE
    if row_state == "deleted":
        return (row_state, lifecycle, fstate)  # 删除期间的更新不能恢复该行
    if field in fields:
        v, pos = fields[field]
        new_f = ("known", v, txn_id, pos)
    elif row_state == "unknown":
        new_f = ("unknown",)
    else:
        new_f = fstate
    if row_state == "unknown":
        # 首条可见操作是更新：只知道行存在，生命周期未知
        return ("exists", "unknown", new_f)
    return (row_state, lifecycle, new_f)


def _canon(v) -> str:
    return json.dumps(v, sort_keys=True)


def _key(row_state, lifecycle, fstate) -> tuple:
    if fstate is None or fstate[0] == "unknown":
        fk = fstate
    else:
        fk = ("known", _canon(fstate[1]), fstate[2], fstate[3])
    return (row_state, lifecycle, fk)


def _fmt(frm, to, row_state, lifecycle, fstate) -> dict:
    iv = {"start": frm, "end": to, "row_state": row_state}
    if row_state == "exists":
        iv["lifecycle"] = {"insert_pos": lifecycle} if isinstance(lifecycle, int) else "unknown"
        if fstate is not None and fstate[0] == "known":
            iv["field"] = {"state": "known", "value": fstate[1],
                           "source": {"txn_id": fstate[2], "op_pos": fstate[3]}}
        else:
            iv["field"] = {"state": "unknown"}
    else:
        iv["lifecycle"] = None
        iv["field"] = None
    return iv


def field_history(segments: list[dict], row_id: str, field: str,
                  start: int, end: int, cutoff: int) -> list[dict]:
    """参考实现：顺序重放全部已知事务，输出与产品查询相同的区间列表。

    segments 按到达顺序给出；cutoff K 只让 arrival_seq <= K 的事务可见。
    """
    known = [seg for i, seg in enumerate(segments) if i + 1 <= cutoff]
    effects = []  # (end_pos, kind, lifecycle_pos, fields, txn_id)
    for seg in known:
        eff = _txn_row_effect(seg["ops"], row_id)
        if eff is None:
            continue
        end_pos = seg["ops"][-1]["source_position"]
        kind, lifecycle_pos, fields = eff
        effects.append((end_pos, kind, lifecycle_pos, fields, seg["txn_id"]))
    effects.sort(key=lambda x: x[0])

    row_state, lifecycle, fstate = "unknown", None, None
    intervals: list[dict] = []
    cur_from = 0  # 当前状态在位置轴上的生效起点

    def emit(frm: int, to: int) -> None:
        lo, hi = max(frm, start), min(to, end)
        if lo < hi:
            intervals.append(_fmt(lo, hi, row_state, lifecycle, fstate))

    for end_pos, kind, lifecycle_pos, fields, txn_id in effects:
        if end_pos >= end:
            break
        nxt = _apply(row_state, lifecycle, fstate, kind, lifecycle_pos, fields,
                     txn_id, field)
        if _key(*nxt) != _key(row_state, lifecycle, fstate):
            emit(cur_from, end_pos)
            cur_from = end_pos
        row_state, lifecycle, fstate = nxt
    emit(cur_from, end)
    return intervals
