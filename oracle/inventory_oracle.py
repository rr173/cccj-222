"""当前库存的独立预期模型（oracle）。不 import ccs，与产品实现完全隔离。

与产品 ccs/inventory.py 刻意相反的路线：
  * 产品把每行当前贡献存在可定位的索引/摘要里，每次摄入只重算受影响的行；
  * 本参考实现每次都把**迄今收到的全部事务按源位置从头折叠一遍**（全量重放，
    产品被禁止的求值路线），只维护内存里的行状态，再聚合成库位汇总。

两边路线完全不同，逐次对拍一致才说明增量物化与源顺序折叠等价。

输入是与摄入 JSON 同构的普通 dict 列表（按到达顺序排列，arrival_seq=下标+1），
输出 (watermark, {bin: (total_qty, contributing_rows)})，与产品只读命令对齐。
"""
from __future__ import annotations

from typing import Any, Optional

# 字段缺失（INSERT 未提供 / UPDATE 一直省略）与"显式写入但无效"必须区分：
# 缺失沿用生命周期内更早的值；显式无效值清掉该字段的贡献资格。
MISSING = object()


def _valid_bin(value: Any) -> bool:
    return type(value) is str and len(value) > 0


def _valid_qty(value: Any) -> bool:
    # bool 是 int 子类，必须显式排除
    return type(value) is int and value >= 0


def _new_lifecycle(after: Optional[dict]) -> dict:
    """INSERT：两个字段整体重置；after 里没出现就是缺失，不继承旧生命周期。"""
    after = after if isinstance(after, dict) else {}
    return {
        "bin": after["bin"] if "bin" in after else MISSING,
        "qty": after["qty"] if "qty" in after else MISSING,
    }


def _all_ops_sorted(segments: list[dict]) -> list[dict]:
    """把已收到的事务全部摊平，按源位置（=源顺序）重排。"""
    ops = [op for seg in segments for op in seg["ops"]]
    return sorted(ops, key=lambda op: op["source_position"])


def fold_inventory(segments: list[dict]) -> tuple[int, dict[str, tuple[int, int]]]:
    """全量源顺序折叠，返回 (arrival 水位, {bin: (总件数, 贡献行数)})。

    水位就是已收到的事务数（arrival_seq 从 1 开始连续分配）。
    """
    # row_id -> 存活生命周期 {"bin": 值|MISSING, "qty": 值|MISSING}；
    # None 表示该行当前不存在（从未插入，或已被删除）。
    rows: dict[str, Optional[dict]] = {}

    for op in _all_ops_sorted(segments):
        rid = op["row_id"]
        kind = op["op"]
        after = op.get("after")
        fields = after if isinstance(after, dict) else {}

        if kind == "INSERT":
            rows[rid] = _new_lifecycle(after)
        elif kind == "DELETE":
            # 删除清空该行；之后的更新不能复活，重插则另起全新生命周期
            rows[rid] = None
        else:  # UPDATE：尚未插入或已删除的行不接受更新
            state = rows.get(rid)
            if state is None:
                continue
            # 只改明确出现的字段；省略=保留，显式 null/无效值=覆盖并使资格失效
            if "bin" in fields:
                state["bin"] = fields["bin"]
            if "qty" in fields:
                state["qty"] = fields["qty"]

    summary: dict[str, list[int]] = {}
    for state in rows.values():
        if state is None:
            continue
        b, q = state["bin"], state["qty"]
        if _valid_bin(b) and _valid_qty(q):
            slot = summary.setdefault(b, [0, 0])
            slot[0] += q
            slot[1] += 1
    return len(segments), {b: (v[0], v[1]) for b, v in summary.items()}


def expected_payload(segments: list[dict]) -> dict:
    """与产品 inventory 只读命令的 data 部分完全同构（库位按名排序）。"""
    watermark, summary = fold_inventory(segments)
    return {
        "arrival_seq": watermark,
        "bins": [
            {"bin": b, "total_qty": t, "contributing_rows": n}
            for b, (t, n) in sorted(summary.items())
        ],
    }
