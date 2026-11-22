"""可复现事务包选样的独立预期模型（oracle）。不 import ccs。

与产品实现刻意不同的路线：
  * 产品从已选事务的行出发，用 (row_id, begin_pos) 索引做闭包；
  * 本模型**全量遍历**所有可见事务，对每对事务逐行比较，再求传递闭包。
这种写法在真实规模上不可接受，只用于小日志对拍，验证索引定位/前沿推进没有
改变选样结果。

输入 segments 按到达顺序排列，arrival_seq = 下标 + 1，与摄入 JSON 同构。
输出按源位置（事务区间起点）排序的原始段对象列表。
"""
from __future__ import annotations

from typing import Optional


def _begin(seg: dict) -> int:
    return min(op["source_position"] for op in seg["ops"])


def _rows(seg: dict) -> set[str]:
    return {op["row_id"] for op in seg["ops"]}


def select(segments: list[dict], target_txn_id: str,
           cutoff: int) -> Optional[list[dict]]:
    """返回选中的原始段（按 begin_pos, txn_id 排序）；目标不可见时返回 None。"""
    visible = {seg["txn_id"]: seg for i, seg in enumerate(segments) if i + 1 <= cutoff}
    if target_txn_id not in visible:
        return None
    target = visible[target_txn_id]

    # 先算出每对"可见事务 -> 同行的更早可见事务"，再全量求传递闭包。
    earlier: dict[str, set[str]] = {}
    for tid, seg in visible.items():
        need: set[str] = set()
        for other_id, other in visible.items():
            if other_id == tid:
                continue
            if _begin(other) >= _begin(seg):
                continue
            if _rows(other) & _rows(seg):
                need.add(other_id)
        earlier[tid] = need

    chosen = {target_txn_id}
    changed = True
    while changed:
        changed = False
        for tid in list(chosen):
            for dep in earlier[tid]:
                if dep not in chosen:
                    chosen.add(dep)
                    changed = True
    return sorted((visible[t] for t in chosen),
                  key=lambda s: (_begin(s), s["txn_id"]))
