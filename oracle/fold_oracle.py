"""独立预期模型（oracle）。

与产品代码完全隔离：本文件不 import ccs，净变化折叠用另一种写法
（函数式 reduce + 世代状态机），供 tests/test_deterministic.py 对拍。
如果两边输出不一致，说明产品实现或预期模型至少有一个错了——
预期数据绝不是调用产品的 netchange 函数生成的。

输入是与摄入 JSON 同构的普通 dict，输出是普通 dict，无数据库依赖。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# --------------------------------------------------------------------------- #
# 世代计算：独立实现。扫描该 row_id 在当前位置之前的 INSERT 次数。
# --------------------------------------------------------------------------- #
def generation_of(segments: list[dict], txn_index: int, op_index: int) -> int:
    row_id = segments[txn_index]["ops"][op_index]["row_id"]
    pos = segments[txn_index]["ops"][op_index]["source_position"]
    op = segments[txn_index]["ops"][op_index]["op"]
    inserts_before = 0
    for ti, seg in enumerate(segments):
        for oi, e in enumerate(seg["ops"]):
            if e["row_id"] == row_id and e["source_position"] < pos and e["op"] == "INSERT":
                inserts_before += 1
    if op == "INSERT":
        return inserts_before + 1
    return inserts_before  # 0 表示未知世代（对从未插入过的行做 UPDATE/DELETE）


# --------------------------------------------------------------------------- #
# 折叠：reduce 风格。每个 op 是一次纯状态转换，状态不可变地替换。
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class FoldState:
    # row_id -> 活着的世代累加器
    live: dict = field(default_factory=dict)
    # 已终结世代（墓碑）
    dead: tuple = ()


def _acc(row_id, gen, net_op, lo, hi, before, unknown, all_unknown, after, positions, ops):
    return {
        "row_id": row_id, "generation": gen, "net_op": net_op,
        "lo": lo, "hi": hi, "before": dict(before),
        "unknown": tuple(sorted(unknown)), "all_unknown": all_unknown,
        "after": None if after is None else dict(after),
        "positions": tuple(positions), "ops": tuple(ops),
    }


def _step(state: FoldState, entry: dict, gen: int) -> FoldState:
    rid = entry["row_id"]
    pos = entry["source_position"]
    op = entry["op"]
    before = entry.get("before")
    after = entry.get("after")
    missing = entry.get("before_missing", before is None)

    live = dict(state.live)
    dead = list(state.dead)

    if op == "INSERT":
        assert rid not in live, "INSERT without DELETE is illegal input"
        live[rid] = _acc(rid, gen, "INSERT", pos, pos, {}, (), False,
                         dict(after or {}), (pos,), ("INSERT",))
        return FoldState(live, tuple(dead))

    if rid not in live:
        if op == "DELETE":
            dead.append(_acc(rid, gen, "DELETE", pos, pos,
                             {} if missing else dict(before or {}),
                             tuple(sorted((after or {}).keys())) if missing else (),
                             missing, None, (pos,), ("DELETE",)))
        else:
            unk = tuple(sorted((after or {}).keys())) if missing else ()
            live[rid] = _acc(rid, gen, "UPDATE", pos, pos,
                             {} if missing else dict(before or {}), unk, missing,
                             dict(after or {}), (pos,), ("UPDATE",))
        return FoldState(live, tuple(dead))

    a = live[rid]
    new_before = dict(a["before"])
    unk = set(a["unknown"])
    if not missing and before:
        for k, v in before.items():
            if k not in new_before and v is not None:
                new_before[k] = v
    if missing:
        # 缺前镜像：只有"已知前镜像里没有、且本次 after 涉及"的字段才算未知；
        # 已经从更早的 UPDATE 拿到过旧值的字段保持已知，绝不重复标记。
        for k in (after or {}):
            if k not in new_before:
                unk.add(k)
    new_after = dict(a["after"]) if a["after"] is not None else None
    if op == "UPDATE":
        if after and new_after is not None:
            new_after.update(after)
        elif after:
            new_after = dict(after)
        net = "INSERT" if "INSERT" in a["ops"] else "UPDATE"
        live[rid] = _acc(rid, gen, net, a["lo"], pos, new_before, unk,
                         len(new_before) == 0 and len(unk) > 0, new_after,
                         a["positions"] + (pos,), a["ops"] + ("UPDATE",))
    else:  # DELETE
        if "INSERT" in a["ops"]:
            del live[rid]  # INSERT -> DELETE 完全抵消
        else:
            tomb = _acc(rid, gen, "DELETE", a["lo"], pos, new_before, unk,
                        len(new_before) == 0 and len(unk) > 0, None,
                        a["positions"] + (pos,), a["ops"] + ("DELETE",))
            dead.append(tomb)
            del live[rid]
    return FoldState(live, tuple(dead))


def fold_segment(segments: list[dict], txn_index: int) -> list[dict]:
    """预期模型的单笔事务折叠，输出结构与产品 netchange 对齐（键名做映射）。"""
    seg = segments[txn_index]
    state = FoldState()
    for oi, entry in enumerate(seg["ops"]):
        gen = generation_of(segments, txn_index, oi)
        state = _step(state, entry, gen)
    out = list(state.live.values()) + list(state.dead)
    out.sort(key=lambda x: (x["row_id"], x["generation"], x["lo"]))
    return [{
        "row_id": x["row_id"],
        "generation": x["generation"],
        "net_op": x["net_op"],
        "position_min": x["lo"],
        "position_max": x["hi"],
        "source_positions": list(x["positions"]),
        "folded_from_ops": list(x["ops"]),
        "before": x["before"],
        "before_field_unknown": list(x["unknown"]),
        "before_all_unknown": x["all_unknown"],
        "after": x["after"],
    } for x in out]


# --------------------------------------------------------------------------- #
# 消费者级预期：给定摄入顺序和 ack 行为，推导每次 pull 应看到的交付。
# --------------------------------------------------------------------------- #
@dataclass
class ExpectedConsumer:
    name: str
    retention_txns: int
    pull_limit: int
    need_before: bool
    start_seq: int = 0
    acked_seq: int = 0

    def expected_pull(self, segments_in_commit_order: list[dict],
                      segment_commit_seq: dict) -> list[dict]:
        """返回从当前前沿起、一次 pull（受 pull_limit 限制）应交付的净变化。"""
        ordered = sorted(segments_in_commit_order,
                         key=lambda s: segment_commit_seq[s["txn_id"]])
        result = []
        taken = 0
        for seg in ordered:
            seq = segment_commit_seq[seg["txn_id"]]
            if seq <= self.acked_seq:
                continue
            if taken >= self.pull_limit:
                break
            txn_index = segments_in_commit_order.index(seg)
            records = fold_segment(segments_in_commit_order, txn_index)
            if not self.need_before:
                for r in records:
                    r["before"] = None
                    r["before_suppressed"] = True
            result.append({
                "txn_id": seg["txn_id"],
                "commit_seq": seq,
                "records": records,
                "record_count": len(records),
            })
            taken += 1
        return result

    def expected_floor_seq(self, reg_seq: int) -> int:
        floor = self.acked_seq - self.retention_txns + 1
        if floor < 1:
            floor = 1
        if floor <= reg_seq:
            floor = reg_seq + 1
        return floor
