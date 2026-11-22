#!/usr/bin/env python3
"""生成演练资料：9 个事务段，覆盖题目列出的全部场景。

位置规划（步长 100，迟到事务插进位置空隙）：
  T1 [100..102]  基线快照：r1,r2 插入
  T2 [200..202]  同一行插入后更新再删除 -> 事务内完全抵消，无净变化
  T3 [300..303]  一个事务内多次修改（两行，多 UPDATE，含缺前镜像）
  T4 [400..401]  删除后同身份重新插入 -> 两个世代
  T5 [500..502]  跨事务再更新 r1（世代延续）
  T7 [700..701]  正常新事务（先于迟到的 T6 到达）
  T6 [600..600]  迟到：位置落在已确认范围（晚于 T7 摄入）
  T8 [800..801]  供撤销/清理竞态使用
  T9 [900..902]  多折叠组合，供重放窗口使用
"""
from __future__ import annotations

import json
import os
import sys

SEGMENTS = [
    {
        "txn_id": "T1",
        "ops": [
            {"source_position": 100, "row_id": "r1", "op": "INSERT",
             "before": None, "after": {"v": 1, "tag": "a"}},
            {"source_position": 101, "row_id": "r2", "op": "INSERT",
             "before": None, "after": {"v": 10, "tag": "b"}},
            {"source_position": 102, "row_id": "r3", "op": "INSERT",
             "before": None, "after": {"v": 30}},
        ],
    },
    {
        # 插入 -> 更新 -> 删除：同一行同事务内多次修改，净变化完全抵消
        "txn_id": "T2",
        "ops": [
            {"source_position": 200, "row_id": "r4", "op": "INSERT",
             "before": None, "after": {"v": 1}},
            {"source_position": 201, "row_id": "r4", "op": "UPDATE",
             "before": {"v": 1}, "after": {"v": 2}},
            {"source_position": 202, "row_id": "r4", "op": "DELETE",
             "before": {"v": 2}, "after": None},
        ],
    },
    {
        # 一个事务内多次修改；r5 的第二次 UPDATE 缺前镜像 -> 字段未知标记
        "txn_id": "T3",
        "ops": [
            {"source_position": 300, "row_id": "r5", "op": "INSERT",
             "before": None, "after": {"v": 1, "k": "x"}},
            {"source_position": 301, "row_id": "r5", "op": "UPDATE",
             "before": {"v": 1, "k": "x"}, "after": {"v": 2}},
            {"source_position": 302, "row_id": "r5", "op": "UPDATE",
             "before_missing": True, "after": {"v": 3, "k": "z", "w": 99}},
            {"source_position": 303, "row_id": "r2", "op": "UPDATE",
             "before": {"v": 10, "tag": "b"}, "after": {"v": 11}},
        ],
    },
    {
        # 删除后同身份重新插入：DELETE 世代 1 墓碑 + INSERT 世代 2
        "txn_id": "T4",
        "ops": [
            {"source_position": 400, "row_id": "r3", "op": "DELETE",
             "before": {"v": 30}, "after": None},
            {"source_position": 401, "row_id": "r3", "op": "INSERT",
             "before": None, "after": {"v": 31, "tag": "new"}},
        ],
    },
    {
        # 跨事务延续：r1 已在 T1 插入（世代 1），这里两次 UPDATE 折叠成一个净 UPDATE
        "txn_id": "T5",
        "ops": [
            {"source_position": 500, "row_id": "r1", "op": "UPDATE",
             "before": {"v": 1, "tag": "a"}, "after": {"v": 5}},
            {"source_position": 501, "row_id": "r1", "op": "UPDATE",
             "before": {"v": 5, "tag": "a"}, "after": {"v": 6}},
            {"source_position": 502, "row_id": "r2", "op": "DELETE",
             "before": {"v": 11, "tag": "b"}, "after": None},
        ],
    },
    {
        # 正常到达的新事务
        "txn_id": "T7",
        "ops": [
            {"source_position": 700, "row_id": "r6", "op": "INSERT",
             "before": None, "after": {"v": 70}},
            {"source_position": 701, "row_id": "r6", "op": "UPDATE",
             "before": {"v": 70}, "after": {"v": 71}},
        ],
    },
    {
        # 迟到事务：位置 600 落在 T5(<=502) 与 T7(700) 之间，
        # 但在 T7 之后才摄入；对已确认到 T5/T7 的消费者进入迟到补发
        "txn_id": "T6",
        "ops": [
            {"source_position": 600, "row_id": "r1", "op": "UPDATE",
             "before_missing": True, "after": {"v": 60, "late": True}},
        ],
    },
    {
        # 撤销/清理竞态使用
        "txn_id": "T8",
        "ops": [
            {"source_position": 800, "row_id": "r7", "op": "INSERT",
             "before": None, "after": {"v": 80}},
            {"source_position": 801, "row_id": "r5", "op": "DELETE",
             "before": {"v": 3, "k": "z"}, "after": None},
        ],
    },
    {
        # 重放窗口组合
        "txn_id": "T9",
        "ops": [
            {"source_position": 900, "row_id": "r8", "op": "INSERT",
             "before": None, "after": {"v": 90}},
            {"source_position": 901, "row_id": "r8", "op": "UPDATE",
             "before": {"v": 90}, "after": {"v": 91}},
            {"source_position": 902, "row_id": "r3", "op": "UPDATE",
             "before_missing": True, "after": {"v": 32}},
        ],
    },
]

# 摄入顺序（T6 故意迟到）
INGEST_ORDER = ["T1", "T2", "T3", "T4", "T5", "T7", "T6", "T8", "T9"]


def main(out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    by_id = {s["txn_id"]: s for s in SEGMENTS}
    for txn_id in INGEST_ORDER:
        path = os.path.join(out_dir, f"{txn_id}.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(by_id[txn_id], fh, ensure_ascii=False, indent=2)
    manifest = {"ingest_order": INGEST_ORDER,
                "files": [f"{t}.json" for t in INGEST_ORDER]}
    with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)
    print(json.dumps({"written": len(SEGMENTS), "dir": out_dir,
                      "ingest_order": INGEST_ORDER}, ensure_ascii=False))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "demo/segments")
