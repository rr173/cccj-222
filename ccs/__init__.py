"""本地变更订阅服务 (Change Capture Service)。

模块边界：
  source_store   源端存储：源版本、事务、变更行、身份世代
  netchange      净变化计算：事务内折叠（纯函数，不跨事务）
  consumer_state 消费者状态：注册/拉取预留/确认/迟到补发/重放窗口
  cleanup        清理：按全员保留需求 + 迟到保留裁决删除
  field_history  可选字段历史归档与审计查询（默认关闭，空库启用，不回填）
  inventory      可选当前库存物化：随摄入同事务增量维护库位汇总（默认关闭）
  repro          可选可复现事务包归档与导出（默认关闭，空库启用，不回填）
"""
from . import source_store, netchange, consumer_state, cleanup  # noqa: F401

__all__ = ["source_store", "netchange", "consumer_state", "cleanup"]
