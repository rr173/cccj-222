"""本地变更订阅服务 (Change Capture Service)。

模块边界：
  source_store   源端存储：源版本、事务、变更行、身份世代
  netchange      净变化计算：事务内折叠（纯函数，不跨事务）
  consumer_state 消费者状态：注册/拉取预留/确认/迟到补发/重放窗口
  cleanup        清理：按全员保留需求 + 迟到保留裁决删除
"""
from . import source_store, netchange, consumer_state, cleanup  # noqa: F401

__all__ = ["source_store", "netchange", "consumer_state", "cleanup"]
