# 本地变更订阅服务（Change Capture Service, CCS）

把持续变更的湖表按每个消费者自己的读取进度，交付给多个分析副本。
纯 Python 标准库 + 单文件 SQLite，无外部依赖。

## 1. 模块边界（四个独立模块）

| 模块 | 职责 | 不做什么 |
|---|---|---|
| `ccs/source_store.py` | 源版本、事务段摄入、位置唯一性、`commit_seq` 全序、身份世代 `generation`、到达序号 `arrival_seq` | 不感知消费者、不折叠 |
| `ccs/netchange.py` | **单笔事务内**把同一行的多次修改折叠成净变化（纯函数） | 绝不跨事务合并、不碰消费者状态 |
| `ccs/consumer_state.py` | 注册/撤销/改参、拉取预留令牌、连续前沿确认、迟到补发、重放窗口 | 不删除源数据 |
| `ccs/cleanup.py` | 按全员保留需求 + 迟到保留裁决删除，写审计日志 | 唯一允许删除变更的地方 |

`oracle/fold_oracle.py` 是**独立预期模型**：不 import 任何 `ccs` 代码，
用函数式 reduce + 世代状态机的另一种写法逐笔折叠，测试只拿它对拍，
预期数据从不调用产品的净变化函数生成。

## 2. 关键语义

- **事务边界**：一个事务段 = 一段连续 `source_position` 区间；同一位置全局唯一，
  区间重叠 / 重复事务 / 段内乱序一律拒绝（`POSITION_OVERLAP`、`DUP_POSITION` 等）。
- **提交顺序**：事务完成时按 `end_pos` 分配单调 `commit_seq`，消费者按此全序重放。
- **事务内折叠**：同一行多次修改按位置归约；`INSERT→UPDATE→DELETE` 完全抵消则无净变化；
  `UPDATE→UPDATE` 合并；不同事务永不合并。
- **身份世代**：`generation` = 该行在该位置之前的 INSERT 次数 + 1（确定性推导，崩溃重放不变）。
  删除终结当前世代，删除后同身份重新插入产出 `DELETE(gen=N)` + `INSERT(gen=N+1)` 两条净变化。
- **前镜像缺失**：只交付 `before_field_unknown`（字段级）和 `before_all_unknown`（整行级）
  标记，`before` 里永远不会出现伪造的旧值。
- **连续前沿**：确认只接受当前前沿的下一个连续事务区间；跳跃确认返回 `JUMP_ACK`。
- **幂等**：重复拉取同一令牌返回预留快照、不推进；重复确认返回首次结果、前沿不重复推进。
- **迟到补发**：事务在消费者注册之后到达（`arrival_seq > reg_arrival_seq`）且位置整体落在
  已确认前沿之内（`end_pos <= acked_end_pos`）时，进入独立 `late_notice` 旁路：
  带 `late=true` 标记投递、独立确认、**不推进前沿、不改写已确认快照**。
- **令牌绑定**：`(source_version, 签发时前沿, 查询参数哈希)` 三元组。换源 →
  `TOKEN_SOURCE_STALE`/`SOURCE_SWITCHED`；改参 → `TOKEN_PARAMS_STALE`；前沿漂移 →
  `TOKEN_FRONTIER_STALE`；已消费 → `TOKEN_CONSUMED`。
- **清理**：删除条件三者同时满足——事务 complete、序号低于**最慢消费者**的保留下界
  （所有活跃消费者 floor 的最小值）、没有未 ACK 的迟到通知钉住。撤销的消费者不再贡献
  保留需求；重新注册从新前沿算起；窗口缩短不立即删数据，只改变下一次裁决。
- **持久化**：每个接口的“读状态→决策→写状态”都在单个 `BEGIN IMMEDIATE` 事务里；
  拉取预留先把响应快照写入 `pull_token`。进程在响应前退出，事务整体回滚，
  重开后无重复推进、无空洞。慢消费者积压全部留盘，单次内存只装 `pull_limit` 个事务的行。

## 3. 目录

```
ccs/            产品代码（schema.sql + 四个模块 + cli.py）
oracle/         独立预期模型（禁止 import ccs）
tests/          确定性对拍、真实多进程 worker 与驱动、崩溃恢复
demo/           演练资料、可复跑脚本、输出归档
bin/ccs         隔离模式（python -I）CLI 启动器
```

## 4. 从空目录复跑

```bash
# 全部自动化测试（确定性 oracle 对拍 + 双消费者/清理真实进程交错 + 崩溃恢复）
bash tests/run_all.sh

# 端到端 CLI 演练：在临时空库上跑全部场景，逐命令 JSON/退出码归档到 demo/output/
bash demo/run_demo.sh
```

### 手动 CLI 示例

```bash
DB=$(mktemp -u)/svc.db
bin/ccs --db "$DB" init
bin/ccs --db "$DB" begin-source lake-v1
bin/ccs --db "$DB" register alpha --retention 3 --pull-limit 2 --need-before --start-at-zero
bin/ccs --db "$DB" ingest demo/segments/T1.json
TOK=$(bin/ccs --db "$DB" pull alpha | python3 -I -c 'import json,sys;print(json.load(sys.stdin)["data"]["token"])')
bin/ccs --db "$DB" ack alpha "$TOK"
bin/ccs --db "$DB" pull-late alpha
bin/ccs --db "$DB" replay alpha 100 202
bin/ccs --db "$DB" cleanup-plan
bin/ccs --db "$DB" cleanup-run
bin/ccs --db "$DB" state
```

所有输出统一为 `{"ok": true, "data": ...}` 或 `{"ok": false, "error": {"code", "message"}}`，
失败退出码为 2（崩溃钩子为 17）。

## 5. 演练资料覆盖的场景

`demo/make_fixtures.py` 生成 9 个事务：

| 事务 | 位置 | 覆盖点 |
|---|---|---|
| T1 | 100–102 | 基线快照，多行插入 |
| T2 | 200–202 | 同一行插入→更新→删除，事务内抵消为零净变化 |
| T3 | 300–303 | 一事务多次修改 + 缺前镜像 UPDATE（字段未知标记） |
| T4 | 400–401 | 删除后同身份重新插入 → 世代 1 墓碑 + 世代 2 插入 |
| T5 | 500–502 | 跨事务延续：两次 UPDATE 折叠成一个净 UPDATE；删除 |
| T7 | 700–701 | 正常新事务（先于迟到的 T6 到达） |
| T6 | 600 | **迟到**：位置落在已确认范围，旁路补发、前沿不动 |
| T8 | 800–801 | 撤销/清理竞态使用 |
| T9 | 900–902 | 重放窗口组合 + 缺前镜像更新 |

`tests/test_deterministic.py` 另覆盖：两个消费者不同速度、重复拉取/确认、跳跃确认拒绝、
令牌换源/改参失效、撤销后其他消费者继续、重新注册前沿重置、清理依据与审计、窗口缩短。
`tests/test_concurrent.py` 用 3+1 个真实进程安排确认与清理交错，并含 `os._exit(17)`
崩溃恢复核对：不漏交付、不越过前沿、不提前删除。
