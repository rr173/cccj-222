# 本地变更订阅服务（Change Capture Service, CCS）

把持续变更的湖表按每个消费者自己的读取进度，交付给多个分析副本。
纯 Python 标准库 + 单文件 SQLite，无外部依赖。

## 1. 模块边界（四个独立模块）

| 模块 | 职责 | 不做什么 |
|---|---|---|
| `ccs/source_store.py` | 源版本、事务段摄入、位置唯一性、`commit_seq` 全序、身份世代 `generation`、到达序号 `arrival_seq` | 不感知消费者、不折叠 |
| `ccs/netchange.py` | **单笔事务内**把同一行的多次修改折叠成净变化（纯函数） | 绝不跨事务合并、不碰消费者状态 |
| `ccs/consumer_state.py` | 注册/撤销/改参、拉取预留令牌、连续前沿确认、迟到补发、重放窗口 | 不删除源数据 |
| `ccs/cleanup.py` | 按全员保留需求 + 迟到保留裁决删除，写审计日志 | 唯一允许删除**订阅数据**的地方；不碰归档 |
| `ccs/field_history.py` | **可选**字段历史归档与审计查询（默认关闭）：摄入同事务写归档索引，按 `(行键, 字段, 位置窗口, 知识截点)` 输出历史区间 | 不回填旧库、不跨源、不感知消费者 |
| `ccs/inventory.py` | **可选**当前库存物化（默认关闭）：摄入同事务增量维护"每个库位多少件、多少行贡献"的现存货表 | 不回填旧库、不跨源；只看 `after.bin`/`after.qty`；不改变订阅与字段查询合同 |

`oracle/fold_oracle.py` 是**独立预期模型**：不 import 任何 `ccs` 代码，
用函数式 reduce + 世代状态机的另一种写法逐笔折叠，测试只拿它对拍，
预期数据从不调用产品的净变化函数生成。
`oracle/history_oracle.py` 是字段历史的**独立参考实现**：栈式配对归约 +
从位置 0 顺序重放（产品被禁止的求值路线），同样不 import 任何 `ccs` 代码。
`oracle/inventory_oracle.py` 是当前库存的**独立参考实现**：每次都把全部已收到
事务按源位置从头全量折叠（产品被禁止的求值路线），与增量物化逐次对拍。

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
ccs/            产品代码（schema.sql + 四个模块 + field_history.py + inventory.py + cli.py）
oracle/         独立预期模型（fold_oracle.py / history_oracle.py / inventory_oracle.py，禁止 import ccs）
tests/          确定性对拍、真实多进程 worker 与驱动、崩溃恢复、字段历史与库存物化演练
demo/           演练资料、可复跑脚本、输出归档
bin/ccs         隔离模式（python -I）CLI 启动器
```

## 4. 从空目录复跑

```bash
# 全部自动化测试（确定性 oracle 对拍 + 双消费者/清理真实进程交错 + 崩溃恢复
# + 字段历史归档/查询演练）
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

## 6. 字段历史审计查询（可选归档，默认关闭）

对账场景：上周出了一版对账单，本周补到一笔源位置更早的迟到事务。审计员需要针对
一个行键的一个字段，重现出单时（知识截点 `K`）看得到的值与来源，再用较新的截点
查同一段源位置，厘清差异从哪里开始。订阅重放交付的是逐笔净变化，本功能给出的是
按源位置解释的字段历史区间。

### 启用（仅限空库）

```bash
bin/ccs --db "$DB" enable-history
```

- 只能在还没有任何事务的库上启用（`HISTORY_REQUIRES_EMPTY_DB`）：不回填旧库，
  启用前已存在的数据不进归档；
- 启用后 `ingest` 在**同一个 SQLite 事务**里写归档索引
  （`hist_txn` / `hist_row_event` / `hist_field_write`），归档与摄入一同成功或失败；
- 未启用的库保持原订阅行为：不写归档、不做归档校验，连归档表都不会创建；
- 启用后只支持单源实例，`switch-source` 会被拒绝（`HISTORY_SINGLE_SOURCE`）。

### 查询

```bash
bin/ccs --db "$DB" field-history <row_id> <field> <start> <end> --cutoff K
```

- 窗口为源位置半开区间 `[start, end)`；两端不必恰好有事务，窗首承接此前已经
  生效的状态；
- `K` 是正常摄入返回的 `arrival_seq`：先以 `arrival_seq <= K` 决定当时知道哪些
  事务，再按这些事务的源位置解释字段历史。`K=0` 表示尚未见过任何事务；超出已
  归档到达水位报错 `CUTOFF_BEYOND_WATERMARK`；
- 一笔事务的效果在它的 `end_pos` 一起生效：窗口切到事务内部看不到中间写入，
  同一事务内按操作位置决定最终状态（同一字段最后写者胜）；
- 迟到事务只改变较新截点的答案，原截点的答案不变（归档只增不改）。

### 输出区间语义

输出按位置排序、无缝覆盖请求窗口的半开区间列表。每段标明：

- `row_state`：`unknown` / `deleted` / `exists`；
- `exists` 时 `lifecycle`：`{"insert_pos": N}`（插入开启新生命周期，以该插入的
  操作位置标识）或 `"unknown"`（没看到插入的生命周期，不推测）。首条可见操作之前，
  行是否存在未知；第一条是更新时，只知道它存在以及更新带来的字段；
- `exists` 时 `field`：`{"state": "unknown"}` 或
  `{"state": "known", "value": ..., "source": {"txn_id", "op_pos"}}`——来源是最后
  实际写入它的事务标识与操作位置。更新只覆盖 `after` 明确包含的字段，没提供
  `after` 时不改变任何字段，绝不拿前镜像补值；显式 JSON `null` 是已知值；
- 删除使行处于已删除状态，直到下一次插入；这期间的更新不能恢复该行；重新插入
  不继承旧字段或其来源；
- 同一事务内不输出零长度中间区间；相邻区间只有在状态、生命周期、值和来源均相同
  才合并——两次写入即使值一样，来源变了也会留下分界。

### 与订阅清理的关系

`cleanup` 仍只释放订阅数据（`txn` / `change_row` / `late_notice`），归档索引保留；
已归档的事务标识与源位置区间永不复用（重用返回 `DUP_TXN` / `POSITION_OVERLAP`）。
查询只走落盘的归档索引、只访问目标行的历史：窗首用索引定位已有状态，不从该行的
第一笔事务顺序重放到起点，也不逐位置枚举窗口。

### 手动示例

```bash
DB=$(mktemp -u)/hist.db
bin/ccs --db "$DB" enable-history
bin/ccs --db "$DB" begin-source lake-v1
bin/ccs --db "$DB" ingest demo/segments/T1.json     # 返回 arrival_seq=1
bin/ccs --db "$DB" ingest demo/segments/T3.json     # 返回 arrival_seq=2
bin/ccs --db "$DB" field-history r5 v 150 400 --cutoff 1   # 出单时的答案
bin/ccs --db "$DB" field-history r5 v 150 400 --cutoff 2   # 较新截点的答案
```

测试见 `tests/test_field_history.py`：手算小例子 + 独立参考实现矩阵对拍 +
真实清理后重开核对 + 未启用库行为 + CLI 端到端。

## 7. 当前库存物化（可选功能，默认关闭）

配货服务需要一张随 CCS 摄入自动更新的**当前库存表**：每个库位现在有多少件、
由多少条库存行贡献。服务重启后直接读这张落盘表，不遍历订阅记录
（`txn`/`change_row`），也不逐行查询字段历史。

### 启用（仅限单源空库、首次摄入前）

```bash
bin/ccs --db "$DB" enable-inventory
```

- 只能在还没有任何事务的库上启用（`INVENTORY_REQUIRES_EMPTY_DB`）：不回填旧库，
  启用前已存在的数据不进库存日志；
- 启用后 `ingest` 在**同一个 SQLite 事务**里增量维护库存日志与汇总
  （`inv_op` / `inv_field_write` / `inv_row_state` / `inv_bin_summary` / `inv_meta`），
  库存计算与源端摄入一同成功或失败；未启用的库连 `inv_*` 表都不创建，行为完全不变；
- 启用后只支持单源实例，`switch-source` 会被拒绝（`INVENTORY_SINGLE_SOURCE`）；
- 可与 `enable-history` 同时启用，两个特性各自独立，互不改变对方的输出合同；
- 启用是幂等的，重复调用返回 `already_enabled: true`。

### 库存规则（只看 `after` 中的 `bin` 和 `qty`，不从前镜像取值）

- 按**源位置**解释迄今成功摄入的事务，按**操作位置**解释同一事务中的库存行：
  INSERT 建立新行并重置两个字段（不继承上一生命周期）；DELETE 清空该行；
  UPDATE 只改 `after` 里明确出现的字段；**尚未插入或已删除的行不接受 UPDATE**；
- 一行只有同时具备"非空字符串 `bin`"与"非负整数 `qty`（布尔值不算整数）"
  时才贡献一个库位。字段缺失与显式无效值都不贡献；UPDATE 省略某字段=保留，
  显式写 `null` 或其他无效值=清掉该字段的贡献资格；
- 库位名按原字符串区分（SQLite BINARY 排序）。一条有效行在一个库位算一条贡献行；
  某库位贡献行数降到 0 时该库位整行移除。

**迟到事务不打补丁、按源位置重算受影响行。** 例如一行先入 A、后收到移去 B 的
更新，若之后才到达一笔源位置介于两者之间的出库删除，则该删除在源顺序上终结了
该行，后来的 B 更新不再生效，B 的总数与贡献行相应扣减；若再补到一笔同区间的
重插，更新又会在新生命周期上重新生效。产品从不把新到达的操作简单当成对当前行
的补丁。

### 读取（只读，单快照）

```bash
bin/ccs --db "$DB" inventory
```

输出已提交的最新 `arrival_seq`（空库为 0）和按库位名排序的 JSON 汇总；
水位与全部库位在同一个读事务里取自同一提交快照：

```json
{
  "ok": true,
  "data": {
    "arrival_seq": 3,
    "bins": [
      {"bin": "B", "total_qty": 5, "contributing_rows": 1}
    ]
  }
}
```

只交付当前汇总：没有历史汇总查询，也没有新的订阅接口。

### 增量维护与验证方式（不全量重算）

- 每次摄入只收集**本变更段涉及的 row_id**，对每个这样的行用索引定位其当前贡献：
  最后一次 INSERT、其后是否存在 DELETE、本生命周期内 `bin`/`qty` 各自的最后一次
  显式写入（`(row_id, ...)` 前缀上的索引搜索/有界区间扫描），不线性重放该行的
  全部历史，也不扫描其他行；随后只调整该行**原先和现在贡献的库位**；
- 库存日志（`inv_op`/`inv_field_write`）与物化结果不参与订阅清理：`cleanup`
  仍只释放 `txn`/`change_row`/`late_notice`。因此清理后重开库再补入旧位置的迟到
  事务，仍能修正汇总；已占用的事务标识与源位置区间永不复用
  （重用返回 `DUP_TXN` / `POSITION_OVERLAP`），不会与已接受事务重叠；
- 原子性：汇总写入失败会让整笔摄入回滚，不留下半次调拨、不前进 `arrival_seq`、
  不占用源位置。

测试见 `tests/test_inventory.py`（由 `tests/run_all.sh` 统一调度）：

- **手算反序到达**：逐次核对迟到删除使"未来补丁"失效、迟到重插又使补丁生效，
  以及同一事务里的多行多库位调拨；
- **独立全量折叠对拍**：`oracle/inventory_oracle.py` 不 import 任何 `ccs` 代码，
  每次把全部已收事务按源位置从头折叠；多组随机段在随机反序到达下，每摄入一笔
  就与产品物化汇总对拍一次；
- **真实清理后重开**：删掉全部订阅数据、重开库，再补一笔旧位置事务，汇总仍被
  正确修正；
- **回滚**：用测试侧 SQLite 触发器制造一次可复现的汇总写入失败（生产代码无故障
  开关），核对源端数据、到达水位、库存日志与汇总整体回到摄入前；
- **不全量重算的可检查依据**：重算查询的 `EXPLAIN QUERY PLAN` 全部为索引
  SEARCH；单行摄入在每行历史差 50 倍的两个库上追踪到的语句数恒定、每条库存日志
  读取都带行/事务/位置定位；未涉及库位的汇总行逐字节不变。

### 手动示例

```bash
DB=$(mktemp -u)/inv.db
bin/ccs --db "$DB" enable-inventory
bin/ccs --db "$DB" begin-source lake-v1
bin/ccs --db "$DB" inventory                        # 空库：arrival_seq=0, bins=[]
bin/ccs --db "$DB" ingest demo/segments/T1.json
bin/ccs --db "$DB" inventory
```
