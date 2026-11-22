-- 本地变更订阅服务 (Change Capture Service, CCS) 的全部持久化结构。
-- 所有写路径都在单个 SQLite 事务内完成，崩溃后由 SQLite 原子提交保证一致。

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- 全局元数据，只有一行 (id = 1)
CREATE TABLE IF NOT EXISTS meta (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    source_version TEXT NOT NULL,
    next_arrival  INTEGER NOT NULL DEFAULT 1  -- 摄入到达序号，只用于"迟到"判定
);

-- 一个完整事务 = 一段连续 source_position 区间，同一位置只属于一个事务
CREATE TABLE IF NOT EXISTS txn (
    txn_id        TEXT PRIMARY KEY,
    source_version TEXT NOT NULL,
    begin_pos     INTEGER NOT NULL,
    end_pos       INTEGER NOT NULL,
    state         TEXT NOT NULL CHECK (state IN ('open', 'complete')),
    commit_seq    INTEGER,                      -- 完成时刻按 end_pos 全序分配，未完成时为 NULL
    arrival_seq   INTEGER NOT NULL,            -- 摄入顺序，用于识别迟到
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_txn_commit
    ON txn(source_version, commit_seq) WHERE state = 'complete';

-- 事务内的每一条变更（同一 source_position 全局唯一）
CREATE TABLE IF NOT EXISTS change_row (
    source_version TEXT NOT NULL,
    source_position INTEGER NOT NULL,
    txn_id        TEXT NOT NULL REFERENCES txn(txn_id),
    row_id        TEXT NOT NULL,
    op            TEXT NOT NULL CHECK (op IN ('INSERT', 'UPDATE', 'DELETE')),
    before_json   TEXT,                         -- NULL 表示源端没有提供前镜像
    after_json    TEXT,
    generation    INTEGER NOT NULL,            -- 该行身份世代，见 source_store 中的确定性算法
    arrival_seq   INTEGER NOT NULL,
    PRIMARY KEY (source_version, source_position)
);
CREATE INDEX IF NOT EXISTS ix_row_txn ON change_row(txn_id);
CREATE INDEX IF NOT EXISTS ix_row_identity
    ON change_row(row_id, source_version, source_position);

-- 消费者注册状态
CREATE TABLE IF NOT EXISTS consumer (
    name           TEXT PRIMARY KEY,
    source_version TEXT NOT NULL,
    reg_commit_seq INTEGER NOT NULL,            -- 注册时刻最新完整事务，新消费者的起点前沿
    reg_arrival_seq INTEGER NOT NULL,           -- 注册时刻到达序号，迟到判定只看此后到达的事务
    acked_commit_seq INTEGER NOT NULL,          -- 连续确认前沿（完整事务序号）
    acked_end_pos  INTEGER NOT NULL,            -- 前沿对应的源位置
    retention_txns INTEGER NOT NULL,            -- 保留窗口：最少保留多少个历史事务
    pull_limit     INTEGER NOT NULL,            -- 每次拉取最多多少个事务
    need_before    INTEGER NOT NULL,            -- 1 = 需要完整行前镜像
    active         INTEGER NOT NULL DEFAULT 1,
    revoked_at     TEXT
);

-- 拉取预留令牌：令牌绑定 (源版本, 前沿, 查询参数)，重复拉取/确认幂等
CREATE TABLE IF NOT EXISTS pull_token (
    token        TEXT PRIMARY KEY,
    consumer     TEXT NOT NULL REFERENCES consumer(name),
    source_version TEXT NOT NULL,
    params_hash  TEXT NOT NULL,
    issued_at_acked_seq INTEGER NOT NULL,       -- 签发时的连续前沿
    high_commit_seq INTEGER NOT NULL,           -- 预留到的事务序号（含）
    high_end_pos INTEGER NOT NULL,
    response_json TEXT NOT NULL,                -- 预留内容，重放同一令牌返回同一结果
    consumed     INTEGER NOT NULL DEFAULT 0,    -- 是否已被 ack 使用
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 迟到补发通知：某完整事务的位置落入消费者已确认区间时生成，独立 ack，不推进前沿
CREATE TABLE IF NOT EXISTS late_notice (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    consumer      TEXT NOT NULL REFERENCES consumer(name),
    txn_id        TEXT NOT NULL REFERENCES txn(txn_id),
    status        TEXT NOT NULL CHECK (status IN ('PENDING', 'DELIVERED', 'ACKED')),
    UNIQUE(consumer, txn_id)
);
CREATE INDEX IF NOT EXISTS ix_late ON late_notice(consumer, status);

-- 清理审计
CREATE TABLE IF NOT EXISTS cleanup_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    deleted_txn_id TEXT NOT NULL,
    commit_seq     INTEGER NOT NULL,
    begin_pos      INTEGER NOT NULL,
    end_pos        INTEGER NOT NULL,
    reason         TEXT NOT NULL,
    created_at     TEXT NOT NULL DEFAULT (datetime('now'))
);
