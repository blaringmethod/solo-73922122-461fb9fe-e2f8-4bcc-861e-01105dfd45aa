"""SQLite 持久化层。"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE_DIR, "data", "app.db"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    name           TEXT NOT NULL UNIQUE,
    granularity    TEXT NOT NULL CHECK (granularity IN ('daily', 'hourly')),
    config_version INTEGER NOT NULL DEFAULT 1,
    description    TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS dependencies (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    downstream_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    upstream_id   INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    offset_days   INTEGER NOT NULL DEFAULT 0 CHECK (offset_days >= 0),
    UNIQUE (downstream_id, upstream_id)
);

CREATE TABLE IF NOT EXISTS run_records (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id        INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    partition      TEXT NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('success', 'failed')),
    config_version INTEGER NOT NULL CHECK (config_version >= 1),
    finished_at    TEXT NOT NULL,
    revision       INTEGER NOT NULL DEFAULT 1 CHECK (revision >= 1)
);
CREATE INDEX IF NOT EXISTS idx_records_task_part
    ON run_records (task_id, partition, finished_at);

-- 运行记录更正历史：run_records 行内保存原件（revision 1），
-- 每次更正在此追加一行（revision_no >= 2）。
-- 生效修订 = revision_no == run_records.revision 的那一行；无更正时即原件。
-- 外键不级联：已更正记录禁止删除，从约束上杜绝孤立修订。
CREATE TABLE IF NOT EXISTS record_revisions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    record_id   INTEGER NOT NULL REFERENCES run_records(id),
    revision_no INTEGER NOT NULL CHECK (revision_no >= 2),
    status      TEXT NOT NULL CHECK (status IN ('success', 'failed')),
    finished_at TEXT NOT NULL,
    reason      TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    UNIQUE (record_id, revision_no)
);
CREATE INDEX IF NOT EXISTS idx_revisions_record
    ON record_revisions (record_id, revision_no);

CREATE TABLE IF NOT EXISTS plans (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at    TEXT NOT NULL,
    target_name   TEXT NOT NULL,
    biz_date      TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('generated', 'rejected')),
    reject_reason TEXT NOT NULL DEFAULT '',
    snapshot_json TEXT NOT NULL,
    items_json    TEXT NOT NULL DEFAULT '[]',
    summary_json  TEXT NOT NULL DEFAULT '{}'
);
"""

# 各记录的生效修订视图：计划与总览统一以此为准（再按完成时间挑分区最新记录）
EFFECTIVE_RECORDS_SQL = """
SELECT r.id, r.task_id, r.partition, r.config_version, r.revision,
       COALESCE(rev.status, r.status)           AS status,
       COALESCE(rev.finished_at, r.finished_at) AS finished_at,
       rev.reason      AS correction_reason,
       rev.created_at  AS corrected_at
FROM run_records r
LEFT JOIN record_revisions rev
       ON rev.record_id = r.id AND rev.revision_no = r.revision
"""


def get_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


@contextmanager
def immediate_tx():
    """BEGIN IMMEDIATE 事务：让「检查-修改」串行化，避免并发交错。

    进入即取写锁，并发请求按 busy_timeout 排队后到齐，
    后进入者读到的是前者已提交的状态（乐观锁校验在其后执行）。
    """
    conn = get_conn()
    try:
        conn.isolation_level = None
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        # 旧库迁移：补 run_records.revision 列
        cols = [r["name"] for r in
                conn.execute("PRAGMA table_info(run_records)").fetchall()]
        if "revision" not in cols:
            conn.execute(
                "ALTER TABLE run_records"
                " ADD COLUMN revision INTEGER NOT NULL DEFAULT 1")


def rows(conn: sqlite3.Connection, sql: str, params=()) -> list[dict]:
    return [dict(r) for r in conn.execute(sql, params).fetchall()]
