"""封装 SQLite 连接、建表和事务边界。"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS organizations (
    organization_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS actors (
    actor_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sites (
    site_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    name TEXT NOT NULL,
    timezone_name TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version >= 1),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS domain_records (
    record_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    category TEXT NOT NULL,
    external_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES actors(actor_id),
    created_at TEXT NOT NULL,
    UNIQUE(site_id, category, external_key)
);
CREATE TABLE IF NOT EXISTS request_receipts (
    request_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    occurred_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS teams (
    team_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(site_id, code)
);
CREATE TABLE IF NOT EXISTS persons (
    person_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    code TEXT NOT NULL,
    display_name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('instructor', 'trainee')),
    qualifications_json TEXT NOT NULL,
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(site_id, code)
);
CREATE TABLE IF NOT EXISTS team_members (
    team_id TEXT NOT NULL REFERENCES teams(team_id),
    person_id TEXT NOT NULL REFERENCES persons(person_id),
    PRIMARY KEY (team_id, person_id)
);
CREATE TABLE IF NOT EXISTS task_templates (
    template_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    task_type TEXT NOT NULL,
    duration_minutes INTEGER NOT NULL CHECK(duration_minutes BETWEEN 5 AND 480),
    area_tag TEXT NOT NULL,
    equipment_types_json TEXT NOT NULL,
    crew_size INTEGER NOT NULL CHECK(crew_size >= 0),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(site_id, code)
);
CREATE TABLE IF NOT EXISTS training_areas (
    area_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    tag TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('available', 'closed')),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(site_id, code)
);
CREATE TABLE IF NOT EXISTS equipment (
    equipment_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    equipment_type TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('available', 'maintenance', 'retired')),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(site_id, code)
);
CREATE TABLE IF NOT EXISTS restriction_windows (
    window_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    scope TEXT NOT NULL CHECK(scope IN ('site', 'area', 'equipment')),
    resource_id TEXT NOT NULL,
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    reason TEXT NOT NULL,
    level TEXT NOT NULL CHECK(level IN ('normal', 'escalated')),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    escalated_at TEXT
);
CREATE TABLE IF NOT EXISTS plan_versions (
    plan_version_id TEXT PRIMARY KEY,
    plan_key TEXT NOT NULL,
    version_no INTEGER NOT NULL,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    status TEXT NOT NULL CHECK(status IN ('candidate', 'released', 'superseded')),
    horizon_start TEXT NOT NULL,
    horizon_end TEXT NOT NULL,
    generated_by TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    released_by TEXT,
    released_at TEXT,
    UNIQUE(plan_key, version_no)
);
CREATE TABLE IF NOT EXISTS plan_items (
    item_id TEXT PRIMARY KEY,
    plan_version_id TEXT NOT NULL REFERENCES plan_versions(plan_version_id),
    demand_id TEXT NOT NULL,
    rank_no INTEGER NOT NULL,
    team_id TEXT NOT NULL,
    template_id TEXT NOT NULL,
    area_id TEXT,
    equipment_ids_json TEXT NOT NULL,
    instructor_id TEXT,
    trainee_ids_json TEXT NOT NULL,
    planned_start TEXT,
    planned_end TEXT,
    decision TEXT NOT NULL CHECK(decision IN ('approved', 'rescheduled', 'unschedulable')),
    lifecycle TEXT NOT NULL DEFAULT 'planned'
        CHECK(lifecycle IN ('planned', 'revoked', 'manual')),
    reasons_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS resource_reservations (
    reservation_id TEXT PRIMARY KEY,
    plan_version_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    resource_type TEXT NOT NULL CHECK(resource_type IN ('site', 'area', 'equipment', 'instructor', 'team')),
    resource_id TEXT NOT NULL,
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('held', 'released', 'revoked')),
    created_at TEXT NOT NULL,
    released_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_reservations_lookup
    ON resource_reservations(resource_type, resource_id, status, starts_at, ends_at);
CREATE TABLE IF NOT EXISTS item_events (
    item_event_id TEXT PRIMARY KEY,
    item_id TEXT NOT NULL REFERENCES plan_items(item_id),
    kind TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_item_events_item ON item_events(item_id, created_at);
CREATE TABLE IF NOT EXISTS task_instances (
    task_id TEXT PRIMARY KEY,
    item_id TEXT NOT NULL UNIQUE REFERENCES plan_items(item_id),
    site_id TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_task_instances_site ON task_instances(site_id, state);
CREATE TABLE IF NOT EXISTS task_raw_events (
    event_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES task_instances(task_id),
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    source TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    accepted INTEGER NOT NULL CHECK(accepted IN (0, 1)),
    reject_reason TEXT,
    received_seq INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_raw_events_task ON task_raw_events(task_id, occurred_at, received_seq);
CREATE TABLE IF NOT EXISTS task_forced_states (
    anchor_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES task_instances(task_id),
    to_state TEXT NOT NULL,
    trigger TEXT NOT NULL,
    reason TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_forced_task ON task_forced_states(task_id, occurred_at);
CREATE TABLE IF NOT EXISTS task_transitions (
    transition_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT NOT NULL,
    trigger TEXT NOT NULL,
    reason TEXT,
    occurred_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_transitions_task ON task_transitions(task_id, occurred_at);
CREATE TABLE IF NOT EXISTS task_reviews (
    review_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK(status IN ('pending', 'completed')),
    telemetry_json TEXT,
    note TEXT,
    created_at TEXT NOT NULL,
    completed_by TEXT,
    completed_at TEXT
);
CREATE TABLE IF NOT EXISTS review_jobs (
    job_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    summary_id TEXT NOT NULL,
    telemetry_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'done')),
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(task_id, summary_id)
);
"""


class Database:
    """管理 SQLite 数据库并为服务提供短事务。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self.connection = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        # 单连接在多线程下使用，用进程内锁串行化事务；配合 BEGIN IMMEDIATE
        # 保证资源预占/释放在并发放行下仍原子完成。
        self._tx_lock = threading.RLock()
        self.connection.executescript(SCHEMA)
        self._migrate()

    def _migrate(self) -> None:
        """为早期版本数据库补齐后增的列。"""

        columns = {
            row["name"] for row in self.connection.execute("PRAGMA table_info(plan_items)")
        }
        if columns and "lifecycle" not in columns:
            self.connection.execute(
                "ALTER TABLE plan_items ADD COLUMN lifecycle TEXT NOT NULL DEFAULT 'planned'"
            )

    @contextmanager
    def transaction(self, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        """在异常时回滚，在成功时提交；同一进程内事务串行执行。"""

        with self._tx_lock:
            self.connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            try:
                yield self.connection
            except Exception:
                self.connection.rollback()
                raise
            else:
                self.connection.commit()

    def close(self) -> None:
        """关闭底层连接。"""

        self.connection.close()
