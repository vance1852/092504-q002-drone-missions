"""封装 SQLite 连接、建表和事务边界。"""

from __future__ import annotations

import sqlite3
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
CREATE TABLE IF NOT EXISTS task_templates (
    template_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    name TEXT NOT NULL,
    skill_type TEXT NOT NULL,
    duration_minutes INTEGER NOT NULL CHECK(duration_minutes > 0),
    required_qualification TEXT NOT NULL,
    required_level TEXT NOT NULL,
    required_equipment_json TEXT NOT NULL,
    area_type TEXT NOT NULL,
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS personnel_qualifications (
    qual_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    holder_actor_id TEXT NOT NULL REFERENCES actors(actor_id),
    qualification_code TEXT NOT NULL,
    level TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    valid_until TEXT NOT NULL,
    granted_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(site_id, holder_actor_id, qualification_code)
);
CREATE TABLE IF NOT EXISTS training_areas (
    area_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    name TEXT NOT NULL,
    area_type TEXT NOT NULL,
    capacity INTEGER NOT NULL CHECK(capacity >= 1),
    status TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version >= 1),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS equipment_items (
    equipment_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    name TEXT NOT NULL,
    equipment_type TEXT NOT NULL,
    status TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version >= 1),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS restriction_windows (
    window_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    scope_type TEXT NOT NULL,
    scope_id TEXT,
    severity TEXT NOT NULL,
    reason TEXT NOT NULL,
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    status TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version >= 1),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS schedule_plans (
    plan_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    template_id TEXT NOT NULL REFERENCES task_templates(template_id),
    team_id TEXT NOT NULL,
    instructor_id TEXT NOT NULL REFERENCES actors(actor_id),
    desired_start TEXT NOT NULL,
    desired_end TEXT NOT NULL,
    status TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version >= 1),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS plan_slots (
    slot_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES schedule_plans(plan_id),
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    plan_version INTEGER NOT NULL,
    area_id TEXT NOT NULL REFERENCES training_areas(area_id),
    instructor_id TEXT NOT NULL REFERENCES actors(actor_id),
    equipment_json TEXT NOT NULL,
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    status TEXT NOT NULL,
    merge_state TEXT,
    explanations_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_plan_slots_plan ON plan_slots(plan_id);
CREATE INDEX IF NOT EXISTS idx_plan_slots_status ON plan_slots(site_id, status);
CREATE TABLE IF NOT EXISTS resource_holds (
    hold_id TEXT PRIMARY KEY,
    slot_id TEXT NOT NULL REFERENCES plan_slots(slot_id),
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    released_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_resource_holds_resource ON resource_holds(resource_type, resource_id, status);
CREATE TABLE IF NOT EXISTS task_events (
    event_id TEXT PRIMARY KEY,
    slot_id TEXT NOT NULL REFERENCES plan_slots(slot_id),
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    source TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    merge_status TEXT NOT NULL,
    merge_order INTEGER,
    merge_note TEXT,
    recorded_by TEXT NOT NULL,
    received_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_task_events_slot ON task_events(slot_id);
CREATE TABLE IF NOT EXISTS review_records (
    review_id TEXT PRIMARY KEY,
    slot_id TEXT NOT NULL UNIQUE REFERENCES plan_slots(slot_id),
    plan_id TEXT NOT NULL REFERENCES schedule_plans(plan_id),
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    status TEXT NOT NULL,
    outcome TEXT,
    summary_json TEXT NOT NULL,
    notes TEXT,
    reviewed_by TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_review_records_status ON review_records(site_id, status);
CREATE TABLE IF NOT EXISTS dispositions (
    case_id TEXT PRIMARY KEY,
    slot_id TEXT NOT NULL REFERENCES plan_slots(slot_id),
    plan_id TEXT NOT NULL REFERENCES schedule_plans(plan_id),
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    reason TEXT NOT NULL,
    status TEXT NOT NULL,
    resolution TEXT,
    notes TEXT,
    resolved_by TEXT,
    created_at TEXT NOT NULL,
    resolved_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_dispositions_status ON dispositions(site_id, status);
CREATE TABLE IF NOT EXISTS schedule_decisions (
    decision_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    plan_id TEXT NOT NULL REFERENCES schedule_plans(plan_id),
    slot_id TEXT,
    decision TEXT NOT NULL,
    reasons_json TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_schedule_decisions_plan ON schedule_decisions(plan_id);
CREATE INDEX IF NOT EXISTS idx_schedule_decisions_slot ON schedule_decisions(slot_id);
"""


class Database:
    """管理 SQLite 数据库并为服务提供短事务。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self.connection = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self.connection.executescript(SCHEMA)

    @contextmanager
    def transaction(self, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        """在异常时回滚，在成功时提交。"""

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
