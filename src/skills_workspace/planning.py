"""训练计划排程、放行、执行归并与复盘的领域服务。

只处理训练计划与结构化遥测摘要，不接收原始飞控数据。
核心约定：

- 候选排程只产出可解释的方案，资源预占发生在值班教员放行的单个事务里；
- 限制升级只撤销尚未开始的放行，进行中的任务转人工处置；
- 执行事件按事件编号去重、按发生时刻归并重算唯一状态，原始事件全部保留；
- 遥测摘要先入待办，进程重启后可继续完成待复盘记录。
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from .audit import append_event, canonical_json, digest
from .clock import Clock, SystemClock
from .errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .storage import Database

GRID_MINUTES = 15

# 事件类型到允许的源状态：键为事件，值为可触发迁移的状态集合。
EVENT_RULES: dict[str, set[str]] = {
    "started": {"scheduled"},
    "paused": {"running"},
    "resumed": {"paused"},
    "ended": {"running", "paused"},
    "anomaly": {"running", "paused"},
}
# 事件到达但任务已经处于的状态（视为可忽略的重复）。
EVENT_DUPLICATE_STATES: dict[str, set[str]] = {
    "started": {"running", "paused"},
    "paused": {"paused"},
    "resumed": {"running"},
    "ended": {"ended", "aborted"},
    "anomaly": {"anomalous", "manual"},
}
TERMINAL_STATES = {"ended", "aborted", "revoked"}
RELEASABLE_ROLES = ("reviewer", "admin")
WRITER_ROLES = ("admin", "operator", "reviewer")


def parse_dt(value: Any, field: str) -> datetime:
    """解析必须带时区的时间字符串并归一到 UTC。"""

    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field} 必须是带时区的 ISO 时间字符串")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValidationError(f"{field} 时间格式无效") from exc
    if parsed.tzinfo is None:
        raise ValidationError(f"{field} 必须携带时区")
    return parsed.astimezone(timezone.utc)


def fmt_dt(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class TrainingService:
    """协调训练主数据、排程、放行与执行归并。"""

    def __init__(self, database: Database, clock: Clock | None = None) -> None:
        self.database = database
        self.clock = clock or SystemClock()

    def _now(self) -> str:
        return self.clock.now().isoformat().replace("+00:00", "Z")

    # ------------------------------------------------------------------ 通用

    def _actor(self, connection, actor_id: str):
        row = connection.execute("SELECT * FROM actors WHERE actor_id=?", (actor_id,)).fetchone()
        if row is None:
            raise NotFoundError("操作者不存在")
        if not row["active"]:
            raise PermissionDenied("操作者已停用")
        return row

    def _require(self, actor, *roles: str) -> None:
        if actor["role"] not in roles:
            raise PermissionDenied("当前角色不能执行该动作")

    def _site(self, connection, site_id: str):
        row = connection.execute("SELECT * FROM sites WHERE site_id=?", (site_id,)).fetchone()
        if row is None:
            raise NotFoundError("场所不存在")
        return row

    def _check_site_access(self, actor, site) -> None:
        if actor["organization_id"] != site["organization_id"] and actor["role"] != "admin":
            raise PermissionDenied("不能操作其他组织的场所")

    def _idempotent(self, connection, *, request_id: str, action: str,
                    payload: dict[str, Any], create) -> dict[str, Any]:
        request_id = str(request_id).strip()
        if not request_id:
            raise ValidationError("request_id 不能为空")
        payload_hash = digest(payload)
        row = connection.execute(
            "SELECT * FROM request_receipts WHERE request_id=?", (request_id,)
        ).fetchone()
        if row:
            if row["action"] != action or row["payload_hash"] != payload_hash:
                raise ConflictError("request_id 已被不同内容使用")
            result = {"request_id": request_id, "resource_type": row["resource_type"],
                      "resource_id": row["resource_id"], "replayed": True}
            result.update(json.loads(row["response_json"]))
            return result
        resource_type, resource_id, response = create()
        connection.execute(
            "INSERT INTO request_receipts(request_id,action,payload_hash,resource_type,"
            "resource_id,response_json,created_at) VALUES(?,?,?,?,?,?,?)",
            (request_id, action, payload_hash, resource_type, resource_id,
             canonical_json(response), self._now()),
        )
        return {"request_id": request_id, "resource_type": resource_type,
                "resource_id": resource_id, "replayed": False}

    @staticmethod
    def _codes(value: Any, field: str) -> list[str]:
        if not isinstance(value, list) or not value or any(not isinstance(v, str) or not v.strip() for v in value):
            raise ValidationError(f"{field} 必须是非空字符串数组")
        return [v.strip() for v in value]

    # ------------------------------------------------------------ 主数据登记

    def register_team(self, *, request_id: str, actor_id: str, site_id: str,
                      code: str, name: str) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "site_id": site_id, "code": code, "name": name}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, *WRITER_ROLES)
            self._check_site_access(actor, self._site(conn, site_id))
            code, name = str(code).strip(), str(name).strip()
            if not code or not name:
                raise ValidationError("code/name 不能为空")

            def create():
                try:
                    conn.execute(
                        "INSERT INTO teams(team_id,site_id,code,name,created_by,created_at) VALUES(?,?,?,?,?,?)",
                        (uuid.uuid4().hex, site_id, code, name, actor_id, self._now()),
                    )
                except Exception as exc:
                    raise ConflictError("队伍编号已经存在") from exc
                team_id = conn.execute("SELECT team_id FROM teams WHERE site_id=? AND code=?",
                                       (site_id, code)).fetchone()["team_id"]
                append_event(conn, actor_id=actor_id, action="team.registered", resource_type="team",
                             resource_id=team_id, detail={"site_id": site_id, "code": code},
                             occurred_at=self._now())
                return "team", team_id, {"team_id": team_id, "code": code}

            return self._idempotent(conn, request_id=request_id, action="register_team",
                                    payload=payload, create=create)

    def register_person(self, *, request_id: str, actor_id: str, site_id: str, code: str,
                        display_name: str, kind: str, qualifications: list[str]) -> dict[str, Any]:
        if not isinstance(qualifications, list) or any(
                not isinstance(v, str) or not v.strip() for v in qualifications):
            raise ValidationError("qualifications 必须是字符串数组")
        qualifications = [v.strip() for v in qualifications]
        payload = {"actor_id": actor_id, "site_id": site_id, "code": code,
                   "display_name": display_name, "kind": kind, "qualifications": qualifications}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, *WRITER_ROLES)
            self._check_site_access(actor, self._site(conn, site_id))
            code = str(code).strip()
            display_name = str(display_name).strip()
            if kind not in ("instructor", "trainee") or not code or not display_name:
                raise ValidationError("kind 必须是 instructor/trainee，code/name 不能为空")

            def create():
                try:
                    conn.execute(
                        "INSERT INTO persons(person_id,site_id,code,display_name,kind,"
                        "qualifications_json,active,created_by,created_at) VALUES(?,?,?,?,?,?,1,?,?)",
                        (uuid.uuid4().hex, site_id, code, display_name, kind,
                         canonical_json(qualifications), actor_id, self._now()),
                    )
                except Exception as exc:
                    raise ConflictError("人员编号已经存在") from exc
                person_id = conn.execute("SELECT person_id FROM persons WHERE site_id=? AND code=?",
                                         (site_id, code)).fetchone()["person_id"]
                append_event(conn, actor_id=actor_id, action="person.registered", resource_type="person",
                             resource_id=person_id,
                             detail={"site_id": site_id, "code": code, "kind": kind,
                             "qualifications": qualifications}, occurred_at=self._now())
                return "person", person_id, {"person_id": person_id, "code": code}

            return self._idempotent(conn, request_id=request_id, action="register_person",
                                    payload=payload, create=create)

    def add_team_members(self, *, request_id: str, actor_id: str, team_id: str,
                         person_codes: list[str]) -> dict[str, Any]:
        person_codes = self._codes(person_codes, "person_codes")
        payload = {"actor_id": actor_id, "team_id": team_id, "person_codes": person_codes}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, *WRITER_ROLES)
            team = conn.execute("SELECT * FROM teams WHERE team_id=?", (team_id,)).fetchone()
            if team is None:
                raise NotFoundError("队伍不存在")
            self._check_site_access(actor, self._site(conn, team["site_id"]))
            added = []
            for code in person_codes:
                person = conn.execute("SELECT * FROM persons WHERE site_id=? AND code=?",
                                      (team["site_id"], code)).fetchone()
                if person is None:
                    raise NotFoundError(f"人员 {code} 不存在")
                conn.execute("INSERT OR IGNORE INTO team_members(team_id,person_id) VALUES(?,?)",
                             (team_id, person["person_id"]))
                added.append(person["person_id"])
            append_event(conn, actor_id=actor_id, action="team.members_added", resource_type="team",
                         resource_id=team_id, detail={"persons": added}, occurred_at=self._now())

            def create():
                return "team", team_id, {"team_id": team_id, "member_count": len(added)}

            return self._idempotent(conn, request_id=request_id, action="add_team_members",
                                    payload=payload, create=create)

    def register_template(self, *, request_id: str, actor_id: str, site_id: str, code: str,
                          name: str, task_type: str, duration_minutes: int, area_tag: str,
                          equipment_types: list[str], crew_size: int = 0) -> dict[str, Any]:
        equipment_types = self._codes(equipment_types, "equipment_types")
        payload = {"actor_id": actor_id, "site_id": site_id, "code": code, "name": name,
                   "task_type": task_type, "duration_minutes": duration_minutes, "area_tag": area_tag,
                   "equipment_types": equipment_types, "crew_size": crew_size}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, *WRITER_ROLES)
            self._check_site_access(actor, self._site(conn, site_id))
            code, name, task_type, area_tag = (str(code).strip(), str(name).strip(),
                                               str(task_type).strip(), str(area_tag).strip())
            if not code or not name or not task_type or not area_tag:
                raise ValidationError("code/name/task_type/area_tag 不能为空")
            if not isinstance(duration_minutes, int) or not 5 <= duration_minutes <= 480:
                raise ValidationError("duration_minutes 必须在 5 到 480 之间")
            if not isinstance(crew_size, int) or crew_size < 0:
                raise ValidationError("crew_size 必须是非负整数")

            def create():
                try:
                    conn.execute(
                        "INSERT INTO task_templates(template_id,site_id,code,name,task_type,"
                        "duration_minutes,area_tag,equipment_types_json,crew_size,created_by,created_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (uuid.uuid4().hex, site_id, code, name, task_type, duration_minutes,
                         area_tag, canonical_json(equipment_types), crew_size, actor_id, self._now()),
                    )
                except Exception as exc:
                    raise ConflictError("模板编号已经存在") from exc
                template_id = conn.execute(
                    "SELECT template_id FROM task_templates WHERE site_id=? AND code=?",
                    (site_id, code)).fetchone()["template_id"]
                append_event(conn, actor_id=actor_id, action="template.registered",
                             resource_type="task_template", resource_id=template_id,
                             detail={"site_id": site_id, "code": code, "task_type": task_type},
                             occurred_at=self._now())
                return "task_template", template_id, {"template_id": template_id, "code": code}

            return self._idempotent(conn, request_id=request_id, action="register_template",
                                    payload=payload, create=create)

    def register_area(self, *, request_id: str, actor_id: str, site_id: str, code: str,
                      name: str, tag: str) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "site_id": site_id, "code": code, "name": name, "tag": tag}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, *WRITER_ROLES)
            self._check_site_access(actor, self._site(conn, site_id))
            code, name, tag = str(code).strip(), str(name).strip(), str(tag).strip()
            if not code or not name or not tag:
                raise ValidationError("code/name/tag 不能为空")

            def create():
                try:
                    conn.execute(
                        "INSERT INTO training_areas(area_id,site_id,code,name,tag,status,created_by,created_at)"
                        " VALUES(?,?,?,?,?,'available',?,?)",
                        (uuid.uuid4().hex, site_id, code, name, tag, actor_id, self._now()),
                    )
                except Exception as exc:
                    raise ConflictError("区域编号已经存在") from exc
                area_id = conn.execute("SELECT area_id FROM training_areas WHERE site_id=? AND code=?",
                                       (site_id, code)).fetchone()["area_id"]
                append_event(conn, actor_id=actor_id, action="area.registered", resource_type="training_area",
                             resource_id=area_id, detail={"site_id": site_id, "code": code, "tag": tag},
                             occurred_at=self._now())
                return "training_area", area_id, {"area_id": area_id, "code": code}

            return self._idempotent(conn, request_id=request_id, action="register_area",
                                    payload=payload, create=create)

    def register_equipment(self, *, request_id: str, actor_id: str, site_id: str, code: str,
                           name: str, equipment_type: str) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "site_id": site_id, "code": code,
                   "name": name, "equipment_type": equipment_type}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, *WRITER_ROLES)
            self._check_site_access(actor, self._site(conn, site_id))
            code, name, equipment_type = (str(code).strip(), str(name).strip(),
                                          str(equipment_type).strip())
            if not code or not name or not equipment_type:
                raise ValidationError("code/name/equipment_type 不能为空")

            def create():
                try:
                    conn.execute(
                        "INSERT INTO equipment(equipment_id,site_id,code,name,equipment_type,status,"
                        "created_by,created_at) VALUES(?,?,?,?,?,'available',?,?)",
                        (uuid.uuid4().hex, site_id, code, name, equipment_type, actor_id, self._now()),
                    )
                except Exception as exc:
                    raise ConflictError("设备编号已经存在") from exc
                equipment_id = conn.execute(
                    "SELECT equipment_id FROM equipment WHERE site_id=? AND code=?",
                    (site_id, code)).fetchone()["equipment_id"]
                append_event(conn, actor_id=actor_id, action="equipment.registered",
                             resource_type="equipment", resource_id=equipment_id,
                             detail={"site_id": site_id, "code": code, "equipment_type": equipment_type},
                             occurred_at=self._now())
                return "equipment", equipment_id, {"equipment_id": equipment_id, "code": code}

            return self._idempotent(conn, request_id=request_id, action="register_equipment",
                                    payload=payload, create=create)

    def set_equipment_status(self, *, request_id: str, actor_id: str, equipment_id: str,
                             status: str) -> dict[str, Any]:
        if status not in ("available", "maintenance", "retired"):
            raise ValidationError("设备状态无效")
        payload = {"actor_id": actor_id, "equipment_id": equipment_id, "status": status}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, *WRITER_ROLES)
            row = conn.execute("SELECT * FROM equipment WHERE equipment_id=?",
                               (equipment_id,)).fetchone()
            if row is None:
                raise NotFoundError("设备不存在")
            self._check_site_access(actor, self._site(conn, row["site_id"]))
            conn.execute("UPDATE equipment SET status=? WHERE equipment_id=?",
                         (status, equipment_id))
            append_event(conn, actor_id=actor_id, action="equipment.status_changed",
                         resource_type="equipment", resource_id=equipment_id,
                         detail={"status": status}, occurred_at=self._now())

            def create():
                return "equipment", equipment_id, {"equipment_id": equipment_id, "status": status}

            return self._idempotent(conn, request_id=request_id, action="set_equipment_status",
                                    payload=payload, create=create)

    def register_restriction(self, *, request_id: str, actor_id: str, site_id: str, scope: str,
                             resource_id: str, starts_at: str, ends_at: str, reason: str,
                             level: str = "normal") -> dict[str, Any]:
        start, end = parse_dt(starts_at, "starts_at"), parse_dt(ends_at, "ends_at")
        if start >= end:
            raise ValidationError("限制窗口开始时间必须早于结束时间")
        if scope not in ("site", "area", "equipment") or level not in ("normal", "escalated"):
            raise ValidationError("scope/level 取值无效")
        reason = str(reason).strip()
        if not reason:
            raise ValidationError("reason 不能为空")
        payload = {"actor_id": actor_id, "site_id": site_id, "scope": scope,
                   "resource_id": resource_id, "starts_at": fmt_dt(start), "ends_at": fmt_dt(end),
                   "reason": reason, "level": level}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, *WRITER_ROLES)
            site = self._site(conn, site_id)
            self._check_site_access(actor, site)
            if scope == "site" and resource_id != site_id:
                raise ValidationError("site 级限制的 resource_id 必须是当前场所")
            table = {"area": "training_areas", "equipment": "equipment"}[scope] if scope != "site" else None
            if table:
                if conn.execute(f"SELECT 1 FROM {table} WHERE {scope}_id=? AND site_id=?",
                                (resource_id, site_id)).fetchone() is None:
                    raise NotFoundError(f"限制对象不存在：{resource_id}")

            created: dict[str, Any] = {}

            def create():
                window_id = uuid.uuid4().hex
                conn.execute(
                    "INSERT INTO restriction_windows(window_id,site_id,scope,resource_id,starts_at,"
                    "ends_at,reason,level,created_by,created_at,escalated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (window_id, site_id, scope, resource_id, fmt_dt(start), fmt_dt(end), reason,
                     level, actor_id, self._now(), self._now() if level == "escalated" else None),
                )
                append_event(conn, actor_id=actor_id, action="restriction.registered",
                             resource_type="restriction_window", resource_id=window_id,
                             detail=payload, occurred_at=self._now())
                affected = []
                if level == "escalated":
                    affected = self._apply_escalation(conn,
                                                      conn.execute(
                                                          "SELECT * FROM restriction_windows WHERE window_id=?",
                                                          (window_id,)).fetchone(),
                                                      actor_id)
                created.update({"window_id": window_id, "affected_items": affected})
                return "restriction_window", window_id, dict(created)

            receipt = self._idempotent(conn, request_id=request_id, action="register_restriction",
                                       payload=payload, create=create)
            if not receipt["replayed"]:
                receipt.update(created)
            return receipt

    # ------------------------------------------------------------- 候选与放行

    def _active_windows(self, conn, site_id: str, start: datetime, end: datetime):
        rows = conn.execute(
            "SELECT * FROM restriction_windows WHERE site_id=? "
            "AND starts_at < ? AND ends_at > ? ORDER BY starts_at",
            (site_id, fmt_dt(end), fmt_dt(start))).fetchall()
        return rows

    @staticmethod
    def _window_blocks(window, area_id: str, equipment_ids: set[str]) -> bool:
        if window["scope"] == "site":
            return True
        if window["scope"] == "area":
            return window["resource_id"] == area_id
        return window["resource_id"] in equipment_ids

    def _held_rows(self, conn, resource_type: str, resource_id: str,
                   start: datetime, end: datetime):
        return conn.execute(
            "SELECT * FROM resource_reservations WHERE resource_type=? AND resource_id=? "
            "AND status='held' AND starts_at < ? AND ends_at > ?",
            (resource_type, resource_id, fmt_dt(end), fmt_dt(start))).fetchall()

    @staticmethod
    def _local_busy(local_holds, kind: str, resource_id: str,
                    start: datetime, end: datetime) -> bool:
        for held_start, held_end in local_holds.get((kind, resource_id), ()):
            if held_start < end and start < held_end:
                return True
        return False

    def _evaluate_slot(self, conn, site_id, template, start, end, team_id,
                       trainee_ids, windows, local_holds):
        """返回 (可行, 阻塞原因列表, 分配方案)。"""

        blockers: list[dict[str, str]] = []
        assignment: dict[str, Any] = {}

        def busy(kind: str, resource_id: str) -> bool:
            return self._local_busy(local_holds, kind, resource_id, start, end) or bool(
                self._held_rows(conn, kind, resource_id, start, end))

        site_windows = [w for w in windows if w["scope"] == "site"]
        if site_windows:
            blockers.append({"code": "site_restricted",
                             "detail": f"场所处于限制窗口：{site_windows[0]['reason']}"})

        # 区域：匹配标签、可用、无预占、无限制。
        area_id = None
        for area in conn.execute(
                "SELECT * FROM training_areas WHERE site_id=? AND tag=? ORDER BY code",
                (site_id, template["area_tag"])):
            if area["status"] != "available":
                continue
            if busy("area", area["area_id"]):
                continue
            if any(self._window_blocks(w, area["area_id"], set()) for w in windows):
                continue
            area_id = area["area_id"]
            break
        if area_id is None:
            blockers.append({"code": "area_unavailable",
                             "detail": f"标签 {template['area_tag']} 的训练区域无可用且无冲突时段"})

        # 设备：每种类型挑一台可用、无预占、无限制。
        equipment_ids: list[str] = []
        blocked_types: list[str] = []
        needed_types = json.loads(template["equipment_types_json"])
        used_equipment: set[str] = set()
        for eq_type in needed_types:
            pick = None
            for eq in conn.execute(
                    "SELECT * FROM equipment WHERE site_id=? AND equipment_type=? AND status='available' ORDER BY code",
                    (site_id, eq_type)):
                if eq["equipment_id"] in used_equipment:
                    continue
                if busy("equipment", eq["equipment_id"]):
                    continue
                if any(self._window_blocks(w, area_id or "", {eq["equipment_id"]}) for w in windows):
                    continue
                pick = eq["equipment_id"]
                break
            if pick is None:
                blocked_types.append(eq_type)
            else:
                equipment_ids.append(pick)
                used_equipment.add(pick)
        if blocked_types:
            blockers.append({"code": "equipment_unavailable",
                             "detail": f"设备类型无空闲：{','.join(blocked_types)}"})

        # 资质教员：具备任务类型资质、无预占。
        instructor_id = None
        qualified = []
        for person in conn.execute("SELECT * FROM persons WHERE site_id=? AND kind='instructor' AND active=1",
                                   (site_id,)):
            quals = json.loads(person["qualifications_json"])
            if template["task_type"] in quals:
                qualified.append(person)
        for person in sorted(qualified, key=lambda p: p["code"]):
            if busy("instructor", person["person_id"]):
                continue
            instructor_id = person["person_id"]
            break
        if instructor_id is None and not site_windows:
            if qualified:
                blockers.append({"code": "instructor_busy",
                                 "detail": f"具 {template['task_type']} 资质的教员均被预占"})
            else:
                blockers.append({"code": "instructor_unqualified",
                                 "detail": f"没有具备 {template['task_type']} 资质的在岗教员"})

        # 队伍：同一时段不能并行两项任务。
        if busy("team", team_id):
            blockers.append({"code": "team_busy", "detail": "队伍在该时段已有其他训练任务"})

        # 机组人数。
        if template["crew_size"] > len(trainee_ids):
            blockers.append({"code": "crew_insufficient",
                             "detail": f"需要 {template['crew_size']} 名学员，队伍仅 {len(trainee_ids)} 名"})

        if not blockers:
            assignment = {"area_id": area_id, "equipment_ids": equipment_ids,
                          "instructor_id": instructor_id}
            return True, [], assignment
        return False, blockers, {}

    def generate_plan(self, *, request_id: str, actor_id: str, plan_key: str, site_id: str,
                      horizon_start: str, horizon_end: str,
                      demands: list[dict[str, str]]) -> dict[str, Any]:
        """生成可解释的候选排程版本，不预占任何资源。"""

        h_start, h_end = parse_dt(horizon_start, "horizon_start"), parse_dt(horizon_end, "horizon_end")
        if h_start >= h_end:
            raise ValidationError("排程窗口开始时间必须早于结束时间")
        if not isinstance(demands, list) or not demands:
            raise ValidationError("demands 必须是非空数组")
        for demand in demands:
            if not isinstance(demand, dict) or "team_code" not in demand or "template_code" not in demand:
                raise ValidationError("每条需求必须包含 team_code 与 template_code")
        step = timedelta(minutes=GRID_MINUTES)
        payload = {"actor_id": actor_id, "plan_key": plan_key, "site_id": site_id,
                   "horizon_start": fmt_dt(h_start), "horizon_end": fmt_dt(h_end),
                   "demands": demands}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, *WRITER_ROLES)
            self._check_site_access(actor, self._site(conn, site_id))
            plan_key = str(plan_key).strip()
            if not plan_key:
                raise ValidationError("plan_key 不能为空")

            version_no = conn.execute(
                "SELECT COALESCE(MAX(version_no),0) + 1 AS next FROM plan_versions WHERE plan_key=?",
                (plan_key,)).fetchone()["next"]
            plan_version_id = uuid.uuid4().hex
            conn.execute(
                "INSERT INTO plan_versions(plan_version_id,plan_key,version_no,site_id,status,"
                "horizon_start,horizon_end,generated_by,generated_at) VALUES(?,?,?,?,'candidate',?,?,?,?)",
                (plan_version_id, plan_key, version_no, site_id, fmt_dt(h_start),
                 fmt_dt(h_end), actor_id, self._now()),
            )

            items_out: list[dict[str, Any]] = []
            # 候选版本内部占用簿：保证版本内各条任务互不重叠。
            local_holds: dict[tuple[str, str], list[tuple[datetime, datetime]]] = {}
            for rank, demand in enumerate(demands, start=1):
                team = conn.execute("SELECT * FROM teams WHERE site_id=? AND code=?",
                                    (site_id, demand["team_code"])).fetchone()
                if team is None:
                    raise NotFoundError(f"队伍不存在：{demand['team_code']}")
                template = conn.execute("SELECT * FROM task_templates WHERE site_id=? AND code=?",
                                        (site_id, demand["template_code"])).fetchone()
                if template is None:
                    raise NotFoundError(f"任务模板不存在：{demand['template_code']}")
                duration = timedelta(minutes=template["duration_minutes"])
                trainee_rows = conn.execute(
                    "SELECT p.person_id FROM persons p JOIN team_members tm ON tm.person_id=p.person_id "
                    "WHERE tm.team_id=? AND p.kind='trainee' AND p.active=1 ORDER BY p.code",
                    (team["team_id"],)).fetchall()
                trainee_ids = [r["person_id"] for r in trainee_rows]

                preferred_reasons: list[dict[str, str]] = []
                assignment = None
                chosen_start = None
                cursor = h_start
                while cursor + duration <= h_end:
                    windows = self._active_windows(conn, site_id, cursor, cursor + duration)
                    ok, blockers, candidate_assignment = self._evaluate_slot(
                        conn, site_id, template, cursor, cursor + duration,
                        team["team_id"], trainee_ids, windows, local_holds)
                    if ok:
                        assignment = candidate_assignment
                        chosen_start = cursor
                        break
                    if cursor == h_start:
                        preferred_reasons = blockers
                    cursor += step

                item_id = uuid.uuid4().hex
                if assignment is not None and chosen_start == h_start:
                    decision, reasons = "approved", [
                        {"code": "approved", "detail": "首选时段满足区域、设备、教员、资质与限制全部约束"}]
                    planned_start, planned_end = chosen_start, chosen_start + duration
                elif assignment is not None:
                    decision = "rescheduled"
                    reasons = preferred_reasons + [{
                        "code": "rescheduled_to",
                        "detail": f"首选时段不可行，顺延至 {fmt_dt(chosen_start)} 找到可行窗口"}]
                    planned_start, planned_end = chosen_start, chosen_start + duration
                else:
                    decision = "unschedulable"
                    reasons = preferred_reasons + [{"code": "no_feasible_slot",
                                                    "detail": "整个排程窗口内均无可行时段"}]
                    planned_start = planned_end = None

                conn.execute(
                    "INSERT INTO plan_items(item_id,plan_version_id,demand_id,rank_no,team_id,template_id,"
                    "area_id,equipment_ids_json,instructor_id,trainee_ids_json,planned_start,planned_end,"
                    "decision,lifecycle,reasons_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'planned',?)",
                    (item_id, plan_version_id, f"d{rank}", rank, team["team_id"],
                     template["template_id"], assignment["area_id"] if assignment else None,
                     canonical_json(assignment["equipment_ids"] if assignment else []),
                     assignment["instructor_id"] if assignment else None,
                     canonical_json(trainee_ids),
                     fmt_dt(planned_start) if planned_start else None,
                     fmt_dt(planned_end) if planned_end else None,
                     decision, canonical_json(reasons)),
                )
                items_out.append({"item_id": item_id, "rank_no": rank,
                                  "team_code": demand["team_code"],
                                  "template_code": demand["template_code"],
                                  "decision": decision, "reasons": reasons,
                                  "planned_start": fmt_dt(planned_start) if planned_start else None,
                                  "planned_end": fmt_dt(planned_end) if planned_end else None})
                if assignment is not None:
                    booked = [("area", assignment["area_id"]), ("team", team["team_id"])]
                    if assignment["instructor_id"]:
                        booked.append(("instructor", assignment["instructor_id"]))
                    booked.extend(("equipment", eq) for eq in assignment["equipment_ids"])
                    for kind, resource_id in booked:
                        local_holds.setdefault((kind, resource_id), []).append(
                            (planned_start, planned_end))

            append_event(conn, actor_id=actor_id, action="plan.generated", resource_type="plan_version",
                         resource_id=plan_version_id,
                         detail={"plan_key": plan_key, "version_no": version_no,
                                 "items": len(items_out)}, occurred_at=self._now())

            def create():
                return ("plan_version", plan_version_id,
                        {"plan_version_id": plan_version_id, "plan_key": plan_key,
                         "version_no": version_no, "items": items_out})

            receipt = self._idempotent(conn, request_id=request_id, action="generate_plan",
                                       payload=payload, create=create)
            receipt.update({"plan_version_id": plan_version_id, "plan_key": plan_key,
                            "version_no": version_no, "items": items_out})
            return receipt

    def _item_resources(self, item) -> list[tuple[str, str]]:
        resources = [("area", item["area_id"]), ("team", item["team_id"])]
        if item["instructor_id"]:
            resources.append(("instructor", item["instructor_id"]))
        for equipment_id in json.loads(item["equipment_ids_json"]):
            resources.append(("equipment", equipment_id))
        return [(kind, rid) for kind, rid in resources if rid]

    def _insert_reservation(self, conn, item, kind: str, resource_id: str) -> None:
        conn.execute(
            "INSERT INTO resource_reservations(reservation_id,plan_version_id,item_id,resource_type,"
            "resource_id,starts_at,ends_at,status,created_at) VALUES(?,?,?,?,?,?,?,'held',?)",
            (uuid.uuid4().hex, item["plan_version_id"], item["item_id"], kind, resource_id,
             item["planned_start"], item["planned_end"], self._now()),
        )

    def release_plan(self, *, request_id: str, actor_id: str, plan_version_id: str) -> dict[str, Any]:
        """值班教员放行候选版本：原子预占资源并创建任务实例。"""

        payload = {"actor_id": actor_id, "plan_version_id": plan_version_id}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, *RELEASABLE_ROLES)
            version = conn.execute("SELECT * FROM plan_versions WHERE plan_version_id=?",
                                   (plan_version_id,)).fetchone()
            if version is None:
                raise NotFoundError("计划版本不存在")
            self._check_site_access(actor, self._site(conn, version["site_id"]))
            if version["status"] != "candidate":
                raise ConflictError("只有候选状态的计划版本可以放行")

            items = conn.execute("SELECT * FROM plan_items WHERE plan_version_id=? ORDER BY rank_no",
                                 (plan_version_id,)).fetchall()
            # 获批与改期项都有可行时段，放行时一起预占；不可排程项不占资源、不建任务。
            bookable = [item for item in items if item["decision"] in ("approved", "rescheduled")]

            # 二次校验：放行瞬间重新确认资源仍然空闲且无新限制。
            for item in bookable:
                start, end = parse_dt(item["planned_start"], "planned_start"), parse_dt(item["planned_end"], "planned_end")
                windows = self._active_windows(conn, version["site_id"], start, end)
                equipment_ids = set(json.loads(item["equipment_ids_json"]))
                blocking = [w for w in windows
                            if self._window_blocks(w, item["area_id"], equipment_ids)]
                if blocking:
                    raise ConflictError(
                        f"第 {item['rank_no']} 项在放行时落入限制窗口，需重新生成候选：{blocking[0]['reason']}")
                area = conn.execute("SELECT status FROM training_areas WHERE area_id=?",
                                    (item["area_id"],)).fetchone()
                if area is None or area["status"] != "available":
                    raise ConflictError(
                        f"第 {item['rank_no']} 项训练区域已关闭或不存在，需重新生成候选")
                for eq_id in equipment_ids:
                    eq = conn.execute("SELECT status FROM equipment WHERE equipment_id=?",
                                      (eq_id,)).fetchone()
                    if eq is None or eq["status"] != "available":
                        raise ConflictError(
                            f"第 {item['rank_no']} 项设备 {eq_id} 已进入维护/退役，需重新生成候选")
                if item["instructor_id"]:
                    person = conn.execute("SELECT active FROM persons WHERE person_id=?",
                                          (item["instructor_id"],)).fetchone()
                    if person is None or not person["active"]:
                        raise ConflictError(
                            f"第 {item['rank_no']} 项指派教员已停用，需重新生成候选")
                for kind, resource_id in self._item_resources(item):
                    if self._held_rows(conn, kind, resource_id, start, end):
                        raise ConflictError(
                            f"第 {item['rank_no']} 项资源 {kind}:{resource_id} 已被其他放行计划占用")

            # 同一计划键的旧版本：候选直接作废；已放行版本中尚未开始的任务撤销并释放预占。
            old_versions = conn.execute(
                "SELECT * FROM plan_versions WHERE plan_key=? AND plan_version_id<>?",
                (version["plan_key"], plan_version_id)).fetchall()
            revoked_items: list[str] = []
            for old in old_versions:
                conn.execute("UPDATE plan_versions SET status='superseded' WHERE plan_version_id=?",
                             (old["plan_version_id"],))
                if old["status"] != "released":
                    continue
                for old_item in conn.execute(
                        "SELECT * FROM plan_items WHERE plan_version_id=? "
                        "AND decision IN ('approved','rescheduled')",
                        (old["plan_version_id"],)).fetchall():
                    task = conn.execute("SELECT * FROM task_instances WHERE item_id=?",
                                        (old_item["item_id"],)).fetchone()
                    if task and task["state"] == "scheduled":
                        conn.execute("UPDATE plan_items SET lifecycle='revoked' WHERE item_id=?",
                                     (old_item["item_id"],))
                        conn.execute(
                            "UPDATE resource_reservations SET status='revoked', released_at=? "
                            "WHERE item_id=? AND status='held'", (self._now(), old_item["item_id"]))
                        conn.execute(
                            "INSERT INTO item_events(item_event_id,item_id,kind,reason,created_by,created_at)"
                            " VALUES(?,?, 'revoked', ?, ?, ?)",
                            (uuid.uuid4().hex, old_item["item_id"],
                             f"计划 {version['version_no']} 放行，旧版本未开始任务撤销",
                             actor_id, self._now()))
                        conn.execute(
                            "INSERT INTO task_forced_states(anchor_id,task_id,to_state,trigger,reason,"
                            "occurred_at) VALUES(?,?,'revoked','plan_superseded',?,?)",
                            (uuid.uuid4().hex, task["task_id"],
                             f"计划 {version['version_no']} 放行，旧版本撤销", self._now()))
                        self._replay_task(conn, task["task_id"])
                        revoked_items.append(old_item["item_id"])

            conn.execute("UPDATE plan_versions SET status='released', released_by=?, released_at=? "
                         "WHERE plan_version_id=?", (actor_id, self._now(), plan_version_id))
            task_ids: list[str] = []
            for item in bookable:
                for kind, resource_id in self._item_resources(item):
                    self._insert_reservation(conn, item, kind, resource_id)
                task_id = uuid.uuid4().hex
                conn.execute(
                    "INSERT INTO task_instances(task_id,item_id,site_id,state,created_at,updated_at)"
                    " VALUES(?,?,?, 'scheduled', ?, ?)",
                    (task_id, item["item_id"], version["site_id"], self._now(), self._now()))
                conn.execute(
                    "INSERT INTO task_reviews(review_id,task_id,status,created_at) VALUES(?,?,'pending',?)",
                    (uuid.uuid4().hex, task_id, self._now()))
                task_ids.append(task_id)

            append_event(conn, actor_id=actor_id, action="plan.released", resource_type="plan_version",
                         resource_id=plan_version_id,
                         detail={"plan_key": version["plan_key"], "bookable": len(bookable),
                                 "revoked_old_items": revoked_items}, occurred_at=self._now())

            def create():
                return ("plan_version", plan_version_id,
                        {"plan_version_id": plan_version_id, "status": "released",
                         "task_ids": task_ids, "revoked_old_items": revoked_items})

            receipt = self._idempotent(conn, request_id=request_id, action="release_plan",
                                       payload=payload, create=create)
            receipt.update({"status": "released", "task_ids": task_ids,
                            "revoked_old_items": revoked_items})
            return receipt

    # ----------------------------------------------------------- 限制升级处置

    def _apply_escalation(self, conn, window, actor_id: str) -> list[dict[str, str]]:
        """升级在单个事务内生效：未开始撤销，进行中转人工。"""

        affected: list[dict[str, str]] = []
        rows = conn.execute(
            "SELECT res.*, pi.area_id, pi.plan_version_id, pv.site_id AS site_id "
            "FROM resource_reservations res JOIN plan_items pi ON pi.item_id=res.item_id "
            "JOIN plan_versions pv ON pv.plan_version_id=pi.plan_version_id "
            "WHERE res.status='held' AND pv.site_id=? AND res.starts_at < ? AND res.ends_at > ?",
            (window["site_id"], window["ends_at"], window["starts_at"])).fetchall()
        item_ids = set()
        for row in rows:
            equipment_ids = {r["resource_id"] for r in rows
                             if r["resource_type"] == "equipment" and r["item_id"] == row["item_id"]}
            if self._window_blocks(window, row["area_id"], equipment_ids):
                item_ids.add(row["item_id"])
        for item_id in sorted(item_ids):
            task = conn.execute("SELECT * FROM task_instances WHERE item_id=?", (item_id,)).fetchone()
            if task is None or task["state"] in TERMINAL_STATES or task["state"] == "manual":
                continue
            if task["state"] == "scheduled":
                conn.execute("UPDATE plan_items SET lifecycle='revoked' WHERE item_id=?", (item_id,))
                conn.execute(
                    "UPDATE resource_reservations SET status='revoked', released_at=? "
                    "WHERE item_id=? AND status='held'", (self._now(), item_id))
                conn.execute(
                    "INSERT INTO item_events(item_event_id,item_id,kind,reason,created_by,created_at)"
                    " VALUES(?,?, 'revoked', ?, ?, ?)",
                    (uuid.uuid4().hex, item_id, f"限制升级，尚未开始，自动撤销：{window['reason']}",
                     actor_id, self._now()))
                conn.execute(
                    "INSERT INTO task_forced_states(anchor_id,task_id,to_state,trigger,reason,occurred_at)"
                    " VALUES(?,?,'revoked','restriction_escalated',?,?)",
                    (uuid.uuid4().hex, task["task_id"], window["reason"], self._now()))
                self._replay_task(conn, task["task_id"])
                affected.append({"item_id": item_id, "task_id": task["task_id"], "action": "revoked"})
            else:
                # running / paused / anomalous 都进入人工处置，预占继续保留。
                conn.execute("UPDATE plan_items SET lifecycle='manual' WHERE item_id=?", (item_id,))
                conn.execute(
                    "INSERT INTO item_events(item_event_id,item_id,kind,reason,created_by,created_at)"
                    " VALUES(?,?, 'manual', ?, ?, ?)",
                    (uuid.uuid4().hex, item_id, f"限制升级时任务进行中，转人工处置：{window['reason']}",
                     actor_id, self._now()))
                conn.execute(
                    "INSERT INTO task_forced_states(anchor_id,task_id,to_state,trigger,reason,occurred_at)"
                    " VALUES(?,?,'manual','restriction_escalated',?,?)",
                    (uuid.uuid4().hex, task["task_id"], window["reason"], self._now()))
                self._replay_task(conn, task["task_id"])
                affected.append({"item_id": item_id, "task_id": task["task_id"], "action": "manual"})
        append_event(conn, actor_id=actor_id, action="restriction.escalated",
                     resource_type="restriction_window", resource_id=window["window_id"],
                     detail={"window_id": window["window_id"], "affected": affected},
                     occurred_at=self._now())
        return affected

    def escalate_restriction(self, *, request_id: str, actor_id: str, window_id: str) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "window_id": window_id}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, *WRITER_ROLES)
            window = conn.execute("SELECT * FROM restriction_windows WHERE window_id=?",
                                  (window_id,)).fetchone()
            if window is None:
                raise NotFoundError("限制窗口不存在")
            self._check_site_access(actor, self._site(conn, window["site_id"]))

            created: dict[str, Any] = {}

            def create():
                affected: list[dict[str, str]] = []
                if window["level"] != "escalated":
                    conn.execute(
                        "UPDATE restriction_windows SET level='escalated', escalated_at=? WHERE window_id=?",
                        (self._now(), window_id))
                    updated = conn.execute("SELECT * FROM restriction_windows WHERE window_id=?",
                                           (window_id,)).fetchone()
                    affected = self._apply_escalation(conn, updated, actor_id)
                else:
                    affected = self._apply_escalation(conn, window, actor_id)
                created.update({"window_id": window_id, "affected_items": affected})
                return "restriction_window", window_id, dict(created)

            receipt = self._idempotent(conn, request_id=request_id, action="escalate_restriction",
                                       payload=payload, create=create)
            if not receipt["replayed"]:
                receipt.update(created)
            return receipt

    # -------------------------------------------------------------- 执行与归并

    def _release_task_holds(self, conn, task_id: str) -> None:
        conn.execute(
            "UPDATE resource_reservations SET status='released', released_at=? "
            "WHERE item_id=(SELECT item_id FROM task_instances WHERE task_id=?) AND status='held'",
            (self._now(), task_id))

    def _replay_task(self, conn, task_id: str) -> str:
        """按发生时刻重放全部原始事件与强制锚点，重建唯一状态过程。

        乱序事件在此被归位；重复事件不产生迁移但保留原始记录；
        task_transitions 整体重建，保证唯一过程可重复计算。
        """

        entries: list[tuple[str, int, str, Any]] = []
        for seq, row in enumerate(conn.execute(
                "SELECT * FROM task_raw_events WHERE task_id=? ORDER BY occurred_at, received_seq",
                (task_id,))):
            entries.append((row["occurred_at"], seq, "event", row))
        for seq, row in enumerate(conn.execute(
                "SELECT * FROM task_forced_states WHERE task_id=? ORDER BY occurred_at, anchor_id",
                (task_id,))):
            # 同时刻下事件先于外部锚点，保证锚点判定基于该时刻前的过程。
            entries.append((row["occurred_at"], seq, "anchor", row))
        entries.sort(key=lambda e: (e[0], 0 if e[2] == "event" else 1, e[1]))

        state = "scheduled"
        rebuilt: list[dict[str, str]] = []
        verdicts: dict[str, tuple[bool, str | None]] = {}
        for _, _, kind, row in entries:
            if kind == "anchor":
                if row["to_state"] != state:
                    rebuilt.append({"from_state": state, "to_state": row["to_state"],
                                    "trigger": row["trigger"], "reason": row["reason"],
                                    "occurred_at": row["occurred_at"]})
                state = row["to_state"]
                continue
            event_type = row["event_type"]
            target = {"started": "running", "paused": "paused", "resumed": "running",
                      "ended": "ended", "anomaly": "anomalous"}[event_type]
            if state in EVENT_RULES[event_type]:
                rebuilt.append({"from_state": state, "to_state": target, "trigger": event_type,
                                "reason": json.loads(row["payload_json"]).get("reason", ""),
                                "occurred_at": row["occurred_at"]})
                state = target
                verdicts[row["event_id"]] = (True, None)
            elif state in EVENT_DUPLICATE_STATES[event_type]:
                verdicts[row["event_id"]] = (
                    True, "duplicate: 与已归并状态一致，仅保留原始来源，不重复迁移")
            elif state in TERMINAL_STATES:
                verdicts[row["event_id"]] = (False, f"终态 {state} 之后的事件被拒绝")
            else:
                verdicts[row["event_id"]] = (
                    False, f"按发生时刻归并时顺序无效：{event_type} 不能处于 {state}")

        conn.execute("DELETE FROM task_transitions WHERE task_id=?", (task_id,))
        for transition in rebuilt:
            conn.execute(
                "INSERT INTO task_transitions(transition_id,task_id,from_state,to_state,trigger,"
                "reason,occurred_at) VALUES(?,?,?,?,?,?,?)",
                (uuid.uuid4().hex, task_id, transition["from_state"], transition["to_state"],
                 transition["trigger"], transition["reason"], transition["occurred_at"]))
        for event_id, (accepted, note) in verdicts.items():
            conn.execute("UPDATE task_raw_events SET accepted=?, reject_reason=? WHERE event_id=?",
                         (1 if accepted else 0, note, event_id))

        conn.execute("UPDATE task_instances SET state=?, updated_at=? WHERE task_id=?",
                     (state, self._now(), task_id))
        if state in ("ended", "aborted"):
            self._release_task_holds(conn, task_id)
        return state

    def ingest_task_event(self, *, request_id: str, actor_id: str, event_id: str, task_id: str,
                          event_type: str, occurred_at: str, source: str,
                          payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """接收可能乱序或重复的执行事件，归并为唯一过程并保留原始来源。"""

        if event_type not in EVENT_RULES:
            raise ValidationError("event_type 必须是 started/paused/resumed/ended/anomaly")
        happened = parse_dt(occurred_at, "occurred_at")
        source = str(source).strip()
        if not source:
            raise ValidationError("source 不能为空")
        payload = payload or {}
        if not isinstance(payload, dict):
            raise ValidationError("payload 必须是对象")
        # 事件身份不含投递通道：同一 event_id 经主/备链路重投仍视为同一事件。
        payload_hash = digest({"event_type": event_type, "occurred_at": fmt_dt(happened),
                               "payload": payload})
        envelope = {"actor_id": actor_id, "event_id": event_id, "task_id": task_id,
                    "event_type": event_type, "occurred_at": fmt_dt(happened),
                    "source": source, "payload": payload}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, *WRITER_ROLES)
            task = conn.execute("SELECT * FROM task_instances WHERE task_id=?", (task_id,)).fetchone()
            if task is None:
                raise NotFoundError("任务不存在")

            # 相同事件编号的重投：原样识别，不再归并。
            existing = conn.execute("SELECT * FROM task_raw_events WHERE event_id=?",
                                    (event_id,)).fetchone()
            if existing:
                if existing["payload_hash"] != payload_hash:
                    raise ConflictError("event_id 已用于不同内容的事件")
                return {"request_id": request_id, "resource_type": "task_event",
                        "resource_id": event_id, "replayed": True,
                        "state": task["state"], "applied": bool(existing["accepted"]),
                        "note": existing["reject_reason"] or "事件编号重复，已保留原始记录"}

            received_seq = conn.execute(
                "SELECT COALESCE(MAX(received_seq),0)+1 AS next FROM task_raw_events WHERE task_id=?",
                (task_id,)).fetchone()["next"]
            conn.execute(
                "INSERT INTO task_raw_events(event_id,task_id,event_type,occurred_at,source,"
                "payload_json,payload_hash,accepted,reject_reason,received_seq) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (event_id, task_id, event_type, fmt_dt(happened), source,
                 canonical_json(payload), payload_hash, 0, "pending_replay", received_seq))

            state = self._replay_task(conn, task_id)
            verdict = conn.execute("SELECT accepted,reject_reason FROM task_raw_events WHERE event_id=?",
                                   (event_id,)).fetchone()
            # 产生迁移才算 applied；重复识别但不迁移的事件 applied=False。
            applied = bool(verdict["accepted"]) and verdict["reject_reason"] is None
            note = verdict["reject_reason"] or "已按发生时刻归并到唯一过程"
            append_event(conn, actor_id=actor_id, action=f"task.event_{'applied' if applied else 'ignored'}",
                         resource_type="task", resource_id=task_id,
                         detail={"event_id": event_id, "event_type": event_type, "source": source,
                                 "state": state, "applied": applied}, occurred_at=self._now())

            def create():
                return ("task_event", event_id,
                        {"event_id": event_id, "task_id": task_id, "state": state, "applied": applied})

            receipt = self._idempotent(conn, request_id=request_id, action="ingest_task_event",
                                       payload=envelope, create=create)
            receipt.update({"state": state, "applied": applied, "note": note})
            return receipt

    def manual_resolve_task(self, *, request_id: str, actor_id: str, task_id: str,
                            resolution: str, reason: str) -> dict[str, Any]:
        """人工处置异常或限制升级后挂起的任务。"""

        if resolution not in ("ended", "aborted"):
            raise ValidationError("resolution 必须是 ended 或 aborted")
        reason = str(reason).strip()
        if not reason:
            raise ValidationError("reason 不能为空")
        payload = {"actor_id": actor_id, "task_id": task_id, "resolution": resolution, "reason": reason}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, *RELEASABLE_ROLES)
            task = conn.execute("SELECT * FROM task_instances WHERE task_id=?", (task_id,)).fetchone()
            if task is None:
                raise NotFoundError("任务不存在")
            if task["state"] not in ("anomalous", "manual"):
                raise ConflictError("只有异常或人工处置中的任务可以人工结案")
            conn.execute(
                "INSERT INTO task_forced_states(anchor_id,task_id,to_state,trigger,reason,occurred_at)"
                " VALUES(?,?,?,?,?,?)",
                (uuid.uuid4().hex, task_id, resolution, "manual_resolve", reason, self._now()))
            state = self._replay_task(conn, task_id)
            conn.execute(
                "INSERT INTO item_events(item_event_id,item_id,kind,reason,created_by,created_at)"
                " VALUES(?,?, ?, ?, ?, ?)",
                (uuid.uuid4().hex, task["item_id"],
                 "completed" if resolution == "ended" else "aborted", reason, actor_id, self._now()))
            append_event(conn, actor_id=actor_id, action="task.manual_resolved", resource_type="task",
                         resource_id=task_id, detail={"resolution": resolution, "reason": reason},
                         occurred_at=self._now())

            def create():
                return "task", task_id, {"task_id": task_id, "state": state}

            receipt = self._idempotent(conn, request_id=request_id, action="manual_resolve_task",
                                       payload=payload, create=create)
            if not receipt["replayed"]:
                receipt["state"] = state
            return receipt

    # -------------------------------------------------------------- 遥测与复盘

    def ingest_telemetry_summary(self, *, request_id: str, actor_id: str, task_id: str,
                                 summary_id: str, metrics: dict[str, Any]) -> dict[str, Any]:
        """只接收结构化遥测摘要并入待复盘队列，不接收原始飞控流。"""

        if not isinstance(metrics, dict) or not metrics:
            raise ValidationError("metrics 必须是非空对象")
        payload = {"actor_id": actor_id, "task_id": task_id, "summary_id": summary_id,
                   "metrics": metrics}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, *WRITER_ROLES)
            task = conn.execute("SELECT * FROM task_instances WHERE task_id=?", (task_id,)).fetchone()
            if task is None:
                raise NotFoundError("任务不存在")

            def create():
                try:
                    conn.execute(
                        "INSERT INTO review_jobs(job_id,task_id,summary_id,telemetry_json,status,"
                        "attempts,created_at,updated_at) VALUES(?,?,?,?,'pending',0,?,?)",
                        (uuid.uuid4().hex, task_id, summary_id, canonical_json(metrics),
                         self._now(), self._now()),
                    )
                except Exception as exc:
                    raise ConflictError("该任务的摘要编号已经存在") from exc
                append_event(conn, actor_id=actor_id, action="telemetry.summary_received",
                             resource_type="task", resource_id=task_id,
                             detail={"summary_id": summary_id, "metric_keys": sorted(metrics)},
                             occurred_at=self._now())
                job_id = conn.execute("SELECT job_id FROM review_jobs WHERE task_id=? AND summary_id=?",
                                      (task_id, summary_id)).fetchone()["job_id"]
                return "review_job", job_id, {"job_id": job_id, "status": "pending"}

            return self._idempotent(conn, request_id=request_id, action="ingest_telemetry_summary",
                                    payload=payload, create=create)

    def run_pending_reviews(self, limit: int = 50) -> dict[str, Any]:
        """完成待复盘记录；进程重启后调用即可继续未完成部分。"""

        if not isinstance(limit, int) or limit <= 0:
            raise ValidationError("limit 必须是正整数")
        processed: list[dict[str, str]] = []
        with self.database.transaction(immediate=True) as conn:
            jobs = conn.execute(
                "SELECT * FROM review_jobs WHERE status='pending' ORDER BY created_at, job_id LIMIT ?",
                (limit,)).fetchall()
            for job in jobs:
                conn.execute("UPDATE review_jobs SET status='done', attempts=attempts+1, updated_at=? "
                             "WHERE job_id=?", (self._now(), job["job_id"]))
                review = conn.execute("SELECT * FROM task_reviews WHERE task_id=?",
                                      (job["task_id"],)).fetchone()
                if review is None:
                    # 任务复盘行理论上在放行时建立，缺失时补齐。
                    review_id = uuid.uuid4().hex
                    conn.execute(
                        "INSERT INTO task_reviews(review_id,task_id,status,telemetry_json,created_at,"
                        "completed_by,completed_at) VALUES(?,?,'completed',?,?,'system',?)",
                        (review_id, job["task_id"], job["telemetry_json"], self._now(), self._now()),
                    )
                else:
                    conn.execute(
                        "UPDATE task_reviews SET status='completed', telemetry_json=?, "
                        "completed_by='system', completed_at=? WHERE review_id=?",
                        (job["telemetry_json"], self._now(), review["review_id"]))
                append_event(conn, actor_id="system", action="review.completed", resource_type="task",
                             resource_id=job["task_id"], detail={"summary_id": job["summary_id"]},
                             occurred_at=self._now())
                processed.append({"task_id": job["task_id"], "summary_id": job["summary_id"]})
        return {"processed": len(processed), "items": processed,
                "remaining": self.pending_review_count()}

    def pending_review_count(self) -> int:
        return self.database.connection.execute(
            "SELECT COUNT(*) AS c FROM review_jobs WHERE status='pending'").fetchone()["c"]

    def resume_pending_work(self) -> dict[str, Any]:
        """进程重启后调用：续办所有待复盘记录。"""

        total = self.pending_review_count()
        result = {"processed": 0, "items": []}
        while True:
            batch = self.run_pending_reviews(100)
            result["processed"] += batch["processed"]
            result["items"].extend(batch["items"])
            if not batch["processed"]:
                break
        result["found_on_startup"] = total
        return result

    # ------------------------------------------------------------------ 查询

    def get_plan(self, plan_version_id: str) -> dict[str, Any]:
        conn = self.database.connection
        version = conn.execute("SELECT * FROM plan_versions WHERE plan_version_id=?",
                               (plan_version_id,)).fetchone()
        if version is None:
            raise NotFoundError("计划版本不存在")
        items = []
        for item in conn.execute("SELECT * FROM plan_items WHERE plan_version_id=? ORDER BY rank_no",
                                 (plan_version_id,)):
            team = conn.execute("SELECT code FROM teams WHERE team_id=?", (item["team_id"],)).fetchone()
            template = conn.execute("SELECT code FROM task_templates WHERE template_id=?",
                                    (item["template_id"],)).fetchone()
            task = conn.execute("SELECT task_id,state FROM task_instances WHERE item_id=?",
                                (item["item_id"],)).fetchone()
            items.append({
                "item_id": item["item_id"], "demand_id": item["demand_id"],
                "rank_no": item["rank_no"], "team_code": team["code"] if team else None,
                "template_code": template["code"] if template else None,
                "decision": item["decision"], "lifecycle": item["lifecycle"],
                "planned_start": item["planned_start"], "planned_end": item["planned_end"],
                "area_id": item["area_id"], "instructor_id": item["instructor_id"],
                "equipment_ids": json.loads(item["equipment_ids_json"]),
                "reasons": json.loads(item["reasons_json"]),
                "task_id": task["task_id"] if task else None,
                "task_state": task["state"] if task else None,
            })
        return {"plan_version_id": plan_version_id, "plan_key": version["plan_key"],
                "version_no": version["version_no"], "site_id": version["site_id"],
                "status": version["status"], "horizon_start": version["horizon_start"],
                "horizon_end": version["horizon_end"],
                "released_by": version["released_by"], "released_at": version["released_at"],
                "items": items}

    def list_plan_versions(self, plan_key: str) -> list[dict[str, Any]]:
        rows = self.database.connection.execute(
            "SELECT * FROM plan_versions WHERE plan_key=? ORDER BY version_no", (plan_key,)).fetchall()
        return [{"plan_version_id": r["plan_version_id"], "version_no": r["version_no"],
                 "status": r["status"], "generated_at": r["generated_at"],
                 "released_at": r["released_at"]} for r in rows]

    def get_task(self, task_id: str) -> dict[str, Any]:
        """返回任务为何获批、改期、撤销或中止的完整解释。"""

        conn = self.database.connection
        task = conn.execute("SELECT * FROM task_instances WHERE task_id=?", (task_id,)).fetchone()
        if task is None:
            raise NotFoundError("任务不存在")
        item = conn.execute("SELECT * FROM plan_items WHERE item_id=?", (task["item_id"],)).fetchone()
        version = conn.execute("SELECT * FROM plan_versions WHERE plan_version_id=?",
                               (item["plan_version_id"],)).fetchone()
        template = conn.execute("SELECT * FROM task_templates WHERE template_id=?",
                                (item["template_id"],)).fetchone()
        team = conn.execute("SELECT code FROM teams WHERE team_id=?", (item["team_id"],)).fetchone()
        transitions = [{"from_state": r["from_state"], "to_state": r["to_state"],
                        "trigger": r["trigger"], "reason": r["reason"],
                        "occurred_at": r["occurred_at"]}
                       for r in conn.execute(
                           "SELECT * FROM task_transitions WHERE task_id=? ORDER BY occurred_at, rowid",
                           (task_id,))]
        raw_events = [{"event_id": r["event_id"], "event_type": r["event_type"],
                       "occurred_at": r["occurred_at"], "source": r["source"],
                       "payload": json.loads(r["payload_json"]),
                       "accepted": bool(r["accepted"]), "reject_reason": r["reject_reason"],
                       "received_seq": r["received_seq"]}
                      for r in conn.execute(
                          "SELECT * FROM task_raw_events WHERE task_id=? ORDER BY received_seq",
                          (task_id,))]
        reservations = [{"resource_type": r["resource_type"], "resource_id": r["resource_id"],
                         "starts_at": r["starts_at"], "ends_at": r["ends_at"],
                         "status": r["status"], "released_at": r["released_at"]}
                        for r in conn.execute(
                            "SELECT * FROM resource_reservations WHERE item_id=? ORDER BY resource_type",
                            (task["item_id"],))]
        lifecycle_events = [{"kind": r["kind"], "reason": r["reason"],
                             "created_by": r["created_by"], "created_at": r["created_at"]}
                            for r in conn.execute(
                                "SELECT * FROM item_events WHERE item_id=? ORDER BY created_at",
                                (task["item_id"],))]
        review = conn.execute("SELECT * FROM task_reviews WHERE task_id=?", (task_id,)).fetchone()
        return {
            "task_id": task_id, "state": task["state"], "site_id": task["site_id"],
            "plan_version_id": version["plan_version_id"], "plan_key": version["plan_key"],
            "plan_status": version["status"],
            "team_code": team["code"] if team else None,
            "template_code": template["code"], "task_type": template["task_type"],
            "scheduling": {"decision": item["decision"], "lifecycle": item["lifecycle"],
                           "planned_start": item["planned_start"], "planned_end": item["planned_end"],
                           "reasons": json.loads(item["reasons_json"])},
            "transitions": transitions,
            "raw_events": raw_events,
            "reservations": reservations,
            "lifecycle_events": lifecycle_events,
            "review": None if review is None else
                {"status": review["status"], "completed_at": review["completed_at"],
                 "telemetry": json.loads(review["telemetry_json"])
                 if review["telemetry_json"] else None},
        }

    def list_tasks(self, site_id: str, state: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM task_instances WHERE site_id=?"
        parameters: list[Any] = [site_id]
        if state:
            query += " AND state=?"
            parameters.append(state)
        rows = self.database.connection.execute(query + " ORDER BY created_at", parameters).fetchall()
        return [{"task_id": r["task_id"], "state": r["state"], "item_id": r["item_id"],
                 "created_at": r["created_at"]} for r in rows]
