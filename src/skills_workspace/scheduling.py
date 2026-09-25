"""无人机训练排程、放行、事件归并、限制升级与复盘领域服务。

本模块只处理训练计划和结构化遥测摘要：

- 维护任务模板、人员资质、训练区域、设备状态与限制窗口；
- 生成带说明的候选排程，由值班教员按计划版本放行；
- 放行后的任务事件可能乱序或重复，系统归并为唯一过程并保留原始来源；
- 限制升级只撤销尚未开始的放行，进行中的任务转入人工处置；
- 资源预占与释放在同一事务内原子完成；
- 决策日志说明任务为何获批、改期或中止；
- 复盘记录持久化在 SQLite，进程重启后可继续完成。
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from .audit import append_event, canonical_json, digest
from .clock import Clock
from .errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .service import DomainService
from .storage import Database

SKILL_TYPES = frozenset({"assembly", "route_planning", "fault_handling"})
AREA_STATUSES = frozenset({"active", "maintenance", "closed"})
EQUIPMENT_STATUSES = frozenset({"available", "maintenance", "offline"})
SEVERITIES = frozenset({"advisory", "blocking"})
SCOPE_TYPES = frozenset({"site", "airspace", "area", "equipment"})
LEVEL_RANK = {"basic": 1, "intermediate": 2, "advanced": 3}
EVENT_TYPES = frozenset({"started", "paused", "resumed", "ended", "exception"})
EVENT_RANK = {"started": 0, "resumed": 1, "paused": 2, "exception": 3, "ended": 4}
RESOLUTIONS = frozenset({"continue", "abort", "reschedule"})
REVIEW_OUTCOMES = frozenset({"passed", "failed", "needs_repeat"})
SLOT_ACTIVE_STATUSES = ("released", "in_progress", "paused")
MAX_CANDIDATES = 8
MAX_REJECTIONS = 8
CANDIDATE_STEP_MINUTES = 30
MAX_WINDOW_HOURS = 16
SWEEP_START = "0000-01-01T00:00:00Z"
SWEEP_END = "9999-12-31T23:59:59Z"


def _transition(state: str | None, event_type: str) -> tuple[bool, str, str | None]:
    """事件状态机：返回 (是否接受, 拒绝原因, 新状态)。"""

    if state is None:
        if event_type == "started":
            return True, "", "in_progress"
        return False, "任务尚未开始，首个有效事件必须是 started", None
    if state == "in_progress":
        if event_type == "paused":
            return True, "", "paused"
        if event_type == "ended":
            return True, "", "completed"
        if event_type == "exception":
            return True, "", "in_progress"
        if event_type == "started":
            return False, "重复的开始事件", state
        return False, "任务未处于暂停状态，忽略恢复事件", state
    if state == "paused":
        if event_type == "resumed":
            return True, "", "in_progress"
        if event_type == "ended":
            return True, "", "completed"
        if event_type == "exception":
            return True, "", "paused"
        if event_type == "started":
            return False, "重复的开始事件", state
        return False, "重复的暂停事件", state
    return False, "任务已结束，事件仅保留原始记录", state


class SchedulingService:
    """协调训练排程的登记、放行、归并、升级处置与复盘规则。"""

    def __init__(self, database: Database, clock: Clock | None = None,
                 domain: DomainService | None = None) -> None:
        self.database = database
        self.domain = domain or DomainService(database, clock)
        self.clock = self.domain.clock

    # ---------- 基础工具 ----------

    def _now(self) -> str:
        return self.clock.now().replace(microsecond=0).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _fmt(value: datetime) -> str:
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def _parse_time(self, value: Any, field: str) -> datetime:
        text = str(value).strip()
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValidationError(f"{field} 必须是 ISO 8601 时间") from exc
        if parsed.tzinfo is None:
            raise ValidationError(f"{field} 必须包含时区信息")
        return parsed.astimezone(timezone.utc).replace(microsecond=0)

    def _identifier(self, value: Any, field: str) -> str:
        return self.domain._identifier(str(value), field)

    def _text(self, value: Any, field: str, limit: int = 200) -> str:
        return self.domain._text(str(value), field, limit)

    def _optional_text(self, value: Any, field: str, limit: int = 500) -> str:
        text = str(value or "").strip()
        if len(text) > limit:
            raise ValidationError(f"{field} 不能超过 {limit} 个字符")
        return text

    def _idempotent(self, connection, *, request_id: str, action: str,
                    payload: dict[str, Any],
                    create: Callable[[], tuple[str, str, dict[str, Any]]]) -> tuple[dict[str, Any], bool]:
        request_id = self._identifier(request_id, "request_id")
        payload_hash = digest(payload)
        row = connection.execute("SELECT * FROM request_receipts WHERE request_id=?", (request_id,)).fetchone()
        if row:
            if row["action"] != action or row["payload_hash"] != payload_hash:
                raise ConflictError("request_id 已被不同内容使用")
            return json.loads(row["response_json"]), True
        resource_type, resource_id, response = create()
        connection.execute(
            "INSERT INTO request_receipts(request_id,action,payload_hash,resource_type,resource_id,response_json,created_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (request_id, action, payload_hash, resource_type, resource_id, canonical_json(response), self._now()),
        )
        return response, False

    def _site(self, connection, site_id: str):
        row = connection.execute("SELECT * FROM sites WHERE site_id=?", (site_id,)).fetchone()
        if row is None:
            raise NotFoundError("场所不存在")
        return row

    def _guard_site(self, actor, site_row) -> None:
        if actor.role != "admin" and actor.organization_id != site_row["organization_id"]:
            raise PermissionDenied("不能操作其他组织的场所")

    def _decision(self, connection, *, site_id: str, plan_id: str, slot_id: str | None,
                  decision: str, reasons: list[str], actor_id: str) -> None:
        connection.execute(
            "INSERT INTO schedule_decisions(decision_id,site_id,plan_id,slot_id,decision,reasons_json,actor_id,created_at) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (uuid.uuid4().hex, site_id, plan_id, slot_id, decision, canonical_json(reasons), actor_id, self._now()),
        )

    # ---------- 资源可用性检查 ----------

    def _hold_conflicts(self, connection, resource_type: str, resource_id: str,
                        starts_at: str, ends_at: str) -> list[Any]:
        return connection.execute(
            "SELECT hold_id, slot_id, starts_at, ends_at FROM resource_holds "
            "WHERE resource_type=? AND resource_id=? AND status='active' AND starts_at < ? AND ends_at > ?",
            (resource_type, resource_id, ends_at, starts_at),
        ).fetchall()

    def _restrictions(self, connection, site_id: str, *, severity: str, area_id: str | None = None,
                      equipment_ids: tuple = (), starts_at: str, ends_at: str,
                      include_site_scope: bool = True) -> list[Any]:
        rows = connection.execute(
            "SELECT * FROM restriction_windows WHERE site_id=? AND status='active' AND severity=? "
            "AND starts_at < ? AND ends_at > ?",
            (site_id, severity, ends_at, starts_at),
        ).fetchall()
        matched = []
        for row in rows:
            scope_type, scope_id = row["scope_type"], row["scope_id"]
            if scope_type in ("site", "airspace"):
                if include_site_scope:
                    matched.append(row)
            elif scope_type == "area" and area_id is not None and scope_id == area_id:
                matched.append(row)
            elif scope_type == "equipment" and scope_id in equipment_ids:
                matched.append(row)
        return matched

    def _check_instructor(self, connection, site_id: str, template, instructor_id: str,
                          starts_at: str, ends_at: str, explanations: list[dict[str, Any]]) -> bool:
        row = connection.execute("SELECT * FROM actors WHERE actor_id=?", (instructor_id,)).fetchone()
        if row is None or not row["active"]:
            explanations.append({"check": "instructor", "result": "fail",
                                 "detail": f"教员 {instructor_id} 不存在或已停用"})
            return False
        ok = True
        if self._hold_conflicts(connection, "instructor", instructor_id, starts_at, ends_at):
            explanations.append({"check": "instructor", "result": "fail",
                                 "detail": f"教员 {row['display_name']}({instructor_id}) 在该时段已被预占"})
            ok = False
        qual = connection.execute(
            "SELECT * FROM personnel_qualifications WHERE site_id=? AND holder_actor_id=? AND qualification_code=?",
            (site_id, instructor_id, template["required_qualification"]),
        ).fetchone()
        if qual is None:
            explanations.append({"check": "instructor", "result": "fail",
                                 "detail": f"教员 {row['display_name']}({instructor_id}) 未持有资质 {template['required_qualification']}"})
            ok = False
        elif LEVEL_RANK[qual["level"]] < LEVEL_RANK[template["required_level"]]:
            explanations.append({"check": "instructor", "result": "fail",
                                 "detail": f"教员资质等级 {qual['level']} 低于模板要求 {template['required_level']}"})
            ok = False
        elif not (qual["valid_from"] <= starts_at and qual["valid_until"] >= ends_at):
            explanations.append({"check": "instructor", "result": "fail",
                                 "detail": f"教员资质有效期 {qual['valid_from']}~{qual['valid_until']} 未覆盖训练时段"})
            ok = False
        if ok:
            explanations.append({"check": "instructor", "result": "pass",
                                 "detail": f"教员 {row['display_name']}({instructor_id}) 持有 "
                                           f"{template['required_qualification']}({qual['level']})，时段内空闲"})
        return ok

    def _check_resources(self, connection, site_id: str, template, instructor_id: str,
                         starts_at: datetime, ends_at: datetime) -> tuple[bool, list[dict[str, Any]], dict[str, Any] | None]:
        """为候选时段挑选资源并记录每项检查的说明。"""

        explanations: list[dict[str, Any]] = []
        start_s, end_s = self._fmt(starts_at), self._fmt(ends_at)
        site_blocking = self._restrictions(connection, site_id, severity="blocking",
                                           starts_at=start_s, ends_at=end_s)
        if site_blocking:
            explanations.append({"check": "restriction", "result": "fail",
                                 "detail": f"场所存在阻断限制: {site_blocking[0]['reason']}"})
            return False, explanations, None

        area_id = None
        areas = connection.execute(
            "SELECT * FROM training_areas WHERE site_id=? AND area_type=? ORDER BY area_id",
            (site_id, template["area_type"]),
        ).fetchall()
        if not areas:
            explanations.append({"check": "area", "result": "fail",
                                 "detail": f"没有类型为 {template['area_type']} 的训练区域"})
        else:
            failures: list[str] = []
            for area in areas:
                if area["status"] != "active":
                    failures.append(f"区域 {area['name']}({area['area_id']}) 状态为 {area['status']}")
                    continue
                if self._hold_conflicts(connection, "area", area["area_id"], start_s, end_s):
                    failures.append(f"区域 {area['name']}({area['area_id']}) 在该时段已被预占")
                    continue
                blocking = self._restrictions(connection, site_id, severity="blocking",
                                              area_id=area["area_id"], starts_at=start_s, ends_at=end_s,
                                              include_site_scope=False)
                if blocking:
                    failures.append(f"区域 {area['name']}({area['area_id']}) 受阻断限制: {blocking[0]['reason']}")
                    continue
                area_id = area["area_id"]
                explanations.append({"check": "area", "result": "pass",
                                     "detail": f"区域 {area['name']}({area['area_id']}) 在时段内空闲"})
                break
            if area_id is None:
                explanations.extend({"check": "area", "result": "fail", "detail": detail} for detail in failures)

        equipment_ids: list[str] = []
        equipment_ok = True
        for equipment_type in json.loads(template["required_equipment_json"]):
            items = connection.execute(
                "SELECT * FROM equipment_items WHERE site_id=? AND equipment_type=? ORDER BY equipment_id",
                (site_id, equipment_type),
            ).fetchall()
            chosen = None
            failures = []
            for item in items:
                if item["status"] != "available":
                    failures.append(f"设备 {item['name']}({item['equipment_id']}) 状态为 {item['status']}")
                    continue
                if self._hold_conflicts(connection, "equipment", item["equipment_id"], start_s, end_s):
                    failures.append(f"设备 {item['name']}({item['equipment_id']}) 在该时段已被预占")
                    continue
                blocking = self._restrictions(connection, site_id, severity="blocking",
                                              equipment_ids=(item["equipment_id"],),
                                              starts_at=start_s, ends_at=end_s, include_site_scope=False)
                if blocking:
                    failures.append(f"设备 {item['name']}({item['equipment_id']}) 受阻断限制: {blocking[0]['reason']}")
                    continue
                chosen = item
                break
            if chosen is None:
                equipment_ok = False
                if not items:
                    explanations.append({"check": "equipment", "result": "fail",
                                         "detail": f"没有类型为 {equipment_type} 的设备"})
                else:
                    explanations.extend({"check": "equipment", "result": "fail", "detail": detail} for detail in failures)
            else:
                equipment_ids.append(chosen["equipment_id"])
                explanations.append({"check": "equipment", "result": "pass",
                                     "detail": f"设备 {chosen['name']}({chosen['equipment_id']}) 可用"})

        instructor_ok = self._check_instructor(connection, site_id, template, instructor_id,
                                               start_s, end_s, explanations)
        for window in self._restrictions(connection, site_id, severity="advisory", area_id=area_id,
                                         equipment_ids=tuple(equipment_ids),
                                         starts_at=start_s, ends_at=end_s):
            explanations.append({"check": "restriction", "result": "warn",
                                 "detail": f"时段内存在提示级限制: {window['reason']}"})

        feasible = area_id is not None and equipment_ok and instructor_ok
        resources = {"area_id": area_id, "equipment_ids": equipment_ids,
                     "instructor_id": instructor_id} if feasible else None
        return feasible, explanations, resources

    def _verify_slot_resources(self, connection, slot, template) -> list[str]:
        """放行前复查候选时段记录的资源仍然可用，返回失败原因列表。"""

        failures: list[str] = []
        starts_at, ends_at = slot["starts_at"], slot["ends_at"]
        area = connection.execute("SELECT * FROM training_areas WHERE area_id=?", (slot["area_id"],)).fetchone()
        if area is None or area["status"] != "active":
            failures.append(f"区域 {slot['area_id']} 当前不可用")
        elif self._hold_conflicts(connection, "area", slot["area_id"], starts_at, ends_at):
            failures.append(f"区域 {slot['area_id']} 在该时段已被其他任务预占")
        equipment_ids = json.loads(slot["equipment_json"])
        for equipment_id in equipment_ids:
            item = connection.execute("SELECT * FROM equipment_items WHERE equipment_id=?",
                                      (equipment_id,)).fetchone()
            if item is None or item["status"] != "available":
                failures.append(f"设备 {equipment_id} 当前不可用")
            elif self._hold_conflicts(connection, "equipment", equipment_id, starts_at, ends_at):
                failures.append(f"设备 {equipment_id} 在该时段已被其他任务预占")
        instructor = connection.execute("SELECT * FROM actors WHERE actor_id=?",
                                        (slot["instructor_id"],)).fetchone()
        if instructor is None or not instructor["active"]:
            failures.append(f"教员 {slot['instructor_id']} 不存在或已停用")
        elif self._hold_conflicts(connection, "instructor", slot["instructor_id"], starts_at, ends_at):
            failures.append(f"教员 {slot['instructor_id']} 在该时段已被其他任务预占")
        blocking = self._restrictions(connection, slot["site_id"], severity="blocking",
                                      area_id=slot["area_id"], equipment_ids=tuple(equipment_ids),
                                      starts_at=starts_at, ends_at=ends_at)
        for window in blocking:
            failures.append(f"时段内存在阻断限制: {window['reason']}")
        return failures

    # ---------- 注册表维护 ----------

    def create_template(self, *, request_id: str, actor_id: str, site_id: str, template_id: str,
                        name: str, skill_type: str, duration_minutes: int,
                        required_qualification: str, area_type: str,
                        required_level: str = "basic",
                        required_equipment: list[str] | None = None) -> tuple[dict[str, Any], bool]:
        required_equipment = list(required_equipment or [])
        payload = {"actor_id": actor_id, "site_id": site_id, "template_id": template_id, "name": name,
                   "skill_type": skill_type, "duration_minutes": duration_minutes,
                   "required_qualification": required_qualification, "required_level": required_level,
                   "required_equipment": required_equipment, "area_type": area_type}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain._actor(connection, actor_id)
            self.domain._require(actor, "admin", "operator")
            site = self._site(connection, site_id)
            self._guard_site(actor, site)
            template_id = self._identifier(template_id, "template_id")
            name = self._text(name, "name")
            if skill_type not in SKILL_TYPES:
                raise ValidationError("skill_type 不在允许范围内")
            try:
                duration = int(duration_minutes)
            except (TypeError, ValueError) as exc:
                raise ValidationError("duration_minutes 必须是整数") from exc
            if not 5 <= duration <= 480:
                raise ValidationError("duration_minutes 必须在 5 到 480 之间")
            self._identifier(required_qualification, "required_qualification")
            if required_level not in LEVEL_RANK:
                raise ValidationError("required_level 不在允许范围内")
            self._identifier(area_type, "area_type")
            for equipment_type in required_equipment:
                self._identifier(equipment_type, "required_equipment")

            def create() -> tuple[str, str, dict[str, Any]]:
                try:
                    connection.execute(
                        "INSERT INTO task_templates(template_id,site_id,name,skill_type,duration_minutes,"
                        "required_qualification,required_level,required_equipment_json,area_type,active,created_by,created_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,1,?,?)",
                        (template_id, site_id, name, skill_type, duration, required_qualification,
                         required_level, canonical_json(required_equipment), area_type, actor_id, self._now()),
                    )
                except Exception as exc:
                    raise ConflictError("任务模板编号已经存在") from exc
                append_event(connection, actor_id=actor_id, action="scheduling.template_registered",
                             resource_type="task_template", resource_id=template_id,
                             detail={"site_id": site_id, "name": name, "skill_type": skill_type,
                                     "duration_minutes": duration},
                             occurred_at=self._now())
                return "task_template", template_id, {"template_id": template_id, "skill_type": skill_type,
                                                      "duration_minutes": duration}

            return self._idempotent(connection, request_id=request_id,
                                    action="scheduling.create_template", payload=payload, create=create)

    def grant_qualification(self, *, request_id: str, actor_id: str, site_id: str, holder_actor_id: str,
                            qualification_code: str, level: str, valid_from: str,
                            valid_until: str) -> tuple[dict[str, Any], bool]:
        payload = {"actor_id": actor_id, "site_id": site_id, "holder_actor_id": holder_actor_id,
                   "qualification_code": qualification_code, "level": level,
                   "valid_from": valid_from, "valid_until": valid_until}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain._actor(connection, actor_id)
            self.domain._require(actor, "admin", "operator")
            site = self._site(connection, site_id)
            self._guard_site(actor, site)
            holder = self.domain._actor(connection, holder_actor_id)
            if holder.organization_id != site["organization_id"]:
                raise ValidationError("资质持有人必须属于场所所在组织")
            self._identifier(qualification_code, "qualification_code")
            if level not in LEVEL_RANK:
                raise ValidationError("level 不在允许范围内")
            start = self._parse_time(valid_from, "valid_from")
            end = self._parse_time(valid_until, "valid_until")
            if end <= start:
                raise ValidationError("valid_until 必须晚于 valid_from")
            valid_from_s, valid_until_s = self._fmt(start), self._fmt(end)

            def create() -> tuple[str, str, dict[str, Any]]:
                qual_id = f"qual:{site_id}:{holder_actor_id}:{qualification_code}"
                try:
                    connection.execute(
                        "INSERT INTO personnel_qualifications(qual_id,site_id,holder_actor_id,qualification_code,"
                        "level,valid_from,valid_until,granted_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                        (qual_id, site_id, holder_actor_id, qualification_code, level,
                         valid_from_s, valid_until_s, actor_id, self._now()),
                    )
                except Exception as exc:
                    raise ConflictError("该人员在此场所已持有同代码资质") from exc
                append_event(connection, actor_id=actor_id, action="scheduling.qualification_granted",
                             resource_type="personnel_qualification", resource_id=qual_id,
                             detail={"site_id": site_id, "holder_actor_id": holder_actor_id,
                                     "qualification_code": qualification_code, "level": level},
                             occurred_at=self._now())
                return "personnel_qualification", qual_id, {"qual_id": qual_id}

            return self._idempotent(connection, request_id=request_id,
                                    action="scheduling.grant_qualification", payload=payload, create=create)

    def register_area(self, *, request_id: str, actor_id: str, site_id: str, area_id: str,
                      name: str, area_type: str, capacity: int = 1) -> tuple[dict[str, Any], bool]:
        payload = {"actor_id": actor_id, "site_id": site_id, "area_id": area_id, "name": name,
                   "area_type": area_type, "capacity": capacity}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain._actor(connection, actor_id)
            self.domain._require(actor, "admin", "operator")
            site = self._site(connection, site_id)
            self._guard_site(actor, site)
            area_id = self._identifier(area_id, "area_id")
            name = self._text(name, "name")
            self._identifier(area_type, "area_type")
            try:
                capacity = int(capacity)
            except (TypeError, ValueError) as exc:
                raise ValidationError("capacity 必须是整数") from exc
            if capacity < 1:
                raise ValidationError("capacity 必须大于 0")

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                try:
                    connection.execute(
                        "INSERT INTO training_areas(area_id,site_id,name,area_type,capacity,status,version,"
                        "created_by,created_at,updated_at) VALUES(?,?,?,?,?,'active',1,?,?,?)",
                        (area_id, site_id, name, area_type, capacity, actor_id, now, now),
                    )
                except Exception as exc:
                    raise ConflictError("训练区域编号已经存在") from exc
                append_event(connection, actor_id=actor_id, action="scheduling.area_registered",
                             resource_type="training_area", resource_id=area_id,
                             detail={"site_id": site_id, "name": name, "area_type": area_type},
                             occurred_at=now)
                return "training_area", area_id, {"area_id": area_id, "status": "active", "version": 1}

            return self._idempotent(connection, request_id=request_id,
                                    action="scheduling.register_area", payload=payload, create=create)

    def set_area_status(self, *, request_id: str, actor_id: str, area_id: str,
                        status: str, reason: str = "") -> tuple[dict[str, Any], bool]:
        payload = {"actor_id": actor_id, "area_id": area_id, "status": status, "reason": reason}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain._actor(connection, actor_id)
            self.domain._require(actor, "admin", "operator")
            area = connection.execute("SELECT * FROM training_areas WHERE area_id=?", (area_id,)).fetchone()
            if area is None:
                raise NotFoundError("训练区域不存在")
            site = self._site(connection, area["site_id"])
            self._guard_site(actor, site)
            if status not in AREA_STATUSES:
                raise ValidationError("status 不在允许范围内")
            reason = self._optional_text(reason, "reason")

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                connection.execute(
                    "UPDATE training_areas SET status=?, version=version+1, updated_at=? WHERE area_id=?",
                    (status, now, area_id),
                )
                append_event(connection, actor_id=actor_id, action="scheduling.area_status_changed",
                             resource_type="training_area", resource_id=area_id,
                             detail={"old_status": area["status"], "new_status": status, "reason": reason},
                             occurred_at=now)
                affected: list[dict[str, Any]] = []
                if status != "active":
                    affected = self._sweep_resource(
                        connection, site_id=area["site_id"], scope_type="area", scope_id=area_id,
                        reason=reason or f"区域 {area['name']}({area_id}) 状态变更为 {status}",
                        actor_id=actor_id)
                return "training_area", area_id, {"area_id": area_id, "status": status,
                                                  "version": area["version"] + 1,
                                                  "affected_slots": affected}

            return self._idempotent(connection, request_id=request_id,
                                    action="scheduling.set_area_status", payload=payload, create=create)

    def register_equipment(self, *, request_id: str, actor_id: str, site_id: str, equipment_id: str,
                           name: str, equipment_type: str) -> tuple[dict[str, Any], bool]:
        payload = {"actor_id": actor_id, "site_id": site_id, "equipment_id": equipment_id,
                   "name": name, "equipment_type": equipment_type}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain._actor(connection, actor_id)
            self.domain._require(actor, "admin", "operator")
            site = self._site(connection, site_id)
            self._guard_site(actor, site)
            equipment_id = self._identifier(equipment_id, "equipment_id")
            name = self._text(name, "name")
            self._identifier(equipment_type, "equipment_type")

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                try:
                    connection.execute(
                        "INSERT INTO equipment_items(equipment_id,site_id,name,equipment_type,status,version,"
                        "created_by,created_at,updated_at) VALUES(?,?,?,?,'available',1,?,?,?)",
                        (equipment_id, site_id, name, equipment_type, actor_id, now, now),
                    )
                except Exception as exc:
                    raise ConflictError("设备编号已经存在") from exc
                append_event(connection, actor_id=actor_id, action="scheduling.equipment_registered",
                             resource_type="equipment_item", resource_id=equipment_id,
                             detail={"site_id": site_id, "name": name, "equipment_type": equipment_type},
                             occurred_at=now)
                return "equipment_item", equipment_id, {"equipment_id": equipment_id,
                                                        "status": "available", "version": 1}

            return self._idempotent(connection, request_id=request_id,
                                    action="scheduling.register_equipment", payload=payload, create=create)

    def set_equipment_status(self, *, request_id: str, actor_id: str, equipment_id: str,
                             status: str, reason: str = "") -> tuple[dict[str, Any], bool]:
        payload = {"actor_id": actor_id, "equipment_id": equipment_id, "status": status, "reason": reason}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain._actor(connection, actor_id)
            self.domain._require(actor, "admin", "operator")
            item = connection.execute("SELECT * FROM equipment_items WHERE equipment_id=?",
                                      (equipment_id,)).fetchone()
            if item is None:
                raise NotFoundError("设备不存在")
            site = self._site(connection, item["site_id"])
            self._guard_site(actor, site)
            if status not in EQUIPMENT_STATUSES:
                raise ValidationError("status 不在允许范围内")
            reason = self._optional_text(reason, "reason")

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                connection.execute(
                    "UPDATE equipment_items SET status=?, version=version+1, updated_at=? WHERE equipment_id=?",
                    (status, now, equipment_id),
                )
                append_event(connection, actor_id=actor_id, action="scheduling.equipment_status_changed",
                             resource_type="equipment_item", resource_id=equipment_id,
                             detail={"old_status": item["status"], "new_status": status, "reason": reason},
                             occurred_at=now)
                affected: list[dict[str, Any]] = []
                if status != "available":
                    affected = self._sweep_resource(
                        connection, site_id=item["site_id"], scope_type="equipment", scope_id=equipment_id,
                        reason=reason or f"设备 {item['name']}({equipment_id}) 状态变更为 {status}",
                        actor_id=actor_id)
                return "equipment_item", equipment_id, {"equipment_id": equipment_id, "status": status,
                                                        "version": item["version"] + 1,
                                                        "affected_slots": affected}

            return self._idempotent(connection, request_id=request_id,
                                    action="scheduling.set_equipment_status", payload=payload, create=create)

    # ---------- 限制窗口 ----------

    def create_restriction(self, *, request_id: str, actor_id: str, site_id: str, window_id: str,
                           scope_type: str, severity: str, reason: str, starts_at: str, ends_at: str,
                           scope_id: str | None = None) -> tuple[dict[str, Any], bool]:
        payload = {"actor_id": actor_id, "site_id": site_id, "window_id": window_id,
                   "scope_type": scope_type, "scope_id": scope_id, "severity": severity,
                   "reason": reason, "starts_at": starts_at, "ends_at": ends_at}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain._actor(connection, actor_id)
            self.domain._require(actor, "admin", "operator", "reviewer")
            site = self._site(connection, site_id)
            self._guard_site(actor, site)
            window_id = self._identifier(window_id, "window_id")
            if scope_type not in SCOPE_TYPES:
                raise ValidationError("scope_type 不在允许范围内")
            if severity not in SEVERITIES:
                raise ValidationError("severity 不在允许范围内")
            reason = self._text(reason, "reason")
            start = self._parse_time(starts_at, "starts_at")
            end = self._parse_time(ends_at, "ends_at")
            if end <= start:
                raise ValidationError("ends_at 必须晚于 starts_at")
            starts_s, ends_s = self._fmt(start), self._fmt(end)
            if scope_type in ("area", "equipment"):
                scope_id = self._identifier(scope_id or "", "scope_id")
                table = "training_areas" if scope_type == "area" else "equipment_items"
                found = connection.execute(
                    f"SELECT 1 FROM {table} WHERE site_id=? AND "
                    f"{'area_id' if scope_type == 'area' else 'equipment_id'}=?",
                    (site_id, scope_id),
                ).fetchone()
                if found is None:
                    raise NotFoundError("限制窗口引用的资源不存在")
            elif scope_id is not None:
                raise ValidationError("site/airspace 级限制不允许指定 scope_id")

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                try:
                    connection.execute(
                        "INSERT INTO restriction_windows(window_id,site_id,scope_type,scope_id,severity,reason,"
                        "starts_at,ends_at,status,version,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,'active',1,?,?)",
                        (window_id, site_id, scope_type, scope_id, severity, reason,
                         starts_s, ends_s, actor_id, now),
                    )
                except Exception as exc:
                    raise ConflictError("限制窗口编号已经存在") from exc
                append_event(connection, actor_id=actor_id, action="scheduling.restriction_created",
                             resource_type="restriction_window", resource_id=window_id,
                             detail={"site_id": site_id, "scope_type": scope_type, "scope_id": scope_id,
                                     "severity": severity, "reason": reason,
                                     "starts_at": starts_s, "ends_at": ends_s},
                             occurred_at=now)
                affected: list[dict[str, Any]] = []
                if severity == "blocking":
                    affected = self._sweep_resource(
                        connection, site_id=site_id, scope_type=scope_type, scope_id=scope_id,
                        reason=f"限制窗口生效: {reason}", actor_id=actor_id,
                        starts_at=starts_s, ends_at=ends_s)
                return "restriction_window", window_id, {"window_id": window_id, "severity": severity,
                                                         "status": "active", "affected_slots": affected}

            return self._idempotent(connection, request_id=request_id,
                                    action="scheduling.create_restriction", payload=payload, create=create)

    def escalate_restriction(self, *, request_id: str, actor_id: str,
                             window_id: str) -> tuple[dict[str, Any], bool]:
        payload = {"actor_id": actor_id, "window_id": window_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain._actor(connection, actor_id)
            self.domain._require(actor, "admin", "operator", "reviewer")
            window = connection.execute("SELECT * FROM restriction_windows WHERE window_id=?",
                                        (window_id,)).fetchone()
            if window is None:
                raise NotFoundError("限制窗口不存在")
            site = self._site(connection, window["site_id"])
            self._guard_site(actor, site)

            def create() -> tuple[str, str, dict[str, Any]]:
                if window["status"] != "active":
                    raise ConflictError("限制窗口已解除，不能升级")
                if window["severity"] != "advisory":
                    raise ConflictError("限制窗口已处于阻断级别")
                now = self._now()
                connection.execute(
                    "UPDATE restriction_windows SET severity='blocking', version=version+1 WHERE window_id=?",
                    (window_id,),
                )
                append_event(connection, actor_id=actor_id, action="scheduling.restriction_escalated",
                             resource_type="restriction_window", resource_id=window_id,
                             detail={"reason": window["reason"]}, occurred_at=now)
                affected = self._sweep_resource(
                    connection, site_id=window["site_id"], scope_type=window["scope_type"],
                    scope_id=window["scope_id"], reason=f"限制窗口升级: {window['reason']}",
                    actor_id=actor_id, starts_at=window["starts_at"], ends_at=window["ends_at"])
                return "restriction_window", window_id, {"window_id": window_id, "severity": "blocking",
                                                         "version": window["version"] + 1,
                                                         "affected_slots": affected}

            return self._idempotent(connection, request_id=request_id,
                                    action="scheduling.escalate_restriction", payload=payload, create=create)

    def lift_restriction(self, *, request_id: str, actor_id: str,
                         window_id: str) -> tuple[dict[str, Any], bool]:
        payload = {"actor_id": actor_id, "window_id": window_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain._actor(connection, actor_id)
            self.domain._require(actor, "admin", "operator", "reviewer")
            window = connection.execute("SELECT * FROM restriction_windows WHERE window_id=?",
                                        (window_id,)).fetchone()
            if window is None:
                raise NotFoundError("限制窗口不存在")
            site = self._site(connection, window["site_id"])
            self._guard_site(actor, site)

            def create() -> tuple[str, str, dict[str, Any]]:
                if window["status"] != "active":
                    raise ConflictError("限制窗口已经解除")
                now = self._now()
                connection.execute(
                    "UPDATE restriction_windows SET status='lifted', version=version+1 WHERE window_id=?",
                    (window_id,),
                )
                append_event(connection, actor_id=actor_id, action="scheduling.restriction_lifted",
                             resource_type="restriction_window", resource_id=window_id,
                             detail={"reason": window["reason"]}, occurred_at=now)
                return "restriction_window", window_id, {"window_id": window_id, "status": "lifted",
                                                         "version": window["version"] + 1}

            return self._idempotent(connection, request_id=request_id,
                                    action="scheduling.lift_restriction", payload=payload, create=create)

    # ---------- 限制升级与资源变更的清扫 ----------

    @staticmethod
    def _in_scope(slot, scope_type: str, scope_id: str | None) -> bool:
        if scope_type in ("site", "airspace"):
            return True
        if scope_type == "area":
            return slot["area_id"] == scope_id
        if scope_type == "equipment":
            return scope_id in json.loads(slot["equipment_json"])
        return False

    def _sweep_resource(self, connection, *, site_id: str, scope_type: str, scope_id: str | None,
                        reason: str, actor_id: str, starts_at: str = SWEEP_START,
                        ends_at: str = SWEEP_END) -> list[dict[str, Any]]:
        """限制升级或资源不可用：未开始的放行撤销，进行中的任务转人工处置。"""

        rows = connection.execute(
            "SELECT s.* FROM plan_slots s JOIN schedule_plans p ON p.plan_id=s.plan_id "
            "WHERE p.site_id=? AND s.status IN ('released','in_progress','paused') "
            "AND s.starts_at < ? AND s.ends_at > ?",
            (site_id, ends_at, starts_at),
        ).fetchall()
        affected: list[dict[str, Any]] = []
        for slot in rows:
            if not self._in_scope(slot, scope_type, scope_id):
                continue
            if slot["status"] == "released":
                self._revoke_slot(connection, slot=slot, reasons=[reason],
                                  actor_id=actor_id, decision="revoked")
                affected.append({"slot_id": slot["slot_id"], "action": "revoked", "reason": reason})
            else:
                case_id = self._open_disposition(connection, slot=slot, reason=reason, actor_id=actor_id)
                affected.append({"slot_id": slot["slot_id"], "action": "manual_review",
                                 "case_id": case_id, "reason": reason})
        return affected

    def _release_holds(self, connection, slot_id: str) -> None:
        connection.execute(
            "UPDATE resource_holds SET status='released', released_at=? WHERE slot_id=? AND status='active'",
            (self._now(), slot_id),
        )

    def _revoke_slot(self, connection, *, slot, reasons: list[str], actor_id: str, decision: str) -> None:
        now = self._now()
        self._release_holds(connection, slot["slot_id"])
        connection.execute("UPDATE plan_slots SET status='revoked' WHERE slot_id=?", (slot["slot_id"],))
        connection.execute("UPDATE schedule_plans SET status='candidates' WHERE plan_id=? AND status='released'",
                           (slot["plan_id"],))
        self._decision(connection, site_id=slot["site_id"], plan_id=slot["plan_id"],
                       slot_id=slot["slot_id"], decision=decision, reasons=reasons, actor_id=actor_id)
        append_event(connection, actor_id=actor_id, action="scheduling.slot_revoked",
                     resource_type="plan_slot", resource_id=slot["slot_id"],
                     detail={"plan_id": slot["plan_id"], "decision": decision, "reasons": reasons},
                     occurred_at=now)

    def _open_disposition(self, connection, *, slot, reason: str, actor_id: str) -> str:
        now = self._now()
        connection.execute("UPDATE plan_slots SET status='manual_review' WHERE slot_id=?",
                           (slot["slot_id"],))
        existing = connection.execute(
            "SELECT case_id FROM dispositions WHERE slot_id=? AND status='open'",
            (slot["slot_id"],),
        ).fetchone()
        if existing:
            case_id = existing["case_id"]
        else:
            sequence = connection.execute(
                "SELECT COUNT(*) AS count FROM dispositions WHERE slot_id=?", (slot["slot_id"],)
            ).fetchone()["count"] + 1
            case_id = f"case-{slot['slot_id']}-{sequence}"
            connection.execute(
                "INSERT INTO dispositions(case_id,slot_id,plan_id,site_id,reason,status,created_at) "
                "VALUES(?,?,?,?,?,'open',?)",
                (case_id, slot["slot_id"], slot["plan_id"], slot["site_id"], reason, now),
            )
        self._decision(connection, site_id=slot["site_id"], plan_id=slot["plan_id"],
                       slot_id=slot["slot_id"], decision="manual_review", reasons=[reason], actor_id=actor_id)
        append_event(connection, actor_id=actor_id, action="scheduling.disposition_opened",
                     resource_type="disposition", resource_id=case_id,
                     detail={"slot_id": slot["slot_id"], "reason": reason}, occurred_at=now)
        return case_id

    # ---------- 训练计划与候选排程 ----------

    def _validate_window(self, desired_start: str, desired_end: str,
                         duration_minutes: int) -> tuple[str, str]:
        start = self._parse_time(desired_start, "desired_start")
        end = self._parse_time(desired_end, "desired_end")
        if end <= start:
            raise ValidationError("desired_end 必须晚于 desired_start")
        if end - start > timedelta(hours=MAX_WINDOW_HOURS):
            raise ValidationError(f"训练窗口不能超过 {MAX_WINDOW_HOURS} 小时")
        if end - start < timedelta(minutes=duration_minutes):
            raise ValidationError("训练窗口不足以容纳模板时长")
        return self._fmt(start), self._fmt(end)

    def _generate_candidates(self, connection, site_id: str, template, instructor_id: str,
                             start_s: str, end_s: str, plan_id: str,
                             version: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        duration = timedelta(minutes=template["duration_minutes"])
        step = timedelta(minutes=CANDIDATE_STEP_MINUTES)
        start = self._parse_time(start_s, "desired_start")
        end = self._parse_time(end_s, "desired_end")
        candidates: list[dict[str, Any]] = []
        rejections: list[dict[str, Any]] = []
        cursor = start
        while cursor + duration <= end and len(candidates) < MAX_CANDIDATES:
            slot_start, slot_end = cursor, cursor + duration
            feasible, explanations, resources = self._check_resources(
                connection, site_id, template, instructor_id, slot_start, slot_end)
            if feasible:
                slot_id = f"{plan_id}-v{version}-s{len(candidates) + 1}"
                candidates.append({"slot_id": slot_id, "starts_at": self._fmt(slot_start),
                                   "ends_at": self._fmt(slot_end), "explanations": explanations,
                                   **resources})
            elif len(rejections) < MAX_REJECTIONS:
                rejections.append({"starts_at": self._fmt(slot_start), "ends_at": self._fmt(slot_end),
                                   "reasons": [item["detail"] for item in explanations
                                               if item["result"] == "fail"]})
            cursor += step
        return candidates, rejections

    def _insert_slots(self, connection, plan_id: str, site_id: str, version: int,
                      candidates: list[dict[str, Any]]) -> None:
        now = self._now()
        for candidate in candidates:
            connection.execute(
                "INSERT INTO plan_slots(slot_id,plan_id,site_id,plan_version,area_id,instructor_id,"
                "equipment_json,starts_at,ends_at,status,merge_state,explanations_json,created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,'candidate',NULL,?,?)",
                (candidate["slot_id"], plan_id, site_id, version, candidate["area_id"],
                 candidate["instructor_id"], canonical_json(candidate["equipment_ids"]),
                 candidate["starts_at"], candidate["ends_at"],
                 canonical_json(candidate["explanations"]), now),
            )

    @staticmethod
    def _plan_response(plan, candidates: list[dict[str, Any]],
                       rejections: list[dict[str, Any]]) -> dict[str, Any]:
        return {"plan_id": plan["plan_id"], "site_id": plan["site_id"],
                "template_id": plan["template_id"], "team_id": plan["team_id"],
                "instructor_id": plan["instructor_id"], "status": plan["status"],
                "version": plan["version"], "desired_start": plan["desired_start"],
                "desired_end": plan["desired_end"], "candidates": candidates,
                "rejections": rejections}

    def create_plan(self, *, request_id: str, actor_id: str, site_id: str, plan_id: str,
                    template_id: str, team_id: str, instructor_id: str, desired_start: str,
                    desired_end: str) -> tuple[dict[str, Any], bool]:
        payload = {"actor_id": actor_id, "site_id": site_id, "plan_id": plan_id,
                   "template_id": template_id, "team_id": team_id, "instructor_id": instructor_id,
                   "desired_start": desired_start, "desired_end": desired_end}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain._actor(connection, actor_id)
            self.domain._require(actor, "admin", "operator")
            site = self._site(connection, site_id)
            self._guard_site(actor, site)
            plan_id = self._identifier(plan_id, "plan_id")
            team_id = self._identifier(team_id, "team_id")
            template = connection.execute("SELECT * FROM task_templates WHERE template_id=?",
                                          (template_id,)).fetchone()
            if template is None or template["site_id"] != site_id or not template["active"]:
                raise NotFoundError("任务模板不存在或已停用")
            instructor = self.domain._actor(connection, instructor_id)
            if instructor.organization_id != site["organization_id"]:
                raise ValidationError("带教教员必须属于场所所在组织")
            start_s, end_s = self._validate_window(desired_start, desired_end,
                                                   template["duration_minutes"])

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                try:
                    connection.execute(
                        "INSERT INTO schedule_plans(plan_id,site_id,template_id,team_id,instructor_id,"
                        "desired_start,desired_end,status,version,created_by,created_at) "
                        "VALUES(?,?,?,?,?,?,?,'candidates',1,?,?)",
                        (plan_id, site_id, template_id, team_id, instructor_id,
                         start_s, end_s, actor_id, now),
                    )
                except Exception as exc:
                    raise ConflictError("训练计划编号已经存在") from exc
                candidates, rejections = self._generate_candidates(
                    connection, site_id, template, instructor_id, start_s, end_s, plan_id, 1)
                self._insert_slots(connection, plan_id, site_id, 1, candidates)
                reasons = [f"按模板 {template['name']} 生成 {len(candidates)} 个候选时段"]
                if rejections:
                    reasons.append(f"{len(rejections)} 个时段因资源或限制不可用，详见 rejections")
                self._decision(connection, site_id=site_id, plan_id=plan_id, slot_id=None,
                               decision="candidates_generated", reasons=reasons, actor_id=actor_id)
                append_event(connection, actor_id=actor_id, action="scheduling.plan_created",
                             resource_type="schedule_plan", resource_id=plan_id,
                             detail={"site_id": site_id, "template_id": template_id, "team_id": team_id,
                                     "candidates": len(candidates)},
                             occurred_at=now)
                plan = connection.execute("SELECT * FROM schedule_plans WHERE plan_id=?",
                                          (plan_id,)).fetchone()
                return "schedule_plan", plan_id, self._plan_response(plan, candidates, rejections)

            return self._idempotent(connection, request_id=request_id,
                                    action="scheduling.create_plan", payload=payload, create=create)

    def regenerate_plan(self, *, request_id: str, actor_id: str, plan_id: str,
                        desired_start: str | None = None, desired_end: str | None = None,
                        reason: str = "") -> tuple[dict[str, Any], bool]:
        payload = {"actor_id": actor_id, "plan_id": plan_id, "desired_start": desired_start,
                   "desired_end": desired_end, "reason": reason}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain._actor(connection, actor_id)
            self.domain._require(actor, "admin", "operator")
            plan = connection.execute("SELECT * FROM schedule_plans WHERE plan_id=?",
                                      (plan_id,)).fetchone()
            if plan is None:
                raise NotFoundError("训练计划不存在")
            site = self._site(connection, plan["site_id"])
            self._guard_site(actor, site)
            reason = self._optional_text(reason, "reason")

            def create() -> tuple[str, str, dict[str, Any]]:
                if plan["status"] != "candidates":
                    raise ConflictError(f"计划当前状态为 {plan['status']}，不能重新生成候选")
                template = connection.execute("SELECT * FROM task_templates WHERE template_id=?",
                                              (plan["template_id"],)).fetchone()
                start_s, end_s = self._validate_window(
                    desired_start or plan["desired_start"], desired_end or plan["desired_end"],
                    template["duration_minutes"])
                new_version = plan["version"] + 1
                now = self._now()
                connection.execute(
                    "UPDATE plan_slots SET status='superseded' WHERE plan_id=? AND status='candidate'",
                    (plan_id,),
                )
                connection.execute(
                    "UPDATE schedule_plans SET version=?, desired_start=?, desired_end=? WHERE plan_id=?",
                    (new_version, start_s, end_s, plan_id),
                )
                candidates, rejections = self._generate_candidates(
                    connection, plan["site_id"], template, plan["instructor_id"],
                    start_s, end_s, plan_id, new_version)
                self._insert_slots(connection, plan_id, plan["site_id"], new_version, candidates)
                reasons = [reason or "训练窗口调整，重新生成候选时段",
                           f"版本 {new_version} 生成 {len(candidates)} 个候选时段，旧候选已作废"]
                self._decision(connection, site_id=plan["site_id"], plan_id=plan_id, slot_id=None,
                               decision="rescheduled", reasons=reasons, actor_id=actor_id)
                append_event(connection, actor_id=actor_id, action="scheduling.plan_regenerated",
                             resource_type="schedule_plan", resource_id=plan_id,
                             detail={"version": new_version, "candidates": len(candidates),
                                     "reason": reason},
                             occurred_at=now)
                updated = connection.execute("SELECT * FROM schedule_plans WHERE plan_id=?",
                                             (plan_id,)).fetchone()
                return "schedule_plan", plan_id, self._plan_response(updated, candidates, rejections)

            return self._idempotent(connection, request_id=request_id,
                                    action="scheduling.regenerate_plan", payload=payload, create=create)

    # ---------- 放行 ----------

    def release_plan(self, *, request_id: str, actor_id: str, plan_id: str, slot_id: str,
                     plan_version: int) -> tuple[dict[str, Any], bool]:
        payload = {"actor_id": actor_id, "plan_id": plan_id, "slot_id": slot_id,
                   "plan_version": plan_version}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain._actor(connection, actor_id)
            self.domain._require(actor, "admin", "reviewer")
            plan = connection.execute("SELECT * FROM schedule_plans WHERE plan_id=?",
                                      (plan_id,)).fetchone()
            if plan is None:
                raise NotFoundError("训练计划不存在")
            site = self._site(connection, plan["site_id"])
            self._guard_site(actor, site)
            slot = connection.execute("SELECT * FROM plan_slots WHERE slot_id=?", (slot_id,)).fetchone()
            if slot is None or slot["plan_id"] != plan_id:
                raise NotFoundError("候选时段不存在")

            def create() -> tuple[str, str, dict[str, Any]]:
                if plan["version"] != int(plan_version):
                    raise ConflictError(
                        f"计划版本已变更：当前版本 {plan['version']}，请求版本 {plan_version}")
                if plan["status"] != "candidates":
                    raise ConflictError(f"计划当前状态为 {plan['status']}，不能放行")
                if slot["plan_version"] != plan["version"] or slot["status"] != "candidate":
                    raise ConflictError("候选时段已失效，请按最新版本重新选择")
                template = connection.execute("SELECT * FROM task_templates WHERE template_id=?",
                                              (plan["template_id"],)).fetchone()
                failures = self._verify_slot_resources(connection, slot, template)
                if failures:
                    raise ConflictError("；".join(failures))
                now = self._now()
                holds: list[dict[str, Any]] = []
                resources = [("area", slot["area_id"])]
                resources += [("equipment", item) for item in json.loads(slot["equipment_json"])]
                resources.append(("instructor", slot["instructor_id"]))
                for resource_type, resource_id in resources:
                    hold_id = f"hold-{slot_id}-{resource_type}-{resource_id}"
                    connection.execute(
                        "INSERT INTO resource_holds(hold_id,slot_id,resource_type,resource_id,starts_at,"
                        "ends_at,status,created_at) VALUES(?,?,?,?,?,?, 'active',?)",
                        (hold_id, slot_id, resource_type, resource_id,
                         slot["starts_at"], slot["ends_at"], now),
                    )
                    holds.append({"hold_id": hold_id, "resource_type": resource_type,
                                  "resource_id": resource_id})
                connection.execute("UPDATE plan_slots SET status='released' WHERE slot_id=?", (slot_id,))
                connection.execute("UPDATE schedule_plans SET status='released' WHERE plan_id=?", (plan_id,))
                explanations = json.loads(slot["explanations_json"])
                reasons = [f"值班教员按计划版本 {plan['version']} 放行"] + [
                    item["detail"] for item in explanations if item["result"] == "pass"]
                self._decision(connection, site_id=plan["site_id"], plan_id=plan_id, slot_id=slot_id,
                               decision="approved", reasons=reasons, actor_id=actor_id)
                append_event(connection, actor_id=actor_id, action="scheduling.plan_released",
                             resource_type="schedule_plan", resource_id=plan_id,
                             detail={"slot_id": slot_id, "version": plan["version"],
                                     "starts_at": slot["starts_at"], "ends_at": slot["ends_at"]},
                             occurred_at=now)
                return "schedule_plan", plan_id, {"plan_id": plan_id, "slot_id": slot_id,
                                                  "version": plan["version"], "status": "released",
                                                  "starts_at": slot["starts_at"],
                                                  "ends_at": slot["ends_at"], "holds": holds}

            return self._idempotent(connection, request_id=request_id,
                                    action="scheduling.release_plan", payload=payload, create=create)

    # ---------- 任务事件归并 ----------

    def record_task_event(self, *, request_id: str, actor_id: str, slot_id: str, event_id: str,
                          event_type: str, occurred_at: str, source: str,
                          payload: dict[str, Any] | None = None) -> tuple[dict[str, Any], bool]:
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            raise ValidationError("payload 必须是对象")
        request_payload = {"actor_id": actor_id, "slot_id": slot_id, "event_id": event_id,
                           "event_type": event_type, "occurred_at": occurred_at,
                           "source": source, "payload": payload}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain._actor(connection, actor_id)
            self.domain._require(actor, "admin", "operator")
            slot = connection.execute("SELECT * FROM plan_slots WHERE slot_id=?", (slot_id,)).fetchone()
            if slot is None:
                raise NotFoundError("任务时段不存在")
            if event_type not in EVENT_TYPES:
                raise ValidationError("event_type 不在允许范围内")
            if "telemetry" in payload and not isinstance(payload["telemetry"], dict):
                raise ValidationError("telemetry 必须是结构化遥测摘要对象")
            event_id = self._identifier(event_id, "event_id")
            source = self._text(source, "source", 80)
            occurred_s = self._fmt(self._parse_time(occurred_at, "occurred_at"))
            content = {"slot_id": slot_id, "event_type": event_type, "occurred_at": occurred_s,
                       "source": source, "payload": payload}
            content_hash = digest(content)

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                existing = connection.execute("SELECT * FROM task_events WHERE event_id=?",
                                              (event_id,)).fetchone()
                duplicate = False
                if existing:
                    if existing["payload_hash"] != content_hash or existing["slot_id"] != slot_id:
                        raise ConflictError("event_id 已被不同内容使用")
                    duplicate = True
                else:
                    connection.execute(
                        "INSERT INTO task_events(event_id,slot_id,event_type,occurred_at,source,payload_json,"
                        "payload_hash,merge_status,recorded_by,received_at) VALUES(?,?,?,?,?,?,?,'pending',?,?)",
                        (event_id, slot_id, event_type, occurred_s, source, canonical_json(payload),
                         content_hash, actor_id, now),
                    )
                merge_state = self._merge_slot(connection, slot_id)
                updated = connection.execute("SELECT * FROM plan_slots WHERE slot_id=?",
                                             (slot_id,)).fetchone()
                counts = connection.execute(
                    "SELECT merge_status, COUNT(*) AS count FROM task_events WHERE slot_id=? "
                    "GROUP BY merge_status",
                    (slot_id,),
                ).fetchall()
                accepted = next((row["count"] for row in counts if row["merge_status"] == "accepted"), 0)
                rejected = next((row["count"] for row in counts if row["merge_status"] == "rejected"), 0)
                append_event(connection, actor_id=actor_id, action="scheduling.task_event_recorded",
                             resource_type="plan_slot", resource_id=slot_id,
                             detail={"event_id": event_id, "event_type": event_type,
                                     "occurred_at": occurred_s, "duplicate": duplicate},
                             occurred_at=now)
                return "task_event", event_id, {"slot_id": slot_id, "event_id": event_id,
                                                "duplicate": duplicate, "slot_status": updated["status"],
                                                "merge_state": merge_state,
                                                "merged_events": accepted,
                                                "rejected_events": rejected}

            return self._idempotent(connection, request_id=request_id,
                                    action="scheduling.record_task_event", payload=request_payload,
                                    create=create)

    def _merge_slot(self, connection, slot_id: str) -> str | None:
        """把某任务的全部原始事件按确定顺序重放状态机，归并为唯一过程。"""

        rows = connection.execute("SELECT * FROM task_events WHERE slot_id=?", (slot_id,)).fetchall()
        ordered = sorted(rows, key=lambda row: (row["occurred_at"],
                                                EVENT_RANK[row["event_type"]], row["event_id"]))
        state: str | None = None
        order = 0
        for row in ordered:
            accepted, note, next_state = _transition(state, row["event_type"])
            if accepted:
                order += 1
                connection.execute(
                    "UPDATE task_events SET merge_status='accepted', merge_order=?, merge_note=NULL "
                    "WHERE event_id=?",
                    (order, row["event_id"]),
                )
                state = next_state
            else:
                connection.execute(
                    "UPDATE task_events SET merge_status='rejected', merge_order=NULL, merge_note=? "
                    "WHERE event_id=?",
                    (note, row["event_id"]),
                )
        slot = connection.execute("SELECT * FROM plan_slots WHERE slot_id=?", (slot_id,)).fetchone()
        connection.execute("UPDATE plan_slots SET merge_state=? WHERE slot_id=?", (state, slot_id))
        if slot["status"] in SLOT_ACTIVE_STATUSES:
            if state == "completed":
                self._complete_slot(connection, slot)
            elif state is not None and state != slot["status"]:
                connection.execute("UPDATE plan_slots SET status=? WHERE slot_id=?",
                                   (state, slot_id))
        return state

    def _review_summary(self, connection, slot_id: str) -> dict[str, Any]:
        rows = connection.execute(
            "SELECT * FROM task_events WHERE slot_id=? ORDER BY occurred_at, event_id", (slot_id,)
        ).fetchall()
        accepted = sorted((row for row in rows if row["merge_status"] == "accepted"),
                          key=lambda row: row["merge_order"])
        rejected = [row for row in rows if row["merge_status"] == "rejected"]
        timeline = [{"merge_order": row["merge_order"], "event_id": row["event_id"],
                     "event_type": row["event_type"], "occurred_at": row["occurred_at"],
                     "source": row["source"]} for row in accepted]
        telemetry = []
        exceptions = []
        for row in accepted:
            data = json.loads(row["payload_json"])
            if isinstance(data.get("telemetry"), dict):
                telemetry.append({"event_id": row["event_id"], **data["telemetry"]})
            if row["event_type"] == "exception":
                exceptions.append({"event_id": row["event_id"], "occurred_at": row["occurred_at"],
                                   "detail": data})
        return {"timeline": timeline, "telemetry": telemetry, "exceptions": exceptions,
                "rejected_events": [{"event_id": row["event_id"], "event_type": row["event_type"],
                                     "reason": row["merge_note"]} for row in rejected]}

    def _create_review(self, connection, slot) -> str:
        existing = connection.execute("SELECT review_id FROM review_records WHERE slot_id=?",
                                      (slot["slot_id"],)).fetchone()
        if existing:
            return existing["review_id"]
        now = self._now()
        review_id = f"review-{slot['slot_id']}"
        summary = self._review_summary(connection, slot["slot_id"])
        connection.execute(
            "INSERT INTO review_records(review_id,slot_id,plan_id,site_id,status,summary_json,created_at) "
            "VALUES(?,?,?,?,'pending',?,?)",
            (review_id, slot["slot_id"], slot["plan_id"], slot["site_id"],
             canonical_json(summary), now),
        )
        append_event(connection, actor_id="system", action="scheduling.review_created",
                     resource_type="review_record", resource_id=review_id,
                     detail={"slot_id": slot["slot_id"], "plan_id": slot["plan_id"]},
                     occurred_at=now)
        return review_id

    def _complete_slot(self, connection, slot) -> None:
        now = self._now()
        connection.execute("UPDATE plan_slots SET status='completed' WHERE slot_id=?",
                           (slot["slot_id"],))
        self._release_holds(connection, slot["slot_id"])
        connection.execute("UPDATE schedule_plans SET status='completed' WHERE plan_id=? AND status='released'",
                           (slot["plan_id"],))
        self._create_review(connection, slot)
        self._decision(connection, site_id=slot["site_id"], plan_id=slot["plan_id"],
                       slot_id=slot["slot_id"], decision="completed",
                       reasons=["任务按计划完成，资源预占已释放，转入待复盘"], actor_id="system")
        append_event(connection, actor_id="system", action="scheduling.slot_completed",
                     resource_type="plan_slot", resource_id=slot["slot_id"],
                     detail={"plan_id": slot["plan_id"]}, occurred_at=now)

    def _abort_slot(self, connection, slot, reasons: list[str], actor_id: str) -> None:
        now = self._now()
        connection.execute("UPDATE plan_slots SET status='aborted' WHERE slot_id=?",
                           (slot["slot_id"],))
        self._release_holds(connection, slot["slot_id"])
        connection.execute("UPDATE schedule_plans SET status='aborted' WHERE plan_id=?",
                           (slot["plan_id"],))
        self._create_review(connection, slot)
        self._decision(connection, site_id=slot["site_id"], plan_id=slot["plan_id"],
                       slot_id=slot["slot_id"], decision="aborted", reasons=reasons, actor_id=actor_id)
        append_event(connection, actor_id=actor_id, action="scheduling.slot_aborted",
                     resource_type="plan_slot", resource_id=slot["slot_id"],
                     detail={"plan_id": slot["plan_id"], "reasons": reasons}, occurred_at=now)

    # ---------- 人工处置 ----------

    def resolve_disposition(self, *, request_id: str, actor_id: str, case_id: str,
                            resolution: str, notes: str = "") -> tuple[dict[str, Any], bool]:
        payload = {"actor_id": actor_id, "case_id": case_id, "resolution": resolution, "notes": notes}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain._actor(connection, actor_id)
            self.domain._require(actor, "admin", "reviewer")
            case = connection.execute("SELECT * FROM dispositions WHERE case_id=?",
                                      (case_id,)).fetchone()
            if case is None:
                raise NotFoundError("处置单不存在")
            site = self._site(connection, case["site_id"])
            self._guard_site(actor, site)
            if resolution not in RESOLUTIONS:
                raise ValidationError("resolution 不在允许范围内")
            notes = self._optional_text(notes, "notes")

            def create() -> tuple[str, str, dict[str, Any]]:
                if case["status"] != "open":
                    raise ConflictError("处置单已关闭")
                slot = connection.execute("SELECT * FROM plan_slots WHERE slot_id=?",
                                          (case["slot_id"],)).fetchone()
                now = self._now()
                if resolution == "continue":
                    merge_state = slot["merge_state"]
                    if merge_state == "completed":
                        self._complete_slot(connection, slot)
                        slot_status = "completed"
                    else:
                        slot_status = merge_state if merge_state in ("in_progress", "paused") else "released"
                        connection.execute("UPDATE plan_slots SET status=? WHERE slot_id=?",
                                           (slot_status, slot["slot_id"]))
                    reasons = ["人工处置：任务继续", case["reason"]]
                    if notes:
                        reasons.append(notes)
                    self._decision(connection, site_id=slot["site_id"], plan_id=slot["plan_id"],
                                   slot_id=slot["slot_id"], decision="continued",
                                   reasons=reasons, actor_id=actor_id)
                elif resolution == "abort":
                    reasons = ["人工处置：任务中止", case["reason"]]
                    if notes:
                        reasons.append(notes)
                    self._abort_slot(connection, slot, reasons, actor_id)
                    slot_status = "aborted"
                else:
                    reasons = ["人工处置：任务改期，资源预占已释放", case["reason"]]
                    if notes:
                        reasons.append(notes)
                    self._revoke_slot(connection, slot=slot, reasons=reasons,
                                      actor_id=actor_id, decision="rescheduled")
                    slot_status = "revoked"
                connection.execute(
                    "UPDATE dispositions SET status='resolved', resolution=?, notes=?, resolved_by=?, "
                    "resolved_at=? WHERE case_id=?",
                    (resolution, notes, actor_id, now, case_id),
                )
                append_event(connection, actor_id=actor_id, action="scheduling.disposition_resolved",
                             resource_type="disposition", resource_id=case_id,
                             detail={"slot_id": case["slot_id"], "resolution": resolution},
                             occurred_at=now)
                return "disposition", case_id, {"case_id": case_id, "resolution": resolution,
                                                "slot_id": case["slot_id"],
                                                "slot_status": slot_status}

            return self._idempotent(connection, request_id=request_id,
                                    action="scheduling.resolve_disposition", payload=payload,
                                    create=create)

    # ---------- 复盘 ----------

    def complete_review(self, *, request_id: str, actor_id: str, review_id: str, outcome: str,
                        notes: str = "") -> tuple[dict[str, Any], bool]:
        payload = {"actor_id": actor_id, "review_id": review_id, "outcome": outcome, "notes": notes}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain._actor(connection, actor_id)
            self.domain._require(actor, "admin", "reviewer")
            review = connection.execute("SELECT * FROM review_records WHERE review_id=?",
                                        (review_id,)).fetchone()
            if review is None:
                raise NotFoundError("复盘记录不存在")
            site = self._site(connection, review["site_id"])
            self._guard_site(actor, site)
            if outcome not in REVIEW_OUTCOMES:
                raise ValidationError("outcome 不在允许范围内")
            notes = self._optional_text(notes, "notes")

            def create() -> tuple[str, str, dict[str, Any]]:
                if review["status"] != "pending":
                    raise ConflictError("复盘记录已完成")
                now = self._now()
                connection.execute(
                    "UPDATE review_records SET status='completed', outcome=?, notes=?, reviewed_by=?, "
                    "completed_at=? WHERE review_id=?",
                    (outcome, notes, actor_id, now, review_id),
                )
                append_event(connection, actor_id=actor_id, action="scheduling.review_completed",
                             resource_type="review_record", resource_id=review_id,
                             detail={"slot_id": review["slot_id"], "outcome": outcome},
                             occurred_at=now)
                return "review_record", review_id, {"review_id": review_id, "status": "completed",
                                                    "outcome": outcome}

            return self._idempotent(connection, request_id=request_id,
                                    action="scheduling.complete_review", payload=payload, create=create)

    # ---------- 查询 ----------

    def get_plan(self, plan_id: str) -> dict[str, Any]:
        connection = self.database.connection
        plan = connection.execute("SELECT * FROM schedule_plans WHERE plan_id=?", (plan_id,)).fetchone()
        if plan is None:
            raise NotFoundError("训练计划不存在")
        slots = connection.execute(
            "SELECT * FROM plan_slots WHERE plan_id=? ORDER BY plan_version, starts_at, slot_id",
            (plan_id,),
        ).fetchall()
        decisions = connection.execute(
            "SELECT * FROM schedule_decisions WHERE plan_id=? ORDER BY rowid", (plan_id,)
        ).fetchall()
        holds = connection.execute(
            "SELECT h.* FROM resource_holds h JOIN plan_slots s ON s.slot_id=h.slot_id "
            "WHERE s.plan_id=? AND h.status='active' ORDER BY h.resource_type, h.resource_id",
            (plan_id,),
        ).fetchall()
        cases = connection.execute(
            "SELECT * FROM dispositions WHERE plan_id=? ORDER BY created_at, case_id", (plan_id,)
        ).fetchall()
        return {
            "plan": {"plan_id": plan["plan_id"], "site_id": plan["site_id"],
                     "template_id": plan["template_id"], "team_id": plan["team_id"],
                     "instructor_id": plan["instructor_id"], "status": plan["status"],
                     "version": plan["version"], "desired_start": plan["desired_start"],
                     "desired_end": plan["desired_end"]},
            "slots": [self._slot_dict(slot) for slot in slots],
            "decisions": [self._decision_dict(row) for row in decisions],
            "active_holds": [dict(row) for row in holds],
            "dispositions": [dict(row) for row in cases],
        }

    def get_task(self, slot_id: str) -> dict[str, Any]:
        connection = self.database.connection
        slot = connection.execute("SELECT * FROM plan_slots WHERE slot_id=?", (slot_id,)).fetchone()
        if slot is None:
            raise NotFoundError("任务时段不存在")
        events = connection.execute(
            "SELECT * FROM task_events WHERE slot_id=? ORDER BY occurred_at, event_id", (slot_id,)
        ).fetchall()
        timeline = sorted((row for row in events if row["merge_status"] == "accepted"),
                          key=lambda row: row["merge_order"])
        review = connection.execute("SELECT * FROM review_records WHERE slot_id=?",
                                    (slot_id,)).fetchone()
        decisions = connection.execute(
            "SELECT * FROM schedule_decisions WHERE slot_id=? ORDER BY rowid", (slot_id,)
        ).fetchall()
        holds = connection.execute(
            "SELECT * FROM resource_holds WHERE slot_id=? ORDER BY resource_type, resource_id",
            (slot_id,),
        ).fetchall()
        return {
            "slot": self._slot_dict(slot),
            "timeline": [{"merge_order": row["merge_order"], "event_id": row["event_id"],
                          "event_type": row["event_type"], "occurred_at": row["occurred_at"],
                          "source": row["source"]} for row in timeline],
            "raw_events": [{"event_id": row["event_id"], "event_type": row["event_type"],
                            "occurred_at": row["occurred_at"], "source": row["source"],
                            "payload": json.loads(row["payload_json"]),
                            "merge_status": row["merge_status"],
                            "merge_note": row["merge_note"],
                            "received_at": row["received_at"]} for row in events],
            "review": self._review_dict(review) if review else None,
            "decisions": [self._decision_dict(row) for row in decisions],
            "holds": [dict(row) for row in holds],
        }

    def list_reviews(self, site_id: str, status: str | None = None) -> list[dict[str, Any]]:
        parameters: list[Any] = [site_id]
        query = "SELECT * FROM review_records WHERE site_id=?"
        if status:
            query += " AND status=?"
            parameters.append(status)
        query += " ORDER BY created_at, review_id"
        return [self._review_dict(row)
                for row in self.database.connection.execute(query, parameters)]

    def list_dispositions(self, site_id: str, status: str | None = None) -> list[dict[str, Any]]:
        parameters: list[Any] = [site_id]
        query = "SELECT * FROM dispositions WHERE site_id=?"
        if status:
            query += " AND status=?"
            parameters.append(status)
        query += " ORDER BY created_at, case_id"
        return [dict(row) for row in self.database.connection.execute(query, parameters)]

    def get_resources(self, site_id: str) -> dict[str, Any]:
        connection = self.database.connection
        templates = connection.execute(
            "SELECT * FROM task_templates WHERE site_id=? ORDER BY template_id", (site_id,)
        ).fetchall()
        areas = connection.execute(
            "SELECT * FROM training_areas WHERE site_id=? ORDER BY area_id", (site_id,)
        ).fetchall()
        equipment = connection.execute(
            "SELECT * FROM equipment_items WHERE site_id=? ORDER BY equipment_id", (site_id,)
        ).fetchall()
        restrictions = connection.execute(
            "SELECT * FROM restriction_windows WHERE site_id=? AND status='active' "
            "ORDER BY starts_at, window_id",
            (site_id,),
        ).fetchall()
        qualifications = connection.execute(
            "SELECT * FROM personnel_qualifications WHERE site_id=? ORDER BY holder_actor_id, qualification_code",
            (site_id,),
        ).fetchall()
        return {
            "templates": [{**dict(row),
                           "required_equipment": json.loads(row["required_equipment_json"])}
                          for row in templates],
            "areas": [dict(row) for row in areas],
            "equipment": [dict(row) for row in equipment],
            "restrictions": [dict(row) for row in restrictions],
            "qualifications": [dict(row) for row in qualifications],
        }

    @staticmethod
    def _slot_dict(slot) -> dict[str, Any]:
        return {"slot_id": slot["slot_id"], "plan_id": slot["plan_id"],
                "plan_version": slot["plan_version"], "area_id": slot["area_id"],
                "instructor_id": slot["instructor_id"],
                "equipment_ids": json.loads(slot["equipment_json"]),
                "starts_at": slot["starts_at"], "ends_at": slot["ends_at"],
                "status": slot["status"], "merge_state": slot["merge_state"],
                "explanations": json.loads(slot["explanations_json"])}

    @staticmethod
    def _decision_dict(row) -> dict[str, Any]:
        return {"decision_id": row["decision_id"], "plan_id": row["plan_id"],
                "slot_id": row["slot_id"], "decision": row["decision"],
                "reasons": json.loads(row["reasons_json"]), "actor_id": row["actor_id"],
                "created_at": row["created_at"]}

    @staticmethod
    def _review_dict(row) -> dict[str, Any]:
        return {"review_id": row["review_id"], "slot_id": row["slot_id"],
                "plan_id": row["plan_id"], "site_id": row["site_id"], "status": row["status"],
                "outcome": row["outcome"], "summary": json.loads(row["summary_json"]),
                "notes": row["notes"], "reviewed_by": row["reviewed_by"],
                "created_at": row["created_at"], "completed_at": row["completed_at"]}
