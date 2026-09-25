"""运行训练排程子系统的离线端到端验收。

覆盖：基础数据登记、候选排程生成与说明、按版本放行、资源互斥、
乱序与重复事件归并、限制升级撤销未开始放行、进行中任务人工处置、
进程重启后继续完成待复盘记录，以及审计链校验。
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .clock import FixedClock
from .scheduling import SchedulingService
from .service import DomainService
from .storage import Database

CLOCK = FixedClock(datetime(2026, 9, 25, 8, 0, tzinfo=timezone.utc))


def _bootstrap(database: Database) -> tuple[DomainService, SchedulingService]:
    domain = DomainService(database, CLOCK)
    service = SchedulingService(database, CLOCK, domain)
    domain.register_organization(request_id="acc-org", actor_id="bootstrap",
                                 organization_id="org-uav", name="无人机训练基地")
    domain.register_actor(request_id="acc-admin", actor_id="bootstrap", new_actor_id="admin-1",
                          display_name="系统管理员", role="admin", organization_id="org-uav")
    domain.register_actor(request_id="acc-planner", actor_id="admin-1", new_actor_id="planner-1",
                          display_name="训练参谋", role="operator", organization_id="org-uav")
    domain.register_actor(request_id="acc-duty", actor_id="admin-1", new_actor_id="duty-1",
                          display_name="值班教员", role="reviewer", organization_id="org-uav")
    domain.register_actor(request_id="acc-coach", actor_id="admin-1", new_actor_id="coach-1",
                          display_name="带教教员甲", role="operator", organization_id="org-uav")
    domain.register_site(request_id="acc-site", actor_id="admin-1", site_id="base-1",
                         organization_id="org-uav", name="一号训练基地", timezone_name="Asia/Shanghai")
    service.create_template(request_id="acc-tpl", actor_id="planner-1", site_id="base-1",
                            template_id="tpl-assembly", name="装调基础训练", skill_type="assembly",
                            duration_minutes=60, required_qualification="uav-assembly",
                            required_level="basic", required_equipment=["assembly-bench"],
                            area_type="assembly-shop")
    service.grant_qualification(request_id="acc-qual", actor_id="planner-1", site_id="base-1",
                                holder_actor_id="coach-1", qualification_code="uav-assembly",
                                level="advanced", valid_from="2026-01-01T00:00:00Z",
                                valid_until="2026-12-31T23:59:59Z")
    service.register_area(request_id="acc-area", actor_id="planner-1", site_id="base-1",
                          area_id="area-a", name="装调车间A", area_type="assembly-shop", capacity=2)
    service.register_equipment(request_id="acc-eq", actor_id="planner-1", site_id="base-1",
                               equipment_id="bench-1", name="装调台1", equipment_type="assembly-bench")
    return domain, service


def run() -> dict[str, object]:
    """执行完整排程故事并返回验收结果。"""

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "scheduling.sqlite3"
        database = Database(path)
        domain, service = _bootstrap(database)

        # 1. 生成可解释候选排程并按版本放行
        plan1, _ = service.create_plan(
            request_id="acc-plan1", actor_id="planner-1", site_id="base-1", plan_id="plan-alpha",
            template_id="tpl-assembly", team_id="team-alpha", instructor_id="coach-1",
            desired_start="2026-09-25T09:00:00Z", desired_end="2026-09-25T12:00:00Z")
        slot1 = plan1["candidates"][0]
        explained = bool(slot1["explanations"]) and all(
            item["result"] in ("pass", "warn", "fail") for item in slot1["explanations"])
        release1, _ = service.release_plan(request_id="acc-rel1", actor_id="duty-1",
                                           plan_id="plan-alpha", slot_id=slot1["slot_id"],
                                           plan_version=1)

        # 2. 第二支队伍同时段申请：候选必须避开已预占资源
        plan2, _ = service.create_plan(
            request_id="acc-plan2", actor_id="planner-1", site_id="base-1", plan_id="plan-beta",
            template_id="tpl-assembly", team_id="team-beta", instructor_id="coach-1",
            desired_start="2026-09-25T09:00:00Z", desired_end="2026-09-25T12:00:00Z")
        avoids_hold = all(c["starts_at"] >= "2026-09-25T10:00:00Z" for c in plan2["candidates"])
        rejections_explained = bool(plan2["rejections"]) and bool(plan2["rejections"][0]["reasons"])

        # 3. 乱序与重复事件归并为唯一过程，原始来源保留
        service.record_task_event(request_id="acc-ev-end", actor_id="planner-1",
                                  slot_id=slot1["slot_id"], event_id="evt-end-1",
                                  event_type="ended", occurred_at="2026-09-25T10:00:00Z",
                                  source="flight-log",
                                  payload={"telemetry": {"sorties": 1, "max_altitude_m": 60,
                                                         "anomalies": 0}})
        service.record_task_event(request_id="acc-ev-start", actor_id="planner-1",
                                  slot_id=slot1["slot_id"], event_id="evt-start-1",
                                  event_type="started", occurred_at="2026-09-25T09:00:00Z",
                                  source="flight-log")
        duplicate, _ = service.record_task_event(
            request_id="acc-ev-start-dup", actor_id="planner-1", slot_id=slot1["slot_id"],
            event_id="evt-start-1", event_type="started",
            occurred_at="2026-09-25T09:00:00Z", source="flight-log")
        task1 = service.get_task(slot1["slot_id"])
        merged_ok = ([item["event_type"] for item in task1["timeline"]] == ["started", "ended"]
                     and task1["slot"]["status"] == "completed"
                     and len(task1["raw_events"]) == 2
                     and duplicate["duplicate"])

        # 4. 限制升级：只撤销尚未开始的放行
        slot2 = plan2["candidates"][0]
        service.release_plan(request_id="acc-rel2", actor_id="duty-1", plan_id="plan-beta",
                             slot_id=slot2["slot_id"], plan_version=1)
        restriction, _ = service.create_restriction(
            request_id="acc-rst", actor_id="duty-1", site_id="base-1", window_id="rst-1",
            scope_type="airspace", severity="blocking", reason="临时空域管制",
            starts_at="2026-09-25T10:00:00Z", ends_at="2026-09-25T11:00:00Z")
        revoked = any(item["slot_id"] == slot2["slot_id"] and item["action"] == "revoked"
                      for item in restriction["affected_slots"])
        plan2_view = service.get_plan("plan-beta")
        revoke_explained = any(d["decision"] == "revoked" and d["slot_id"] == slot2["slot_id"]
                               for d in plan2_view["decisions"])

        # 5. 进行中任务遇设备检修：进入人工处置并中止
        plan3, _ = service.create_plan(
            request_id="acc-plan3", actor_id="planner-1", site_id="base-1", plan_id="plan-gamma",
            template_id="tpl-assembly", team_id="team-gamma", instructor_id="coach-1",
            desired_start="2026-09-25T13:00:00Z", desired_end="2026-09-25T16:00:00Z")
        slot3 = plan3["candidates"][0]
        service.release_plan(request_id="acc-rel3", actor_id="duty-1", plan_id="plan-gamma",
                             slot_id=slot3["slot_id"], plan_version=1)
        service.record_task_event(request_id="acc-ev3", actor_id="planner-1",
                                  slot_id=slot3["slot_id"], event_id="evt-start-3",
                                  event_type="started", occurred_at="2026-09-25T13:00:00Z",
                                  source="flight-log")
        maintenance, _ = service.set_equipment_status(
            request_id="acc-eq-off", actor_id="planner-1", equipment_id="bench-1",
            status="maintenance", reason="装调台突发检修")
        manual = any(item["slot_id"] == slot3["slot_id"] and item["action"] == "manual_review"
                     for item in maintenance["affected_slots"])
        open_cases = service.list_dispositions("base-1", status="open")
        resolved, _ = service.resolve_disposition(
            request_id="acc-resolve", actor_id="duty-1", case_id=open_cases[0]["case_id"],
            resolution="abort", notes="设备检修，任务中止")

        # 6. 进程重启后继续完成待复盘记录
        database.close()
        database2 = Database(path)
        domain2 = DomainService(database2, CLOCK)
        service2 = SchedulingService(database2, CLOCK, domain2)
        pending = service2.list_reviews("base-1", status="pending")
        completed_review, _ = service2.complete_review(
            request_id="acc-review", actor_id="duty-1", review_id=pending[0]["review_id"],
            outcome="passed", notes="过程完整，遥测摘要正常")
        remaining = service2.list_reviews("base-1", status="pending")
        valid, event_count = domain2.verify_audit()

        result = {
            "status": "ok",
            "candidates": len(plan1["candidates"]),
            "candidate_explained": explained,
            "release_holds": len(release1["holds"]),
            "second_plan_avoids_hold": avoids_hold,
            "rejections_explained": rejections_explained,
            "events_merged": merged_ok,
            "revoked_on_escalation": revoked,
            "revoke_explained": revoke_explained,
            "manual_case_opened": manual,
            "manual_case_aborted": resolved["slot_status"] == "aborted",
            "pending_reviews_after_restart": len(pending),
            "review_completed_after_restart": completed_review["status"] == "completed",
            "reviews_remaining": len(remaining),
            "audit_valid": valid,
            "audit_events": event_count,
        }
        database2.close()
        return result


def main() -> int:
    """打印验收结果并设置退出码。"""

    result = run()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    checks = [value for key, value in result.items()
              if key not in ("status", "candidates", "release_holds",
                             "pending_reviews_after_restart", "reviews_remaining", "audit_events")]
    ok = (result["status"] == "ok" and all(checks)
          and result["candidates"] > 0 and result["release_holds"] >= 3
          and result["pending_reviews_after_restart"] >= 2)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
