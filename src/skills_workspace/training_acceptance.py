"""运行训练计划与结构化遥测后台的离线端到端验收。"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .clock import FixedClock
from .planning import TrainingService, fmt_dt, parse_dt
from .service import DomainService
from .storage import Database

T = "2026-09-26T{:02d}:{:02d}:00+08:00"


def run() -> dict[str, object]:
    """执行一条训练计划、放行、事件归并、升级处置与复盘续办链。"""

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "training_acceptance.sqlite3"
        clock = FixedClock(parse_dt(T.format(8, 0), "t"))
        database = Database(path)
        dom = DomainService(database, clock)
        svc = TrainingService(database, clock)

        dom.register_organization(request_id="req-org", actor_id="bootstrap",
                                  organization_id="org-001", name="示范训练机构")
        dom.register_actor(request_id="req-admin", actor_id="bootstrap", new_actor_id="admin-001",
                           display_name="系统管理员", role="admin", organization_id="org-001")
        dom.register_actor(request_id="req-rev", actor_id="admin-001", new_actor_id="reviewer-001",
                           display_name="值班教员", role="reviewer", organization_id="org-001")
        dom.register_actor(request_id="req-op", actor_id="admin-001", new_actor_id="operator-001",
                           display_name="训练负责人", role="operator", organization_id="org-001")
        dom.register_site(request_id="req-site", actor_id="admin-001", site_id="site-001",
                          organization_id="org-001", name="无人机训练场", timezone_name="Asia/Shanghai")

        svc.register_team(request_id="req-team", actor_id="operator-001", site_id="site-001",
                          code="team-alpha", name="阿尔法队")
        svc.register_person(request_id="req-instructor", actor_id="operator-001", site_id="site-001",
                            code="ins-001", display_name="李教官", kind="instructor",
                            qualifications=["assembly", "route", "fault"])
        svc.register_person(request_id="req-trainee", actor_id="operator-001", site_id="site-001",
                            code="stu-001", display_name="学员甲", kind="trainee", qualifications=[])
        team_id = database.connection.execute(
            "SELECT team_id FROM teams WHERE code='team-alpha'").fetchone()["team_id"]
        svc.add_team_members(request_id="req-members", actor_id="operator-001", team_id=team_id,
                             person_codes=["stu-001"])
        svc.register_template(request_id="req-tpl-asm", actor_id="operator-001", site_id="site-001",
                              code="assembly", name="装调练习", task_type="assembly",
                              duration_minutes=60, area_tag="outdoor",
                              equipment_types=["drone", "toolkit"], crew_size=1)
        svc.register_template(request_id="req-tpl-route", actor_id="operator-001", site_id="site-001",
                              code="route-plan", name="航线规划", task_type="route",
                              duration_minutes=45, area_tag="outdoor",
                              equipment_types=["drone"], crew_size=1)
        svc.register_area(request_id="req-area-1", actor_id="operator-001", site_id="site-001",
                          code="north", name="北训练场", tag="outdoor")
        svc.register_equipment(request_id="req-eq-drone", actor_id="operator-001", site_id="site-001",
                               code="uav-001", name="一号机", equipment_type="drone")
        svc.register_equipment(request_id="req-eq-tool", actor_id="operator-001", site_id="site-001",
                               code="tool-001", name="装调工具", equipment_type="toolkit")

        # 1) 候选排程：两条任务应被排到不重叠时段。
        plan = svc.generate_plan(
            request_id="req-plan", actor_id="operator-001", plan_key="2026-09-26",
            site_id="site-001", horizon_start=T.format(8, 0), horizon_end=T.format(18, 0),
            demands=[{"team_code": "team-alpha", "template_code": "assembly"},
                     {"team_code": "team-alpha", "template_code": "route-plan"}])
        decisions = [item["decision"] for item in plan["items"]]
        starts = [item["planned_start"] for item in plan["items"]]

        # 2) 值班教员放行，资源原子预占。
        released = svc.release_plan(request_id="req-release", actor_id="reviewer-001",
                                    plan_version_id=plan["plan_version_id"])
        assembly_task = released["task_ids"][0]
        held = database.connection.execute(
            "SELECT COUNT(*) c FROM resource_reservations WHERE status='held'").fetchone()["c"]

        # 3) 乱序/重复事件归并：先收结束，再收开始，过程仍唯一。
        clock2 = FixedClock(parse_dt(T.format(8, 0), "t"))
        svc.clock = dom.clock = clock2
        svc.ingest_task_event(request_id="req-ev-end", actor_id="operator-001", event_id="ev-end",
                              task_id=assembly_task, event_type="ended",
                              occurred_at=T.format(9, 0), source="gcs-primary")
        svc.ingest_task_event(request_id="req-ev-start", actor_id="operator-001", event_id="ev-start",
                              task_id=assembly_task, event_type="started",
                              occurred_at=T.format(8, 0), source="gcs-primary")
        replay = svc.ingest_task_event(request_id="req-ev-end-dup", actor_id="operator-001",
                                       event_id="ev-end", task_id=assembly_task,
                                       event_type="ended", occurred_at=T.format(9, 0),
                                       source="gcs-backup")
        task = svc.get_task(assembly_task)
        triggers = [t["trigger"] for t in task["transitions"]]

        # 4) 结构化遥测摘要入队，随后模拟进程重启并续办复盘。
        svc.ingest_telemetry_summary(request_id="req-tel", actor_id="operator-001",
                                     task_id=assembly_task, summary_id="summary-001",
                                     metrics={"flight_minutes": 26, "battery_pct": 81,
                                              "fault_codes": []})
        database.close()

        database2 = Database(path)
        svc2 = TrainingService(database2, FixedClock(parse_dt(T.format(9, 30), "t")))
        resumed = svc2.resume_pending_work()
        review = svc2.get_task(assembly_task)["review"]
        valid, event_count = DomainService(database2, svc2.clock).verify_audit()
        database2.close()

        return {
            "status": "ok",
            "decisions": decisions,
            "starts": starts,
            "released_tasks": len(released["task_ids"]),
            "held_reservations": held,
            "end_event_replayed": replay["replayed"],
            "transitions": triggers,
            "raw_events_preserved": len(task["raw_events"]),
            "reviews_resumed_on_startup": resumed["found_on_startup"],
            "review_status": review["status"],
            "audit_events": event_count,
            "audit_valid": valid,
        }


def main() -> int:
    result = run()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    ok = (result["status"] == "ok" and result["audit_valid"]
          and result["transitions"] == ["started", "ended"]
          and result["end_event_replayed"]
          and result["review_status"] == "completed")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
