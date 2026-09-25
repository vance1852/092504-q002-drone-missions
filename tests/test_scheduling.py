import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from skills_workspace.clock import FixedClock
from skills_workspace.errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from skills_workspace.scheduling import SchedulingService
from skills_workspace.service import DomainService
from skills_workspace.storage import Database

CLOCK = FixedClock(datetime(2026, 9, 25, 8, 0, tzinfo=timezone.utc))
START = "2026-09-25T09:00:00Z"
END = "2026-09-25T12:00:00Z"


class SchedulingTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.domain = DomainService(self.database, CLOCK)
        self.service = SchedulingService(self.database, CLOCK, self.domain)
        self.domain.register_organization(request_id="org", actor_id="bootstrap",
                                          organization_id="o1", name="无人机训练基地")
        self.domain.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="a1",
                                   display_name="管理员", role="admin", organization_id="o1")
        self.domain.register_actor(request_id="planner", actor_id="a1", new_actor_id="op1",
                                   display_name="训练参谋", role="operator", organization_id="o1")
        self.domain.register_actor(request_id="duty", actor_id="a1", new_actor_id="r1",
                                   display_name="值班教员", role="reviewer", organization_id="o1")
        self.domain.register_actor(request_id="coach", actor_id="a1", new_actor_id="c1",
                                   display_name="带教教员", role="operator", organization_id="o1")
        self.domain.register_actor(request_id="auditor", actor_id="a1", new_actor_id="au1",
                                   display_name="审计员", role="auditor", organization_id="o1")
        self.domain.register_site(request_id="site", actor_id="a1", site_id="s1",
                                  organization_id="o1", name="一号基地", timezone_name="Asia/Shanghai")
        self.service.create_template(request_id="tpl", actor_id="op1", site_id="s1",
                                     template_id="tpl-1", name="装调训练", skill_type="assembly",
                                     duration_minutes=60, required_qualification="uav-assembly",
                                     required_equipment=["bench"], area_type="shop")
        self.service.grant_qualification(request_id="qual", actor_id="op1", site_id="s1",
                                         holder_actor_id="c1", qualification_code="uav-assembly",
                                         level="advanced", valid_from="2026-01-01T00:00:00Z",
                                         valid_until="2026-12-31T23:59:59Z")
        self.service.register_area(request_id="area", actor_id="op1", site_id="s1",
                                   area_id="area-1", name="装调间", area_type="shop")
        self.service.register_equipment(request_id="eq", actor_id="op1", site_id="s1",
                                        equipment_id="eq-1", name="装调台", equipment_type="bench")

    def tearDown(self):
        self.database.close()

    def _plan(self, request_id, plan_id, start=START, end=END, instructor="c1"):
        response, _ = self.service.create_plan(
            request_id=request_id, actor_id="op1", site_id="s1", plan_id=plan_id,
            template_id="tpl-1", team_id=f"team-{plan_id}", instructor_id=instructor,
            desired_start=start, desired_end=end)
        return response

    def _release(self, request_id, plan_id, slot_id, version=1):
        response, _ = self.service.release_plan(
            request_id=request_id, actor_id="r1", plan_id=plan_id, slot_id=slot_id,
            plan_version=version)
        return response

    def _release_first(self, request_id, plan):
        return self._release(request_id, plan["plan_id"], plan["candidates"][0]["slot_id"],
                             plan["version"])

    # ---------- 候选排程与说明 ----------

    def test_candidates_carry_explanations(self):
        plan = self._plan("p1", "plan-1")
        self.assertEqual("candidates", plan["status"])
        self.assertGreater(len(plan["candidates"]), 0)
        first = plan["candidates"][0]
        self.assertEqual("2026-09-25T09:00:00Z", first["starts_at"])
        checks = {item["check"] for item in first["explanations"]}
        self.assertIn("area", checks)
        self.assertIn("equipment", checks)
        self.assertIn("instructor", checks)

    def test_missing_qualification_yields_zero_candidates_with_reasons(self):
        plan = self._plan("p-noqual", "plan-noqual", instructor="op1")
        self.assertEqual([], plan["candidates"])
        self.assertTrue(plan["rejections"])
        self.assertIn("未持有资质", plan["rejections"][0]["reasons"][0])

    def test_second_plan_avoids_held_resources(self):
        plan1 = self._plan("p1", "plan-1")
        self._release_first("rel-1", plan1)
        plan2 = self._plan("p2", "plan-2")
        self.assertTrue(plan2["candidates"])
        for candidate in plan2["candidates"]:
            self.assertGreaterEqual(candidate["starts_at"], "2026-09-25T10:00:00Z")
        self.assertTrue(plan2["rejections"])

    # ---------- 放行与版本 ----------

    def test_release_creates_atomic_holds(self):
        plan = self._plan("p1", "plan-1")
        release = self._release_first("rel-1", plan)
        self.assertEqual("released", release["status"])
        resources = {(h["resource_type"], h["resource_id"]) for h in release["holds"]}
        self.assertEqual({("area", "area-1"), ("equipment", "eq-1"), ("instructor", "c1")},
                         resources)

    def test_release_rejects_stale_version(self):
        plan = self._plan("p1", "plan-1")
        self.service.regenerate_plan(request_id="regen", actor_id="op1", plan_id="plan-1",
                                     desired_start="2026-09-25T13:00:00Z",
                                     desired_end="2026-09-25T16:00:00Z", reason="窗口调整")
        with self.assertRaisesRegex(ConflictError, "版本已变更"):
            self._release("rel-stale", "plan-1", plan["candidates"][0]["slot_id"], version=1)

    def test_release_rechecks_resources_atomically(self):
        plan1 = self._plan("p1", "plan-1")
        plan2 = self._plan("p2", "plan-2")  # 候选生成时资源尚未被预占
        self._release_first("rel-1", plan1)
        with self.assertRaisesRegex(ConflictError, "已被其他任务预占"):
            self._release("rel-2", "plan-2", plan2["candidates"][0]["slot_id"])
        # 失败回滚：不应留下任何预占
        view = self.service.get_plan("plan-2")
        self.assertEqual([], view["active_holds"])
        self.assertEqual("candidates", view["plan"]["status"])

    def test_release_is_idempotent(self):
        plan = self._plan("p1", "plan-1")
        slot_id = plan["candidates"][0]["slot_id"]
        first, replayed1 = self.service.release_plan(
            request_id="rel-1", actor_id="r1", plan_id="plan-1", slot_id=slot_id, plan_version=1)
        second, replayed2 = self.service.release_plan(
            request_id="rel-1", actor_id="r1", plan_id="plan-1", slot_id=slot_id, plan_version=1)
        self.assertFalse(replayed1)
        self.assertTrue(replayed2)
        self.assertEqual(first, second)
        holds = self.database.connection.execute(
            "SELECT COUNT(*) AS c FROM resource_holds WHERE status='active'").fetchone()["c"]
        self.assertEqual(3, holds)

    def test_operator_cannot_release(self):
        plan = self._plan("p1", "plan-1")
        with self.assertRaises(PermissionDenied):
            self.service.release_plan(request_id="rel-x", actor_id="op1", plan_id="plan-1",
                                      slot_id=plan["candidates"][0]["slot_id"], plan_version=1)

    def test_auditor_cannot_manage_registries(self):
        with self.assertRaises(PermissionDenied):
            self.service.register_equipment(request_id="eq-x", actor_id="au1", site_id="s1",
                                            equipment_id="eq-2", name="装调台2",
                                            equipment_type="bench")

    # ---------- 事件归并 ----------

    def test_out_of_order_events_merge_into_unique_process(self):
        plan = self._plan("p1", "plan-1")
        slot_id = plan["candidates"][0]["slot_id"]
        self._release("rel-1", "plan-1", slot_id)
        self.service.record_task_event(request_id="e2", actor_id="op1", slot_id=slot_id,
                                       event_id="evt-end", event_type="ended",
                                       occurred_at="2026-09-25T10:00:00Z", source="flight-log",
                                       payload={"telemetry": {"sorties": 1}})
        result, _ = self.service.record_task_event(
            request_id="e1", actor_id="op1", slot_id=slot_id, event_id="evt-start",
            event_type="started", occurred_at="2026-09-25T09:00:00Z", source="flight-log")
        self.assertEqual("completed", result["slot_status"])
        task = self.service.get_task(slot_id)
        self.assertEqual(["started", "ended"], [e["event_type"] for e in task["timeline"]])
        self.assertEqual(2, len(task["raw_events"]))
        # 完成后资源预占全部释放
        self.assertTrue(all(h["status"] == "released" for h in task["holds"]))
        # 复盘摘要包含结构化遥测
        self.assertEqual("pending", task["review"]["status"])
        self.assertEqual(1, len(task["review"]["summary"]["telemetry"]))

    def test_duplicate_event_id_is_deduped(self):
        plan = self._plan("p1", "plan-1")
        slot_id = plan["candidates"][0]["slot_id"]
        self._release("rel-1", "plan-1", slot_id)
        first, _ = self.service.record_task_event(
            request_id="e1", actor_id="op1", slot_id=slot_id, event_id="evt-1",
            event_type="started", occurred_at="2026-09-25T09:00:00Z", source="flight-log")
        second, _ = self.service.record_task_event(
            request_id="e1-dup", actor_id="op1", slot_id=slot_id, event_id="evt-1",
            event_type="started", occurred_at="2026-09-25T09:00:00Z", source="flight-log")
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        task = self.service.get_task(slot_id)
        self.assertEqual(1, len(task["raw_events"]))
        with self.assertRaises(ConflictError):
            self.service.record_task_event(
                request_id="e1-bad", actor_id="op1", slot_id=slot_id, event_id="evt-1",
                event_type="started", occurred_at="2026-09-25T09:05:00Z", source="flight-log")

    def test_illegal_events_kept_raw_with_reason(self):
        plan = self._plan("p1", "plan-1")
        slot_id = plan["candidates"][0]["slot_id"]
        self._release("rel-1", "plan-1", slot_id)
        self.service.record_task_event(request_id="e1", actor_id="op1", slot_id=slot_id,
                                       event_id="evt-pause", event_type="paused",
                                       occurred_at="2026-09-25T09:10:00Z", source="flight-log")
        task = self.service.get_task(slot_id)
        self.assertEqual("released", task["slot"]["status"])
        self.assertEqual([], task["timeline"])
        self.assertEqual("rejected", task["raw_events"][0]["merge_status"])
        self.assertIn("started", task["raw_events"][0]["merge_note"])

    def test_pause_resume_exception_flow(self):
        plan = self._plan("p1", "plan-1")
        slot_id = plan["candidates"][0]["slot_id"]
        self._release("rel-1", "plan-1", slot_id)
        events = [("evt-1", "started", "09:00"), ("evt-2", "paused", "09:20"),
                  ("evt-3", "exception", "09:25"), ("evt-4", "resumed", "09:30"),
                  ("evt-5", "ended", "10:00")]
        for index, (event_id, event_type, hhmm) in enumerate(events):
            self.service.record_task_event(
                request_id=f"e-{index}", actor_id="op1", slot_id=slot_id, event_id=event_id,
                event_type=event_type, occurred_at=f"2026-09-25T{hhmm}:00Z", source="flight-log",
                payload={"detail": "链路抖动"} if event_type == "exception" else None)
        task = self.service.get_task(slot_id)
        self.assertEqual(["started", "paused", "exception", "resumed", "ended"],
                         [e["event_type"] for e in task["timeline"]])
        self.assertEqual("completed", task["slot"]["status"])
        self.assertEqual(1, len(task["review"]["summary"]["exceptions"]))

    # ---------- 限制升级 ----------

    def test_escalation_revokes_only_unstarted_releases(self):
        plan1 = self._plan("p1", "plan-1")
        slot1 = plan1["candidates"][0]["slot_id"]  # 09:00-10:00，保持未开始
        self._release("rel-1", "plan-1", slot1)
        plan2 = self._plan("p2", "plan-2")
        slot2 = plan2["candidates"][0]["slot_id"]  # 10:00-11:00，进行中
        self._release("rel-2", "plan-2", slot2)
        self.service.record_task_event(request_id="e1", actor_id="op1", slot_id=slot2,
                                       event_id="evt-s2", event_type="started",
                                       occurred_at="2026-09-25T10:00:00Z", source="flight-log")
        _, _ = self.service.create_restriction(
            request_id="rst-adv", actor_id="r1", site_id="s1", window_id="rst-1",
            scope_type="airspace", severity="advisory", reason="空域流量控制",
            starts_at="2026-09-25T09:30:00Z", ends_at="2026-09-25T10:30:00Z")
        result, _ = self.service.escalate_restriction(request_id="rst-esc", actor_id="r1",
                                                      window_id="rst-1")
        actions = {item["slot_id"]: item["action"] for item in result["affected_slots"]}
        self.assertEqual("revoked", actions[slot1])
        self.assertEqual("manual_review", actions[slot2])
        # 未开始的放行被撤销并释放资源，计划回到候选状态
        task1 = self.service.get_task(slot1)
        self.assertEqual("revoked", task1["slot"]["status"])
        self.assertTrue(all(h["status"] == "released" for h in task1["holds"]))
        self.assertEqual("candidates", self.service.get_plan("plan-1")["plan"]["status"])
        # 进行中的任务进入人工处置且资源仍被预占
        task2 = self.service.get_task(slot2)
        self.assertEqual("manual_review", task2["slot"]["status"])
        self.assertTrue(any(h["status"] == "active" for h in task2["holds"]))
        cases = self.service.list_dispositions("s1", status="open")
        self.assertEqual(1, len(cases))
        # 决策日志说明撤销原因
        reasons = [d for d in task1["decisions"] if d["decision"] == "revoked"]
        self.assertTrue(reasons and "空域流量控制" in reasons[0]["reasons"][0])

    def test_blocking_restriction_sweeps_on_creation(self):
        plan = self._plan("p1", "plan-1")
        slot_id = plan["candidates"][0]["slot_id"]
        self._release("rel-1", "plan-1", slot_id)
        result, _ = self.service.create_restriction(
            request_id="rst-1", actor_id="r1", site_id="s1", window_id="rst-1",
            scope_type="site", severity="blocking", reason="全场临时管制",
            starts_at="2026-09-25T09:00:00Z", ends_at="2026-09-25T12:00:00Z")
        self.assertEqual([{"slot_id": slot_id, "action": "revoked",
                           "reason": "限制窗口生效: 全场临时管制"}], result["affected_slots"])

    def test_equipment_maintenance_triggers_manual_review(self):
        plan = self._plan("p1", "plan-1")
        slot_id = plan["candidates"][0]["slot_id"]
        self._release("rel-1", "plan-1", slot_id)
        self.service.record_task_event(request_id="e1", actor_id="op1", slot_id=slot_id,
                                       event_id="evt-s", event_type="started",
                                       occurred_at="2026-09-25T09:00:00Z", source="flight-log")
        result, _ = self.service.set_equipment_status(
            request_id="eq-off", actor_id="op1", equipment_id="eq-1", status="maintenance",
            reason="装调台检修")
        self.assertEqual("manual_review", result["affected_slots"][0]["action"])
        self.assertEqual("manual_review", self.service.get_task(slot_id)["slot"]["status"])

    # ---------- 人工处置 ----------

    def _manual_case(self):
        plan = self._plan("p1", "plan-1")
        slot_id = plan["candidates"][0]["slot_id"]
        self._release("rel-1", "plan-1", slot_id)
        self.service.record_task_event(request_id="e1", actor_id="op1", slot_id=slot_id,
                                       event_id="evt-s", event_type="started",
                                       occurred_at="2026-09-25T09:00:00Z", source="flight-log")
        self.service.create_restriction(
            request_id="rst-1", actor_id="r1", site_id="s1", window_id="rst-1",
            scope_type="airspace", severity="blocking", reason="临时空域管制",
            starts_at="2026-09-25T09:00:00Z", ends_at="2026-09-25T12:00:00Z")
        case = self.service.list_dispositions("s1", status="open")[0]
        return slot_id, case["case_id"]

    def test_disposition_abort_creates_review_and_frees_resources(self):
        slot_id, case_id = self._manual_case()
        result, _ = self.service.resolve_disposition(
            request_id="res-1", actor_id="r1", case_id=case_id, resolution="abort",
            notes="空域管制，任务中止")
        self.assertEqual("aborted", result["slot_status"])
        task = self.service.get_task(slot_id)
        self.assertTrue(all(h["status"] == "released" for h in task["holds"]))
        self.assertEqual("pending", task["review"]["status"])
        self.assertEqual("aborted", self.service.get_plan("plan-1")["plan"]["status"])
        self.assertTrue(any(d["decision"] == "aborted" for d in task["decisions"]))
        with self.assertRaises(ConflictError):
            self.service.resolve_disposition(request_id="res-2", actor_id="r1",
                                             case_id=case_id, resolution="abort")

    def test_disposition_reschedule_returns_plan_to_candidates(self):
        slot_id, case_id = self._manual_case()
        result, _ = self.service.resolve_disposition(
            request_id="res-1", actor_id="r1", case_id=case_id, resolution="reschedule")
        self.assertEqual("revoked", result["slot_status"])
        view = self.service.get_plan("plan-1")
        self.assertEqual("candidates", view["plan"]["status"])
        self.assertTrue(any(d["decision"] == "rescheduled" for d in view["decisions"]))

    def test_disposition_continue_restores_task(self):
        slot_id, case_id = self._manual_case()
        result, _ = self.service.resolve_disposition(
            request_id="res-1", actor_id="r1", case_id=case_id, resolution="continue",
            notes="管制解除")
        self.assertEqual("in_progress", result["slot_status"])
        self.assertEqual("in_progress", self.service.get_task(slot_id)["slot"]["status"])

    # ---------- 复盘与重启恢复 ----------

    def test_review_completion_is_guarded(self):
        plan = self._plan("p1", "plan-1")
        slot_id = plan["candidates"][0]["slot_id"]
        self._release("rel-1", "plan-1", slot_id)
        for index, (event_id, event_type) in enumerate([("evt-s", "started"), ("evt-e", "ended")]):
            self.service.record_task_event(
                request_id=f"e-{index}", actor_id="op1", slot_id=slot_id, event_id=event_id,
                event_type=event_type, occurred_at=f"2026-09-25T09:0{index}:00Z",
                source="flight-log")
        review = self.service.list_reviews("s1", status="pending")[0]
        done, _ = self.service.complete_review(request_id="rv-1", actor_id="r1",
                                               review_id=review["review_id"], outcome="passed")
        self.assertEqual("completed", done["status"])
        with self.assertRaises(ConflictError):
            self.service.complete_review(request_id="rv-2", actor_id="r1",
                                         review_id=review["review_id"], outcome="failed")

    def test_pending_reviews_survive_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "restart.sqlite3"
            database = Database(path)
            domain = DomainService(database, CLOCK)
            service = SchedulingService(database, CLOCK, domain)
            domain.register_organization(request_id="org", actor_id="bootstrap",
                                         organization_id="o1", name="无人机训练基地")
            domain.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="a1",
                                  display_name="管理员", role="admin", organization_id="o1")
            domain.register_actor(request_id="planner", actor_id="a1", new_actor_id="op1",
                                  display_name="训练参谋", role="operator", organization_id="o1")
            domain.register_actor(request_id="duty", actor_id="a1", new_actor_id="r1",
                                  display_name="值班教员", role="reviewer", organization_id="o1")
            domain.register_actor(request_id="coach", actor_id="a1", new_actor_id="c1",
                                  display_name="带教教员", role="operator", organization_id="o1")
            domain.register_site(request_id="site", actor_id="a1", site_id="s1",
                                 organization_id="o1", name="一号基地",
                                 timezone_name="Asia/Shanghai")
            service.create_template(request_id="tpl", actor_id="op1", site_id="s1",
                                    template_id="tpl-1", name="装调训练", skill_type="assembly",
                                    duration_minutes=60, required_qualification="uav-assembly",
                                    required_equipment=["bench"], area_type="shop")
            service.grant_qualification(request_id="qual", actor_id="op1", site_id="s1",
                                        holder_actor_id="c1", qualification_code="uav-assembly",
                                        level="advanced", valid_from="2026-01-01T00:00:00Z",
                                        valid_until="2026-12-31T23:59:59Z")
            service.register_area(request_id="area", actor_id="op1", site_id="s1",
                                  area_id="area-1", name="装调间", area_type="shop")
            service.register_equipment(request_id="eq", actor_id="op1", site_id="s1",
                                       equipment_id="eq-1", name="装调台", equipment_type="bench")
            plan = service.create_plan(request_id="p1", actor_id="op1", site_id="s1",
                                       plan_id="plan-1", template_id="tpl-1", team_id="team-1",
                                       instructor_id="c1", desired_start=START, desired_end=END)[0]
            slot_id = plan["candidates"][0]["slot_id"]
            service.release_plan(request_id="rel-1", actor_id="r1", plan_id="plan-1",
                                 slot_id=slot_id, plan_version=1)
            for index, (event_id, event_type) in enumerate([("evt-s", "started"),
                                                            ("evt-e", "ended")]):
                service.record_task_event(
                    request_id=f"e-{index}", actor_id="op1", slot_id=slot_id, event_id=event_id,
                    event_type=event_type, occurred_at=f"2026-09-25T09:0{index}:00Z",
                    source="flight-log")
            database.close()
            # 模拟进程重启：同一数据库文件上的全新服务实例
            database2 = Database(path)
            domain2 = DomainService(database2, CLOCK)
            service2 = SchedulingService(database2, CLOCK, domain2)
            pending = service2.list_reviews("s1", status="pending")
            self.assertEqual(1, len(pending))
            done, _ = service2.complete_review(request_id="rv-1", actor_id="r1",
                                               review_id=pending[0]["review_id"],
                                               outcome="passed", notes="复盘通过")
            self.assertEqual("completed", done["status"])
            self.assertEqual([], service2.list_reviews("s1", status="pending"))
            valid, _ = domain2.verify_audit()
            self.assertTrue(valid)
            database2.close()

    # ---------- 输入校验 ----------

    def test_validation_rejects_bad_window(self):
        with self.assertRaises(ValidationError):
            self._plan("p-bad", "plan-bad", start="2026-09-25T12:00:00Z", end=START)

    def test_unknown_plan_raises_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.get_plan("plan-missing")


if __name__ == "__main__":
    unittest.main()
