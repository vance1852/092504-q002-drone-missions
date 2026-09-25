import threading
import unittest

from skills_workspace.clock import FixedClock
from skills_workspace.errors import ConflictError, PermissionDenied
from skills_workspace.planning import TrainingService, fmt_dt, parse_dt
from skills_workspace.service import DomainService
from skills_workspace.storage import Database

T = "2026-09-26T{:02d}:{:02d}:00+08:00"


def utc(hh: int, mm: int = 0) -> str:
    return fmt_dt(parse_dt(T.format(hh, mm), "t"))


def bootstrap_world(database, clock):
    dom = DomainService(database, clock)
    svc = TrainingService(database, clock)
    dom.register_organization(request_id="req-org", actor_id="bootstrap",
                              organization_id="o1", name="基地")
    dom.register_actor(request_id="req-admin", actor_id="bootstrap", new_actor_id="adm",
                       display_name="管理员", role="admin", organization_id="o1")
    dom.register_actor(request_id="req-rev", actor_id="adm", new_actor_id="rev",
                       display_name="值班教员", role="reviewer", organization_id="o1")
    dom.register_actor(request_id="req-op", actor_id="adm", new_actor_id="op",
                       display_name="操作员", role="operator", organization_id="o1")
    dom.register_site(request_id="req-site", actor_id="adm", site_id="s1",
                      organization_id="o1", name="训练场", timezone_name="Asia/Shanghai")
    return dom, svc


class PlanningTestBase(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        # 情景从训练日 08:00（本地）开始。
        clock = FixedClock(parse_dt(T.format(8, 0), "t"))
        self.clock = clock
        self.dom, self.svc = bootstrap_world(self.database, clock)

    def advance_clock(self, hh: int, mm: int = 0):
        clock = FixedClock(parse_dt(T.format(hh, mm), "t"))
        self.clock = clock
        self.dom.clock = clock
        self.svc.clock = clock

    def tearDown(self):
        self.database.close()

    def seed(self, *, equipment_types=("drone",), quals=("assembly",), crew=1, team_code="t-a"):
        self.svc.register_team(request_id="req-team-" + team_code, actor_id="op", site_id="s1",
                               code=team_code, name=team_code)
        for i, q in enumerate(quals):
            self.svc.register_person(request_id=f"req-ins-{q}", actor_id="op", site_id="s1",
                                     code=f"ins-{q}", display_name=q, kind="instructor",
                                     qualifications=[q])
        for i in range(max(crew, 1)):
            self.svc.register_person(request_id=f"req-stu-{team_code}-{i}", actor_id="op", site_id="s1",
                                     code=f"stu-{team_code}-{i}", display_name=str(i),
                                     kind="trainee", qualifications=[])
        team_id = self.database.connection.execute(
            "SELECT team_id FROM teams WHERE code=?", (team_code,)).fetchone()["team_id"]
        self.svc.add_team_members(
            request_id="req-tm-" + team_code, actor_id="op", team_id=team_id,
            person_codes=[f"stu-{team_code}-{i}" for i in range(max(crew, 1))])
        self.svc.register_template(request_id="req-tpl-" + "-".join(equipment_types), actor_id="op",
                                   site_id="s1", code="asm", name="装调", task_type="assembly",
                                   duration_minutes=60, area_tag="field",
                                   equipment_types=list(equipment_types), crew_size=crew)
        self.svc.register_area(request_id="req-area", actor_id="op", site_id="s1",
                               code="area-n", name="北区", tag="field")
        for t in equipment_types:
            self.svc.register_equipment(request_id="req-eq-" + t, actor_id="op", site_id="s1",
                                        code="uav-" + t, name=t, equipment_type=t)
        return team_id

    def generate_and_release(self, plan_key="d1", demands=None, **kwargs):
        demands = demands or [{"team_code": "t-a", "template_code": "asm"}]
        plan = self.svc.generate_plan(
            request_id="req-gen-" + plan_key, actor_id="op", plan_key=plan_key, site_id="s1",
            horizon_start=T.format(8, 0), horizon_end=T.format(18, 0), demands=demands)
        rel = self.svc.release_plan(request_id="req-rel-" + plan_key, actor_id="rev",
                                    plan_version_id=plan["plan_version_id"])
        return plan, rel


class CandidateTests(PlanningTestBase):
    def test_approved_candidate_explains_reasons(self):
        self.seed()
        plan, _ = self.generate_and_release()
        item = plan["items"][0]
        self.assertEqual("approved", item["decision"])
        self.assertEqual("approved", item["reasons"][0]["code"])

    def test_equipment_maintenance_causes_reschedule_or_unschedulable(self):
        self.seed()
        eq = self.database.connection.execute(
            "SELECT equipment_id FROM equipment WHERE code='uav-drone'").fetchone()["equipment_id"]
        # 整个窗口维护 -> 不可排程并给出原因。
        self.svc.set_equipment_status(request_id="req-maint", actor_id="op",
                                      equipment_id=eq, status="maintenance")
        plan = self.svc.generate_plan(
            request_id="req-gen-x", actor_id="op", plan_key="dx", site_id="s1",
            horizon_start=T.format(8, 0), horizon_end=T.format(18, 0),
            demands=[{"team_code": "t-a", "template_code": "asm"}])
        item = plan["items"][0]
        self.assertEqual("unschedulable", item["decision"])
        self.assertIn("equipment_unavailable", {r["code"] for r in item["reasons"]})

    def test_restriction_in_first_slot_forces_reschedule(self):
        self.seed()
        self.svc.register_restriction(
            request_id="req-win", actor_id="op", site_id="s1", scope="site", resource_id="s1",
            starts_at=T.format(8, 0), ends_at=T.format(9, 0), reason="早间管制", level="normal")
        plan = self.svc.generate_plan(
            request_id="req-gen-y", actor_id="op", plan_key="dy", site_id="s1",
            horizon_start=T.format(8, 0), horizon_end=T.format(18, 0),
            demands=[{"team_code": "t-a", "template_code": "asm"}])
        item = plan["items"][0]
        self.assertEqual("rescheduled", item["decision"])
        self.assertEqual(utc(9, 0), item["planned_start"])
        self.assertIn("rescheduled_to", {r["code"] for r in item["reasons"]})

    def test_unqualified_instructor_is_explained(self):
        self.seed(quals=("route",))
        plan = self.svc.generate_plan(
            request_id="req-gen-q", actor_id="op", plan_key="dq", site_id="s1",
            horizon_start=T.format(8, 0), horizon_end=T.format(18, 0),
            demands=[{"team_code": "t-a", "template_code": "asm"}])
        item = plan["items"][0]
        self.assertEqual("unschedulable", item["decision"])
        self.assertIn("instructor_unqualified", {r["code"] for r in item["reasons"]})


class ReleaseTests(PlanningTestBase):
    def test_release_is_atomic_and_operator_cannot_release(self):
        self.seed()
        plan = self.svc.generate_plan(
            request_id="req-g1", actor_id="op", plan_key="d1", site_id="s1",
            horizon_start=T.format(8, 0), horizon_end=T.format(18, 0),
            demands=[{"team_code": "t-a", "template_code": "asm"}])
        with self.assertRaises(PermissionDenied):
            self.svc.release_plan(request_id="req-rel-deny", actor_id="op",
                                  plan_version_id=plan["plan_version_id"])
        rel = self.svc.release_plan(request_id="req-rel-ok", actor_id="rev",
                                    plan_version_id=plan["plan_version_id"])
        self.assertEqual(1, len(rel["task_ids"]))
        # area + team + instructor + equipment
        held = self.database.connection.execute(
            "SELECT COUNT(*) c FROM resource_reservations WHERE status='held'").fetchone()["c"]
        self.assertEqual(4, held)

    def test_two_released_plans_cannot_double_book(self):
        self.seed()
        _, rel1 = self.generate_and_release(plan_key="d1")
        # 第二支队伍、同一唯一资源 → 首选 8:00 冲突，应顺延到 9:00 而非抢占。
        self.seed(team_code="t-b")
        plan2 = self.svc.generate_plan(
            request_id="req-g2", actor_id="op", plan_key="d2", site_id="s1",
            horizon_start=T.format(8, 0), horizon_end=T.format(18, 0),
            demands=[{"team_code": "t-b", "template_code": "asm"}])
        item = plan2["items"][0]
        self.assertEqual("rescheduled", item["decision"])
        self.assertEqual(utc(9, 0), item["planned_start"])

    def test_release_rejects_when_new_restriction_appeared(self):
        self.seed()
        plan = self.svc.generate_plan(
            request_id="req-g1", actor_id="op", plan_key="d1", site_id="s1",
            horizon_start=T.format(8, 0), horizon_end=T.format(18, 0),
            demands=[{"team_code": "t-a", "template_code": "asm"}])
        self.svc.register_restriction(
            request_id="req-win", actor_id="op", site_id="s1", scope="site", resource_id="s1",
            starts_at=T.format(8, 0), ends_at=T.format(9, 0), reason="突发管制", level="normal")
        with self.assertRaises(ConflictError):
            self.svc.release_plan(request_id="req-rel-blocked", actor_id="rev",
                                  plan_version_id=plan["plan_version_id"])
        held = self.database.connection.execute(
            "SELECT COUNT(*) c FROM resource_reservations").fetchone()["c"]
        self.assertEqual(0, held)

    def test_new_version_revokes_only_unstarted_old_tasks(self):
        self.seed()
        _, rel1 = self.generate_and_release(plan_key="d1")
        task_id = rel1["task_ids"][0]
        # 同一 plan_key 生成新版本并放行，旧任务尚未开始 → 撤销并释放。
        plan2 = self.svc.generate_plan(
            request_id="req-g2", actor_id="op", plan_key="d1", site_id="s1",
            horizon_start=T.format(8, 0), horizon_end=T.format(18, 0),
            demands=[{"team_code": "t-a", "template_code": "asm"}])
        self.svc.release_plan(request_id="req-rel2", actor_id="rev",
                              plan_version_id=plan2["plan_version_id"])
        self.assertEqual("revoked", self.svc.get_task(task_id)["state"])
        statuses = [r["status"] for r in self.svc.get_task(task_id)["reservations"]]
        self.assertTrue(statuses)
        self.assertTrue(all(s == "revoked" for s in statuses))

    def test_concurrent_releases_cannot_double_book(self):
        self.seed()
        self.seed(team_code="t-b")
        # 两支队伍都把同一唯一资源排在首选 8:00。
        plans = []
        for key, team in (("c1", "t-a"), ("c2", "t-b")):
            plans.append(self.svc.generate_plan(
                request_id="req-g-" + key, actor_id="op", plan_key=key, site_id="s1",
                horizon_start=T.format(8, 0), horizon_end=T.format(18, 0),
                demands=[{"team_code": team, "template_code": "asm"}]))
        results = [None, None]

        def release(index, version_id):
            try:
                results[index] = self.svc.release_plan(
                    request_id=f"req-rel-c{index}", actor_id="rev", plan_version_id=version_id)
            except Exception as exc:  # noqa: BLE001 - 记录到结果
                results[index] = exc

        threads = [threading.Thread(target=release, args=(i, p["plan_version_id"]))
                   for i, p in enumerate(plans)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        successes = [r for r in results if not isinstance(r, Exception)]
        conflicts = [r for r in results if isinstance(r, ConflictError)]
        self.assertEqual(1, len(successes), f"应恰好一个放行成功：{results}")
        self.assertEqual(1, len(conflicts))
        # 不存在同一资源在同一重叠时段的两条 held 预占。
        rows = self.database.connection.execute(
            "SELECT resource_type, resource_id, COUNT(*) c FROM resource_reservations "
            "WHERE status='held' GROUP BY resource_type, resource_id, starts_at, ends_at HAVING c > 1"
        ).fetchall()
        self.assertEqual([], [dict(r) for r in rows])


class EventMergeTests(PlanningTestBase):
    def _started_task(self):
        self.seed()
        _, rel = self.generate_and_release()
        task_id = rel["task_ids"][0]
        self.svc.ingest_task_event(request_id="req-ev-s", actor_id="op", event_id="ev-s",
                                   task_id=task_id, event_type="started",
                                   occurred_at=T.format(8, 0), source="gcs")
        return task_id

    def test_out_of_order_events_rebuild_single_process(self):
        task_id = self._started_task()
        # 先收到 9:30 的结束，再补 8:30 暂停、8:45 恢复（乱序到达）。
        self.svc.ingest_task_event(request_id="req-ev-e", actor_id="op", event_id="ev-e",
                                   task_id=task_id, event_type="ended",
                                   occurred_at=T.format(9, 30), source="gcs")
        self.svc.ingest_task_event(request_id="req-ev-p", actor_id="op", event_id="ev-p",
                                   task_id=task_id, event_type="paused",
                                   occurred_at=T.format(8, 30), source="gcs")
        self.svc.ingest_task_event(request_id="req-ev-r", actor_id="op", event_id="ev-r",
                                   task_id=task_id, event_type="resumed",
                                   occurred_at=T.format(8, 45), source="gcs")
        task = self.svc.get_task(task_id)
        triggers = [t["trigger"] for t in task["transitions"]]
        self.assertEqual(["started", "paused", "resumed", "ended"], triggers)
        self.assertEqual("ended", task["state"])

    def test_duplicate_event_id_is_idempotent_and_kept(self):
        task_id = self._started_task()
        args = dict(actor_id="op", event_id="ev-dup", task_id=task_id, event_type="paused",
                    occurred_at=T.format(8, 20), source="gcs")
        first = self.svc.ingest_task_event(request_id="req-d1", **args)
        second = self.svc.ingest_task_event(request_id="req-d2", **args)
        self.assertTrue(first["applied"])
        self.assertTrue(second["replayed"])
        raw = self.database.connection.execute(
            "SELECT COUNT(*) c FROM task_raw_events WHERE event_id='ev-dup'").fetchone()["c"]
        self.assertEqual(1, raw)

    def test_duplicate_event_with_new_id_does_not_double_transition(self):
        task_id = self._started_task()
        self.svc.ingest_task_event(request_id="req-a", actor_id="op", event_id="ev-a",
                                   task_id=task_id, event_type="started",
                                   occurred_at=T.format(8, 0), source="relay")
        task = self.svc.get_task(task_id)
        self.assertEqual(1, len(task["transitions"]))
        duplicate = next(r for r in task["raw_events"] if r["event_id"] == "ev-a")
        self.assertTrue(duplicate["accepted"])
        self.assertIsNotNone(duplicate["reject_reason"])

    def test_event_after_terminal_state_is_rejected_but_retained(self):
        task_id = self._started_task()
        self.svc.ingest_task_event(request_id="req-e", actor_id="op", event_id="ev-e",
                                   task_id=task_id, event_type="ended",
                                   occurred_at=T.format(9, 0), source="gcs")
        late = self.svc.ingest_task_event(request_id="req-late", actor_id="op", event_id="ev-late",
                                          task_id=task_id, event_type="paused",
                                          occurred_at=T.format(9, 10), source="gcs")
        self.assertFalse(late["applied"])
        raw = next(r for r in self.svc.get_task(task_id)["raw_events"] if r["event_id"] == "ev-late")
        self.assertFalse(raw["accepted"])
        self.assertIn("终态", raw["reject_reason"])


class EscalationTests(PlanningTestBase):
    def test_escalation_revokes_unstarted_and_manuals_in_progress(self):
        self.seed()
        # 任务 A：尚未开始；任务 B：进行中（第二支队伍顺延至 9:00）。
        _, rel_a = self.generate_and_release(plan_key="d1")
        self.seed(team_code="t-b")
        plan_b = self.svc.generate_plan(
            request_id="req-gb", actor_id="op", plan_key="db", site_id="s1",
            horizon_start=T.format(8, 0), horizon_end=T.format(18, 0),
            demands=[{"team_code": "t-b", "template_code": "asm"}])
        rel_b = self.svc.release_plan(request_id="req-relb", actor_id="rev",
                                      plan_version_id=plan_b["plan_version_id"])
        task_b = rel_b["task_ids"][0]
        self.advance_clock(9, 0)
        self.svc.ingest_task_event(request_id="req-sb", actor_id="op", event_id="ev-sb",
                                   task_id=task_b, event_type="started",
                                   occurred_at=T.format(9, 0), source="gcs")
        self.advance_clock(9, 5)

        win = self.svc.register_restriction(
            request_id="req-win", actor_id="op", site_id="s1", scope="site", resource_id="s1",
            starts_at=T.format(8, 5), ends_at=T.format(10, 0), reason="空域升级", level="escalated")
        actions = {a["task_id"]: a["action"] for a in win["affected_items"]}
        self.assertEqual("revoked", actions[rel_a["task_ids"][0]])
        self.assertEqual("manual", actions[task_b])

        # 未开始任务：预占已释放（revoked）。
        a_info = self.svc.get_task(rel_a["task_ids"][0])
        self.assertTrue(all(r["status"] == "revoked" for r in a_info["reservations"]))
        # 进行中任务：预占保留，等待人工处置。
        b_info = self.svc.get_task(task_b)
        self.assertEqual("manual", b_info["state"])
        self.assertTrue(all(r["status"] == "held" for r in b_info["reservations"]))

    def test_manual_resolve_releases_holds(self):
        self.seed()
        _, rel = self.generate_and_release()
        task_id = rel["task_ids"][0]
        self.advance_clock(8, 0)
        self.svc.ingest_task_event(request_id="req-s", actor_id="op", event_id="ev-s",
                                   task_id=task_id, event_type="started",
                                   occurred_at=T.format(8, 0), source="gcs")
        self.advance_clock(8, 30)
        self.svc.register_restriction(
            request_id="req-win", actor_id="op", site_id="s1", scope="site", resource_id="s1",
            starts_at=T.format(8, 30), ends_at=T.format(9, 30), reason="管制", level="escalated")
        self.advance_clock(8, 35)
        self.svc.manual_resolve_task(request_id="req-mr", actor_id="rev", task_id=task_id,
                                     resolution="aborted", reason="安全回收")
        info = self.svc.get_task(task_id)
        self.assertEqual("aborted", info["state"])
        self.assertTrue(all(r["status"] == "released" for r in info["reservations"]))

    def test_only_reviewer_can_manual_resolve(self):
        self.seed()
        _, rel = self.generate_and_release()
        task_id = rel["task_ids"][0]
        self.svc.ingest_task_event(request_id="req-s", actor_id="op", event_id="ev-s",
                                   task_id=task_id, event_type="anomaly",
                                   occurred_at=T.format(8, 10), source="gcs")
        with self.assertRaises(PermissionDenied):
            self.svc.manual_resolve_task(request_id="req-no", actor_id="op", task_id=task_id,
                                         resolution="ended", reason="x")


class ReviewTests(PlanningTestBase):
    def test_pending_reviews_completed_and_resumable(self):
        self.seed()
        _, rel = self.generate_and_release()
        task_id = rel["task_ids"][0]
        self.svc.ingest_telemetry_summary(request_id="req-t1", actor_id="op", task_id=task_id,
                                          summary_id="sum-1", metrics={"flight_minutes": 20})
        self.svc.ingest_telemetry_summary(request_id="req-t2", actor_id="op", task_id=task_id,
                                          summary_id="sum-2", metrics={"flight_minutes": 12})
        self.assertEqual(2, self.svc.pending_review_count())
        out = self.svc.run_pending_reviews(1)
        self.assertEqual(1, out["processed"])
        rest = self.svc.resume_pending_work()
        self.assertEqual(1, rest["processed"])
        self.assertEqual(0, self.svc.pending_review_count())
        review = self.svc.get_task(task_id)["review"]
        self.assertEqual("completed", review["status"])

    def test_review_state_survives_restart(self):
        import tempfile
        from pathlib import Path
        from skills_workspace.storage import Database as DB
        from skills_workspace.planning import TrainingService as TS
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "r.sqlite3"
            db = DB(path)
            _, svc = bootstrap_world(db, self.clock)
            old_svc, old_db = self.svc, self.database
            self.svc, self.database = svc, db
            task_id = None
            try:
                self.seed()
                _, rel = self.generate_and_release()
                task_id = rel["task_ids"][0]
                svc.ingest_telemetry_summary(request_id="req-t", actor_id="op", task_id=task_id,
                                             summary_id="sum-1", metrics={"ok": True})
            finally:
                self.svc, self.database = old_svc, old_db
            db.close()
            db2 = DB(path)
            svc2 = TS(db2, self.clock)
            resumed = svc2.resume_pending_work()
            self.assertEqual(1, resumed["found_on_startup"])
            self.assertEqual("completed", svc2.get_task(task_id)["review"]["status"])
            db2.close()


if __name__ == "__main__":
    unittest.main()
