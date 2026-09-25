import unittest
from datetime import datetime, timezone

from skills_workspace.api import route
from skills_workspace.clock import FixedClock
from skills_workspace.planning import TrainingService
from skills_workspace.service import DomainService
from skills_workspace.storage import Database

T = "2026-09-26T{:02d}:{:02d}:00+08:00"


class PlanningApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        clock = FixedClock(datetime(2026, 9, 26, 0, tzinfo=timezone.utc))
        self.dom = DomainService(self.database, clock)
        self.training = TrainingService(self.database, clock)
        self.dom.register_organization(request_id="req-org", actor_id="bootstrap",
                                       organization_id="o1", name="基地")
        self.dom.register_actor(request_id="req-admin", actor_id="bootstrap", new_actor_id="adm",
                                display_name="管理员", role="admin", organization_id="o1")
        self.dom.register_actor(request_id="req-rev", actor_id="adm", new_actor_id="rev",
                                display_name="值班", role="reviewer", organization_id="o1")
        self.dom.register_actor(request_id="req-op", actor_id="adm", new_actor_id="op",
                                display_name="操作", role="operator", organization_id="o1")
        self.dom.register_site(request_id="req-site", actor_id="adm", site_id="s1",
                               organization_id="o1", name="训练场", timezone_name="Asia/Shanghai")
        self.training.register_team(request_id="req-team", actor_id="op", site_id="s1",
                                    code="t-a", name="甲队")
        self.training.register_person(request_id="req-ins", actor_id="op", site_id="s1",
                                      code="ins-1", display_name="教官", kind="instructor",
                                      qualifications=["assembly"])
        self.training.register_person(request_id="req-stu", actor_id="op", site_id="s1",
                                      code="stu-1", display_name="学员", kind="trainee",
                                      qualifications=[])
        team_id = self.database.connection.execute(
            "SELECT team_id FROM teams WHERE code='t-a'").fetchone()["team_id"]
        self.training.add_team_members(request_id="req-tm", actor_id="op", team_id=team_id,
                                       person_codes=["stu-1"])
        self.training.register_template(request_id="req-tpl", actor_id="op", site_id="s1",
                                        code="asm", name="装调", task_type="assembly",
                                        duration_minutes=60, area_tag="field",
                                        equipment_types=["drone"], crew_size=1)
        self.training.register_area(request_id="req-area", actor_id="op", site_id="s1",
                                    code="a1", name="北区", tag="field")
        self.training.register_equipment(request_id="req-eq", actor_id="op", site_id="s1",
                                         code="u1", name="机1", equipment_type="drone")

    def tearDown(self):
        self.database.close()

    def call(self, method, path, body=None, actor="op"):
        return route(self.dom, method, path, body or {},
                     {"X-Actor-Id": actor}, self.training)

    def test_generate_release_and_query_explains_task(self):
        status, plan = self.call("POST", "/plans/generate", {
            "request_id": "req-gen", "plan_key": "d1", "site_id": "s1",
            "horizon_start": T.format(8, 0), "horizon_end": T.format(18, 0),
            "demands": [{"team_code": "t-a", "template_code": "asm"}]})
        self.assertIn(status, (200, 201))
        version_id = plan["plan_version_id"]

        status, released = self.call("POST", "/plans/release",
                                     {"request_id": "req-rel", "plan_version_id": version_id},
                                     actor="rev")
        self.assertIn(status, (200, 201), released)
        task_id = released["task_ids"][0]

        # 开始 → 结束
        self.call("POST", "/task-events", {"request_id": "req-s", "event_id": "ev-s",
                                           "task_id": task_id, "event_type": "started",
                                           "occurred_at": T.format(8, 0), "source": "gcs"})
        self.call("POST", "/task-events", {"request_id": "req-e", "event_id": "ev-e",
                                           "task_id": task_id, "event_type": "ended",
                                           "occurred_at": T.format(9, 0), "source": "gcs"})

        status, detail = self.call("GET", f"/tasks/{task_id}")
        self.assertEqual(200, status)
        self.assertEqual("ended", detail["state"])
        self.assertEqual("approved", detail["scheduling"]["decision"])
        self.assertTrue(any(r["code"] == "approved" for r in detail["scheduling"]["reasons"]))
        self.assertEqual(["started", "ended"], [t["trigger"] for t in detail["transitions"]])
        self.assertEqual(2, len(detail["raw_events"]))

    def test_operator_cannot_release_via_http(self):
        _, plan = self.call("POST", "/plans/generate", {
            "request_id": "req-gen2", "plan_key": "d2", "site_id": "s1",
            "horizon_start": T.format(8, 0), "horizon_end": T.format(18, 0),
            "demands": [{"team_code": "t-a", "template_code": "asm"}]})
        status, payload = self.call("POST", "/plans/release",
                                    {"request_id": "req-rel-no", "plan_version_id": plan["plan_version_id"]},
                                    actor="op")
        self.assertEqual(403, status)
        self.assertEqual("permission_denied", payload["error"])

    def test_telemetry_and_run_pending_review(self):
        _, plan = self.call("POST", "/plans/generate", {
            "request_id": "req-gen3", "plan_key": "d3", "site_id": "s1",
            "horizon_start": T.format(8, 0), "horizon_end": T.format(18, 0),
            "demands": [{"team_code": "t-a", "template_code": "asm"}]})
        _, released = self.call("POST", "/plans/release",
                                {"request_id": "req-rel3", "plan_version_id": plan["plan_version_id"]},
                                actor="rev")
        task_id = released["task_ids"][0]
        status, _ = self.call("POST", "/telemetry-summaries",
                              {"request_id": "req-tel", "task_id": task_id, "summary_id": "sum-1",
                               "metrics": {"flight_minutes": 30}})
        self.assertIn(status, (200, 201))
        status, run = self.call("POST", "/reviews/run-pending", {"limit": 10})
        self.assertEqual(200, status)
        self.assertEqual(1, run["processed"])

    def test_bad_event_type_returns_400(self):
        _, plan = self.call("POST", "/plans/generate", {
            "request_id": "req-gen4", "plan_key": "d4", "site_id": "s1",
            "horizon_start": T.format(8, 0), "horizon_end": T.format(18, 0),
            "demands": [{"team_code": "t-a", "template_code": "asm"}]})
        _, released = self.call("POST", "/plans/release",
                                {"request_id": "req-rel4", "plan_version_id": plan["plan_version_id"]},
                                actor="rev")
        status, payload = self.call("POST", "/task-events",
                                    {"request_id": "req-bad", "event_id": "ev-x",
                                     "task_id": released["task_ids"][0], "event_type": "exploded",
                                     "occurred_at": T.format(8, 0), "source": "gcs"})
        self.assertEqual(400, status)
        self.assertEqual("validation_error", payload["error"])


if __name__ == "__main__":
    unittest.main()
