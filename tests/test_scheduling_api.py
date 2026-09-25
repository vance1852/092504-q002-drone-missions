import unittest
from datetime import datetime, timezone

from skills_workspace.api import scheduling_route
from skills_workspace.clock import FixedClock
from skills_workspace.scheduling import SchedulingService
from skills_workspace.service import DomainService
from skills_workspace.storage import Database

CLOCK = FixedClock(datetime(2026, 9, 25, 8, 0, tzinfo=timezone.utc))


class SchedulingApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.domain = DomainService(self.database, CLOCK)
        self.service = SchedulingService(self.database, CLOCK, self.domain)
        self.domain.register_organization(request_id="org", actor_id="bootstrap",
                                          organization_id="o1", name="无人机训练基地")
        self.domain.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="a1",
                                   display_name="管理员", role="admin", organization_id="o1")
        self.domain.register_site(request_id="site", actor_id="a1", site_id="s1",
                                  organization_id="o1", name="一号基地",
                                  timezone_name="Asia/Shanghai")

    def tearDown(self):
        self.database.close()

    def _post(self, path, body, actor="a1"):
        return scheduling_route(self.service, "POST", path, body, {"X-Actor-Id": actor})

    def test_create_template_returns_201_then_replays(self):
        body = {"request_id": "tpl-1", "site_id": "s1", "template_id": "tpl-1",
                "name": "装调训练", "skill_type": "assembly", "duration_minutes": 60,
                "required_qualification": "uav-assembly", "area_type": "shop"}
        status, payload = self._post("/scheduling/templates", body)
        self.assertEqual(201, status)
        self.assertFalse(payload["replayed"])
        status, payload = self._post("/scheduling/templates", body)
        self.assertEqual(200, status)
        self.assertTrue(payload["replayed"])

    def test_unknown_scheduling_route_returns_404(self):
        status, payload = scheduling_route(self.service, "GET", "/scheduling/missing", None)
        self.assertEqual(404, status)
        self.assertEqual("route_not_found", payload["error"])

    def test_missing_actor_is_denied(self):
        status, payload = self._post("/scheduling/templates",
                                     {"request_id": "tpl-1", "site_id": "s1",
                                      "template_id": "tpl-1", "name": "装调训练",
                                      "skill_type": "assembly", "duration_minutes": 60,
                                      "required_qualification": "uav-assembly",
                                      "area_type": "shop"}, actor="")
        self.assertEqual(404, status)
        self.assertEqual("not_found", payload["error"])

    def test_get_plan_requires_plan_id(self):
        status, payload = scheduling_route(self.service, "GET", "/scheduling/plan", None)
        self.assertEqual(400, status)
        self.assertEqual("validation_error", payload["error"])

    def test_invalid_body_shape_returns_400(self):
        status, payload = self._post("/scheduling/templates", {"request_id": "x"})
        self.assertEqual(400, status)
        self.assertEqual("invalid_request", payload["error"])

    def test_resources_listing(self):
        self._post("/scheduling/templates",
                   {"request_id": "tpl-1", "site_id": "s1", "template_id": "tpl-1",
                    "name": "装调训练", "skill_type": "assembly", "duration_minutes": 60,
                    "required_qualification": "uav-assembly", "area_type": "shop"})
        status, payload = scheduling_route(self.service, "GET",
                                           "/scheduling/resources?site_id=s1", None)
        self.assertEqual(200, status)
        self.assertEqual(1, len(payload["templates"]))
        self.assertEqual("tpl-1", payload["templates"][0]["template_id"])


if __name__ == "__main__":
    unittest.main()
