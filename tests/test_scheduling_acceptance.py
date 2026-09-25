import unittest

from skills_workspace.scheduling_acceptance import run


class SchedulingAcceptanceTest(unittest.TestCase):
    def test_offline_acceptance(self):
        result = run()
        self.assertEqual("ok", result["status"])
        self.assertTrue(result["audit_valid"])
        self.assertGreater(result["candidates"], 0)
        self.assertTrue(result["candidate_explained"])
        self.assertGreaterEqual(result["release_holds"], 3)
        self.assertTrue(result["second_plan_avoids_hold"])
        self.assertTrue(result["rejections_explained"])
        self.assertTrue(result["events_merged"])
        self.assertTrue(result["revoked_on_escalation"])
        self.assertTrue(result["revoke_explained"])
        self.assertTrue(result["manual_case_opened"])
        self.assertTrue(result["manual_case_aborted"])
        self.assertGreaterEqual(result["pending_reviews_after_restart"], 2)
        self.assertTrue(result["review_completed_after_restart"])


if __name__ == "__main__":
    unittest.main()
