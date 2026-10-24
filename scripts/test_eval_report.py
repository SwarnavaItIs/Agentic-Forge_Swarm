import copy
import unittest

from eval_report import aggregate


def fixture():
    budget = {"model_calls": 12, "tool_calls": 24, "reserved_token_ceiling": 200000, "wall_seconds": 90, "max_output_tokens": 1024}
    runs = [{"RunID": str(i), "TaskID": "add", "ConfigID": "developer", "Trial": i} for i in range(4)]
    manifest = {"mode": "fixture", "runs": runs, "catalog": [{"id": "add", "base_commit": "base"}], "catalog_sha256": "catalog", "budget": budget}
    records = []
    for i, outcome in enumerate(["passed", "task_failure", "timeout", "infrastructure_failure"]):
        records.append({"run_id": str(i), "task_id": "add", "config_id": "developer", "trial": i, "outcome": outcome, "base_commit": "base", "catalog_sha256": "catalog", "budget": budget, "model_calls_attempted": 5, "reserved_tokens": 50000, "candidate_sha256": "candidate", "protected_test_result": {"status": "passed", "candidate_sha256": "candidate", "base_commit": "base", "sha256": "proof"}, "policy_violations": [], "duration_seconds": i+1, "input_tokens": None, "output_tokens": None, "estimated_cost_microusd": None, "reviews": 0, "review_rejections": 0, "operator_interventions": 0})
    return manifest, records


class ReportTests(unittest.TestCase):
    def test_failures_and_unknown_usage_stay_in_denominator(self):
        manifest, records = fixture()
        summary = aggregate(manifest, records)
        actual = summary["configurations"]["developer"]
        self.assertEqual((actual["successes"], actual["intended_runs"]), (1, 4))
        self.assertEqual(actual["task_trial_pass_rate"], .25)
        self.assertEqual(actual["unknown_cost_runs"], 4)
        self.assertIsNone(actual["estimated_cost_microusd"]["p50"])
        self.assertEqual(actual["wall_seconds_all_observed_outcomes"]["samples"], 4)
        self.assertEqual(summary, aggregate(copy.deepcopy(manifest), copy.deepcopy(records)))

    def test_omitted_record_is_a_failure_with_integrity_error(self):
        manifest, records = fixture()
        summary = aggregate(manifest, records[:-1])
        self.assertEqual(summary["observed_runs"], 3)
        self.assertEqual(summary["intended_runs"], 4)
        self.assertEqual(len(summary["integrity_errors"]), 1)
        self.assertEqual(summary["configurations"]["developer"]["outcomes"]["omitted_result"], 1)

    def test_duplicate_wrong_base_budget_and_fake_pass_are_rejected(self):
        manifest, records = fixture()
        with self.assertRaises(ValueError):
            aggregate(manifest, records+records[:1])
        for key, value in [("base_commit", "wrong"), ("catalog_sha256", "wrong"), ("reserved_tokens", 200001)]:
            changed = copy.deepcopy(records)
            changed[0][key] = value
            with self.assertRaises(ValueError):
                aggregate(manifest, changed)
        changed = copy.deepcopy(records)
        changed[0]["protected_test_result"]["candidate_sha256"] = "stale"
        with self.assertRaises(ValueError):
            aggregate(manifest, changed)


if __name__ == "__main__":
    unittest.main()
