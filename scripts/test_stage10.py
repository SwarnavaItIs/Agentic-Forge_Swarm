"""Pure policy/measurement regressions; service checks remain in the real gate."""
import importlib.util
import json
from pathlib import Path
import unittest

import dev
import performance
import live_demo
import local_platform
import tempfile
from unittest.mock import patch


class FinalPolicyTests(unittest.TestCase):
    def test_db_observation_follows_restore_without_touching_original(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)
            secret = path/"database-url"
            secret.write_text("postgres://private@db/forgeswarm_restore_123456abcdef?sslmode=verify-full")
            session = object.__new__(local_platform.Session)
            session.assets = path
            calls = []
            session.run = lambda *args, **kwargs: (calls.append(args) or (0, "observed"))
            self.assertEqual(session.sql("SELECT 1"), "observed")
            self.assertEqual(calls[-1][calls[-1].index("-d")+1], "forgeswarm_restore_123456abcdef")
            session.sql("SELECT 1", "forgeswarm")
            self.assertEqual(calls[-1][calls[-1].index("-d")+1], "forgeswarm")
            secret.write_text("postgres://private@db/unowned")
            with self.assertRaises(ValueError):
                session.sql("SELECT 1")
    def test_live_demo_default_and_repository_budget_guards(self):
        example = json.loads((dev.ROOT/"config/live-demo-authorization.example.json").read_text())
        with self.assertRaises(ValueError):
            live_demo.validate(example, {}, {})
        import datetime
        repo = {"id": 42, "owner": "owner", "name": "fixture", "target_branch": "main"}
        auth = {"session_authorized": True, "id": "a"*32, "expires_at": "2099-01-01T00:00:00Z", "repository": repo, "max_total_estimate_microusd": 500000, "max_project_estimate_microusd": 500000}
        github = {"repository": repo, "credential_mode": "installation_token"}
        self.assertEqual(live_demo.validate(auth, {"maximum_estimate_microusd": 500000}, github), 500000)
        with self.assertRaises(ValueError):
            live_demo.validate(auth, {"maximum_estimate_microusd": 500001}, github)
        with self.assertRaises(ValueError):
            live_demo.validate(auth, {"maximum_estimate_microusd": 500000}, github | {"credential_mode": "TEST_DEMO"})
    def test_histogram_counter_reset_is_an_error(self):
        def sample(value):
            return [{"metric": {"__name__": "forgeswarm_database_query_duration_seconds_count", "outcome": "ok"}, "value": [0, str(value)]}]
        with self.assertRaises(ValueError):
            performance.histogram_deltas(sample(10), sample(1))
        result = performance.histogram_deltas(sample(1), sample(10))
        self.assertEqual(result["database_query"]["count"], 9)

    def test_failed_admissions_remain_in_percentile_denominator(self):
        rows = [{"status": 201, "ms": 10}, {"status": 429, "ms": 1000}, {"status": 0, "ms": 2000}]
        self.assertEqual(performance.percentile([row["ms"] for row in rows], .5), 1000)

    def test_cloud_execution_secret_boundary(self):
        path = dev.ROOT/"deploy/aws/bootstrap.py"
        spec = importlib.util.spec_from_file_location("aws_bootstrap_test", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertFalse(module.EXECUTION & {"database-url", "operator-auth.json", "github.json", "worker-keys.json", "tls/ca-key.pem"})
        manifest = {"role": "execution", "owner": "a"*32, "release_sha256": "b"*64, "secret_arn": "arn:aws:secretsmanager:ap-south-1:123456789012:secret:execution-abcdef"}
        module.validate(manifest, "execution")
        with self.assertRaises(ValueError):
            module.validate(manifest | {"role": "control"}, "execution")
        with self.assertRaises(ValueError):
            module.validate(manifest | {"provider": "openai"}, "execution")


if __name__ == "__main__":
    unittest.main()
