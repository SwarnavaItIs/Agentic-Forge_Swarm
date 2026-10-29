"""Disposable query-plan and migration evidence; never targets development DBs."""
import json
import statistics
from urllib.parse import urlparse

import dev


def query_plan():
    folder = dev.ROOT/"artifacts/verification/stage-10/query-plan"
    folder.mkdir(parents=True, exist_ok=True)
    with dev.database() as (env, container, ready):
        dev.run(["go", "run", "./cmd/migrate", "up"], env=env, label="query-fixture-migration")
        database = urlparse(env["TEST_DATABASE_URL"]).path.lstrip("/")
        def sql(statement):
            _, value = dev.run(["docker", "exec", container, "psql", "-X", "-U", "forgeswarm", "-d", database, "-At", "-v", "ON_ERROR_STOP=1", "-c", statement], quiet=True, label="disposable-query-fixture")
            return value.strip()
        sql("DROP INDEX IF EXISTS workflow_events_observed_at_idx")
        # Synthetic SQL query data, not accepted workflows or successful engine
        # executions. No scheduler/worker runs against this database.
        sql("INSERT INTO workflows(id,name,status,version,creation_key,request_hash,request_schema_version) SELECT gen_random_uuid(),'query-plan-only','CANCELLED',1,'query-plan-'||n,decode(repeat('00',32),'hex'),1 FROM generate_series(1,2000) n")
        sql("INSERT INTO workflow_events(workflow_id,sequence,event_type,payload,occurred_at) SELECT id,n,'workflow.created','{}',CASE WHEN creation_key='query-plan-1' THEN clock_timestamp() ELSE clock_timestamp()-interval '30 days' END FROM workflows CROSS JOIN generate_series(1,40) n")
        sql("VACUUM ANALYZE workflow_events")
        old = "SELECT count(*) FROM workflow_events WHERE occurred_at>clock_timestamp()-interval '24 hours'"
        new = "SELECT count(*) FROM workflow_events WHERE occurred_at>transaction_timestamp()-interval '24 hours'"
        result = {"schema_version": 1, "dataset": "80000 synthetic query-plan rows; 40 recent; no accepted/executed workflow claim", "sample_count_each": 15, "warmup_each": 3, "predeclared_goal": "same result; use observed-at index; reduce median query execution time by at least 20%", "started_at": dev.timestamp(), "phases": {}}
        for name, statement in [("before", old), ("index_only", old), ("stable_snapshot_and_index", new)]:
            if name == "index_only":
                sql("CREATE INDEX workflow_events_observed_at_idx ON workflow_events(occurred_at,workflow_id)")
                sql("ANALYZE workflow_events")
            samples, plans = [], []
            for trial in range(18):
                plan = json.loads(sql("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) "+statement))[0]
                if trial >= 3:
                    samples.append(plan["Execution Time"])
                    plans.append(plan)
            count = int(sql(statement))
            assert count == 40, "observation predicate changed the result"
            result["phases"][name] = {"query": statement, "matching_rows": count, "execution_ms": samples, "median_execution_ms": statistics.median(samples), "plans": plans}
        before = result["phases"]["before"]["median_execution_ms"]
        after = result["phases"]["stable_snapshot_and_index"]["median_execution_ms"]
        indexed = "workflow_events_observed_at_idx" in json.dumps(result["phases"]["stable_snapshot_and_index"]["plans"])
        result.update({"finished_at": dev.timestamp(), "goal_met": indexed and after <= .8*before, "observed_median_ratio_after_before": after/before, "limitation": "warm single-query microbenchmark; not end-to-end throughput or production capacity"})
        (folder/"comparison.json").write_text(json.dumps(result, indent=2)+"\n")
        print("Query plan comparison:", result["goal_met"], "before/after median ms", before, after, flush=True)
        return result
