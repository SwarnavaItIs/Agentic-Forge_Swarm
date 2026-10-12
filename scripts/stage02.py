"""Stage-02 acceptance, real disposable PostgreSQL and scheduler processes.

No production completion endpoint. All terminal-state seeds below are test-only,
against random labelled resources owned by this invocation. No development data
is deleted. Sanitized evidence has the existing bounded/latest-run policy.
"""
import json
import re
import subprocess
import sys
import time
import uuid

import dev

ROOT=dev.ROOT
EVIDENCE=ROOT / "artifacts/verification/stage-02"


def progress(status,failure=None):
    path=ROOT / ".forgeswarm/progress.json"
    document=json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"schema_version":1,"stages":{}}
    if document.get("schema_version")!=1:
        raise RuntimeError("unsupported progress schema; refusing to overwrite")
    revision=subprocess.run(["git","rev-parse","HEAD"],cwd=ROOT,capture_output=True,text=True)
    dirty=subprocess.run(["git","status","--porcelain","--","."],cwd=ROOT,capture_output=True,text=True)
    stage=document.setdefault("stages",{}).setdefault("02",{})
    stage.update({"stage_number":2,"title":"Persisted DAG scheduler","status":status,"updated_at":dev.timestamp(),
        "implemented_capabilities":["validated immutable DAG and bounded graph snapshot API","normalization-v2 and frozen legacy-v1 retry compatibility",
            "additive composite-FK migration and serialized event allocator","bounded database-locked reconciliation and scan rotation",
            "failure propagation with independent branches and terminal aggregation","scheduler process with contexts, shutdown and sanitized logs",
            "deterministic graph/property tests and real PostgreSQL concurrency/upgrade tests","real scheduler kill/restart, diamond fixture and outage verification"],
        "acceptance_criteria":{key:("passed" if status=="verified_local" else "unverified") for key in [
            "legacy_api_create_cancel_replay_and_v1_hash","graph_validation_limits_and_property_reference","atomic_durable_graph_and_cross_workflow_FKs",
            "immutable_finalized_definitions","consistent_bounded_graph_snapshot","two_schedulers_one_logical_transition","idempotent_reconciliation_and_stable_history",
            "failure_propagation_independent_progress_and_aggregation","cancellation_scheduling_serialized_races","bounded_scan_rotation_skip_locked",
            "stage01_database_migration_upgrade","scheduler_process_kill_restart_and_graceful_shutdown","db_outage_recovery",
            "format_python_vet_unit_race_build_real_db_and_smoke"]},
        "blockers":[dev.sanitize(failure)] if failure else [],
        "external_verification":{"live_models":"not_applicable","broker_or_workers":"not_implemented_until_stage_03","cloud":"not_requested","performance":"not_measured"},
        "decisions":[
            "Located actual ForgeSwarm checkout; ReForge workspace and existing changes left untouched",
            "Stage 01 source was uncommitted with no HEAD; full acceptance rerun passed before implementation",
            "Dependency order semantically irrelevant; duplicates invalid; display order preserved. Kahn validation O(V+E), sorting only for normalization",
            "New creations store normalization v2; explicit empty dependencies match legacy v1, nonempty edges conflict with legacy keys",
            "No applied migration edited; migration 00002 backfills event sequence while parent DDL lock excludes old history writers",
            "Upgrade API after migration before starting scheduler; old binary cancellation cannot allocate the new history sequence",
            "Definitions and edges finalize atomically with workflow.created and are guarded against later SQL mutation",
            "Parent locks before child states/history; version increments once per mutation, sequence once per event; no runtime MAX(sequence)+1",
            "Continue independent branches; failure terminal only when unfinished/runnable/running work is terminal; READY never counts as success",
            "Per-workflow serialization and bounded batch locks trade throughput for consistency; SKIP LOCKED has no strict fairness under persistent contention",
            "Oldest persisted scheduler_checked_at rotates no-op scans without mutating public state/revision/timestamps",
            "FAILED/BLOCKED and individually CANCELLED predecessors are unsuccessful and block unfinished descendants; whole-workflow cancellation wins by transaction order",
            "No production terminal setter, claim/executor/broker/LLM/dynamic DAG. RUNNING is only a state-machine boundary for Stage 03",
            "Stage-01 assumptions of schema_version=1 for every new record and version=event sequence superseded explicitly; legacy records/hashes remain unchanged",
            "Database and process tests use only random owned disposable containers and test-only terminal seeds; race detector is not DB-correctness proof",
            "No extra architecture Markdown or AGENTS files created; interim decisions live in this JSON, code and make help"],
        "sources":["internal/workflow/graph.go:ValidateGraph,Reconcile,TaskTransition","internal/workflow/workflow.go:HashForVersion,Cancellation",
            "migrations/00002_dag_scheduler.sql","internal/postgres/store.go:Create,Cancel","internal/postgres/scheduler.go:Graph,Reconcile,ReconcileBatch",
            "internal/httpapi/api.go:graph,decodeCreate","internal/scheduler/scheduler.go:Run","cmd/scheduler/main.go:run","internal/config/config.go:Parse",
            "tests/integration/scheduler_test.go","tests/fixtures/diamond.json","scripts/stage02.py","Makefile",".github/workflows/stage-02.yaml"],
        "validation_commands":list(dev.COMMANDS),
        "git":{"revision":revision.stdout.strip() if revision.returncode==0 else None,"dirty":bool(dirty.stdout.strip()),"scope":"."},
        "fingerprint":{"algorithm":"sha256","value":dev.fingerprint(),"excludes":[".forgeswarm","artifacts/verification",".cache","bin","__pycache__"]},
        "reference_docs":["https://www.postgresql.org/docs/17/sql-select.html","https://www.postgresql.org/docs/17/ddl-constraints.html",
            "https://www.postgresql.org/docs/17/transaction-iso.html","https://pkg.go.dev/github.com/jackc/pgx/v5"],
        "next_stage":3 if status=="verified_local" else 2})
    path.parent.mkdir(exist_ok=True)
    tmp=path.with_suffix(".tmp");tmp.write_text(json.dumps(document,indent=2)+"\n",encoding="utf-8");tmp.replace(path)
    EVIDENCE.mkdir(parents=True,exist_ok=True)
    (EVIDENCE / "results.json").write_text(json.dumps({"status":status,"failure":dev.sanitize(failure) if failure else None,"commands":dev.COMMANDS},indent=2)+"\n",encoding="utf-8")


def smoke():
    started=dev.timestamp()
    with dev.database() as (env,db,is_ready):
        dev.run(["go","run","./cmd/migrate","up"],env=env,label="dag-migrate")
        for name in ["api","scheduler"]:
            dev.run(["go","build","-o",ROOT / f"bin/{name}-linux",f"./cmd/{name}"],env=dev.ENV | {"GOOS":"linux","GOARCH":"amd64","CGO_ENABLED":"0"},label=f"dag-build-{name}")
        processes=[]
        token=env["TEST_RESOURCE_TOKEN"]
        url=re.sub(r"127\.0\.0\.1:\d+/","db:5432/",env["DATABASE_URL"])
        base=None
        def start(name):
            container=f"forgeswarm-{name}-"+uuid.uuid4().hex
            args=["docker","run","-d","--name",container,"--label",f"forgeswarm.verification={token}","--platform","linux/amd64",
                  "--network",env["TEST_DOCKER_NETWORK"],"--mount",f"type=bind,source={ROOT / 'bin'},target=/app,readonly",
                  "-e",f"DATABASE_URL={url}","-e","SCHEDULER_INTERVAL=100ms","-e","SCHEDULER_BATCH_SIZE=2","-e","SCHEDULER_SCAN_TIMEOUT=1s"]
            if name=="api":args += ["-p","127.0.0.1::8080","-e","HTTP_ADDR=0.0.0.0:8080"]
            dev.run([*args,dev.GO_IMAGE,f"/app/{name}-linux"],label=f"dag-start-{name}")
            processes.append(container)
            return container
        def remove(container):
            code,owner=dev.run(["docker","inspect","--format",'{{ index .Config.Labels "forgeswarm.verification" }}',container],check=False,quiet=True,label="dag-ownership")
            if code==0 and owner.strip()==token:
                dev.run(["docker","rm","-f",container],label="dag-cleanup")
        def stop(container,killed=False):
            if killed:
                dev.run(["docker","kill","--signal","KILL",container],label="scheduler-kill")
            else:
                dev.run(["docker","stop","--timeout","10",container],label="dag-graceful-stop")
            _,exit_code=dev.run(["docker","inspect","--format","{{.State.ExitCode}}",container],quiet=True,label="dag-exit")
            _,logs=dev.run(["docker","logs",container],quiet=True,label="dag-process-log")
            if not killed:
                assert exit_code.strip()=="0", "graceful process failure"
                if "scheduler" in container:assert '"msg":"scheduler_drained"' in logs
            else:
                assert exit_code.strip()=="137", "scheduler not actually killed"
            remove(container);processes.remove(container)
        def get_graph(receipt):
            code,graph,_=dev.http(base,"/v1/workflows/"+receipt["id"]+"/graph")
            assert code==200
            return graph
        def states(receipt):return {n["key"]:n["status"] for n in get_graph(receipt)["nodes"]}
        def events(receipt):
            code,page,_=dev.http(base,"/v1/workflows/"+receipt["id"]+"/events?limit=100")
            assert code==200
            assert [e["sequence"] for e in page["events"]]==list(range(1,len(page["events"])+1))
            return page["events"]
        def seed(receipt,key,status):
            # Fixed allowed fixture keys/states and canonical server UUIDs only.
            workflow_id=str(uuid.UUID(receipt["id"]))
            assert key in {"a","b","c","d","independent"} and status in {"SUCCEEDED","FAILED"}
            # Assert resource ownership again before the explicit test-only write.
            code,owner=dev.run(["docker","inspect","--format",'{{ index .Config.Labels "forgeswarm.verification" }}',db],quiet=True,label="seed-ownership")
            assert code==0 and owner.strip()==token
            sql=f"BEGIN; SELECT id FROM workflows WHERE id='{workflow_id}' FOR UPDATE; UPDATE tasks SET status='{status}',updated_at=clock_timestamp() WHERE workflow_id='{workflow_id}' AND task_key='{key}'; COMMIT;"
            dev.run(["docker","exec",db,"psql","-v","ON_ERROR_STOP=1","-U","forgeswarm","-d",f"forgeswarm_stage01_{token}","-c",sql],quiet=True,label="test-only-terminal-seed")
        phases=[]
        def phase(name,receipt):phases.append({"fixture":"TEST-ONLY generated terminal scenarios","phase":name,"nodes":states(receipt),"workflow_status":get_graph(receipt)["status"],"history_count":len(events(receipt))})
        try:
            api=start("api")
            _,published=dev.run(["docker","port",api,"8080/tcp"],quiet=True,label="dag-api-port")
            base="http://127.0.0.1:"+published.strip().rsplit(":",1)[1]
            def api_ready():
                try:return dev.http(base,"/health/ready")[0]==200
                except OSError:return False
            dev.poll(api_ready,"DAG API readiness")
            fixture=json.loads((ROOT / "tests/fixtures/diamond.json").read_text(encoding="utf-8"))
            body=json.dumps(fixture).encode()
            code,receipt,_=dev.http(base,"/v1/workflows","POST",body,"dag-smoke")
            assert code==201 and set(receipt)=={"id","created_at"}
            workers=[start("scheduler"),start("scheduler")]
            dev.poll(lambda:states(receipt)=={"a":"READY","b":"PENDING","c":"PENDING","d":"PENDING"},"A only READY")
            assert len(events(receipt))==2,"two schedulers duplicated A readiness"
            phase("A only READY, two processes",receipt)
            for worker in workers:stop(worker,killed=True)
            seed(receipt,"a","SUCCEEDED")
            workers=[start("scheduler"),start("scheduler")]
            dev.poll(lambda:states(receipt)=={"a":"SUCCEEDED","b":"READY","c":"READY","d":"PENDING"},"B/C recovered after killed schedulers")
            assert len(events(receipt))==4
            phase("seeded A success, restart reconstructs B/C eligibility",receipt)
            stop(workers.pop())
            seed(receipt,"b","SUCCEEDED")
            # Give the surviving scheduler multiple scans; D must still wait.
            time.sleep(0.4)
            assert states(receipt)["d"]=="PENDING" and len(events(receipt))==4
            phase("B only succeeded, D remains PENDING",receipt)
            seed(receipt,"c","SUCCEEDED")
            dev.poll(lambda:states(receipt)["d"]=="READY","D fan-in eligibility")
            phase("B/C succeeded, D READY",receipt)
            seed(receipt,"d","SUCCEEDED")
            dev.poll(lambda:get_graph(receipt)["status"]=="SUCCEEDED","workflow success aggregation")
            assert len(events(receipt))==6
            phase("all tasks seeded success, workflow SUCCEEDED",receipt)
            fixture["tasks"][3]["depends_on"]=["c","b"]
            assert dev.http(base,"/v1/workflows","POST",json.dumps(fixture).encode(),"dag-smoke")[1]==receipt
            failure=json.loads(body);failure["tasks"].append({"key":"independent","kind":"noop"})
            code,failed,_=dev.http(base,"/v1/workflows","POST",json.dumps(failure).encode(),"failed-branch")
            assert code==201
            dev.poll(lambda:states(failed)["independent"]=="READY" and states(failed)["a"]=="READY","independent roots")
            seed(failed,"a","FAILED")
            dev.poll(lambda:states(failed)["d"]=="BLOCKED","failed ancestor propagation")
            assert get_graph(failed)["status"]=="PENDING" and states(failed)["independent"]=="READY"
            phase("failed ancestor blocks descendants, independent branch stays READY",failed)
            seed(failed,"independent","SUCCEEDED")
            dev.poll(lambda:get_graph(failed)["status"]=="FAILED","failure terminal after independent branch")
            phase("independent branch terminal, workflow FAILED",failed)
            dev.run(["docker","stop","--timeout","5",db],label="scheduler-db-outage")
            time.sleep(0.5)
            _,logs=dev.run(["docker","logs",workers[0]],quiet=True,label="scheduler-outage-log")
            assert '"msg":"scheduler_scan_failed"' in logs,"scheduler did not report DB outage"
            dev.run(["docker","start",db],label="scheduler-db-recovery")
            dev.poll(is_ready,"PostgreSQL restart")
            dev.poll(api_ready,"API after DB recovery")
            code,recovered,_=dev.http(base,"/v1/workflows","POST",body,"post-outage")
            assert code==201
            dev.poll(lambda:states(recovered)["a"]=="READY","scheduler continues after DB outage")
            phase("scheduler recovered from database outage",recovered)
            stop(workers.pop())
            stop(api)
            evidence=EVIDENCE / "scheduler-process-results.json"
            evidence.write_text(json.dumps({"status":"passed","classification":"real local PostgreSQL/process checks; terminal values are test-only seeds","phases":phases},indent=2)+"\n",encoding="utf-8")
            dev.COMMANDS.append({"command":"real scheduler process diamond / SIGKILL restart / graceful drain / outage assertions", "started_at":started,"finished_at":dev.timestamp(),"exit":0,"required":True,"evidence_path":evidence.relative_to(ROOT).as_posix()})
            print("stage 02 process smoke: two schedulers, diamond, SIGKILL/restart, fan-in, independent failure, DB recovery and graceful shutdown passed",flush=True)
        finally:
            for process in list(processes):remove(process)


def verify():
    if not __debug__:raise RuntimeError("verification requires assertions")
    if not EVIDENCE.resolve().is_relative_to(ROOT.resolve()):raise RuntimeError("evidence is outside project")
    EVIDENCE.mkdir(parents=True,exist_ok=True)
    for old in [*EVIDENCE.glob("[0-9][0-9][0-9]-*.log"),EVIDENCE / "smoke-api.log",EVIDENCE / "scheduler-process-results.json",EVIDENCE / "results.json"]:
        if old.is_file() and not old.is_symlink() and old.resolve().is_relative_to(EVIDENCE.resolve()):old.unlink()
    dev.EVIDENCE=EVIDENCE;dev.RECORD=True;dev.COMMANDS.clear()
    progress("in_progress")
    try:
        dev.fmt_check()
        dev.run([sys.executable,"-m","py_compile","scripts/dev.py","scripts/stage02.py"],label="python")
        dev.run(["go","vet","./..."],label="vet")
        dev.run(["go","test","-count=1","./..."],label="unit")
        dev.race(["-count=1","./..."])
        dev.build()
        dev.run(["docker","compose","config","--quiet"],label="compose")
        dev.integration()
        dev.smoke()
        smoke()
        dev.run(["git","diff","--check"],label="diff-check")
    except (RuntimeError,AssertionError,OSError,ValueError) as error:
        progress("implemented_unverified",str(error));raise
    progress("verified_local")
    print("stage 02 verified_local; see .forgeswarm/progress.json and artifacts/verification/stage-02/results.json",flush=True)
