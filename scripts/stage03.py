"""Stage-03 verification owns random labelled DB/broker/process resources only."""
import contextlib
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
import uuid
import dev
import proto

IMAGE="nats:2.11.6-alpine@sha256:7dec3f8f1ff181975dbdfc0d903d2a9724659648294dbc2ebb2fa3a294d573a6"
EVIDENCE=dev.ROOT / "artifacts/verification/stage-03"
CRITERIA=["legacy_hashes_and_migration_compatibility","atomic_ready_generation_outbox","authenticated_rpc_capacity_and_drain",
          "claim_idempotency_and_outcome_retries","expired_and_superseded_attempt_fencing","bounded_attempt_exhaustion",
          "real_jetstream_delivery_redelivery_and_quarantine","relay_publish_ack_crash_recovery","worker_kills_before_claim_after_claim_after_completion",
          "real_two_process_diamond_overlap","active_cancellation_rejects_late_success","db_and_broker_outages_and_process_restart",
          "format_vet_unit_race_build_proto_reproducibility"]
def progress(state,failure=None):
    path=dev.ROOT / ".forgeswarm/progress.json"
    doc=json.loads(path.read_text()) if path.exists() else {"schema_version":1,"stages":{}}
    if doc.get("schema_version")!=1:raise RuntimeError("unsupported progress schema")
    revision=subprocess.run(["git","rev-parse","HEAD"],cwd=dev.ROOT,capture_output=True,text=True)
    dirty=subprocess.run(["git","status","--porcelain"],cwd=dev.ROOT,capture_output=True,text=True)
    doc.setdefault("stages",{}).setdefault("03",{}).update({"stage_number":3,"title":"Broker, outbox, workers, and basic leases","status":state,
        "updated_at":dev.timestamp(),"implemented_capabilities":["additive attempts/workers/outbox schema","normalization v3 with frozen v1/v2 encodings",
        "authenticated versioned Protobuf/gRPC coordination","transactional capacity admission and current-generation fencing",
        "atomic expiry/LOST/requeue/exhaustion","NATS file-backed durable shared pull consumer","outbox relay publish outside locks",
        "bounded noop/delay workers with renewal/drain","quarantine without raw payloads","real PostgreSQL/broker/process verification"],
        "acceptance_criteria":{key:"passed" if state=="verified_local" else "unverified" for key in CRITERIA},"blockers":[dev.sanitize(failure)] if failure else [],
        "broker_decision":{"selected":"NATS JetStream","redis_streams":"AOF/RDB persistence and stream trimming; groups/XACK plus explicit pending recovery via XAUTOCLAIM. Familiar lightweight operation, but more recovery plumbing to learn/build here.",
        "nats_jetstream":"File storage, durable pull groups, explicit acknowledgments/AckWait redelivery, bounded age/size retention. Fits bounded diagnostic queues with a small operational footprint; teaches broker acknowledgments independently of DB execution ownership.",
        "kafka":"Replicated partition logs, consumer groups and offset commits; time/size retention and manual replay. Strong high-throughput ecosystem, but partition/KRaft/offset operations exceed current workload needs; throughput is not measured.",
        "versions":{"server":"2.11.6-alpine (digest pinned)","client":"nats.go v1.47.0"},"failure_limits":"One server/file replica; retained volume survives restart, not node/disk loss. No replication claim."},
        "decisions":["Notification is a hint; durable ClaimTask authorizes diagnostics. Ack after claim/confirmed obsolete ownership; transient failure is delayed NAK.",
            "At-least-once dispatch and potentially repeated execution, with fenced result acceptance; broker duplicate suppression is only an optimization.",
            "Admission advisory lock -> workflow -> task -> worker -> attempt; workflow serialization preserves cancellation/history order; no DB lock spans RPC/broker waits.",
            "Worker heartbeat is only a liveness hint; PostgreSQL clock and current attempt/generation fence all lease/result writes.",
            "Three attempts maximum; expired attempt becomes LOST and atomically queues a new generation or fails on exhaustion.",
            "Outcomes compare canonical object metadata and PostgreSQL JSONB equality; an accepted retry returns original receipt after expiry.",
            "Authenticated TLS is required off-loopback; plaintext test mode is explicit and loopback-only. Worker identity derives from configured bearer credential.",
            "Stage-02 no-execution/normalization-v2-only assumptions superseded; existing hashes, receipts, DAG immutability and dependency policy retained.",
            "Old READY rows acquire first-generation outbox notifications in migration 00003; unowned pre-existing RUNNING rows cause an explicit upgrade refusal.",
            "No arbitrary commands, paid models, GitHub writes or cloud provisioning; process fixtures are diagnostic only."],
        "review_findings":["Fixed invalid explicit zero NATS FetchHeartbeat that prevented production pulls; real Run/drain and TLS diamond verify the correction.",
            "Accepted outcome receipts now remain retryable after a replacement worker instance registers, still bound to original owner/attempt instance/generation.",
            "Docker may reassign ephemeral monitor ports after restart; verification rediscovers the recovered port."],
        "limitations":["Stage 04 must add age-based READY republishing after broker data/retention loss; SENT outbox rows do not currently republish.","Single-node broker storage and per-workflow/global admission serialization limit availability/throughput; performance unmeasured."],
        "sources":["api/worker/v1/worker.proto","migrations/00003_distributed_execution.sql","internal/postgres/execution.go","internal/postgres/scheduler.go",
            "internal/rpc","internal/broker","internal/worker","internal/relay","cmd/coordinator","cmd/relay","cmd/worker","tests/integration/execution_test.go","scripts/stage03.py","scripts/proto.py"],
        "validation_commands":[c for c in dev.COMMANDS if any(word in c["command"] for word in ["gofmt","py_compile","proto","go vet","go test","go build","compose config","assertions","diff --check"])],
        "evidence_index":"artifacts/verification/stage-03/results.json", "evidence_retention":"latest run replaced; logs sanitized and bounded to 128 KiB each; generated evidence ignored by Git",
        "git":{"revision":revision.stdout.strip() if revision.returncode==0 else None,"dirty":bool(dirty.stdout),"scope":"."},
        "fingerprint":{"algorithm":"sha256","value":dev.fingerprint(),"excludes":[".forgeswarm","artifacts/verification",".cache","bin","__pycache__"]},
        "external_verification":{"cloud":"not_requested","live_models":"not_applicable","performance":"not_measured"},
        "reference_docs":["https://docs.nats.io/learn/jetstream/pull-consumers","https://pkg.go.dev/github.com/nats-io/nats.go/jetstream","https://grpc.io/docs/languages/go/quickstart/","https://protobuf.dev/installation/",
            "https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/","https://kafka.apache.org/41/design/design/"],"next_stage":4 if state=="verified_local" else 3})
    tmp=path.with_suffix(".tmp");tmp.write_text(json.dumps(doc,indent=2)+"\n");tmp.replace(path)
    EVIDENCE.mkdir(parents=True,exist_ok=True)
    (EVIDENCE/"results.json").write_text(json.dumps({"status":state,"failure":dev.sanitize(failure) if failure else None,"commands":dev.COMMANDS},indent=2)+"\n")

def remove(name,token):
    code,owner=dev.run(["docker","inspect","--format",'{{ index .Config.Labels "forgeswarm.verification" }}',name],check=False,quiet=True,label="stage03-ownership")
    if code==0 and owner.strip()==token:dev.run(["docker","rm","-f","-v",name],label="stage03-cleanup")

@contextlib.contextmanager
def infrastructure():
    with dev.database() as (env,db,ready):
        token=env["TEST_RESOURCE_TOKEN"];name="forgeswarm-broker-"+token
        env=env | {"NATS_TOKEN":"test-"+uuid.uuid4().hex,"TEST_NATS_TOKEN":"","TEST_NATS_NAMESPACE":token}
        env["TEST_NATS_TOKEN"]=env["NATS_TOKEN"]
        try:
            dev.run(["docker","run","-d","--name",name,"--label",f"forgeswarm.verification={token}","--platform","linux/amd64","--network",env["TEST_DOCKER_NETWORK"],
                     "--network-alias","broker","-p","127.0.0.1::4222","-p","127.0.0.1::8222","--mount",f"type=bind,source={dev.ROOT/'config/nats.conf'},target=/etc/nats/nats.conf,readonly",
                     "--mount","type=volume,target=/data","-e","NATS_TOKEN",IMAGE,"-c","/etc/nats/nats.conf"],env=env,label="stage03-broker")
            _,port=dev.run(["docker","port",name,"8222/tcp"],quiet=True,label="broker-monitor-port")
            monitor="http://127.0.0.1:"+port.strip().rsplit(":",1)[1]
            def healthy():
                try:return urllib.request.urlopen(monitor+"/healthz?js-enabled-only=true",timeout=2).status==200
                except OSError:return False
            dev.poll(healthy,"file-backed JetStream")
            _,port=dev.run(["docker","port",name,"4222/tcp"],quiet=True,label="broker-client-port")
            env["TEST_NATS_URL"]="nats://127.0.0.1:"+port.strip().rsplit(":",1)[1]
            if os.name=="nt":env["TEST_GO_NETWORK"]="container:"+name;env["TEST_NATS_URL"]="nats://127.0.0.1:4222"
            yield env,db,name,healthy
        except BaseException:
            dev.run(["docker","logs",name],check=False,quiet=True,label="broker-failure-logs")
            raise
        finally:remove(name,token)

def verify():
    if not __debug__:raise RuntimeError("verification requires assertions")
    EVIDENCE.mkdir(parents=True,exist_ok=True)
    if not EVIDENCE.resolve().is_relative_to(dev.ROOT.resolve()):raise RuntimeError("invalid evidence path")
    for p in [*EVIDENCE.glob("[0-9][0-9][0-9]-*.log"),EVIDENCE/"results.json",EVIDENCE/"process-results.json"]:
        if p.is_file() and not p.is_symlink():p.unlink()
    dev.EVIDENCE=EVIDENCE;dev.RECORD=True;dev.COMMANDS.clear();progress("in_progress")
    try:
        dev.fmt_check();dev.run([sys.executable,"-m","py_compile","scripts/dev.py","scripts/stage02.py","scripts/stage03.py","scripts/proto.py"],label="python")
        proto.generate(True)
        dev.run(["go","vet","./..."],label="vet");dev.run(["go","test","-count=1","./..."],label="unit");dev.race(["-count=1","./..."])
        dev.build();dev.run(["docker","compose","config","--quiet"],label="compose")
        with infrastructure() as (env,db,broker,healthy):
            dev.race(["-tags=integration,stage03","-count=1","-timeout=120s","-v","./tests/integration"],env=env,container=db,label="stage03-integration")
        dev.smoke()
        smoke()
        dev.run(["git","diff","--check"],label="diff-check")
    except (RuntimeError,AssertionError,OSError,ValueError) as err:progress("implemented_unverified",str(err));raise
    progress("verified_local");print("stage 03 verified_local; see progress and stage-03 evidence",flush=True)

def smoke(recovery=False):
    started=dev.timestamp()
    phases=[]
    with dev.database() as (env,db,db_ready):
        token=env["TEST_RESOURCE_TOKEN"]
        private=dev.ROOT / (".cache/stage03-"+token)
        private.mkdir(parents=True)
        assert private.resolve().is_relative_to((dev.ROOT/".cache").resolve())
        processes={}
        broker="forgeswarm-tls-broker-"+token
        base=None
        env=env|{"NATS_TOKEN":"test-"+uuid.uuid4().hex,"DATABASE_URL":re.sub(r"127\.0\.0\.1:\d+/","db:5432/",env["DATABASE_URL"]),
                 "NATS_URL":"tls://broker:4222","NATS_TLS_CA":"/ca/cert.pem","RPC_ADDR":"coordinator:9090","RPC_TLS_CA":"/ca/cert.pem",
                 "WORKER_CAPACITY":"1","WORKER_MAX_CAPACITY":"1","TOTAL_CAPACITY":"2","ATTEMPT_LEASE":"3s","RPC_TIMEOUT":"1s",
                 "RELAY_LEASE":"1s","DISPATCH_POLL":"100ms","SHUTDOWN_TIMEOUT":"3s","SCHEDULER_INTERVAL":"100ms",
                 "SCHEDULER_SCAN_TIMEOUT":"1s","REQUEST_TIMEOUT":"1s","CONNECT_TIMEOUT":"300ms","READY_TIMEOUT":"300ms"}
        if recovery:env.update({"RETRY_BACKOFF_BASE":"3s","RETRY_BACKOFF_CAP":"3s"})
        keys={"worker_a":uuid.uuid4().hex,"worker_b":uuid.uuid4().hex}
        (private/"keys.json").write_text(json.dumps(keys))
        (private/"nats.conf").write_text('port: 4222\nhttp_port: 8222\nauthorization { token: $NATS_TOKEN }\n'
              'tls { cert_file: /tls/cert.pem; key_file: /tls/key.pem }\n'
              'jetstream { store_dir: /data/jetstream; max_mem_store: 16MB; max_file_store: 128MB }\n')
        def sql(query):
            _,owner=dev.run(["docker","inspect","--format",'{{ index .Config.Labels "forgeswarm.verification" }}',db],quiet=True,label="process-db-ownership")
            assert owner.strip()==token
            _,out=dev.run(["docker","exec",db,"psql","-A","-t","-v","ON_ERROR_STOP=1","-U","forgeswarm","-d","forgeswarm_stage01_"+token,"-c",query],quiet=True,label="process-db-assertion")
            return out.strip()
        def logs(name):
            _,out=dev.run(["docker","logs",processes[name]],quiet=True,label="process-observed-log")
            return out
        def start(name):
            nonlocal base
            role="worker" if name.startswith("worker_") else "scheduler" if name.startswith("scheduler") else name
            container="forgeswarm-"+name+"-"+uuid.uuid4().hex
            settings=env.copy()
            if role=="coordinator":settings.update({"RPC_ADDR":"0.0.0.0:9090","RPC_KEYS_FILE":"/tls/keys.json","RPC_TLS_CERT":"/tls/cert.pem","RPC_TLS_KEY":"/tls/key.pem"})
            if role=="api":settings["HTTP_ADDR"]="0.0.0.0:8080"
            if role=="worker":settings["WORKER_TOKEN"]=keys[name]
            args=["docker","run","-d","--name",container,"--label","forgeswarm.verification="+token,"--platform","linux/amd64","--network",env["TEST_DOCKER_NETWORK"],
                  "--mount",f"type=bind,source={dev.ROOT/'bin'},target=/app,readonly","--mount",f"type=bind,source={private/'cert.pem'},target=/ca/cert.pem,readonly"]
            if role=="coordinator":args += ["--network-alias","coordinator","--mount",f"type=bind,source={private},target=/tls,readonly"]
            if role=="api":args += ["-p","127.0.0.1::8080"]
            for key in ["DATABASE_URL","NATS_TOKEN","NATS_URL","NATS_TLS_CA","RPC_ADDR","RPC_TLS_CA","WORKER_CAPACITY","WORKER_MAX_CAPACITY","TOTAL_CAPACITY",
                        "ATTEMPT_LEASE","RPC_TIMEOUT","RELAY_LEASE","DISPATCH_POLL","SHUTDOWN_TIMEOUT","SCHEDULER_INTERVAL","SCHEDULER_SCAN_TIMEOUT",
                        "REQUEST_TIMEOUT","CONNECT_TIMEOUT","READY_TIMEOUT","RPC_KEYS_FILE","RPC_TLS_CERT","RPC_TLS_KEY","HTTP_ADDR","WORKER_TOKEN"]:
                if key in settings:args += ["-e",key]
            if recovery:
                for key in ["RETRY_BACKOFF_BASE","RETRY_BACKOFF_CAP"]:args += ["-e",key]
            dev.run([*args,dev.GO_IMAGE,"/app/"+role+"-linux"],env=settings,label="process-start-"+name)
            processes[name]=container
            if role=="api":
                _,port=dev.run(["docker","port",container,"8080/tcp"],quiet=True,label="process-api-port")
                base="http://127.0.0.1:"+port.strip().rsplit(":",1)[1]
                def ready():
                    try:return dev.http(base,"/health/ready")[0]==200
                    except OSError:return False
                dev.poll(ready,"distributed API ready")
        def stop(name,killed=False):
            container=processes[name]
            dev.run(["docker","kill","--signal","KILL",container] if killed else ["docker","stop","--timeout","10",container],label="process-kill" if killed else "process-drain")
            _,exit_code=dev.run(["docker","inspect","--format","{{.State.ExitCode}}",container],quiet=True,label="process-exit")
            out=logs(name)
            assert exit_code.strip()==("137" if killed else "0"), name+" did not stop correctly"
            if not killed and name.startswith("worker_"):assert '"msg":"worker_drained"' in out
            remove(container,token);del processes[name]
        def graph(receipt):
            code,value,_=dev.http(base,"/v1/workflows/"+receipt["id"]+"/graph")
            assert code==200
            return value
        def create(key,tasks):
            body=json.dumps({"name":key,"tasks":tasks}).encode()
            code,receipt,_=dev.http(base,"/v1/workflows","POST",body,key)
            assert code==201
            return receipt,body
        def finish(receipt):dev.poll(lambda:graph(receipt)["status"]=="SUCCEEDED","accepted diagnostic results",timeout=45)
        def count(receipt,condition="true"):
            workflow_id=str(uuid.UUID(receipt["id"]))
            return int(sql(f"SELECT count(*) FROM task_attempts WHERE workflow_id='{workflow_id}' AND ({condition})"))
        def running_owner(receipt):
            owner=[None]
            workflow_id=str(uuid.UUID(receipt["id"]))
            def observed():
                owner[0]=sql(f"SELECT worker_id FROM task_attempts WHERE workflow_id='{workflow_id}' AND state='RUNNING'")
                return owner[0] in keys
            dev.poll(observed,"a durable current claim")
            return owner[0]
        def record(name,receipt,extra=None):
            code,page,_=dev.http(base,"/v1/workflows/"+receipt["id"]+"/events?limit=100")
            assert code==200
            assert [e["sequence"] for e in page["events"]]==list(range(1,len(page["events"])+1))
            phases.append({"phase":name,"workflow_id":receipt["id"],"status":graph(receipt)["status"],"attempts":count(receipt),"history_count":len(page["events"])}| (extra or {}))
        try:
            dev.run(["docker","run","--rm","--platform","linux/amd64","--mount",f"type=bind,source={private},target=/tls",dev.GO_IMAGE,
                     "openssl","req","-x509","-newkey","rsa:2048","-nodes","-days","1","-keyout","/tls/key.pem","-out","/tls/cert.pem","-subj","/CN=forgeswarm-test",
                     "-addext","subjectAltName=DNS:broker,DNS:coordinator,IP:127.0.0.1"],quiet=True,label="process-fixture-certificate")
            for role in ["api","scheduler","coordinator","relay","worker"]:
                dev.run(["go","build","-o",dev.ROOT / ("bin/"+role+"-linux"),"./cmd/"+role],env=dev.ENV|{"GOOS":"linux","GOARCH":"amd64","CGO_ENABLED":"0"},label="process-build-"+role)
            # Native migration uses the published loopback DB URL, not private DNS.
            native=env|{"DATABASE_URL":env["TEST_DATABASE_URL"]}
            dev.run(["go","run","./cmd/migrate","up"],env=native,label="process-migrate")
            dev.run(["docker","run","-d","--name",broker,"--label","forgeswarm.verification="+token,"--network",env["TEST_DOCKER_NETWORK"],"--network-alias","broker",
                     "-p","127.0.0.1::8222","--mount",f"type=bind,source={private/'nats.conf'},target=/etc/nats/nats.conf,readonly",
                     "--mount",f"type=bind,source={private},target=/tls,readonly","--mount","type=volume,target=/data","-e","NATS_TOKEN",IMAGE,"-c","/etc/nats/nats.conf"],env=env,label="process-tls-broker")
            _,port=dev.run(["docker","port",broker,"8222/tcp"],quiet=True,label="process-monitor-port")
            monitor="http://127.0.0.1:"+port.strip().rsplit(":",1)[1]
            def broker_ready():
                try:return urllib.request.urlopen(monitor+"/healthz?js-enabled-only=true",timeout=2).status==200
                except OSError:return False
            dev.poll(broker_ready,"authenticated TLS JetStream")
            for name in ["api","coordinator","scheduler","relay"]:start(name)
            if recovery:start("scheduler_2")
            diamond,body=create("distributed-diamond",[{"key":"a","kind":"noop"},{"key":"b","kind":"delay","delay_ms":1500,"depends_on":["a"]},
                    {"key":"c","kind":"delay","delay_ms":1500,"depends_on":["a"]},{"key":"d","kind":"noop","depends_on":["b","c"]}])
            for name in keys:start(name)
            finish(diamond)
            wid=str(uuid.UUID(diamond["id"]))
            overlap=sql(f"SELECT b.started_at<c.finished_at AND c.started_at<b.finished_at FROM task_attempts b JOIN tasks bt ON bt.id=b.task_id JOIN task_attempts c ON c.workflow_id=b.workflow_id JOIN tasks ct ON ct.id=c.task_id WHERE b.workflow_id='{wid}' AND bt.task_key='b' AND ct.task_key='c'")
            fanin=sql(f"SELECT d.started_at>=max(p.finished_at) FROM task_attempts d JOIN tasks dt ON dt.id=d.task_id JOIN task_attempts p ON p.workflow_id=d.workflow_id JOIN tasks pt ON pt.id=p.task_id WHERE d.workflow_id='{wid}' AND dt.task_key='d' AND pt.task_key IN ('b','c') GROUP BY d.started_at")
            assert overlap=="t" and fanin=="t" and count(diamond)==4 and count(diamond,"state='SUCCEEDED'")==4
            assert int(sql(f"SELECT count(DISTINCT worker_id) FROM task_attempts WHERE workflow_id='{wid}'"))==2
            record("real two-worker diamond",diamond,{"b_c_overlap":True,"d_waited_for_b_c":True,"tls_authenticated":True})

            dev.run(["docker","stop","--timeout","5",broker],label="process-broker-outage")
            queued,_=create("broker-outage",[{"key":"a","kind":"noop"}])
            qid=str(uuid.UUID(queued["id"]))
            dev.poll(lambda:sql(f"SELECT count(*) FROM outbox o JOIN tasks t ON t.id=o.task_id WHERE t.workflow_id='{qid}' AND o.state='PENDING'")=="1","durable pending outbox during broker outage")
            assert count(queued)==0
            dev.run(["docker","start",broker],label="process-broker-recovery")
            # Docker can reassign an ephemeral published port after stop/start.
            _,port=dev.run(["docker","port",broker,"8222/tcp"],quiet=True,label="process-recovered-monitor-port")
            monitor="http://127.0.0.1:"+port.strip().rsplit(":",1)[1]
            dev.poll(broker_ready,"retained broker volume")
            finish(queued);assert count(queued)==1
            record("broker outage outbox recovery",queued)

            # Pause both registered processes before queuing. Resume with the DB
            # down: real notifications must receive delayed NAK, never success ack.
            for name in keys:dev.run(["docker","kill","--signal","STOP",processes[name]],label="process-pause-worker")
            unavailable,_=create("db-before-claim",[{"key":"a","kind":"noop"}]);uid=str(uuid.UUID(unavailable["id"]))
            dev.poll(lambda:sql(f"SELECT count(*) FROM outbox o JOIN tasks t ON t.id=o.task_id WHERE t.workflow_id='{uid}' AND o.state='SENT'")=="1","notification before DB outage")
            assert count(unavailable)==0
            before=sum(logs(name).count('"msg":"worker_claim_deferred"') for name in keys)
            dev.run(["docker","stop","--timeout","5",db],label="process-db-before-claim-outage")
            for name in keys:dev.run(["docker","kill","--signal","CONT",processes[name]],label="process-resume-worker")
            dev.poll(lambda:sum(logs(name).count('"msg":"worker_claim_deferred"') for name in keys)>before,"transient claim delayed without acknowledgment")
            dev.run(["docker","start",db],label="process-db-recovery");dev.poll(db_ready,"database recovery")
            finish(unavailable);assert count(unavailable)==1
            record("DB outage before claim and real message recovery",unavailable)

            cancelled,_=create("active-cancel",[{"key":"a","kind":"delay","delay_ms":10000}]);running_owner(cancelled)
            assert dev.http(base,"/v1/workflows/"+cancelled["id"]+"/cancel","POST",b"")[0]==200
            # Stage 04 accepts cancellation first, then waits for stop acknowledgment
            # or lease fencing. Completed diagnostic results remain immutable.
            dev.poll(lambda:graph(cancelled)["status"]=="CANCELLED","cancellation stop handshake")
            assert count(cancelled,"state='CANCELLED' AND result_metadata IS NULL")==1
            record("active diagnostics cancelled",cancelled)

            for killed in [False,True]:
                recovery_started=time.monotonic()
                recovered,_=create("killed-worker" if killed else "stale-resume",[{"key":"a","kind":"delay","delay_ms":6000}])
                owner=running_owner(recovered)
                if killed:stop(owner,killed=True)
                else:dev.run(["docker","kill","--signal","STOP",processes[owner]],label="process-stale-worker-pause")
                finish(recovered)
                assert count(recovered)==2 and count(recovered,"state='LOST'")==1 and count(recovered,"state='SUCCEEDED'")==1
                if killed:start(owner)
                else:
                    dev.run(["docker","kill","--signal","CONT",processes[owner]],label="process-stale-worker-resume")
                    probe,_=create("resumed-worker-probe",[{"key":"a","kind":"noop"}]);finish(probe)
                    assert count(recovered,"state='SUCCEEDED'")==1
                record("killed worker lease failover" if killed else "paused worker lease failover and stale resume",recovered,{"observed_seconds":round(time.monotonic()-recovery_started,3)})

            if recovery:
                # Leave a bounded but ample window for the real Docker
                # inspect/psql/kill round trips. At 1s the task could already
                # finish between observing its claim and killing its worker,
                # so no RETRY_WAIT would exist to test. Assertions stay strict.
                retried,_=create("retry-wait-process-restart",[{"key":"a","kind":"delay","delay_ms":30000}])
                owner=running_owner(retried);stop(owner,killed=True);rid=str(uuid.UUID(retried["id"]))
                dev.poll(lambda:sql(f"SELECT count(*) FROM tasks WHERE workflow_id='{rid}' AND status='RETRY_WAIT'")=="1","persisted RETRY_WAIT after worker kill")
                for name in ["scheduler","scheduler_2"]:stop(name,killed=True)
                budget_query=f"SELECT next_attempt_at::text||'|'||deadline_at::text||'|'||dispatch_generation::text FROM tasks WHERE workflow_id='{rid}'"
                budget=sql(budget_query)
                for name in ["coordinator","api"]:stop(name)
                for name in ["api","coordinator"]:start(name)
                assert sql(budget_query)==budget,"control-plane restart reset durable retry/deadline/generation"
                dev.poll(lambda:sql(f"SELECT count(*) FROM tasks WHERE workflow_id='{rid}' AND next_attempt_at<=clock_timestamp()")=="1","retry due with both schedulers stopped")
                assert graph(retried)["nodes"][0]["status"]=="RETRY_WAIT"
                for name in ["scheduler","scheduler_2",owner]:start(name)
                finish(retried)
                assert count(retried)==2 and count(retried,"state='SUCCEEDED'")==1
                record("scheduler SIGKILL and control-plane restart during RETRY_WAIT",retried,{"due_deadline_generation_preserved":True})
                timeout_started=time.monotonic()
                timed,_=create("diagnostic-deadline",[{"key":"a","kind":"delay","delay_ms":10000,"timeout_ms":1500}]);running_owner(timed)
                dev.poll(lambda:graph(timed)["status"]=="FAILED","bounded diagnostic deadline")
                assert count(timed,"state='SUCCEEDED'")==0 and count(timed)==1
                record("diagnostic deadline stops/fences execution",timed,{"observed_seconds":round(time.monotonic()-timeout_started,3)})

            completed=graph(diamond)
            for name in [*keys,"relay","scheduler",*(["scheduler_2"] if recovery else []),"coordinator","api"]:stop(name)
            for name in ["api","coordinator","scheduler",*(["scheduler_2"] if recovery else []),"relay",*keys]:start(name)
            dev.poll(lambda:sql("SELECT count(*) FROM workers WHERE NOT draining AND heartbeat_at>clock_timestamp()-interval '2 seconds'")=="2","two restarted registered workers")
            assert graph(diamond)==completed and count(diamond)==4
            assert dev.http(base,"/v1/workflows","POST",body,"distributed-diamond")[1]==diamond
            probe,_=create("all-processes-restarted",[{"key":"a","kind":"noop"}]);finish(probe)
            record("all role processes restarted, terminal state preserved",diamond)
            for name in [*keys,"relay","scheduler",*(["scheduler_2"] if recovery else []),"coordinator","api"]:stop(name)
            result={"status":"passed","started_at":started,"finished_at":dev.timestamp(),"fixture":"owned disposable processes; real diagnostic outcomes only","phases":phases}
            result["environment"]={"processes":"Linux amd64 containers on local Docker","workers":2,"slots_per_worker":1,"total_capacity":2,"lease_seconds":3,"scheduler_interval_ms":100,"backoff_base_seconds":3 if recovery else 0.25,"measurement_scope":"individual smoke observations; no throughput inference"}
            EVIDENCE.mkdir(parents=True,exist_ok=True)
            (EVIDENCE/"process-results.json").write_text(json.dumps(result,indent=2)+"\n")
            if dev.RECORD:dev.COMMANDS.append({"command":"Stage03 real TLS diagnostic processes / overlap / outages / kill / resume / restart assertions","started_at":started,"finished_at":dev.timestamp(),"exit":0,"required":True,"evidence_path":(EVIDENCE/"process-results.json").relative_to(dev.ROOT).as_posix()})
            print("stage03 process checks passed: TLS diamond, outages, active cancellation, lease failover, stale resume, restart and drain",flush=True)
        finally:
            for name,container in list(processes.items()):
                dev.run(["docker","logs",container],check=False,quiet=True,label="process-final-log")
                remove(container,token)
            dev.run(["docker","logs",broker],check=False,quiet=True,label="broker-final-log")
            remove(broker,token)
            # Exact owned fixture files only; do not retain credentials or keys.
            for name in ["cert.pem","key.pem","keys.json","nats.conf"]:
                path=private/name
                if path.is_file() and not path.is_symlink():path.unlink()
            private.rmdir()
