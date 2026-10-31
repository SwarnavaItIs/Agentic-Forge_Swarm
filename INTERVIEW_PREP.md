# ForgeSwarm interview framework

## Establish the scope before speaking

Reviewed implementation: **`106fa68747e87e0ce01a2e02184f03011e278084`**, on **2026-10-31 Asia/Calcutta**. Detailed source links, content fingerprint, verification provenance, and caveats are in [ENGINEERING_NOTES.md](ENGINEERING_NOTES.md). Recorded full local evidence is from October 29; fresh scoped Go/Python/component/policy tests passed during this review. Generating these documents does not verify the entire project anew.

Use “the system implements” where personal ownership is unknown. Neither repository inspection nor commit history proves which parts you personally designed or wrote. Before an interview, identify changes you can honestly attribute to yourself and explain their implementation, validation, and limitations. Do not invent production users, outages, leadership, business impact, or human review.

### 30-second introduction

“ForgeSwarm coordinates bounded coding workflows. PostgreSQL stores graphs, attempts, history, budgets, and approvals. A scheduler makes dependencies ready, an outbox relays notifications through NATS JetStream, and authenticated Go workers execute controlled tools and Docker tests. Python supplies structured model proposals. Integrated candidates need exact test and review evidence before approval for a draft PR. The local demo is scripted with fake GitHub publication; real model quality and cloud deployment remain unverified.”

### 90-second explanation

“The main challenge is recovering distributed work without accepting stale results. A workflow contains logical tasks, each of which may have multiple attempts. Messages only notify workers. A database transaction grants an attempt ownership, bounded by worker identity, process instance, generation, lease, and deadlines. If a worker disappears, recovery fences it and schedules a bounded retry. A late result cannot overwrite the current owner.

“The coding layer uses the same ownership boundary. Models propose strict actions; trusted Go code controls reads, conditional edits, and fixed-profile tests. Plans create immutable execution versions. Developer outputs are merged and tested together, then reviewed against the exact candidate. Imported GitHub jobs require durable approval of candidate, patch, tests, review, repository, and base. Publication records intent before HTTP and discovers uncertain effects after restart.

“Local tests exercise real PostgreSQL, NATS, RPC, Docker, and browser behavior. Scripted providers and fake GitHub adapters test plumbing rather than external quality. Benchmarks expose admission saturation and two latency misses; they do not establish production capacity.”

### Five-minute walkthrough

1. **0:00–0:45:** Define workflow/task/attempt/notification/turn and identify PostgreSQL's authority.
2. **0:45–1:45:** Explain idempotent admission, immutable DAG readiness, outbox, and durable claim.
3. **1:45–2:45:** Walk a paused worker through expiry, fencing, persisted retry, and rejected stale completion.
4. **2:45–3:45:** Show trusted tools, provider separation, integrated candidate tests, and read-only review.
5. **3:45–4:30:** Bind human approval to immutable evidence; explain uncertain GitHub outcomes.
6. **4:30–5:00:** State measured numbers, unmet goals, fixture limits, and the next engineering improvements.

### Hardest verified engineering problem

Preserving ownership when time passes **while transactions wait for locks** is a strong example. A renewal started before expiry must not revive ownership after waiting past expiry. The store samples database wall time after locking and repeats deadline/lease predicates in mutation SQL. `TestLeaseBoundaryBlockedRenewCompleteReapAndCancel` deliberately blocks competing operations past expiry and checks stale renewal/completion rejection plus consistent recovery/cancellation. Explain why a transaction-start timestamp or worker heartbeat would be insufficient. This is verified coordination behavior, not a claimed production incident.

## Subsystem answers

Each spoken paragraph follows **PROBLEM → DESIGN CHOICE → MECHANISM → FAILURE BEHAVIOR → TRADEOFF → EVIDENCE**. Expand the paragraph with the concepts and follow-ups rather than memorizing buzzwords.

### 1. API, persistence, migrations, and idempotency

**Spoken answer:** “A lost HTTP response must not duplicate admitted work. The system uses a database uniqueness constraint plus a versioned normalized request hash. Workflow, tasks, edges, and creation history commit together. Retrying the same key and input returns the original receipt; changed content conflicts. That costs durable database coordination but survives restarts and multiple API instances. Tests race twenty requests through two pools and inject history failures to verify rollback.”

**Know:** Read Committed snapshots; unique constraint; normalization version; audit history.

**Questions:** Why a second SELECT after conflict? The winning committed row may be invisible to the insert's original snapshot. Is history event sourcing? No; current state is authoritative and arbitrary replay is absent.

**Probe:** API commits, client disconnects. Retry the original key/body; generating a fresh key admits new work.

**Alternative:** An in-memory cache suits disposable deduplication, not durable admission.

**Inspect/evidence:** [workflow_test.go](tests/integration/workflow_test.go), `TestTwentyCreatesTwoPoolsAndConflicts`.

**Avoid:** “All requests execute exactly once.”

### 2. DAG scheduling

**Spoken answer:** “A dependency graph says what can precede what, but execution readiness changes over time. The system validates cycles with Kahn's algorithm, then reconciles persisted predecessor states under workflow locks. A node becomes ready only after all predecessors succeed. Failed descendants block while independent branches continue. A crashed scan rolls back and another scan reconstructs state. Database scans and coarse serialization simplify recovery but limit throughput; real two-scheduler tests check singular transitions.”

**Know:** O(V+E) validation; readiness; terminal aggregation; SKIP LOCKED.

**Questions:** Does topological order schedule execution? No, it validates/orders dependencies. Does READY mean successful? No, it permits a future claim.

**Probe:** Two schedulers find one root. Serialized predicates prevent two logical readiness transitions.

**Alternative:** Cache graphs in a scheduler when read cost justifies additional failover complexity.

**Inspect/evidence:** [scheduler_test.go](tests/integration/scheduler_test.go), `TestDurableDiamondTwoSchedulersAndRepeatedReconciliation`.

**Avoid:** Strict fairness under persistent lock contention.

### 3. Outbox and NATS JetStream

**Spoken answer:** “A committed task must not disappear because the broker was unavailable. Readiness and outbox intent commit together. A relay claims intent, publishes outside the transaction, then records acknowledgment. If it crashes between publish and record, notification can repeat. Workers still need a durable claim, so duplicate delivery does not grant duplicate accepted ownership. This adds broker operations and eventual dispatch latency. Real JetStream tests cover publish crashes, redelivery, and quarantine.”

**Know:** Transactional outbox; at-least-once delivery; deduplication window; durable consumer.

**Questions:** What does broker acknowledgment prove? Notification handling, not task completion. Why can duplicate detection expire safely? Database ownership remains authoritative.

**Probe:** DB commits while NATS is down. Pending intent survives; relay retries after recovery.

**Alternative:** Database-only polling for a smaller deployment.

**Inspect/evidence:** [execution_test.go](tests/integration/execution_test.go), `TestRealBrokerPublishCrashRedeliveryDuplicatesAndQuarantine`.

**Avoid:** Calling one file-backed replica highly available.

### 4. RPC ownership, leases, and fencing

**Spoken answer:** “A delayed worker can remain alive after losing authority. The coordinator authenticates its identity and grants a persisted attempt with instance, generation, lease, and deadline checks. Renew and complete recheck current ownership under SQL locks. Replacement advances the generation, so the old worker cannot commit. Identical accepted result retries return the stored receipt after a lost reply. This depends on database availability and contention, but RPC fault tests verify the important ambiguity cases.”

**Know:** Identity versus instance; lease versus fence; database clock; accepted receipt.

**Questions:** Is a heartbeat ownership? No, it is a liveness hint. Does fencing stop a process? No, it rejects stale state transitions.

**Probe:** Worker finishes but loses acknowledgment. Retry the same outcome; conflicting content is rejected.

**Alternative:** A separate coordination service when database coordination no longer fits.

**Inspect/evidence:** [execution_test.go](tests/integration/execution_test.go), `TestAuthenticatedRPCLostClaimAndOutcomeReplies`.

**Avoid:** Exactly-once physical execution.

### 5. Recovery, cancellation, deadlines, and quotas

**Spoken answer:** “Retries must recover infrastructure faults without creating infinite work. The system persists capped jittered retry times, limits attempts, and keeps original task/workflow deadlines. Cancellation retires queued work and requests active workers to stop; lease fencing can settle logical state before physical termination is proven. Database-coordinated quotas reject saturation with 429. This favors bounded recovery over guaranteed completion. Real recovery tests cover deadlines, races, draining, and admission across API instances.”

**Know:** RETRY_WAIT; equal jitter; absolute deadline; stop acknowledgment; backpressure.

**Questions:** Can renewal reset a deadline? No. Is cancellation a process-kill guarantee? No; cleanup uncertainty is retained.

**Probe:** A paused worker resumes after expiry. Authorization/result checks reject it; replacement may already be running.

**Alternative:** Dead-letter/manual retry for workflows requiring explicit recovery decisions.

**Inspect/evidence:** [recovery_test.go](tests/integration/recovery_test.go), `TestDeadlinesCannotBeExtendedByRenewalOrRestart`.

**Avoid:** Claiming all cleanup flags cleared; two diagnostic uncertainties remain.

### 6. Docker, workspaces, and artifacts

**Spoken answer:** “Execution needs pinned input, bounded effects, and trustworthy output references. Vetted Git objects become hashed snapshots; trusted code chooses argv, image, environment, and limits. Sandboxes run without network, host source mounts, or credentials, under resource restrictions. Accepted artifact bytes and references commit together in PostgreSQL. Crashes trigger owned-container reconciliation while stale results stay fenced. This is inspectable local confinement with real resource tests, but shares the host kernel and trusts the Docker gateway.”

**Know:** Immutable snapshot; content digest; cgroups; cleanup ownership; artifact provenance.

**Questions:** Can clients choose Docker flags? No. Why store bytes in SQL? Small artifacts can commit atomically with acceptance.

**Probe:** Worker dies leaving an orphan. Inspect exact installation/attempt labels; reconcile only owned resources.

**Alternative:** MicroVMs/dedicated hosts for adversarial code; object storage for large artifacts.

**Inspect/evidence:** [sandbox_process_test.go](tests/integration/sandbox_process_test.go), `TestSandboxWorkerCrashRestartReconcilesOnlyOwnedOrphans`.

**Avoid:** “Docker securely runs any hostile repository.”

### 7. Agent tools, retrieval, and checkpoints

**Spoken answer:** “Repository text and model output are untrusted input. Go grants only strict bounded tools: reads, literal search, digest-conditional edits, and fixed-profile tests. A final proposal cannot claim success without exact candidate evidence. Reads and context truncate explicitly. Interrupted edit boundaries stop the attempt rather than replay uncertain instructions. Narrow permissions reduce coverage, but tool and crash tests show enforcement independent of model compliance.”

**Know:** Authorization versus prompting; optimistic edits; bounded retrieval; restart from pinned input.

**Questions:** Is retrieval semantic RAG? No, it is file access and literal search. Do journal checkpoints replay arbitrary turns? No.

**Probe:** The model requests a secret or shell command. Tool/path/schema policy denies it; repeated denials exhaust the bound.

**Alternative:** Richer retrieval when larger repositories justify indexing and policy work.

**Inspect/evidence:** [tools_test.go](internal/agent/tools_test.go), `TestToolsEnforcePolicyAndExactConditionalPatch`.

**Avoid:** Broad autonomous coding capability from pure-function fixtures.

### 8. Python provider boundary and budget accounting

**Spoken answer:** “Provider variability must not become tool authority or unbounded spending. Python authenticates bounded gRPC requests, validates structured replies, and handles provider I/O; Go controls effects. Before a model call, durable accounting reserves conservative tokens and estimated cost. Missing usage keeps the reservation, including across retries. This may reject affordable work conservatively, but avoids treating uncertainty as free. Fake provider tests and real database budget tests validate mechanics; paid behavior is unverified.”

**Know:** Strict schema; deadline; reservation; unknown usage; estimate versus invoice.

**Questions:** Does adding roles create fresh allowances? No, shared task/project totals constrain them. Are configured prices authorization? No.

**Probe:** Provider processed a call but response vanished. Keep its charge uncertain; do not blindly repeat an unrecorded effect.

**Alternative:** A provider-specific usage reconciliation service at larger spending scale.

**Inspect/evidence:** [agent_test.go](tests/integration/agent_test.go), `TestAgentRetryRetainsUncertainBudget`.

**Avoid:** Calling fixture cost zero or a price estimate an actual bill.

### 9. Plans, integration, and review

**Spoken answer:** “Parallel patches need a common acceptance point. The planner produces a validated immutable plan, developers have declared write scopes, and integration merges accepted inputs deterministically. The combined candidate receives fresh protected tests before read-only review. Overlap or semantic failure becomes visible; a review rejection can create a bounded new version. Extra roles add latency and merge risk. Docker-backed tests prove separate branch passes cannot substitute for combined evidence.”

**Know:** Plan digest; role capability; three-way merge; exact-candidate tests; repair version.

**Questions:** Can an executing DAG be rewritten? No. Is correction the same as infrastructure retry? No; it creates new planning/execution work.

**Probe:** Two patches pass separately but fail together. The integrated profile fails; the project needs human attention, not fabricated success.

**Alternative:** One developer for small tightly coupled tasks.

**Inspect/evidence:** [collaboration_process_test.go](tests/integration/collaboration_process_test.go), `TestCollaborationRealSemanticConflictCannotUseSeparateTestClaims`.

**Avoid:** Claiming every integration failure automatically repairs itself.

### 10. Approvals and GitHub reconciliation

**Spoken answer:** “Publication needs approval of immutable evidence and recovery from uncertain HTTP outcomes. The binding includes candidate, plan, patch, tests, review, repository, base, and action. Approval creates one durable external intent. Publication uses deterministic branch/commit identity, discovers existing effects, and rechecks authorization before writes. Uncertainty retries are bounded and mismatches need review. This cannot make SQL and GitHub atomic; fake HTTP tests deliberately lose responses after remote effects.”

**Know:** Binding digest; intent record; deterministic identity; external uncertainty.

**Questions:** Patch changes after approval? It cannot reuse the old binding; new evidence/approval is required. Does cancellation undo a PR? No.

**Probe:** GitHub created a PR but SQL never recorded it. Discover and validate the matching draft before new writes; retain ambiguity otherwise.

**Alternative:** Export an approved patch for manual publication.

**Inspect/evidence:** [publication_test.go](tests/integration/publication_test.go), `TestGitHubUncertainPublicationRestartsAndBoundsRetries`.

**Avoid:** Claiming actual GitHub publication was verified or cancellation closes every remote-write race.

### 11. Dashboard, SSE, and operator security

**Spoken answer:** “Operators need a consistent view without owning execution in a browser. REST supplies snapshots and SSE streams persisted cursor events; the client deduplicates sequence identities and bounds memory. Separate operator/reader roles protect mutations, with opaque sessions, CSRF checks, and exact HTTPS origin policy. Browser disconnects only interrupt observation. Component and recorded browser tests cover controls and escaping. Process-local sessions simplify this owner model but require login again after API restart.”

**Know:** Cursor replay; deduplication; same origin; CSRF; immutable approval display.

**Questions:** Why SSE? Updates are one-way. Can forwarded headers authorize an origin? No, actual TLS/Host checks matter.

**Probe:** Cancellation races with success. The browser shows the persisted winner; it cannot override already accepted state.

**Alternative:** WebSockets for true bidirectional features; an identity provider for multiple owners.

**Inspect/evidence:** [approval.spec.ts](web/e2e/approval.spec.ts), `operator login, exact approval and observed fake draft`.

**Avoid:** Calling this multi-tenant authorization or assuming private CA trust is automatic.

### 12. Telemetry and performance

**Spoken answer:** “Distributed debugging needs connected observations with honest denominators. Validated trace context crosses outbox, broker, and RPC; bounded labels avoid IDs and secrets in metrics. Durable state distributions differ from process-local histograms. The load matrix measures a diagnostic diamond with explicit quotas, not model throughput. Two p95 goals failed and heavy load mostly triggered 429. That limits capacity conclusions, while raw results and telemetry tests make the observations inspectable.”

**Know:** Correlation; cardinality; percentile denominator; closed-loop load; scrape lag.

**Questions:** Is p95 a completion guarantee? No. Can durable percentile gauges be merged like histogram buckets? No.

**Probe:** Collector disappears. Ownership stays in PostgreSQL; missing telemetry is not zero work or success.

**Alternative:** Repeated open-loop trials after choosing a representative workload.

**Inspect/evidence:** [definitions.json](config/observability/definitions.json) and [telemetry_test.go](tests/integration/telemetry_test.go), `TestTelemetryAsyncRPCWorkerLossRetryCancelAndDurableMeasurements`.

**Avoid:** “500 concurrent workflows” or attributing all latency to one unproven bottleneck.

### 13. Controlled agent evaluation

**Spoken answer:** “More agents may spend more without solving more. The evaluation engine compares four configurations over the same frozen tasks and shared total budget. Protected evidence binds success to the tested candidate; reporting retains missing and failed outcomes. The recorded thirty-two runs all use a scripted oracle, so equal success validates experiment plumbing, not role superiority. Larger real-provider trials would require explicit authorization, measured cost, and uncertainty analysis.”

**Know:** Equal total budget; holdout; paired task comparison; omitted denominator; synthetic usage.

**Questions:** Did multi-agent improve success? No real-model evidence establishes that. Does a seeded run order make models deterministic? No.

**Probe:** Reviewer adds calls without improving success. Compare quality, cost, latency, and rejection behavior under equal totals; prefer simpler roles if justified.

**Alternative:** Developer-only baseline when coordination has no demonstrated benefit.

**Inspect/evidence:** [engine_test.go](internal/evaluation/engine_test.go), `TestEqualAllRoleBudgetsAndFixtureRepeatability`.

**Avoid:** Quoting 100% scripted success as model accuracy.

### 14. CI, deployment, backup, and restore

**Spoken answer:** “Durability also needs repeatable verification and recoverable operations. Local gates use owned disposable infrastructure; resume checks exact source and evidence hashes. Private Compose separates trusted roles and secrets. A restore test rehashes artifacts in a separate database before switching stopped writers. AWS templates describe private control/execution hosts, but only offline validation passed. The tradeoff is operational responsibility for single-node state, rather than a demonstrated managed production deployment.”

**Know:** Source fingerprint; owned cleanup; separate restore; retained resources; offline validation.

**Questions:** Does a CI file prove hosted CI passed? No. Does DB rollback reverse a GitHub write? No.

**Probe:** Restored data contains an uncertain publication. Reconcile remote effects before resuming publisher writes.

**Alternative:** Managed state services when availability/operations justify the cost.

**Inspect/evidence:** [stage10_test.go](tests/integration/stage10_test.go), `TestStage10AllSupportedUpgrades`; [operations.json](deploy/aws/operations.json).

**Avoid:** Claiming cloud deployment, HA, or measured recovery-time guarantees.

## Choices and scaling

| Actual choice | Cost and when to reconsider |
| --- | --- |
| PostgreSQL coordination | Atomic state/history/acceptance; coarse locks and scans can limit throughput. Profile before partitioning coordination. |
| NATS JetStream | Durable notification, redelivery, additional service; database polling is simpler at low scale. |
| REST / gRPC / SSE | Snapshots / typed internal calls / one-way updates; three interfaces require contract discipline. |
| Go / Python | Trusted orchestration/tools / provider schema and I/O; generated RPC contracts add maintenance. |
| Docker | Practical bounded fixtures; use stronger isolation for adversarial input. |
| Bounded retries | Finite time/spending; some recoverable tasks still need an operator. |
| Literal retrieval | Transparent and bounded; indexing may help larger repositories. |
| Single or multiple agents | Decomposition/review versus extra calls and integration risk; no measured real quality winner. |

The system targets one owner. Observed admission plateaus reflect configured quotas; they are not maximum hardware capacity. Two workers improved one task-rate sample while worsening admission p95. Future design could profile admission advisory-lock waiting, pool acquisition, scheduler scanning, and monitoring overhead, then consider narrower locking, state partitioning, or separate execution hosts. Representative repeated experiments must justify those changes. Multiple owners additionally require tenant-scoped authorization, budgets, admission fairness, and stronger isolation; adding workers alone does not provide them.

## Three debugging walkthroughs

**Expired worker / rejected result:** Start with `diagnostic_started`, `diagnostic_outcome_rejected`, attempt ID/generation, and correlated traces. Read `tasks.current_attempt_id/dispatch_generation` and `task_attempts.state/lease_expires_at/cleanup_required` in the selected database. Check expired-attempt and retry metrics. Reproduce through `python scripts/dev.py verify-stage-04`, especially `TestTwoReapersOneRecoveryAndStaleToolFence`. A LOST attempt plus successful replacement is expected; do not revive the old lease.

**Committed readiness / missing execution:** Compare `task.ready` history with pending `outbox` and oldest-pending age, relay failures, and broker consumer health. Distinguish missing delivery from failed claim/capacity. Reproduce `TestRealBrokerPublishCrashRedeliveryDuplicatesAndQuarantine` through `verify-stage-03`. Repair transport availability; do not manually mark the task successful. Duplicate notifications are expected recovery inputs.

**Uncertain publication:** Inspect approval binding/status/expiry, `external_operations.status/attempts/reason`, and the adapter's observed branch/head/PR identity. Reproduce fake `pull_timeout` or `record_crash` in `TestGitHubUncertainPublicationRestartsAndBoundsRetries` through `verify-stage-08`. Discover effects before retrying writes. Drift/conflict needs review; exhausted uncertainty needs manual reconciliation. Never erase the intent or assume cancellation removed remote effects.

## Numbers I can defend

All measurements below are local retained evidence for the matching source, not fresh performance tests or production claims. Raw paths are linked in [ENGINEERING_NOTES.md](ENGINEERING_NOTES.md).

| Number | Required context |
| --- | --- |
| 32 evaluation runs, 8/8 passes per configuration | Eight frozen tasks × four configurations × one trial; scripted provider; monetary cost unknown. |
| 120/120 soak successes, zero 429 | Two submissions/second for approximately 61.97 seconds; bounded diagnostic workload. |
| 32 accepted / 468 rejected at 500 submissions | Closed-loop maximum ten HTTP VUs; 32-workflow quota; one worker/one slot. |
| 418.38 ms and 637.04 ms p95 | Respectively 100 submissions/one worker and 50/two; all statuses; both miss 250 ms goal. |
| 7.59 versus 9.66 logical tasks/s | Fifty submitted, 32 accepted in each case; one versus two workers; one repetition, includes observation interval. |
| 133 workflow rows and 24 artifacts restored | Separate disposable PostgreSQL database; artifact bodies rehashed, source preserved. |
| 5.228 → 0.046 ms median query | 80,000 synthetic rows, 40 recent; three warmups/15 trials; index plus stable observation predicate, not whole-system speedup. |

Benchmark hardware: Windows/WSL2, 16 logical CPUs, Docker 29.5.2 with about 3.9 GB VM RAM. No model workload. Retain errors, 429s, and missing results in denominators. The all-cleanup-zero goal remains unmet because two diagnostic stop uncertainties persist; measured sandbox cleanup backlog is zero.

## Final self-check

Can you explain the transaction, race, recovery, and tradeoff without reading this file? Can you distinguish a message from an attempt, a heartbeat from ownership, cancellation from termination, and a scripted pass from model quality? Can you locate the tests and raw evidence, admit the two p95 misses, and say which personal contributions you can substantiate? If not, inspect the named implementation before strengthening the claim.
