import { useContext, useEffect, useRef, useState } from "react";
import { Identity } from "./Session";
import { Approvals, ImportIssue } from "./Approvals";
import {
  api,
  APIError,
  artifactText,
  mergeEvents,
  type Attempt,
  type Candidate,
  type Details,
  type EventRecord,
  type Graph,
  type Project,
  type Row,
  type Summary,
} from "./api";

const terminal = new Set([
  "SUCCEEDED",
  "FAILED",
  "CANCELLED",
  "NEEDS_HUMAN",
  "PUBLISHED",
]);
const short = (id: string) => id.slice(0, 12);
function age(at: string) {
  const minutes = Math.max(
    0,
    Math.floor((Date.now() - Date.parse(at)) / 60000),
  );
  return minutes < 1
    ? "Just now"
    : minutes < 60
      ? `${minutes}m ago`
      : `${Math.floor(minutes / 60)}h ago`;
}
export function Status({ value }: { value: string }) {
  return (
    <span className={`status status-${value.toLowerCase()}`}>
      {value.replaceAll("_", " ")}
    </span>
  );
}
export function App() {
  const identity = useContext(Identity);
  const initial = () =>
    /^#workflow\/[0-9a-f-]{36}$/.test(location.hash)
      ? location.hash.slice(10)
      : "";
  const [selected, setSelected] = useState(initial),
    [rows, setRows] = useState<Row[]>([]),
    [after, setAfter] = useState(""),
    [next, setNext] = useState(""),
    [listError, setListError] = useState("");
  const [listRevision, setListRevision] = useState(0);
  const artifactRequest = useRef(0);
  const [data, setData] = useState<{
      detail: Details;
      graph: Graph;
      summary: Summary;
      candidates: Candidate[];
      project: Project | null;
    } | null>(null),
    [error, setError] = useState(""),
    [events, setEvents] = useState<(EventRecord & { workflow: string })[]>([]),
    [streamState, setStreamState] = useState("Connecting"),
    [gap, setGap] = useState("");
  const [task, setTask] = useState(""),
    [attempts, setAttempts] = useState<Attempt[]>([]),
    [preview, setPreview] = useState<{
      kind: string;
      text: string;
      sha: string;
    } | null>(null),
    [cancelPending, setCancelPending] = useState(false);
  const reload = useRef<() => void>(() => {});
  useEffect(() => {
    artifactRequest.current++;
    setPreview(null);
  }, [selected, task]);
  useEffect(() => {
    const handler = () => setSelected(initial());
    window.addEventListener("hashchange", handler);
    return () => window.removeEventListener("hashchange", handler);
  }, []);
  useEffect(() => {
    const controller = new AbortController();
    api<{ items: Row[]; next_after: string }>(
      `/v1/workflows?limit=20${after ? "&after=" + encodeURIComponent(after) : ""}`,
      controller.signal,
    )
      .then((v) => {
        setRows(v.items);
        setNext(v.next_after);
        setListError("");
      })
      .catch((e) => {
        if (!controller.signal.aborted) setListError(String(e.message));
      });
    return () => controller.abort();
  }, [after, selected, listRevision]);
  useEffect(() => {
    setData(null);
    setEvents([]);
    setTask("");
    setAttempts([]);
    setPreview(null);
    setGap("");
    setError("");
    setCancelPending(false);
    if (!selected) return;
    const controller = new AbortController();
    let busy = false,
      cursor = 0,
      source: EventSource | null = null,
      disposed = false;
    const load = async () => {
      if (busy || disposed) return;
      busy = true;
      try {
        const [detail, graph, summary, candidates] = await Promise.all([
          api<Details>(`/v1/workflows/${selected}`, controller.signal),
          api<Graph>(`/v1/workflows/${selected}/graph`, controller.signal),
          api<Summary>(`/v1/workflows/${selected}/summary`, controller.signal),
          api<Candidate[]>(
            `/v1/workflows/${selected}/candidates`,
            controller.signal,
          ),
        ]);
        let project: Project | null = null;
        try {
          project = await api<Project>(
            `/v1/projects/${summary.root_id}`,
            controller.signal,
          );
        } catch (e) {
          if (!(e instanceof APIError && e.status === 404)) throw e;
        }
        if (!disposed) {
          setData({ detail, graph, summary, candidates, project });
          setTask((current) => current || graph.nodes[0]?.id || "");
          setError("");
          if (terminal.has(project?.status ?? detail.status))
            setCancelPending(false);
        }
      } catch (e) {
        if (!disposed)
          setError(e instanceof Error ? e.message : "Unable to load workflow.");
      } finally {
        busy = false;
      }
    };
    reload.current = () => {
      void load();
    };
    void load();
    const connect = () => {
      if (disposed) return;
      source = new EventSource(
        `/v1/workflows/${selected}/stream?after=${cursor}`,
      );
      source.onopen = () => setStreamState("Live");
      source.addEventListener("workflow", (event) => {
        try {
          const item = JSON.parse((event as MessageEvent).data) as EventRecord;
          if (!Number.isSafeInteger(item.sequence) || item.sequence <= cursor)
            return;
          cursor = item.sequence;
          setEvents((old) => mergeEvents(selected, old, item));
        } catch {
          setStreamState("Invalid event; reconnecting");
        }
      });
      const recover = async () => {
        if (disposed) return;
        try {
          const summary = await api<Summary>(
            `/v1/workflows/${selected}/summary`,
            controller.signal,
          );
          if (cursor < summary.event_bounds.first - 1) {
            source?.close();
            cursor = summary.event_bounds.last;
            setGap(
              "Earlier events are no longer retained. Current persisted details were refreshed.",
            );
            setEvents([]);
            await load();
            connect();
          }
        } catch {
          /* EventSource reconnects; execution continues independently. */
        }
      };
      source.addEventListener("retention_gap", () => {
        void recover();
      });
      source.onerror = () => {
        if (!disposed) {
          setStreamState("Reconnecting");
          void recover();
        }
      };
    };
    connect();
    const timer = window.setInterval(() => {
      void load();
    }, 2000);
    return () => {
      disposed = true;
      controller.abort();
      source?.close();
      window.clearInterval(timer);
    };
  }, [selected]);
  useEffect(() => {
    if (!selected || !task) return;
    const controller = new AbortController();
    api<{ attempts: Attempt[] }>(
      `/v1/workflows/${selected}/tasks/${task}/attempts?limit=10`,
      controller.signal,
    )
      .then((v) => setAttempts(v.attempts))
      .catch((e) => {
        if (!controller.signal.aborted) setError(e.message);
      });
    return () => controller.abort();
  }, [selected, task, data?.detail.version]);
  const choose = (id: string) => {
    location.hash = "workflow/" + id;
    setSelected(id);
  };
  const inspect = async (id: string, kind: string) => {
    const request = ++artifactRequest.current;
    setPreview(null);
    try {
      const v = await api<{
        body?: string;
        sha256: string;
        truncated: boolean;
      }>(`/v1/workflows/${selected}/artifacts/${id}`);
      if (request !== artifactRequest.current) return;
      setPreview({
        kind,
        text:
          artifactText(v.body ?? "") +
          (v.truncated ? "\n[output truncated]" : ""),
        sha: v.sha256,
      });
    } catch (e) {
      if (request !== artifactRequest.current) return;
      setError(e instanceof Error ? e.message : "Artifact unavailable.");
    }
  };
  const cancel = async () => {
    setCancelPending(true);
    try {
      await api(
        `/v1/workflows/${data?.project?.id ?? selected}/cancel`,
        undefined,
        "POST",
      );
      reload.current();
    } catch (e) {
      setCancelPending(false);
      setError(e instanceof Error ? e.message : "Cancellation failed.");
    }
  };
  const status = data?.project?.status ?? data?.detail.status;
  return (
    <div className="shell">
      <aside className="rail">
        <a className="brand" href="#" onClick={() => setSelected("")}>
          <span className="brand-mark">F</span> ForgeSwarm
        </a>
        <span className="rail-label">WORKSPACE</span>
        <a className="nav-active" href="#" onClick={() => setSelected("")}>
          ▦ &nbsp; Workflows
        </a>
        <div className="rail-bottom">
          <span className="online-dot" /> Local operator console
          <small>Persisted state · bounded execution</small>
        </div>
      </aside>
      <main>
        <header className="topbar">
          <span>
            Workspace <span className="muted">/</span>{" "}
            {selected ? "Workflow detail" : "Workflows"}
          </span>
          <span className="environment">
            <span className="online-dot" /> LOCAL
          </span>
        </header>
        {!selected ? (
          <section className="page">
            <div className="page-heading">
              <div>
                <p className="eyebrow">EXECUTION OVERVIEW</p>
                <h1>Workflows</h1>
                <p className="muted">
                  Follow work from validated plan to reviewed candidate.
                </p>
              </div>
              <button
                onClick={() => {
                  setAfter("");
                  setListRevision((value) => value + 1);
                  setSelected("");
                  location.hash = "";
                }}
              >
                Refresh list
              </button>
            </div>
            {listError && <p role="alert">{listError}</p>}
            <ImportIssue />
            <div className="panel">
              <div className="panel-heading">
                <h2>Persisted workflows</h2>
                <span className="muted">Up to 20 per page</span>
              </div>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Workflow</th>
                      <th>Status</th>
                      <th>Repository</th>
                      <th>Created</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((row) => (
                      <tr key={row.id}>
                        <td>
                          <button
                            className="row-link"
                            onClick={() => choose(row.id)}
                          >
                            {row.name}
                          </button>
                          <small className="mono">
                            {short(row.id)}
                            {row.root_id && row.root_id !== row.id
                              ? " · child execution"
                              : ""}
                          </small>
                        </td>
                        <td>
                          <Status value={row.status} />
                        </td>
                        <td className="mono">
                          {row.repository_id ? short(row.repository_id) : "—"}
                        </td>
                        <td className="muted">{age(row.created_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {!rows.length && !listError && (
                  <p className="empty">No workflows on this page.</p>
                )}
              </div>
              <footer className="table-footer">
                <button onClick={() => setAfter("")} disabled={!after}>
                  First page
                </button>
                <button
                  onClick={() => setAfter(next)}
                  disabled={rows.length < 20 || next === after}
                >
                  Next page →
                </button>
              </footer>
            </div>
          </section>
        ) : (
          <section className="page">
            {error && (
              <p role="alert" className="error">
                {error}
              </p>
            )}
            {!data ? (
              <p className="empty">Loading persisted workflow…</p>
            ) : (
              <>
                <div className="page-heading">
                  <div>
                    <p className="eyebrow">
                      WORKFLOW / <span className="mono">{short(selected)}</span>
                    </p>
                    <h1>{data.detail.name}</h1>
                    <div className="inline">
                      <Status value={status ?? data.detail.status} />
                      <span className="muted">
                        Created {age(data.detail.created_at)}
                      </span>
                    </div>
                  </div>
                  <button
                    className="danger"
                    onClick={() => void cancel()}
                    disabled={
                      cancelPending ||
                      identity.role === "read_only" ||
                      terminal.has(status ?? "") ||
                      status === "CANCELLING"
                    }
                  >
                    Cancel workflow
                  </button>
                </div>
                {(cancelPending || status === "CANCELLING") && (
                  <p role="status" className="notice">
                    Cancellation requested; waiting for owned work and cleanup
                    to settle.
                  </p>
                )}
                {status === "CANCELLED" && (
                  <p role="status" className="notice">
                    Cancellation settled.
                  </p>
                )}
                {gap && (
                  <p role="status" className="notice">
                    {gap}
                  </p>
                )}
                {data.project &&
                  [
                    "WAITING_FOR_APPROVAL",
                    "PUBLISHING",
                    "PUBLISHED",
                    "NEEDS_HUMAN",
                    "CANCELLED",
                  ].includes(data.project.status) && (
                    <Approvals root={data.project.id} />
                  )}
                <div className="metrics">
                  <div>
                    <span>MODEL CALLS</span>
                    <strong>{data.summary.calls}</strong>
                  </div>
                  <div>
                    <span>
                      TOKENS{" "}
                      {data.summary.unknown_usage_calls ? "· ESTIMATED" : ""}
                    </span>
                    <strong>{data.summary.tokens.toLocaleString()}</strong>
                  </div>
                  <div>
                    <span>ESTIMATED COST</span>
                    <strong>
                      ${(data.summary.estimated_microusd / 1e6).toFixed(4)}
                    </strong>
                    <small>
                      {data.summary.unknown_usage_calls
                        ? `${data.summary.unknown_usage_calls} calls with unknown usage; reservations retained`
                        : "Provider-reported or fixture usage; monetary estimate only"}
                    </small>
                  </div>
                  <div>
                    <span>WORKFLOW ALLOWANCE</span>
                    <strong>
                      {data.summary.maximum_microusd === null
                        ? "Unknown"
                        : `$${(data.summary.maximum_microusd / 1e6).toFixed(2)}`}
                    </strong>
                    <small>Shared across roles and correction rounds</small>
                  </div>
                </div>
                {data.project && (
                  <div className="panel version-panel">
                    <div className="panel-heading">
                      <h2>Immutable plan versions</h2>
                      <span>
                        Correction {data.project.round} /{" "}
                        {data.project.max_rounds}
                      </span>
                    </div>
                    <p className="muted">
                      {data.project.reason ||
                        "Each version keeps its original artifacts and exact candidate provenance."}
                    </p>
                    <div className="version-links">
                      {data.project.versions.map((v) => (
                        <button
                          key={v.workflow_id}
                          onClick={() => choose(v.workflow_id)}
                        >
                          v{v.version} · {v.phase} <Status value={v.status} />
                        </button>
                      ))}
                    </div>
                  </div>
                )}
                <div className="detail-grid">
                  <div className="panel">
                    <div className="panel-heading">
                      <h2>Tasks & dependencies</h2>
                      <span className="muted">
                        {data.graph.nodes.length} tasks
                      </span>
                    </div>
                    <div className="tasks">
                      {data.graph.nodes.map((node) => (
                        <button
                          className={`task ${task === node.id ? "task-selected" : ""}`}
                          key={node.id}
                          onClick={() => {
                            setTask(node.id);
                            setPreview(null);
                          }}
                        >
                          <div>
                            <strong>{node.key}</strong>
                            <Status value={node.status} />
                          </div>
                          <small>
                            {node.kind} ·{" "}
                            {data.graph.edges
                              .filter((e) => e.task_id === node.id)
                              .map(
                                (e) =>
                                  data.graph.nodes.find(
                                    (n) => n.id === e.depends_on_task_id,
                                  )?.key,
                              )
                              .join(", ") || "No dependencies"}
                          </small>
                        </button>
                      ))}
                    </div>
                  </div>
                  <div className="panel">
                    <div className="panel-heading">
                      <h2>Attempts & evidence</h2>
                      <span className="muted">Latest page · up to 10</span>
                    </div>
                    <div className="attempts">
                      {!attempts.length && (
                        <p className="muted">
                          No accepted attempt has started.
                        </p>
                      )}
                      {attempts.map((attempt) => (
                        <article key={attempt.id}>
                          <div className="inline">
                            <strong>Attempt {attempt.attempt_number}</strong>
                            <Status value={attempt.state} />
                          </div>
                          <p className="mono muted">
                            {short(attempt.id)} · generation{" "}
                            {attempt.generation}
                          </p>
                          {(attempt.result_metadata?.reason ||
                            attempt.last_error_class ||
                            attempt.stop_reason) && (
                            <p>
                              {attempt.result_metadata?.reason ||
                                attempt.last_error_class ||
                                attempt.stop_reason}
                            </p>
                          )}
                          {attempt.cleanup_required && (
                            <p className="notice">Cleanup is pending.</p>
                          )}
                          <div className="inline">
                            {attempt.result_metadata?.artifacts
                              ?.filter(
                                (a) =>
                                  a.kind === "patch" ||
                                  a.kind === "test_result",
                              )
                              .map((a) => (
                                <button
                                  key={a.id}
                                  onClick={() => void inspect(a.id, a.kind)}
                                >
                                  {a.kind === "patch" ? "Patch" : "Test result"}
                                </button>
                              ))}
                          </div>
                        </article>
                      ))}
                    </div>
                  </div>
                </div>
                {preview && (
                  <div className="panel">
                    <div className="panel-heading">
                      <h2>
                        {preview.kind === "patch"
                          ? "Patch preview"
                          : "Protected test result"}
                      </h2>
                      <button onClick={() => setPreview(null)}>Close</button>
                    </div>
                    <p className="mono muted digest">SHA-256 {preview.sha}</p>
                    <pre aria-label="Artifact content">{preview.text}</pre>
                  </div>
                )}
                {data.candidates.some((c) => c.role === "reviewer") && (
                  <div className="panel">
                    <div className="panel-heading">
                      <h2>Candidate review</h2>
                      <span className="muted">
                        Additional model check · deterministic tests remain
                        required
                      </span>
                    </div>
                    {data.candidates
                      .filter((c) => c.role === "reviewer")
                      .map((c) => (
                        <article className="review" key={c.attempt_id}>
                          <strong>
                            {c.body.review?.decision ?? "Unavailable"}
                          </strong>
                          <p className="mono digest">
                            Candidate {c.candidate_sha256}
                          </p>
                          {c.body.review?.findings.map((f, i) => (
                            <p key={i}>
                              {f.severity} · {f.file}:{f.line} — {f.rationale}
                            </p>
                          ))}
                        </article>
                      ))}
                  </div>
                )}
                <div className="panel">
                  <div className="panel-heading">
                    <h2>Event timeline</h2>
                    <span className="muted">
                      <span className="online-dot" /> {streamState} · latest 100
                      · replay deduplicated by sequence
                    </span>
                  </div>
                  <div className="timeline">
                    {events.map((e) => (
                      <article key={`${e.workflow}:${e.sequence}`}>
                        <span className="event-seq">{e.sequence}</span>
                        <div>
                          <strong>{e.event_type}</strong>
                          <p>{JSON.stringify(e.payload)}</p>
                        </div>
                        <time>
                          {new Date(e.occurred_at).toLocaleTimeString()}
                        </time>
                      </article>
                    ))}
                  </div>
                </div>
              </>
            )}
          </section>
        )}
        <footer className="page-foot">
          ForgeSwarm · Local, controlled execution. Scripted demos do not
          measure live model capability.
        </footer>
      </main>
    </div>
  );
}
