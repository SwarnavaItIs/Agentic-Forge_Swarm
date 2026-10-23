import { useContext, useEffect, useRef, useState } from "react";
import { api } from "./api";
import { Identity } from "./Session";
export type Approval = {
  id: string;
  root_id: string;
  version: number;
  binding_sha256: string;
  status: string;
  expires_at: string;
  actor: string;
  decided_at: string | null;
  binding: {
    workflow_id: string;
    plan_version: number;
    repository: {
      id: number;
      owner: string;
      name: string;
      target_branch: string;
    };
    base_commit: string;
    candidate_sha256: string;
    candidate_tree: string;
    patch_sha256: string;
    test_evidence_sha256: string;
    review_sha256: string;
    requested_action: string;
    policy_version: string;
    policy_flags: string[];
    test_demo: boolean;
  };
  operation?: {
    id: string;
    status: string;
    branch: string;
    head_commit: string;
    pr_number: number;
    url: string;
    reason: string;
  };
};
export function Approvals({ root }: { root: string }) {
  const identity = useContext(Identity),
    [items, setItems] = useState<Approval[]>([]),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(""),
    [ack, setAck] = useState(false);
  useEffect(() => {
    const controller = new AbortController();
    let disposed = false,
      running = false;
    const load = async () => {
      if (running) return;
      running = true;
      try {
        const result = await api<{ items: Approval[] }>(
          "/v1/approvals?root=" + encodeURIComponent(root),
          controller.signal,
        );
        if (!disposed) {
          setItems(result.items);
          setError("");
        }
      } catch (e) {
        if (!disposed)
          setError(e instanceof Error ? e.message : "Approval load failed.");
      } finally {
        running = false;
      }
    };
    void load();
    const timer = setInterval(() => void load(), 3000);
    return () => {
      disposed = true;
      controller.abort();
      clearInterval(timer);
    };
  }, [root]);
  async function decide(a: Approval, decision: string) {
    setBusy(a.id);
    setError("");
    try {
      const result = await api<Approval>(
        "/v1/approvals/" + a.id,
        undefined,
        "POST",
        {
          expected_version: a.version,
          binding_sha256: a.binding_sha256,
          candidate_sha256: a.binding.candidate_sha256,
          decision,
          acknowledge_sensitive: ack,
        },
      );
      setItems((current) =>
        current.map((v) => (v.id === result.id ? result : v)),
      );
    } catch (e) {
      setError(
        e instanceof Error
          ? e.message
          : "Decision failed. Reload the exact approval before retrying.",
      );
    } finally {
      setBusy("");
    }
  }
  return (
    <section className="panel approval-panel">
      <h2>Publication approval</h2>
      <p>
        Publishing requires an operator decision for this exact candidate.
        Target drift requires a new reviewed change. Cancellation cannot undo an
        external write already in flight.
      </p>
      {error && <p role="alert">{error}</p>}
      {!items.length && !error && (
        <p>No publication request for this workflow.</p>
      )}
      {items.map((a) => (
        <article key={a.id}>
          <h3>
            {a.binding.test_demo
              ? "TEST/DEMO GitHub draft"
              : "GitHub draft pull request"}{" "}
            · {a.status.replaceAll("_", " ")}
          </h3>
          <p>
            {a.binding.repository.owner}/{a.binding.repository.name} →{" "}
            {a.binding.repository.target_branch} · Plan v
            {a.binding.plan_version}
          </p>
          <dl className="approval-digests">
            <dt>Base commit</dt>
            <dd>{a.binding.base_commit}</dd>
            <dt>Candidate SHA-256</dt>
            <dd>{a.binding.candidate_sha256}</dd>
            <dt>Candidate Git tree</dt>
            <dd>{a.binding.candidate_tree}</dd>
            <dt>Patch SHA-256</dt>
            <dd>{a.binding.patch_sha256}</dd>
            <dt>Protected test evidence</dt>
            <dd>{a.binding.test_evidence_sha256}</dd>
            <dt>Read-only review evidence</dt>
            <dd>{a.binding.review_sha256}</dd>
            <dt>Action / policy</dt>
            <dd>
              {a.binding.requested_action} / {a.binding.policy_version}
            </dd>
            <dt>Expires</dt>
            <dd>{new Date(a.expires_at).toLocaleString()}</dd>
          </dl>
          <a href={"#workflow/" + a.binding.workflow_id}>
            Inspect tested execution and patch
          </a>
          {a.binding.policy_flags.map((flag) => (
            <p className="notice" key={flag}>
              {flag.replaceAll("_", " ")}
            </p>
          ))}
          {a.status === "PENDING" && identity.role === "operator" && (
            <div className="approval-actions">
              {a.binding.policy_flags.length > 0 && (
                <label>
                  <input
                    type="checkbox"
                    checked={ack}
                    onChange={(e) => setAck(e.target.checked)}
                  />
                  I reviewed the flagged sensitive changes.
                </label>
              )}
              <button
                disabled={
                  busy !== "" ||
                  Date.parse(a.expires_at) <= Date.now() ||
                  (a.binding.policy_flags.length > 0 && !ack)
                }
                onClick={() => void decide(a, "approve")}
              >
                Approve exact candidate
              </button>
              <button
                className="danger"
                disabled={busy !== ""}
                onClick={() => void decide(a, "reject")}
              >
                Reject publication
              </button>
            </div>
          )}
          {identity.role === "read_only" && (
            <p>Read-only access. An operator must decide.</p>
          )}
          {a.actor && (
            <p>
              Decision by {a.actor} ·{" "}
              {a.decided_at ? new Date(a.decided_at).toLocaleString() : ""}
            </p>
          )}
          {a.operation && (
            <div className="notice">
              <strong>
                Publication: {a.operation.status.replaceAll("_", " ")}
              </strong>
              <p>{a.operation.reason.replaceAll("_", " ")}</p>
              <p className="mono">{a.operation.branch}</p>
              {a.operation.pr_number > 0 &&
                (a.binding.test_demo ? (
                  <p>
                    Controlled fake draft PR #{a.operation.pr_number}; no real
                    GitHub PR was created.
                  </p>
                ) : (
                  <a href={a.operation.url} target="_blank" rel="noreferrer">
                    Open observed draft PR #{a.operation.pr_number}
                  </a>
                ))}
            </div>
          )}
        </article>
      ))}
    </section>
  );
}
export function ImportIssue() {
  const identity = useContext(Identity),
    [number, setNumber] = useState(""),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false);
  const attempt = useRef({ number: "", key: "" });
  if (identity.role !== "operator") return null;
  return (
    <section className="panel issue-import">
      <h2>Import an authorized GitHub issue</h2>
      <p>
        The configured repository, trigger policy, protected tests and execution
        budget apply. Importing creates no PR.
      </p>
      <form
        onSubmit={async (e) => {
          e.preventDefault();
          setBusy(true);
          setError("");
          if (attempt.current.number !== number)
            attempt.current = { number, key: crypto.randomUUID() };
          try {
            const receipt = await api<{ id: string }>(
              "/v1/github/import",
              undefined,
              "POST",
              { issue_number: Number(number) },
              attempt.current.key,
            );
            location.hash = "#workflow/" + receipt.id;
          } catch (e) {
            setError(e instanceof Error ? e.message : "Import failed.");
          } finally {
            setBusy(false);
          }
        }}
      >
        <label htmlFor="issue-number">Issue number</label>
        <input
          id="issue-number"
          type="number"
          min="1"
          max="1000000000"
          value={number}
          onChange={(e) => setNumber(e.target.value)}
          required
        />
        <button disabled={busy}>{busy ? "Importing…" : "Import issue"}</button>
      </form>
      {error && <p role="alert">{error}</p>}
    </section>
  );
}
