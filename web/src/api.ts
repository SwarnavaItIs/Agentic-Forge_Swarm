export type EventRecord = {
  sequence: number;
  event_type: string;
  payload: unknown;
  occurred_at: string;
};
export type Row = {
  id: string;
  name: string;
  status: string;
  created_at: string;
  repository_id: string;
  root_id?: string;
};
export type Task = { id: string; key: string; kind: string; status: string };
export type Graph = {
  id: string;
  status: string;
  nodes: Task[];
  edges: { task_id: string; depends_on_task_id: string }[];
};
export type Details = {
  id: string;
  name: string;
  status: string;
  created_at: string;
  terminal_reason?: string;
  version: number;
};
export type Summary = {
  root_id: string;
  calls: number;
  tokens: number;
  estimated_microusd: number;
  maximum_microusd: number | null;
  unknown_usage_calls: number;
  estimate_only: boolean;
  event_bounds: { first: number; last: number };
};
export type Project = {
  id: string;
  status: string;
  reason: string;
  round: number;
  max_rounds: number;
  versions: {
    version: number;
    phase: string;
    workflow_id: string;
    status: string;
    base_sha256: string;
  }[];
  plans: { version: number; digest: string }[];
};
export type Attempt = {
  id: string;
  state: string;
  attempt_number: number;
  generation: number;
  last_error_class: string;
  stop_reason: string;
  cleanup_required: boolean;
  result_metadata?: {
    reason?: string;
    candidate_sha256?: string;
    artifacts?: { id: string; kind: string; sha256: string }[];
  };
};
export type Candidate = {
  attempt_id: string;
  role: string;
  accepted: boolean;
  candidate_sha256: string;
  test_sha256: string;
  body: {
    review?: {
      decision: string;
      findings: {
        severity: string;
        file: string;
        line: number;
        rationale: string;
      }[];
    };
    work: { inputs: { task: string; artifact_sha256: string }[] };
    reason: string;
  };
};
export class APIError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}
export async function api<T>(
  path: string,
  signal?: AbortSignal,
  method = "GET",
): Promise<T> {
  const response = await fetch(path, { method, signal });
  const value = await response.json();
  if (!response.ok)
    throw new APIError(
      response.status,
      value.error?.message ?? "Request failed.",
    );
  return value as T;
}
// The durable identity is (workflow, sequence). Duplicate replay is expected;
// memory stays bounded even after a long-running stream or repeated reconnects.
export function mergeEvents(
  workflow: string,
  items: (EventRecord & { workflow: string })[],
  event: EventRecord,
) {
  const byId = new Map(items.map((v) => [`${v.workflow}:${v.sequence}`, v]));
  byId.set(`${workflow}:${event.sequence}`, { ...event, workflow });
  return [...byId.values()]
    .sort(
      (a, b) => a.workflow.localeCompare(b.workflow) || a.sequence - b.sequence,
    )
    .slice(-100);
}
export function artifactText(body: string) {
  return new TextDecoder().decode(
    Uint8Array.from(atob(body), (c) => c.charCodeAt(0)),
  );
}
