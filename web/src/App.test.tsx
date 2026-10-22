import { afterEach, beforeEach, expect, it, vi } from "vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { App } from "./App";
import { mergeEvents } from "./api";

const id = "11111111-1111-4111-8111-111111111111",
  task = "22222222-2222-4222-8222-222222222222";
let cancelled = false;
class Stream {
  static instances: Stream[] = [];
  onopen: (() => void) | null = null;
  onerror: (() => void) | null = null;
  handlers = new Map<string, (event: MessageEvent) => void>();
  constructor(public url: string) {
    Stream.instances.push(this);
  }
  addEventListener(type: string, handler: (event: MessageEvent) => void) {
    this.handlers.set(type, handler);
  }
  close() {}
}
beforeEach(() => {
  cancelled = false;
  location.hash = "";
  Stream.instances = [];
  vi.stubGlobal("EventSource", Stream);
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string, options?: { method?: string }) => {
      let value: unknown;
      let status = 200;
      if (input.startsWith("/v1/workflows?"))
        value = {
          items: [
            {
              id,
              name: "<img src=x onerror=alert(1)>",
              status: "RUNNING",
              created_at: new Date().toISOString(),
              repository_id: "abc",
            },
          ],
          next_after: id,
        };
      else if (input.endsWith("/cancel") && options?.method === "POST") {
        cancelled = true;
        value = { id, status: "CANCELLING" };
      } else if (input.includes("/attempts"))
        value = {
          attempts: [
            {
              id: task,
              state: "SUCCEEDED",
              attempt_number: 1,
              generation: 1,
              last_error_class: "",
              stop_reason: "",
              cleanup_required: false,
              result_metadata: {
                artifacts: [{ id: "patch-id", kind: "patch", sha256: "a" }],
              },
            },
          ],
        };
      else if (input.includes("/artifacts/"))
        value = {
          body: btoa("<script>window.pwned=true</script>"),
          sha256: "exact-digest",
          truncated: false,
        };
      else if (input.endsWith("/graph"))
        value = {
          id,
          status: "RUNNING",
          nodes: [
            { id: task, key: "code", kind: "agent_go", status: "RUNNING" },
          ],
          edges: [],
        };
      else if (input.endsWith("/summary"))
        value = {
          root_id: id,
          calls: 2,
          tokens: 1000,
          estimated_microusd: 100,
          maximum_microusd: 500000,
          unknown_usage_calls: 1,
          estimate_only: true,
          event_bounds: { first: 1, last: 2 },
        };
      else if (input.endsWith("/candidates")) value = [];
      else if (input.startsWith("/v1/projects/")) {
        status = 404;
        value = { error: { message: "not found" } };
      } else
        value = {
          id,
          name: "<img src=x onerror=alert(1)>",
          status: cancelled ? "CANCELLING" : "RUNNING",
          version: 1,
          created_at: new Date().toISOString(),
        };
      return {
        ok: status === 200,
        status,
        json: async () => value,
      } as Response;
    }),
  );
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  location.hash = "";
});
it("shows real API data, escapes repository text and patch evidence, and reports pending cancellation", async () => {
  const { container } = render(<App />);
  fireEvent.click(
    await screen.findByRole("button", { name: "<img src=x onerror=alert(1)>" }),
  );
  await screen.findByText("1 calls with unknown usage; reservations retained");
  expect(container.querySelector("img")).toBeNull();
  fireEvent.click(await screen.findByRole("button", { name: "Patch" }));
  expect(
    await screen.findByText("<script>window.pwned=true</script>"),
  ).toBeTruthy();
  expect(container.querySelector("script")).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Cancel workflow" }));
  await screen.findByText(
    "Cancellation requested; waiting for owned work and cleanup to settle.",
  );
  await waitFor(() => expect(cancelled).toBe(true));
});
it("deduplicates reconnects by workflow/sequence, preserves stable order, and bounds retained events", () => {
  let events: ReturnType<typeof mergeEvents> = [];
  for (let i = 110; i >= 1; i--)
    events = mergeEvents(id, events, {
      sequence: i,
      event_type: "task.claimed",
      payload: {},
      occurred_at: "now",
    });
  expect(events).toHaveLength(100);
  expect(events[0].sequence).toBe(11);
  events = mergeEvents(id, events, {
    sequence: 110,
    event_type: "task.claimed",
    payload: {},
    occurred_at: "now",
  });
  expect(events).toHaveLength(100);
  expect(events.at(-1)?.sequence).toBe(110);
  events = mergeEvents(task, events, {
    sequence: 110,
    event_type: "task.claimed",
    payload: {},
    occurred_at: "now",
  });
  expect(events.filter((e) => e.sequence === 110)).toHaveLength(2);
});
