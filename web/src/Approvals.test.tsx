import { afterEach, expect, it, vi } from "vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { Approvals, type Approval } from "./Approvals";
import { Identity, Session } from "./Session";
import { setSessionCSRF } from "./api";
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  setSessionCSRF("");
});
const approval: Approval = {
  id: "11111111-1111-4111-8111-111111111111",
  root_id: "22222222-2222-4222-8222-222222222222",
  version: 1,
  binding_sha256: "binding-hash",
  status: "PENDING",
  expires_at: new Date(Date.now() + 60000).toISOString(),
  actor: "",
  decided_at: null,
  binding: {
    workflow_id: "33333333-3333-4333-8333-333333333333",
    plan_version: 2,
    repository: {
      id: 42,
      owner: "<img src=x onerror=alert(1)>",
      name: "math-fixture",
      target_branch: "main",
    },
    base_commit: "pinned-base",
    candidate_sha256: "exact-candidate",
    candidate_tree: "git-tree",
    patch_sha256: "patch-proof",
    test_evidence_sha256: "protected-tests",
    review_sha256: "review-proof",
    requested_action: "draft_pull_request",
    policy_version: "controlled-publish-v1",
    policy_flags: [],
    test_demo: true,
  },
};
it("submits the displayed immutable candidate with CSRF and shows the observed fake draft", async () => {
  setSessionCSRF("session-csrf");
  const fetcher = vi.fn(async (_path: string, options?: RequestInit) => {
    if (options?.method === "POST") {
      expect(JSON.parse(options.body as string)).toEqual({
        expected_version: 1,
        binding_sha256: "binding-hash",
        candidate_sha256: "exact-candidate",
        decision: "approve",
        acknowledge_sensitive: false,
      });
      expect(options.headers).toMatchObject({ "X-CSRF-Token": "session-csrf" });
      return new Response(
        JSON.stringify({
          ...approval,
          status: "APPROVED",
          actor: "verified-owner",
          decided_at: new Date().toISOString(),
          operation: {
            id: "operation",
            status: "confirmed",
            branch: "forgeswarm/op-owned",
            head_commit: "exact-head",
            pr_number: 1,
            url: "https://github.com/fixture-owner/math-fixture/pull/1",
            reason: "reconciled_existing_draft",
          },
        }),
        { status: 200 },
      );
    }
    return new Response(JSON.stringify({ items: [approval] }), { status: 200 });
  });
  vi.stubGlobal("fetch", fetcher);
  render(
    <Identity.Provider value={{ role: "operator", actor: "verified-owner" }}>
      <Approvals root={approval.root_id} />
    </Identity.Provider>,
  );
  await screen.findByText("exact-candidate");
  expect(document.querySelector("img")).toBeNull();
  fireEvent.click(
    screen.getByRole("button", { name: "Approve exact candidate" }),
  );
  await screen.findByText("Publication: confirmed");
  expect(
    screen.getByText(
      "Controlled fake draft PR #1; no real GitHub PR was created.",
    ),
  ).toBeTruthy();
  expect(
    screen.queryByRole("link", { name: /Open observed draft/ }),
  ).toBeNull();
});
it("read-only identity sees evidence but cannot decide", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(JSON.stringify({ items: [approval] }), { status: 200 }),
    ),
  );
  render(
    <Identity.Provider value={{ role: "read_only", actor: "observer" }}>
      <Approvals root={approval.root_id} />
    </Identity.Provider>,
  );
  await screen.findByText("exact-candidate");
  expect(
    screen.queryByRole("button", { name: "Approve exact candidate" }),
  ).toBeNull();
  expect(
    screen.getByText("Read-only access. An operator must decide."),
  ).toBeTruthy();
});
it("session login keeps the credential out of browser storage", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (path: string, options?: RequestInit) => {
      if (path === "/v1/session" && options?.method === "POST") {
        expect(JSON.parse(options.body as string)).toEqual({
          token: "configured-operator-credential",
        });
        return new Response(
          JSON.stringify({
            enabled: true,
            role: "operator",
            actor: "owner",
            csrf: "opaque-csrf",
          }),
          { status: 200 },
        );
      }
      return new Response(
        JSON.stringify({ error: { message: "Sign in required." } }),
        { status: 401 },
      );
    }),
  );
  render(
    <Session>
      <p>Authenticated workspace</p>
    </Session>,
  );
  await screen.findByRole("heading", { name: "Sign in to ForgeSwarm" });
  fireEvent.change(screen.getByLabelText("Access token"), {
    target: { value: "configured-operator-credential" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Sign in" }));
  await waitFor(() =>
    expect(screen.getByText("Authenticated workspace")).toBeTruthy(),
  );
  expect(localStorage.length).toBe(0);
  expect(sessionStorage.length).toBe(0);
  expect(screen.queryByLabelText("Access token")).toBeNull();
});
