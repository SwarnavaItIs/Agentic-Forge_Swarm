import { createContext, useEffect, useState, type ReactNode } from "react";
import { api, setSessionCSRF } from "./api";
export const Identity = createContext({
  role: "local_operator",
  actor: "local",
});
type Session = { enabled: boolean; role: string; actor: string; csrf?: string };
export function Session({ children }: { children: ReactNode }) {
  const [session, setSession] = useState<Session | null>(null);
  const [token, setToken] = useState("");
  const [error, setError] = useState("");
  const [checking, setChecking] = useState(true);
  const [busy, setBusy] = useState(false);
  function accept(value: Session) {
    setSessionCSRF(value.csrf ?? "");
    setSession(value);
    setToken("");
    setError("");
  }
  useEffect(() => {
    const controller = new AbortController();
    api<Session>("/v1/session", controller.signal)
      .then(accept)
      .catch((e) => {
        if (!controller.signal.aborted) setError(e.message);
      })
      .finally(() => {
        if (!controller.signal.aborted) setChecking(false);
      });
    return () => controller.abort();
  }, []);
  if (checking)
    return (
      <main className="page">
        <p>Checking operator session…</p>
      </main>
    );
  if (!session)
    return (
      <main className="page">
        <section className="panel login-panel">
          <p className="eyebrow">PRIVATE OPERATOR CONSOLE</p>
          <h1>Sign in to ForgeSwarm</h1>
          <p>Use your configured operator or read-only access token.</p>
          <form
            onSubmit={async (e) => {
              e.preventDefault();
              setBusy(true);
              try {
                accept(
                  await api<Session>("/v1/session", undefined, "POST", {
                    token,
                  }),
                );
              } catch (e) {
                setError(e instanceof Error ? e.message : "Sign-in failed.");
              } finally {
                setBusy(false);
              }
            }}
          >
            <label htmlFor="access-token">Access token</label>
            <input
              id="access-token"
              type="password"
              autoComplete="off"
              value={token}
              onChange={(e) => setToken(e.target.value)}
              required
            />
            <button disabled={busy || !token}>
              {busy ? "Signing in…" : "Sign in"}
            </button>
          </form>
          {error && <p role="alert">{error}</p>}
        </section>
      </main>
    );
  return (
    <Identity.Provider value={session}>
      {session.enabled && (
        <div className="session-bar">
          <span>
            {session.actor} · {session.role.replaceAll("_", " ")}
          </span>
          <button
            onClick={async () => {
              try {
                await api("/v1/session", undefined, "DELETE");
                setSessionCSRF("");
                setSession(null);
              } catch (e) {
                setError(e instanceof Error ? e.message : "Sign-out failed.");
              }
            }}
          >
            Sign out
          </button>
          {error && <span role="alert">{error}</span>}
        </div>
      )}
      {children}
    </Identity.Provider>
  );
}
