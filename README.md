# ForgeSwarm

ForgeSwarm runs durable workflows for controlled coding tasks. PostgreSQL stores workflow state, task attempts, and approvals. A scheduler and NATS JetStream dispatch work to Go workers, which enforce tool permissions and run protected tests in constrained Docker containers. A separate Python runtime supplies model proposals, and a React dashboard shows workflow progress, results, and approval requests.

The local demo uses vetted Go fixtures, a scripted provider, and a fake GitHub adapter. The deployment recipe targets a private, single-owner installation. Docker provides resource limits and process isolation with a shared kernel, so use vetted inputs.

## Run locally

Install Git, Docker with the Compose plugin and a Linux engine, Go `1.26.10`, Python `3.12.10`, Node.js `24.16.0` with npm, and uv `0.13.0`. On Windows, start Docker Desktop in Linux container mode. The first run needs network access for pinned dependencies and images. Run commands from the repository root.

```sh
python scripts/dev.py demo
```

The command builds the services and starts the private Compose stack. It runs a parallel workflow, checks worker recovery, and runs the coding fixture through protected tests, review, and fake draft publication. It prints the completed workflow link, dashboard and monitoring URLs, and paths to the generated operator credentials and private CA. Trust the generated CA in your browser and use the local operator token to sign in.

Use `python scripts/dev.py platform-up` to start the stack without submitting demo work. `python scripts/dev.py platform-down` stops it and retains local volumes. `python scripts/dev.py help` lists additional development and verification commands. The Compose demo supplies its own settings and credentials. See [`.env.example`](.env.example) for settings when running individual services.

## Build and test

```sh
python scripts/dev.py build
python scripts/dev.py test
python scripts/dev.py test-integration
```

The integration command uses a disposable Docker PostgreSQL instance. To check the runtime and dashboard separately, use the committed lockfiles.

```sh
uv run --project agent-runtime --locked pytest agent-runtime/tests
npm ci --prefix web --ignore-scripts
npm --prefix web test
npm --prefix web run build
```

The full local gate is `python scripts/dev.py verify-all`. It includes browser tests, recovery checks, performance runs, and offline deployment validation. Its tool pins and setup are in the [CI workflow](.github/workflows/stage-10.yaml) and [verification tool configuration](config/security-tools.json). Detailed results stay in `artifacts/verification/`. Use `python scripts/dev.py eval-fixture` for the scripted evaluation or `go run ./cmd/evaluate -catalog` to print its catalog directly from the evaluator.

## Repository map

| Path | Contents |
| --- | --- |
| [`cmd/`](cmd/) and [`internal/`](internal/) | Go services, workflow state, tools, and publication logic |
| [`api/`](api/) and [`migrations/`](migrations/) | RPC contracts and database schema |
| [`agent-runtime/`](agent-runtime/) and [`web/`](web/) | Python provider runtime and React dashboard |
| [`tests/`](tests/) and [`scripts/`](scripts/) | Fixtures, integration tests, and development commands |
| [`config/`](config/), [`performance/`](performance/), and [`deploy/aws/`](deploy/aws/) | Local service configuration, load scenarios, and private AWS deployment templates |

Keep `.cache/` local. It holds build caches as well as credentials, TLS keys, and local installation state. Lockfiles, checked-in RPC bindings, test fixtures, and example configuration are part of the source tree.
