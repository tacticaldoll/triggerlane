# Triggerlane

Triggerlane is the declarative, event-driven trigger and routing plane on a
Worklane execution substrate — the first-class `event -> trigger -> job` layer
(with replay) that Celery-class task queues leave out.

```text
Event
  ↓
Triggerlane
  ↓
Typed Job
  ↓
Worklane
  ↓
Execution
```

It answers one question: **What happened?**

Triggerlane receives replayable events, evaluates typed triggers, and submits
typed Worklane jobs. It sits between upstream event sources and Worklane's
execution plane:

```text
Triggerlane  Event Plane       What happened?
Worklane     Execution Plane   What should run?
```

## Quickstart

Triggerlane builds to a single binary and defaults to an embedded SQLite broker,
so there is nothing to stand up to try it:

```bash
# build the binary (the worklane submodule must be present)
git submodule update --init --recursive
cargo build --release                      # → target/release/triggerlane

# inject one event; it is evaluated against the built-in default triggers and
# submitted to the embedded SQLite broker (triggerlane-jobs.sqlite3)
triggerlane inject event.github.issue.created '{"issue":1}'

# or serve the HTTP receiver and POST events to it
triggerlane serve 127.0.0.1:8080 &
curl -s localhost:8080/events -H 'content-type: application/json' \
  -d '{"event_type":"event.github.issue.created","payload":[]}'

# see what was durably retained, and replay it
triggerlane events list
```

Triggerlane only **enqueues** jobs. To actually run them, start a **Worklane
worker** against the same broker — see [Architecture](#architecture). Load your
own triggers from a JSON file via `TRIGGERLANE_TRIGGERS` (see
`examples/triggers.json`); everything else is in [Configuration](#configuration).

## What's in 0.1.0

The baseline is the durable event-to-job path — usable as a library and
operationally runnable as a binary:

- event envelope, source, event type, metadata, and replay by id and timestamp
  range over a bounded, recent window
- a durable **Hybrid WAL** event store: fsync-on-acknowledgement, crash-torn-line
  recovery, single-writer locking, per-event delivery tracking, automatic bounded
  retention (plus manual prune), and startup replay of undelivered events
- declarative triggers (event-type, payload-field, all-of / any-of), typed
  bindings, registry, priority, and enable/disable — loadable from a JSON config
  without recompiling
- idempotent / replay-safe submission, submission retry, and a durable,
  inspectable, retryable, prunable dead-trigger queue
- a selectable Worklane broker backend (`sqlite` / `postgres` / `redis`) at one
  composition seam
- HTTP receiver (`POST /events`) with optional webhook HMAC verification, CLI
  injection, and a file event source
- operator/consumer surfaces: a `clap` CLI and HTTP read endpoints to list, get,
  and replay events and inspect dead-triggers, with an optional bearer token
- operability: SIGTERM graceful shutdown, `/healthz` + `/readyz` probes,
  structured logging, request timeouts, and a Prometheus `/metrics` endpoint
  (including event-store / dead-trigger size and count)

Triggerlane stays a triggering / routing / replay plane: the job execution
lifecycle belongs to Worklane, and multi-step orchestration to consumers via
events (see [Guarantees & contract](#guarantees--contract)). Scheduling, replay
against changed rules, and a routing / rule DSL are [roadmap](#roadmap).

## Architecture

A running deployment is two processes meeting at the broker store: the
`triggerlane` binary (the producer) and a separate Worklane worker (the executor).

```text
  events                 ┌─ triggerlane (one binary) ─┐     ┌─ worklane worker ─┐
  HTTP · CLI · file ───▶ │ ingest → trigger → bind    │     │ (separate process)│
                         │ event WAL (.jsonl)         │     │ reserve → run     │
                         │ embedded broker library    │     │                   │
                         └─────────────┬──────────────┘     └─────────┬─────────┘
                               enqueue │                       reserve │ / ack
                                       └────────▶ broker store ◀───────┘
                                       sqlite file · postgres · redis
```

Triggerlane owns the event WAL; the broker store is Worklane's, shared with the
worker. We build only the left box — the worker is a separate Worklane executable
(e.g. a `worklane-sqlite` worker pointed at the same `TRIGGERLANE_WORKLANE_DB`
file). Multi-step coordination is choreographed by job handlers emitting follow-up
events back to `POST /events` (see `EventEnvelope::follow_up`), not by a central
engine. The internal component design and its rationale are in
`docs/architecture.md`.

### Durability — a Hybrid WAL

The event store is a **durable write-ahead log with a bounded, recent replay
window**, not an unbounded archive. Each accepted event is `fsync`ed before its
job is submitted (durable-by-acknowledgement) and a crash mid-append is recovered
on the next open. Once Worklane accepts (or dead-letters) every matched trigger's
job, the event is marked **delivered**; `serve` automatically cleans up delivered
events past a grace window and replays any still-undelivered events on startup.
Replay (by id or range, over CLI and HTTP) therefore covers the retained window —
a pruned event returns not-found.

```text
  ingest (POST /events · inject · load-file)
       │
       ▼
  append to event log ──▶ fsync ──▶ acknowledge       (durable-by-ack)
       │
       ▼
  handle: match triggers
       ├─▶ submit job ───────────────▶ Worklane broker
       └─▶ binding/submit failure ───▶ dead-trigger queue
       │
       ▼
  mark delivered ──▶ .delivered journal

  startup     : replay events still undelivered (the in-flight set a crash left)
  maintenance : drop delivered events past the grace window (+ hard age/count/byte bounds)
  replay      : by id or time range, over the retained window (a pruned event ⇒ not-found)
```

So both memory and disk are bounded: a default hard `max_count` caps the event
store **and** the dead-trigger queue even if delivery stalls or triggers keep
failing, so neither grows without limit. The dead-trigger queue, having no
delivery state, is bounded by those hard valves (count / age / bytes) rather than
the grace window. Set the bounds to `0`/`off` to opt out (and own the lifecycle
yourself). `prune` is also available as a one-shot manual command (run it from
cron / a Kubernetes CronJob); durations accept `s`/`m`/`h`/`d` suffixes.

## Running it

The CLI is `clap`-based; run `triggerlane --help` (or `<command> --help`) for the
full grammar. A global `--broker sqlite|postgres|redis` (and `--url` for network
brokers) selects the Worklane broker for any command that submits jobs.

```bash
# inject one event through the ingest pipeline
triggerlane inject event.github.issue.created '{"issue":1}'

# load a batch of events from a file
triggerlane load-file events.json

# serve the HTTP receiver (POST /events; GET /healthz, /readyz for probes)
triggerlane serve 127.0.0.1:8080

# deliver into a shared Worklane cluster instead of the embedded SQLite broker
triggerlane --broker postgres --url postgres://… serve

# inspect and replay the retained event window
triggerlane events list
triggerlane events get <event-id>
triggerlane events replay <event-id>
triggerlane events replay-range 2026-06-01T00:00:00Z 2026-06-02T00:00:00Z
# ...narrowed by type/source, or previewed without submitting:
triggerlane events replay-range 2026-06-01T00:00:00Z 2026-06-02T00:00:00Z \
  --event-type event.github.issue.created --source GitHub --dry-run

# inspect / retry trigger-side failures
triggerlane dead-triggers list
triggerlane dead-triggers retry

# inspect the Worklane broker's job-side dead-letter store for a lane
triggerlane worklane-dlq count projection
triggerlane worklane-dlq list projection --limit 20
triggerlane worklane-dlq purge projection

# prune the durable stores (event store + dead-trigger queue)
triggerlane prune older-than 30d   # or: triggerlane prune keep 100000
```

### HTTP endpoints

`serve` exposes read endpoints for the retained window: `GET /events`
(paged with `?limit=&offset=`, default `limit` 1000, append order),
`GET /events/{id}`, `POST /events/{id}/replay`,
`POST /events/replay?start=…&end=…` (optional `&event_type=…&source=…&dry_run=true`),
and `GET /dead-triggers`. Set `TRIGGERLANE_READ_TOKEN` to require a
`Authorization: Bearer <token>` on these (the replay routes submit jobs, so guard
them on any untrusted network). Job *results* are not served here — they live in
Worklane's result store.

`serve` also exposes `GET /healthz` (liveness — `200` while serving) and
`GET /readyz` (readiness — `200` while accepting traffic, `503` once a SIGINT or
SIGTERM begins graceful shutdown) for load-balancer and Kubernetes probes. On
SIGTERM the server drains in-flight requests instead of being killed.

`GET /metrics` serves runtime metrics in Prometheus text format (events handled,
trigger matches, jobs submitted, trigger failures, handling latency, current
dead-trigger queue depth, and event-store/dead-trigger record counts and on-disk
bytes) for a Prometheus / k8s `ServiceMonitor` to scrape. The per-route counters
carry labels — handled events by `source` and `event_type`, and
matches/submissions/failures by `trigger` — so you can break activity down per
route (sum over the label for a total). Label cardinality stays bounded because
sources are a fixed enum, trigger names are the registered set, and event types are
expected to be a bounded dotted-name vocabulary. `serve` also reports the Worklane
broker backlog as `triggerlane_broker_pending{lane}` for each lane it has submitted
to, so queue depth is visible to autoscaling. Probe and scrape traffic are not
request-logged.

## Configuration

Environment variables are namespaced: `TRIGGERLANE_*` configures Triggerlane
itself, while **`TRIGGERLANE_WORKLANE_*`** configures the embedded Worklane broker
Triggerlane drives. (Only the broker *connection* belongs on Triggerlane's side;
job-lifecycle knobs — lease, max-deliveries, dead-letter retention, result TTL —
are the Worklane worker's concern, set where the worker runs or via `wl`.)

All state is durable and file-backed by default, so it survives a restart:

| Variable | Default | What it is |
| --- | --- | --- |
| `TRIGGERLANE_STORE` | `triggerlane-events.jsonl` | Replayable event log (durable WAL; a sibling `.delivered` journal tracks delivery state). |
| `TRIGGERLANE_WORKLANE_BROKER` | `sqlite` | Worklane broker backend: `sqlite`, `postgres`, or `redis` (overridden by `--broker`). |
| `TRIGGERLANE_WORKLANE_DB` | `triggerlane-jobs.sqlite3` | Worklane SQLite broker file (when `--broker sqlite`). |
| `TRIGGERLANE_WORKLANE_URL` | _(unset)_ | Connection URL for a `postgres`/`redis` broker (after `--url`, before `DATABASE_URL`/`REDIS_URL`). |
| `TRIGGERLANE_WORKLANE_SCHEMA` | _(unset → `public`)_ | Postgres schema the broker's tables live in, so several Triggerlane deployments can share one database. Inert for `sqlite`/`redis`. |
| `TRIGGERLANE_WORKLANE_NAMESPACE` | _(unset → `worklane`)_ | Redis key namespace for the broker, so several deployments can share one server. Inert for `sqlite`/`postgres`. |
| `TRIGGERLANE_SUBMISSION_ATTEMPTS` | `3` | How many times to attempt enqueuing a matched job before recording a dead trigger. Floored at `1` (no retry). Resubmission is safe via the deterministic unique key. |
| `TRIGGERLANE_SUBMISSION_BACKOFF_MS` | `100` | Base backoff in milliseconds before the first retry, doubled each subsequent retry (capped at 30s). `0` disables waiting. |
| `TRIGGERLANE_ASYNC_DISPATCH` | `off` | When truthy (`1`/`true`/`yes`/`on`), `serve` accepts each `POST /events` durably and returns `202 Accepted`, dispatching jobs in the background instead of inline. Default off: synchronous handling that returns submission results (`200`). |
| `TRIGGERLANE_ASYNC_DISPATCH_CAPACITY` | `1024` | Max in-flight accepted events when async dispatch is on; once full, accepts wait (backpressure) rather than growing memory. |
| `TRIGGERLANE_DTQ` | `triggerlane-dead-triggers.jsonl` | Dead-trigger queue (terminal trigger-side failures). |
| `TRIGGERLANE_RETENTION_DELIVERED_GRACE` | `24h` | `serve` auto-cleans delivered events older than this; `0`/`off` disables. |
| `TRIGGERLANE_RETENTION_MAX_COUNT` | `1000000` | Hard cap on retained records for **both** the event store and the dead-trigger queue — the structural ceiling so neither grows without limit; `0`/`off` disables. |
| `TRIGGERLANE_RETENTION_MAX_AGE` / `_MAX_BYTES` | _(unset)_ | Optional extra hard bounds (age, serialized bytes), applied to both stores. Safety valves that may drop even undelivered / unretried records. |
| `TRIGGERLANE_RETENTION_INTERVAL` | `5m` | How often `serve` runs the maintenance sweep (store metrics + retention). |
| `TRIGGERLANE_TRIGGERS` | _(unset)_ | Path to a declarative JSON trigger config (see `examples/triggers.json`). When set, it replaces the built-in default trigger set; a read/parse error fails startup. |
| `TRIGGERLANE_WEBHOOK_SECRET` | _(unset)_ | When set, `serve` verifies an `X-Hub-Signature-256` HMAC-SHA256 over the raw body and rejects unsigned/invalid requests with `401`. Leave unset only on a trusted network. |
| `TRIGGERLANE_READ_TOKEN` | _(unset)_ | When set, the read/replay endpoints require `Authorization: Bearer <token>` (rejecting others with `401`). Separate from the webhook secret because reads have no signable body and the replay routes submit jobs. Leave unset only on a trusted network. |
| `TRIGGERLANE_LOG_FORMAT` | `text` | Structured-log output format on stderr: `json` (one JSON object per line, for log aggregators) or `text` (human-readable). |
| `RUST_LOG` | `info` | Log level / filter (standard `tracing` `EnvFilter` syntax, e.g. `debug`, `triggerlane_runtime=debug`). |
| `TRIGGERLANE_REQUEST_TIMEOUT_SECS` | `30` | Per-request handling timeout; a request exceeding it gets `408`. Set `0` to disable (e.g. when a fronting proxy enforces timeouts). |

### Broker settings: who owns what

As the producer, Triggerlane configures only the broker settings a producer owns.
The job-execution settings belong to the Worklane worker — Triggerlane never sets
them, and setting them on its enqueue-only broker would have no effect (they are
read only on the worker's reserve/fail paths). The *why* is in `docs/architecture.md`
and the `broker-selection` spec.

| Setting | Default | Owned by |
| --- | --- | --- |
| Backend + connection (URL / path / pool) | `sqlite`; pool 8 (sqlite) / 10 (postgres) | **Triggerlane** — `--broker`, `--url`, `TRIGGERLANE_WORKLANE_*` |
| Per-deployment isolation (schema / namespace) | `public` / `worklane` | **Triggerlane** — `TRIGGERLANE_WORKLANE_SCHEMA` / `_NAMESPACE` |
| Visibility lease | `30s` | **Worklane worker** |
| Delivery bound (`max_deliveries`) | unbounded | **Worklane worker** |
| Dead-letter retention | unbounded | **Worklane worker** (or periodic `purge_dead_letters`) |

So the Worklane dead-letter store is bounded by the worker, not by Triggerlane.
Two deployment obligations rest on the operator: a **Redis** broker must disable key
eviction (`maxmemory-policy noeviction`, or Worklane keys corrupt the store), and
because delivery is at-least-once, **job handlers must be idempotent**.

## Guarantees & contract

The authoritative contract is `PROJECT.md`; the testable behavior lives in the
living specs under `openspec/specs/`. The headline guarantees:

- **Durable-by-acknowledgement.** An accepted event is `fsync`ed before any job is
  submitted, and survives a crash (torn trailing line recovered on reopen).
- **At-least-once, replay-safe.** Submission uses a deterministic unique key, so a
  duplicate or replay deduplicates at the broker; ingestion also deduplicates by
  idempotency key within the retained window. Job handlers must be idempotent.
- **Plane separation.** Triggerlane decides *what happened → what to submit*;
  Worklane owns the job lifecycle (reserve / run / retry / dead-letter). Triggerlane
  never grows a stateful orchestration engine — cross-job coordination is
  choreographed via events.
- **Bounded.** A default hard `max_count` caps both durable stores so neither grows
  without limit; the broker the binary builds carries only the producer's
  connection (execution-lifecycle bounds are the worker's).

## Development

This repository uses OpenSpec. Start by reading `AGENTS.md`, `PROJECT.md`, and
the living specs under `openspec/specs/`. Active changes, when present, live
under `openspec/changes/`.

Clone with submodules, or initialize them after cloning:

```bash
git submodule update --init --recursive
```

Common tasks are in the `justfile` (`just` is optional sugar; each recipe is a
plain command): `just lint` (fmt + clippy), `just test`, `just audit`
(`cargo-deny` supply-chain gate), and `just up` to start a local Postgres + Redis
(`docker-compose.yml`) for running against a network broker (copy `.env.example`
to `.env`). CI runs the same `lint` / `test` / `deny` gates on every push. Release
notes live in `CHANGELOG.md`.

Worklane is tracked as the `worklane/` git submodule and provides the
`worklane-core` dependency that Triggerlane submits jobs through.
The submodule commit recorded by this repository is the Worklane pin for
non-local reproducibility. Member crates consume Worklane crates through root
workspace dependencies; `worklane-core` is the contract surface. The shipped CLI
defaults to the durable `worklane-sqlite` broker at its broker seam, while the
`worklane-memory` broker is limited to tests and examples.

The current planning map is maintained in `BACKLOG.md`. The living specifications
under `openspec/specs/` are the source of truth; completed OpenSpec changes are
archived under `openspec/changes/archive/`.

## Roadmap

- **v0.1**: Foundation, event model, trigger model, runtime, and event sources.
- **v0.2**: Scheduling and replay.
- **v0.3**: Routing and rule engine.
