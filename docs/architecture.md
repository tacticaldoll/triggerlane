# Architecture

Triggerlane is the event plane in a typed orchestration runtime. It sits between
upstream event sources and Worklane's execution plane.

```text
Triggerlane  Event Plane       What happened?
Worklane     Execution Plane   What should run?
```

The v0.1 runtime turns an accepted event into zero or more Worklane jobs:

```text
Event
  ↓
Trigger
  ↓
Binding
  ↓
Worklane Job
  ↓
Execution
```

## Event

An Event is a replayable fact that something happened. Triggerlane stores the
fact in an `EventEnvelope` with an id, source, event type, payload bytes,
timestamp, and metadata. Source adapters can discard transport-specific details
only after the envelope contains enough information to evaluate triggers again.

## Event store (Hybrid WAL)

The event store is a durable, append-only JSONL write-ahead log with a bounded,
recent replay window. An append is `fsync`ed before it is acknowledged, the body
and newline are written atomically under an advisory single-writer lock, and a
torn trailing line from a crash mid-append is recovered on open. Each event's
delivery state is tracked in a sibling `.delivered` journal: an event is
*delivered* once every matched trigger's job is accepted by Worklane or recorded
to the dead-trigger queue. `serve` replays still-undelivered events on startup and
automatically cleans up delivered events past a grace window (with optional hard
age/count/byte bounds). Replay by id or range covers the retained window; a pruned
event is not found. Durability and results beyond delivery belong to Worklane.

Two files back the store: an append-only event log (`.jsonl`) and a sibling
`.delivered` journal of delivered ids. The lifecycle over them:

```text
  ingest ─▶ append ─▶ fsync ─▶ ack            (event log; durable-by-ack)
       │
       ▼
  handle event: match triggers
       ├─▶ submit ──────────────▶ Worklane broker (sqlite / postgres / redis)
       └─▶ failure ─────────────▶ dead-trigger queue
       │
       ▼
  mark delivered ─▶ append id to the .delivered journal

  startup     replay events in the log but absent from .delivered (in-flight at a crash)
  maintenance compact: drop delivered events past grace, plus hard age/count/byte bounds
  replay      by id or [start, end) range, re-handled from the retained window
```

## Trigger

A Trigger is a pure rule over an `EventEnvelope`. It answers whether the event
matches. It does not enqueue work and does not decide how a Worklane job is
serialized.

## Binding

A Binding maps a matched event into a typed Worklane job submission. Bindings are
kept separate from triggers so rule evaluation, typed job construction, and
runtime side effects can be tested independently.

## Execution

Execution belongs to Worklane. Triggerlane submits jobs and records trigger-side
failures, but Worklane owns job reservation, running, retry, and dead-letter
behavior after submission.

## Design decisions

The testable contract lives in `openspec/specs/`; this section records the *why*
behind the shape above (the rationale that previously lived in separate ADRs).

### Event store is a Hybrid WAL, not an unbounded archive

The obvious alternative — and the one a downstream consumer (Nexuslane) wanted — is
an unbounded, full-history archive: an append-only log kept in full for the life of
the process, pruned only by an explicit operator command, treated as a replayable
source of truth. It is rejected for two reasons. Operationally, full retention
grows memory and disk without bound, and an archive is easy to get subtly wrong (an
append acknowledged before it reaches disk, or a crash mid-append leaving a torn
line that breaks reopen). Architecturally, it duplicates ownership: once a job is
enqueued, Worklane already owns durability, retries, dead-lettering, and result
storage, so retaining every event forever re-implements a guarantee that already
lives downstream. The Hybrid WAL keeps durability where it is cheap (acknowledged,
crash-recoverable appends) and bounds retention to a recent replay window.

Consequence: replay is scoped to the configured window, so a consumer that wants
"replay all history" must align its expectation to that window (tracked in
`BACKLOG.md`).

### Worklane is an embedded substrate, not part of the core

The event-plane core is Worklane-free: `triggerlane-core` (event model, triggers,
bindings) and `triggerlane-storage` (the event WAL) carry no Worklane dependency,
and `WorklaneJob` / `WorklaneJobBinding` are Triggerlane's own neutral job
description. Worklane enters at exactly one translation seam in
`triggerlane-runtime`, where a matched event's `WorklaneJob` becomes a
`worklane-core` `Lane` / `NewJob` and is enqueued through `Arc<dyn Broker>`. The
CLI selects the backend (`sqlite` / `postgres` / `redis`) at that seam; the
embedded SQLite broker is the durable single-node default, and the network brokers
deliver into a shared Worklane cluster.

What is embedded is the broker *library*, not the executor. Triggerlane links
Worklane's broker crates and constructs the broker in-process to enqueue, but job
execution runs in a separate Worklane worker process that Triggerlane does not
build. Producer and worker meet at the broker's backing store, so a deployment has
**two distinct stores**: Triggerlane's event WAL (its own durability mechanism)
and the Worklane broker store (Worklane's, shared with the worker).

The coupling is therefore compile-time (a Worklane contract change is a build
break, not a runtime negotiation) plus shared-store, rather than a loose network
boundary. Triggerlane never modifies Worklane — a pinned, read-only submodule —
depends only on its published contract behind the `Arc<dyn Broker>` seam, and
reports findings upstream rather than forking.

A consequence for security: the broker is **not** gated by Triggerlane. Its access
boundary is the backing store's own controls (file permissions, database
credentials, Redis ACL) plus per-deployment schema/namespace isolation;
Triggerlane's webhook and read-token authentication guard only the event plane.

### Specs are the source of truth

Behavior is defined by the living specifications under `openspec/specs/`, not by
chat history or agent-specific files. Changes flow through the OpenSpec lifecycle
(see `docs/development-flow.md`); architecture and its rationale live in this
document rather than in a separate decision-record log.
