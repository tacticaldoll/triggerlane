# Triggerlane Backlog

OpenSpec (`openspec/specs/`) is the source of truth for what Triggerlane
currently is. This file is the forward planning map for future OpenSpec changes.

## Product Spine

```text
Triggerlane  Event Plane       What happened?
Worklane     Execution Plane   What should run?
```

Triggerlane's durable product direction:

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

## Backlog Status

- **Later**: planned, but not needed to protect the immediate contract.
- **Research**: code or notes whose main output is a finding.
- **Blocked**: cannot proceed until an upstream (e.g. Worklane) change ships.

## 0.1.0 Baseline

The 0.1.0 baseline is the event → trigger → job plane: replayable event
envelopes, declarative triggers (event-type, payload-field, all-of / any-of),
typed Worklane bindings, idempotent / replay-safe submission, and replay by id
and timestamp range. It is usable as a library, and the shipped binary is
operationally runnable — a selectable Worklane broker (sqlite / postgres / redis),
a durable Hybrid-WAL event store (fsync-on-ack, crash recovery, delivery tracking,
automatic bounded retention plus manual prune) and dead-trigger queue, config-loaded
triggers, webhook HMAC verification, a clap CLI and HTTP read/replay endpoints with
an optional bearer token, SIGTERM graceful shutdown, health / readiness probes,
structured logging, request timeouts, Prometheus metrics (including store size and
count), and dead-trigger retry.

The authoritative, per-capability detail lives in `openspec/specs/`. The items
below are what remains.

## Scheduling and Replay

### TL-501 Cron Trigger

**Status**: Later

**Goal**: Produce scheduled events such as daily, hourly, and weekly.

**Acceptance signal**: Cron source emits replayable envelopes with stable event
types.

### TL-502 Delayed Trigger

**Status**: Later

**Goal**: Run a trigger after a delay such as `10m`.

**Acceptance signal**: Delayed trigger state survives until the delayed event is
handled.

### TL-503 Recurring Trigger

**Status**: Later

**Goal**: Model recurring schedules as first-class trigger inputs.

**Acceptance signal**: Recurring schedules produce deterministic event
envelopes.

### TL-504 Schedule Persistence

**Status**: Later

**Goal**: Persist schedule definitions and next-run state.

**Acceptance signal**: Restarting Triggerlane does not lose scheduled work.

### TL-604 Replay With New Rules

**Status**: Later

**Goal**: Re-evaluate old events against changed trigger rules.

**Acceptance signal**: Operators can preview or execute the job fanout created by
new rules.

### TL-605 Replay Audit

**Status**: Later

**Goal**: Record who replayed what, when, with which rule set.

**Acceptance signal**: Replay output is inspectable after execution.

## Routing and Rule Engine

### TL-701 Topic Routing

**Status**: Later

**Goal**: Route events by topic.

**Acceptance signal**: Topic rules choose trigger groups without changing event
envelopes.

### TL-702 Consumer Routing

**Status**: Later

**Goal**: Route events to consumer groups.

**Acceptance signal**: Multiple consumers can independently process the same
event stream.

### TL-703 Multi Trigger Fanout

**Status**: Later

**Goal**: Allow one event to produce multiple jobs.

```text
1 Event
  ↓
3 Jobs
```

**Acceptance signal**: Fanout is deterministic, observable, and replayable.

### TL-706 Choreography Context Propagation

**Status**: Later

**Goal**: Carry the causing event's context (correlation id, causing event id,
tenant) onto the submitted Worklane job so a handler can emit linked follow-up
events (`EventEnvelope::follow_up`) without re-deriving the chain. Candidate
vehicle: the opaque `NewJob` trace-context map with namespaced keys.

**Acceptance signal**: A job handler can read the causing context and emit a
follow-up event that links into the same correlation chain.

### TL-704 Conditional Routing — richer operators

**Status**: Later (event-type / payload-field-equals / all-of / any-of shipped)

**Goal**: Route on event content beyond the shipped conditions.

**Later work**: richer predicate operators (exists, prefix, numeric comparison
such as `confidence < 0.8`), metadata and source conditions (e.g. match
`tenant_id`), and negation (`Not`).

**Acceptance signal**: Richer conditions can be evaluated during live handling
and replay.

### TL-705 Rule DSL

**Status**: Later (file-loaded JSON trigger config shipped)

**Goal**: A full trigger/routing DSL beyond the shipped declarative JSON config.

**Later work**: versioning and audit of rule sets; config-expressible payload
transforms (today code-defined).

**Acceptance signal**: Rules can be parsed, validated, versioned, and audited.

## Observability

### TL-801 Trigger Trace — per-event detail / queryable store

**Status**: Later (structured-log emission of the per-event trace shipped)

**Goal**: Trace Event → Trigger → Job with full per-trigger detail.

**Later work**: carry trigger name / binding / correlation metadata onto the
per-event trace (today the log carries counts); a queryable trace store.

**Acceptance signal**: A submitted job can be traced back to event id, trigger
name, binding, and metadata.

### TL-802 Metrics — labels / OTLP

**Status**: Later (Prometheus `/metrics` pull endpoint shipped)

**Goal**: Richer metric dimensions and export options.

**Later work**: per-trigger / per-source label dimensions (started unlabelled to
avoid cardinality surprises); OTLP push export if a collector integration is
needed.

**Acceptance signal**: Operators can break metrics down per trigger and source.

### TL-803 Latency Tracking

**Status**: Later

**Goal**: Track source-to-submission latency.

**Acceptance signal**: Runtime reports latency per source and trigger.

### TL-804 Success Rate

**Status**: Later

**Goal**: Track successful trigger handling.

**Acceptance signal**: Success rate can be calculated per trigger.

### TL-805 Failure Rate

**Status**: Later

**Goal**: Track trigger handling failure rate.

**Acceptance signal**: Failure rate can be calculated per trigger and error
class.

## Scale-out

Deferred until there is a concrete need; re-rank before starting a change.

- **TLS.** Conventionally terminated at a fronting proxy; revisit only if direct
  TLS termination is required.

Shipped (was here): config-selected broker backend — `--broker sqlite|postgres|redis`
with URL precedence and credential redaction, at the `connect_broker` seam.
