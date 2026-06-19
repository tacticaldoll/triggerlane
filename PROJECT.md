# Triggerlane Contract

Triggerlane is a durable, declarative, event-driven trigger and routing plane
layered on the Worklane execution substrate. It turns events into typed Worklane
jobs.

## Purpose

Triggerlane receives events from external or internal sources, normalizes them
into replayable envelopes, evaluates triggers, and submits typed jobs to the
Worklane execution substrate.

Its product wedge is the part Celery-class task queues do not provide: a
first-class, declarative `event -> trigger -> job` layer with replay. Worklane is
the Celery-class execution substrate (broker, reservation lifecycle, retry,
dead-letter, scheduling); Triggerlane is the event-trigger plane in front of it,
aimed at the event-triggering gap those task queues leave (only time-based
scheduling and internal signals).

Triggerlane is mechanism, not orchestration. It provides triggering, routing, and
replay primitives. Multi-step coordination across jobs is choreographed by
consumers via events — a job result re-enters as a new event — not run by a
central Triggerlane engine.

## Core Contract

An accepted event MUST be represented as a replayable event envelope and MUST be
durably persisted (fsynced) before any Worklane job is submitted.

The event store is a **Hybrid WAL**: a durable, crash-recoverable write-ahead log
with a bounded, recent replay window — not an unbounded archive. An event is
marked *delivered* once every matched trigger's job has been accepted by Worklane
or recorded to the dead-trigger queue; delivered events are cleaned up
automatically past a grace window, and undelivered events are replayed on startup.
Replay therefore covers the retained window, and durability/results past delivery
are Worklane's responsibility, not Triggerlane's.

The system MUST keep the event-plane and execution-plane responsibilities
separate:

- Triggerlane records what happened and decides what work to submit.
- Worklane owns the job execution lifecycle: reservation, run, retry,
  dead-letter, and result storage.

Worklane integration MUST remain an explicit dependency boundary. Triggerlane may
submit jobs through Worklane contracts, but it MUST NOT blur event-plane logic
into Worklane execution concerns, and MUST NOT grow a stateful multi-step
orchestration engine; cross-job coordination stays choreographed via events.

Concretely, the event-plane core carries no Worklane types — Worklane enters only
at a single submission seam, and the broker is embedded as a library while job
execution and its store belong to a separate Worklane worker (see
`docs/architecture.md` for the structural detail).

## Terminology

- **Event**: A replayable fact that something happened.
- **EventEnvelope**: The normalized event record used inside Triggerlane.
- **Source**: The origin category of an event, such as HTTP, GitHub, Discord,
  Slack, Cron, or Manual.
- **EventType**: A stable dotted event name, such as
  `event.github.issue.created`.
- **Trigger**: A rule that determines whether an event should produce work.
- **Binding**: A typed mapping from a matched event to a Worklane job type.
- **Execution**: Work performed by Worklane after Triggerlane submits a job.
- **Dead Trigger Queue (DTQ)**: Trigger evaluation or submission failures that
  cannot be completed immediately and require inspection or retry.

## Change Prioritization

When comparing possible changes, prefer the one that protects the core contract
earliest:

1. Event replayability, trigger determinism, and safe at-least-once Worklane
   submission.
2. The declarative trigger/routing surface and its replay guarantees — the
   product's differentiator — and a first-class, composable event-ingest
   interface so consumers can close event loops themselves.
3. Operator and developer ergonomics (observability, diagnosability).
4. Source integrations, routing expressiveness, scheduling, and scale-out.

Anything that would pull the job-execution lifecycle or a stateful orchestration
engine into Triggerlane is out of scope by the core contract — not a
prioritization trade-off.
