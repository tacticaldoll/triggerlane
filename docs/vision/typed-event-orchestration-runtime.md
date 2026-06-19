# Typed Event Orchestration Runtime

Triggerlane is the declarative event-trigger plane between upstream event sources
and the Worklane execution substrate:

```text
Triggerlane
  Event Plane
  What happened?

Worklane
  Execution Plane
  What should run?
```

## Positioning

Worklane is a Celery-class execution substrate (broker, reservation lifecycle,
retry, dead-letter, scheduling). Triggerlane is the declarative
`event -> trigger -> job` plane in front of it, with replay — the first-class
event-triggering Celery-class task queues lack (they offer only time-based
scheduling and internal signals). Benchmark the substrate against Celery / RQ /
Sidekiq; benchmark the trigger plane against Inngest / Hatchet / EventBridge.

Triggering, not orchestration: the job execution lifecycle is Worklane's, and
multi-step coordination is choreographed by consumers via events (a job result
re-enters as a new event) rather than run by a central Triggerlane engine.

The core flow is:

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

## v0.1 Backlog

v0.1 is the minimal event-to-job release. It includes:

- Milestone 0: Foundation
- Milestone 1: Event Model
- Milestone 2: Trigger Model
- Milestone 3: Runtime
- Milestone 4: Event Sources

The result should be:

```text
Event
  ↓
Trigger
  ↓
Worklane Job
```

## Roadmap

The releases below are the durable product Triggerlane is building as the
declarative event-trigger plane on the Worklane substrate:

- v0.2 scheduling and replay.
- v0.3 routing and the rule engine (topic / consumer / conditional routing, Rule
  DSL).
- v1.0 formalizing the broader event-trigger architecture.

Each release stays within triggering / routing / replay scope: the job execution
lifecycle is Worklane's, and multi-step orchestration is left to consumers
(choreography over orchestration).
