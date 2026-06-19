# event-ingest Specification

## Purpose

Defines the source-agnostic ingest pipeline that durably records each accepted
event in the event store and then evaluates it through the trigger runtime,
acknowledges delivery once Worklane takes the work, and replays in-flight events
on startup.
## Requirements
### Requirement: Persist-then-handle ingest pipeline

Triggerlane SHALL provide a source-agnostic ingest pipeline that durably records an
accepted event in the event store and then evaluates it through the trigger
runtime. The pipeline SHALL durably append the event to the store — flushed to
stable storage — before submitting any job to the broker, so the event remains
replayable and crash-recoverable regardless of the handling outcome.

#### Scenario: Accepted event is ingested

- **WHEN** an accepted event envelope is submitted to the ingest pipeline
- **THEN** the pipeline durably appends the envelope to the event store
- **AND** then handles the event through the trigger runtime
- **AND** returns a report containing the accepted event id and the handling result

#### Scenario: Persistence precedes handling

- **WHEN** an event is ingested and its trigger handling records failures
- **THEN** the event is still retrievable from the event store by its id

#### Scenario: Durable persistence precedes job submission

- **WHEN** an event is ingested
- **THEN** the event is flushed to stable storage before any job is submitted to
  the broker

#### Scenario: Same pipeline serves every source

- **WHEN** events are accepted from the HTTP receiver, CLI injection, or file source
- **THEN** each event is ingested through the same persist-then-handle pipeline

### Requirement: Idempotent ingestion

When an accepted event carries an idempotency key, the ingest pipeline SHALL
deduplicate it against the retained event window: if an event with the same
idempotency key is still retained, the pipeline SHALL NOT append the event again or
re-evaluate it through the trigger runtime, and SHALL return a report marked as
deduplicated. Deduplication is bounded by retention — an event whose earlier copy
has been pruned is treated as new. An event without an idempotency key SHALL always
be ingested.

#### Scenario: Duplicate idempotency key is deduplicated

- **WHEN** an event whose idempotency key matches a still-retained event is
  submitted to the ingest pipeline
- **THEN** the pipeline does not append a second copy and does not re-evaluate the
  event
- **AND** returns a report marked as deduplicated

#### Scenario: Event without an idempotency key is always ingested

- **WHEN** an event with no idempotency key is submitted
- **THEN** the pipeline appends and evaluates it through the trigger runtime as
  usual

### Requirement: Delivery acknowledgement and startup recovery

The ingest pipeline SHALL mark an event delivered once every trigger matched for it
has had its job accepted by Worklane or recorded to the dead-trigger queue. On
startup, the server SHALL replay events that are retained but not yet delivered
through the normal handling path, relying on Worklane's unique-key deduplication to
make re-submission safe.

#### Scenario: Delivered events are acknowledged

- **WHEN** all matched triggers for an event have had their jobs accepted or
  dead-lettered
- **THEN** the pipeline marks the event delivered

#### Scenario: In-flight events replay on startup

- **WHEN** the server starts and the store holds retained, not-yet-delivered events
- **THEN** the server replays those events through the handling path

### Requirement: Optional asynchronous ingestion

The pipeline SHALL support an opt-in asynchronous mode that separates accept from
dispatch: an accepted event is durably appended (and deduplicated) and then handed
to a background dispatcher that submits it off the request path, rather than handled
inline. Acceptance SHALL remain durable-by-acknowledgement, and an event accepted
but not yet dispatched SHALL be left undelivered so startup recovery handles it
after a crash. In-flight accepted events SHALL be bounded (backpressure once the
bound is reached) so the mode cannot grow memory without limit. The default mode
SHALL remain synchronous, handling inline and returning submission results.

#### Scenario: Asynchronous accept defers handling

- **WHEN** asynchronous ingestion is enabled and an event is accepted
- **THEN** the event is durably appended and left undelivered for the dispatcher,
  and the response indicates acceptance rather than submission results

#### Scenario: Dispatcher handles accepted events

- **WHEN** the background dispatcher processes an accepted event
- **THEN** it handles the event through the trigger runtime and marks it delivered
