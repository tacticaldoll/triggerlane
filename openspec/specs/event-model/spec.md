# event-model Specification

## Purpose

Defines the replayable event envelope and naming model used by Triggerlane
before events are evaluated by triggers.
## Requirements
### Requirement: Event envelope

Triggerlane SHALL represent every accepted event as an `EventEnvelope` with id,
source, event type, payload bytes, timestamp, and metadata.

#### Scenario: Event is accepted

- **WHEN** Triggerlane accepts an event from any source
- **THEN** it produces an `EventEnvelope` containing all required fields

### Requirement: Event source

Triggerlane SHALL classify event origins with a `Source` value including HTTP,
GitHub, Discord, Slack, Cron, and Manual.

#### Scenario: Source is normalized

- **WHEN** an event enters through a supported source adapter
- **THEN** the envelope source records the corresponding `Source` value

### Requirement: Event type naming

Triggerlane SHALL use stable dotted event type names for routing and matching.

#### Scenario: Known event type is emitted

- **WHEN** a GitHub issue, GitHub pull request, Discord message, or daily cron event is normalized
- **THEN** its event type is one of `event.github.issue.created`, `event.github.pr.created`, `event.discord.message.created`, or `event.cron.daily`

### Requirement: Event metadata

Triggerlane SHALL support event metadata fields for trace id, correlation id,
tenant id, and idempotency key.

#### Scenario: Metadata is provided

- **WHEN** a source provides trace, correlation, tenant, or idempotency metadata
- **THEN** the envelope preserves those metadata values for runtime handling and
  replay

### Requirement: Replayable event

Every accepted event SHALL be replayable from its envelope without requiring the
original source request.

#### Scenario: Event is replayed later

- **WHEN** a stored envelope is submitted for replay
- **THEN** trigger evaluation can use the stored envelope fields and payload without reading the original source request

### Requirement: Durable event store

Triggerlane SHALL provide a durable event store implementation that persists
accepted `EventEnvelope` records outside process memory. A durable write failure
SHALL be surfaced to the caller as an error rather than aborting the process, and
the in-memory view SHALL reflect a record only after its durable write succeeds.

#### Scenario: Event survives store reopen

- **WHEN** an event is appended to the durable event store
- **AND** the store is reopened from the same storage location
- **THEN** the event can be retrieved by id with its replay fields preserved

#### Scenario: Persisted events are listed in append order

- **WHEN** multiple events are appended to the durable event store
- **AND** the store is reopened from the same storage location
- **THEN** all persisted events are listed in append order

#### Scenario: Durable write failure is reported

- **WHEN** a durable event-store append fails (for example, a full disk or permission error)
- **THEN** the failure is returned to the caller as an error rather than aborting the process

### Requirement: Event store retention

The durable event store SHALL support pruning stored events on operator demand,
either removing events older than a cutoff timestamp or retaining only the most
recent N events, so the append-only backing does not grow without bound. Pruning
SHALL preserve the events it retains and SHALL be atomic with respect to the
durable backing — an interrupted prune SHALL NOT leave the store in a
partially-written state. Pruning SHALL report how many events were removed.
Retention SHALL be an explicit operation, not automatic, so shortening the
replay window is always an operator action.

#### Scenario: Prune by age keeps newer events

- **WHEN** the event store is pruned to remove events older than a cutoff
- **THEN** events at or after the cutoff are retained and older events are removed, and the count removed is reported

#### Scenario: Prune by count bounds the store

- **WHEN** the event store is pruned to keep only the most recent N events
- **THEN** at most N events remain and the rest are removed

#### Scenario: Pruned durable store survives reopen

- **WHEN** a durable event store is pruned and then reopened from the same location
- **THEN** only the retained events are present, and further appends still persist

### Requirement: Event causation and follow-up emission

`EventMetadata` SHALL carry an optional causation id identifying the event that
directly caused this one. Triggerlane SHALL provide a follow-up constructor that
builds a new event caused by a given event and propagates the choreography chain:
the follow-up SHALL set its causation id to the causing event's id, SHALL inherit
the causing event's correlation id (or start the chain from the causing event's
id when the cause has no correlation id), and SHALL carry the causing event's
tenant.

#### Scenario: Follow-up event links to its cause

- **WHEN** a follow-up event is built from a causing event
- **THEN** the follow-up's causation id is the causing event's id

#### Scenario: Correlation id propagates across the chain

- **WHEN** a follow-up event is built from a causing event that has a correlation id
- **THEN** the follow-up shares that correlation id
- **AND** when the causing event has no correlation id, the follow-up's
  correlation id is the causing event's id

#### Scenario: Tenant is carried across the chain

- **WHEN** a follow-up event is built from a causing event that carries a tenant
- **THEN** the follow-up carries the same tenant

