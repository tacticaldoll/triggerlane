# broker-selection Specification

## Purpose

Defines how the CLI selects the Worklane broker backend (sqlite/postgres/redis)
at its single composition seam, with documented URL-source precedence and
credential redaction, so the trigger plane can deliver into any Worklane broker it
ships against.

## Requirements

### Requirement: Broker backend selection

The CLI SHALL select the Worklane broker backend among the backends it ships
against (at minimum `sqlite`, `postgres`, and `redis`) through an explicit flag,
constructing the chosen broker at the single broker composition seam and injecting
it as an `Arc<dyn Broker>` so the rest of the CLI stays generic over the
`worklane-core` `Broker` contract.

#### Scenario: Backend is chosen by flag

- **WHEN** the operator runs the CLI with a broker-backend selection flag set to a
  supported backend
- **THEN** the CLI connects to that backend and uses it for the command

#### Scenario: Unknown backend is rejected

- **WHEN** the operator selects a broker backend that is not supported
- **THEN** the CLI exits with an error naming the supported backends

#### Scenario: Default backend stays durable

- **WHEN** the operator runs the CLI without selecting a backend
- **THEN** the CLI defaults to a durable, file-backed broker

### Requirement: Broker connection URL precedence

For network broker backends, the CLI SHALL resolve the connection URL with one
documented precedence — an explicit URL flag, then a Triggerlane URL environment
variable, then the backend's conventional environment variable — and SHALL announce
the chosen source on the diagnostic stream without printing the URL itself.

#### Scenario: URL source precedence is applied

- **WHEN** more than one URL source is present for a network backend
- **THEN** the CLI uses the highest-precedence source and reports which source was
  chosen

#### Scenario: Missing URL is reported

- **WHEN** a network backend is selected and no URL source is present
- **THEN** the CLI exits with an error stating which sources it looked for

### Requirement: Per-deployment broker isolation

The CLI SHALL expose, through configuration, the deployment-level isolation that a
network broker backend supports (a Postgres schema, a Redis key namespace), so two
Triggerlane deployments can target one database/server without seeing each other's
jobs. When the isolation setting is unset or empty, the CLI SHALL use the broker's
default, preserving existing behavior; the setting SHALL be inert for backends that
isolate by other means (for example sqlite, which isolates by file path).

#### Scenario: Configured isolation is applied

- **WHEN** a per-deployment isolation setting is configured for a network backend
  that supports it
- **THEN** the CLI constructs the broker scoped to that schema/namespace

#### Scenario: Unset isolation keeps the default

- **WHEN** no per-deployment isolation setting is configured
- **THEN** the CLI constructs the broker with its default schema/namespace

### Requirement: Producer-scoped broker configuration

The CLI SHALL configure only producer-relevant settings on the broker it
constructs — the connection (URL/path/pool) and per-deployment isolation
(schema/namespace). It SHALL NOT set execution-lifecycle policy — the visibility
lease, the delivery bound (`max_deliveries`), or the dead-letter retention policy —
because Triggerlane only enqueues: those settings are enforced solely on the
worker's reserve/fail paths and are inert on an enqueue-only producer instance.
Bounding the Worklane dead-letter store is therefore the Worklane worker operator's
responsibility, not Triggerlane's. When the Redis backend is selected, the
deployment MUST run with key eviction disabled (for example
`maxmemory-policy noeviction`), because evicting Worklane keys corrupts the broker
store.

#### Scenario: Execution-lifecycle policy is left to the worker

- **WHEN** the CLI constructs a broker for a command that submits jobs
- **THEN** it configures the connection and per-deployment isolation but does not
  set the visibility lease, delivery bound, or dead-letter retention

#### Scenario: Redis backend requires eviction disabled

- **WHEN** a deployment selects the Redis broker backend
- **THEN** the Redis server must be configured so Worklane keys are not evicted
  under memory pressure

### Requirement: Credential redaction on broker errors

The CLI SHALL redact credentials from broker connection error messages it emits,
so a connection failure never prints a password or secret.

#### Scenario: Connection error is redacted

- **WHEN** a network broker connection fails with an error that embeds credentials
- **THEN** the message the CLI emits has the credentials redacted
