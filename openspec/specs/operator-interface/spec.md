# operator-interface Specification

## Purpose

Defines the operator/consumer-facing surfaces — a structured CLI and HTTP read
endpoints — that expose the runtime's event inspection, replay, and dead-trigger
capabilities over the retained event window. No job-result store is introduced;
job results live in Worklane.

## Requirements

### Requirement: Structured CLI with subcommands

The CLI SHALL parse its commands and options through a structured argument parser
that provides subcommands, named flags, and generated help, replacing ad-hoc
positional matching.

#### Scenario: Help is available

- **WHEN** the operator invokes the CLI help
- **THEN** the CLI lists its subcommands and their options

#### Scenario: Unknown subcommand is rejected

- **WHEN** the operator invokes an unknown subcommand
- **THEN** the CLI exits with a usage error

### Requirement: CLI event inspection and replay

The CLI SHALL expose the runtime's event inspection and replay capabilities as
subcommands: listing retained events, getting an event by id, replaying an event by
id, and replaying events within a timestamp range.

#### Scenario: Event is replayed by id from the CLI

- **WHEN** the operator runs the replay-by-id subcommand for a retained event id
- **THEN** the CLI replays that event through the runtime and reports the result

#### Scenario: Replay of a pruned event reports not found

- **WHEN** the operator runs the replay-by-id subcommand for an event id that is no
  longer retained
- **THEN** the CLI reports that the event was not found

#### Scenario: Events are replayed by range from the CLI

- **WHEN** the operator runs the replay-range subcommand with a start and end
- **THEN** the CLI replays the retained events in that range and reports per-event
  results

### Requirement: CLI dead-trigger inspection

The CLI SHALL expose listing dead-trigger records and retrying them as subcommands.

#### Scenario: Dead-triggers are listed

- **WHEN** the operator runs the dead-trigger list subcommand
- **THEN** the CLI prints the current dead-trigger records

### Requirement: HTTP read endpoints for events and dead-triggers

The HTTP server SHALL expose read endpoints, alongside the event receiver and
probes, for listing retained events, getting an event by id, replaying an event by
id, replaying events within a timestamp range, and listing dead-trigger records.
These endpoints SHALL call the existing runtime capabilities and SHALL NOT
introduce a job-result store (job results remain in Worklane). The event-listing
endpoint SHALL bound its response by default — paging the retained window rather
than returning all of it — so an unbounded window cannot turn one list call into
an arbitrarily large response.

#### Scenario: Event is fetched over HTTP

- **WHEN** a client requests a retained event by id from the events read endpoint
- **THEN** the server responds with that event

#### Scenario: Missing event returns not found over HTTP

- **WHEN** a client requests an event id that is not retained
- **THEN** the server responds with a not-found status

#### Scenario: Event is replayed over HTTP

- **WHEN** a client posts a replay request for a retained event id
- **THEN** the server replays it through the runtime and responds with the handling
  result

#### Scenario: Dead-triggers are listed over HTTP

- **WHEN** a client requests the dead-triggers read endpoint
- **THEN** the server responds with the current dead-trigger records

#### Scenario: Event listing is bounded by default

- **WHEN** a client lists retained events without specifying a page size
- **THEN** the server returns at most a default page of events rather than the
  entire retained window

### Requirement: Worklane dead-letter inspection

The CLI SHALL let an operator inspect the Worklane broker's job-side dead-letter
store for a lane — listing records, counting them, and purging them — distinct from
Triggerlane's own trigger-side dead-trigger queue. These operations act on the
shared broker store through the broker contract; they SHALL NOT configure the
broker's dead-letter retention, which is the Worklane worker's responsibility.

#### Scenario: Dead-letters are counted for a lane

- **WHEN** the operator runs the Worklane dead-letter count subcommand for a lane
- **THEN** the CLI reports how many dead-letter records the broker holds for that lane

#### Scenario: Dead-letters are purged for a lane

- **WHEN** the operator runs the Worklane dead-letter purge subcommand for a lane
- **THEN** the CLI removes that lane's dead-letter records and reports how many were removed

### Requirement: Read-endpoint authentication

The server SHALL support a bearer token, separate from the inbound webhook secret,
that guards every read-router endpoint — the read endpoints expose stored event
payloads and the replay endpoints submit jobs. When the token is configured, a
request without a valid `Authorization: Bearer <token>` SHALL be rejected as
unauthorized; when it is unset, the read router is open for trusted-network
deployments (mirroring the optional webhook secret). The token comparison SHALL be
constant-time.

#### Scenario: Configured token is required

- **WHEN** a read token is configured and a client calls a read or replay endpoint
  without a valid bearer token
- **THEN** the server responds with an unauthorized status and does not read or
  replay

#### Scenario: Valid token is accepted

- **WHEN** a read token is configured and a client presents the matching bearer
  token
- **THEN** the server serves the read or replay request

#### Scenario: Unset token leaves reads open

- **WHEN** no read token is configured
- **THEN** the read endpoints are reachable without authentication
