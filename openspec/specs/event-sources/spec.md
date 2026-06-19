# event-sources Specification

## Purpose

Defines the source boundaries that normalize incoming HTTP, webhook, CLI, and
file inputs into replayable event envelopes.
## Requirements
### Requirement: HTTP event receiver

Triggerlane SHALL expose an HTTP receiver that accepts valid event JSON with
`POST /events`, normalizes the request into an `EventEnvelope`, submits the event
to the ingest pipeline so it is persisted and handled, and returns a JSON
handling response.

#### Scenario: HTTP event is posted

- **WHEN** a client posts a valid event to `/events`
- **THEN** Triggerlane normalizes it into an `EventEnvelope`, submits it to the ingest pipeline, and returns a successful JSON response containing the accepted event id

#### Scenario: Invalid HTTP event is posted

- **WHEN** a client posts invalid event JSON to `/events`
- **THEN** Triggerlane rejects the request as a client error without invoking the ingest pipeline

### Requirement: Webhook receiver

Triggerlane SHALL provide receiver adapters for webhook-style sources, starting
with GitHub.

#### Scenario: GitHub webhook is received

- **WHEN** Triggerlane receives a supported GitHub webhook
- **THEN** it normalizes the webhook into the corresponding GitHub event type

### Requirement: Discord and Slack receiver placeholders

Triggerlane SHALL reserve source adapter boundaries for Discord and Slack even
if v0.1 implements only minimal normalization.

#### Scenario: Adapter boundary is inspected

- **WHEN** a contributor inspects event source modules
- **THEN** Discord and Slack source boundaries are present and do not require changes to the core event model

### Requirement: CLI event injection

Triggerlane SHALL provide a CLI command for manually injecting events.

#### Scenario: Event is injected

- **WHEN** a developer runs `triggerlane inject` with valid event input
- **THEN** Triggerlane normalizes the input and submits the event to the ingest pipeline so it is persisted and handled

### Requirement: File event source

Triggerlane SHALL support loading events from an `events.json` file.

#### Scenario: Events file is loaded

- **WHEN** a developer provides a valid `events.json`
- **THEN** Triggerlane reads the file events and submits each normalized envelope to the ingest pipeline so each is persisted and handled

### Requirement: Runnable HTTP server entrypoint

Triggerlane SHALL provide a runnable entrypoint that serves the HTTP event
receiver on a configurable bind address, mounting the `POST /events` route on the
ingest pipeline so accepted events are persisted and handled.

#### Scenario: Server serves the events route

- **WHEN** an operator starts the HTTP server entrypoint with a bind address
- **THEN** the server listens on that address and accepts `POST /events` requests through the ingest pipeline

#### Scenario: Bind address is configurable

- **WHEN** an operator provides a bind address to the server entrypoint
- **THEN** the server binds to the provided address instead of the default

### Requirement: Graceful shutdown on termination signals

The runnable HTTP server entrypoint SHALL begin a graceful shutdown when it
receives an operating-system termination signal, draining in-flight requests
rather than being killed abruptly. On Unix targets the entrypoint SHALL treat
both SIGINT and SIGTERM as shutdown signals, so a process supervisor's SIGTERM
stop request triggers the graceful drain. On targets without Unix signals the
entrypoint SHALL fall back to the interrupt signal alone.

#### Scenario: SIGTERM triggers graceful shutdown

- **WHEN** the HTTP server entrypoint is running on a Unix target and the process receives SIGTERM
- **THEN** the entrypoint begins its graceful shutdown and stops accepting new requests while letting in-flight requests complete

#### Scenario: SIGINT triggers graceful shutdown

- **WHEN** the HTTP server entrypoint is running and the process receives SIGINT
- **THEN** the entrypoint begins its graceful shutdown

#### Scenario: Either signal source can trigger shutdown

- **WHEN** the entrypoint's shutdown wait is composed from more than one signal source and any one source fires
- **THEN** the composed shutdown future resolves and graceful shutdown begins

### Requirement: Health and readiness endpoints

The runnable HTTP server entrypoint SHALL expose a liveness endpoint and a
readiness endpoint for load-balancer and orchestrator probes, alongside the
event receiver. The liveness endpoint SHALL respond with a success status
whenever the server is serving, reflecting process liveness rather than
dependency state. The readiness endpoint SHALL respond with a success status
while the service should receive traffic and with a service-unavailable status
while it is draining. When graceful shutdown begins, the entrypoint SHALL mark
readiness as draining so the readiness endpoint reports service-unavailable
before the server stops accepting connections, while the event receiver path
remains unchanged.

#### Scenario: Liveness probe succeeds while serving

- **WHEN** a client probes the liveness endpoint of a running server
- **THEN** the server responds with a success status

#### Scenario: Readiness probe succeeds while accepting traffic

- **WHEN** a client probes the readiness endpoint of a server that is accepting traffic
- **THEN** the server responds with a success status

#### Scenario: Readiness probe reports unavailable while draining

- **WHEN** graceful shutdown has begun and a client probes the readiness endpoint
- **THEN** the server responds with a service-unavailable status

### Requirement: Request timeout

The runnable HTTP server entrypoint SHALL bound the time spent handling a
request with a configurable timeout, so a slow or stalled client cannot hold a
connection and its handling task open indefinitely. A request that does not
complete within the timeout SHALL be terminated with a timeout status rather than
allowed to run without limit. The timeout SHALL be configurable through the
environment with a sane default, and SHALL be disableable for operators who
enforce request timeouts at a fronting proxy.

#### Scenario: Request exceeding the timeout is terminated

- **WHEN** a request to the running server takes longer than the configured timeout to complete
- **THEN** the server terminates it with a request-timeout status instead of waiting indefinitely

#### Scenario: Request within the timeout succeeds

- **WHEN** a request completes within the configured timeout
- **THEN** the server responds normally

#### Scenario: Timeout is configurable and disableable

- **WHEN** an operator configures the request timeout through the environment, including disabling it
- **THEN** the server applies the configured timeout, or applies no request timeout when disabled

### Requirement: Webhook signature verification

The HTTP receiver SHALL support verifying an HMAC-SHA256 signature, computed over
the raw request body with a configured shared secret and read from a configured
signature header, using a constant-time comparison. When a verifier is
configured, the receiver SHALL reject a request whose signature is missing or
does not match as unauthorized, before invoking the ingest pipeline. When no
verifier is configured, the receiver SHALL accept requests without a signature
check.

#### Scenario: Valid signature is accepted

- **WHEN** a verifier is configured and a request carries the correct
  HMAC-SHA256 signature of its body
- **THEN** the receiver accepts the request and submits the event to the ingest
  pipeline

#### Scenario: Missing or invalid signature is rejected

- **WHEN** a verifier is configured and a request's signature header is absent or
  does not match the body
- **THEN** the receiver rejects the request as unauthorized and does not invoke
  the ingest pipeline

#### Scenario: No verifier configured

- **WHEN** no verifier is configured on the receiver
- **THEN** the receiver accepts requests without a signature check

