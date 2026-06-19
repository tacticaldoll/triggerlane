# observability Specification

## Purpose

Defines Triggerlane runtime observability surfaces, starting with Event ->
Trigger -> Job traces for handled events.

## Requirements
### Requirement: Trigger trace

Triggerlane SHALL record a trigger trace for each event handled by the runtime.

#### Scenario: Event produces a job

- **WHEN** the runtime handles an event that matches a trigger and submits a job
- **THEN** the trace records the event id, event type, source, trigger name, and submitted job id

#### Scenario: Event matches no triggers

- **WHEN** the runtime handles an event that matches no triggers
- **THEN** the trace records the event id with no matched triggers or submitted jobs

#### Scenario: Trigger handling fails

- **WHEN** runtime handling records a trigger-side failure
- **THEN** the trace records the failure count for that event

### Requirement: In-memory trace sink

Triggerlane SHALL provide an in-memory trace sink for tests and early inspection.

#### Scenario: Traces are inspected

- **WHEN** events have been handled through a runtime configured with the in-memory trace sink
- **THEN** a caller can read the recorded trace entries

### Requirement: Structured logging output

The runnable binary SHALL emit structured, level-filtered logs to a standard
diagnostic stream, separate from command result output. The log level SHALL be
controllable through the environment, and the output format SHALL be selectable
between a human-readable form and a machine-readable (JSON) form for log
aggregators. The trigger trace recorded for each handled event SHALL be writable
to this log output through a trace sink, carrying the event id, event type,
source, and the matched-trigger, submitted-job, and failure counts; a handled
event with trigger-side failures SHALL be logged at a higher severity than a
clean one.

#### Scenario: Handled event is logged

- **WHEN** the runtime handles an event through a runtime wired to the logging trace sink
- **THEN** a structured log record is emitted carrying the event id, event type, source, and the matched / submitted / failure counts

#### Scenario: Failures raise log severity

- **WHEN** a handled event records one or more trigger-side failures
- **THEN** its log record is emitted at a higher severity than an event handled without failures

#### Scenario: Log level and format are configurable

- **WHEN** an operator sets the log level and selects the JSON output format through the environment
- **THEN** the binary emits machine-readable structured logs filtered to the requested level

### Requirement: HTTP request and error logging

The HTTP server SHALL log each request to the event receiver with its method,
path, response status, and latency, and SHALL log a rejected or failed request
together with the error that caused it. Health and readiness probe requests
SHALL NOT be logged, to avoid flooding the log with probe traffic.

#### Scenario: Event request is logged

- **WHEN** a request is handled by the event receiver
- **THEN** a log record captures its method, path, response status, and latency

#### Scenario: Rejected request logs its error

- **WHEN** a request is rejected (for example, an invalid body or a failed signature check) or fails during handling
- **THEN** a log record captures the error that caused the rejection or failure

#### Scenario: Probe requests are not logged

- **WHEN** a liveness or readiness probe endpoint is requested
- **THEN** no request log record is emitted for it

### Requirement: Metrics endpoint

The runnable HTTP server SHALL expose runtime metrics for scraping in the
Prometheus text exposition format at a dedicated endpoint, alongside the event
receiver and probes. The metrics SHALL cover, at minimum, the number of events
handled, trigger matches, jobs submitted, and trigger-side failures, the event
handling latency, and the current dead-trigger queue depth. Per-route metrics
SHALL carry dimensions for the route they describe — the handled-event count by
event source and type, and the trigger-match / job-submitted / trigger-failure
counts by trigger name — using label sets whose cardinality is bounded (a fixed
source enum, a bounded event-type vocabulary, and the registered trigger set). The
server SHALL also report the Worklane broker backlog (pending job count) as a gauge
labelled by lane, for the lanes the runtime has submitted to, so queue depth is
visible to autoscaling. The metrics endpoint SHALL NOT itself be request-logged, so
scrape traffic does not flood the log. Metric emission SHALL be a no-op when no
metrics recorder is installed, so the trigger runtime can be used without one.

#### Scenario: Metrics are scrapeable

- **WHEN** a client scrapes the metrics endpoint of a running server
- **THEN** the server responds with the current metrics in Prometheus text format

#### Scenario: Handling an event updates metrics

- **WHEN** the server handles an event that matches a trigger and submits a job
- **THEN** the events-handled, trigger-match, and jobs-submitted counters increase and the handling-latency metric is observed

#### Scenario: Dead-trigger depth is reported

- **WHEN** trigger-side failures are present in the dead-trigger queue
- **THEN** the metrics report the current dead-trigger queue depth as a gauge

#### Scenario: Scrape traffic is not logged

- **WHEN** the metrics endpoint is scraped
- **THEN** no request log record is emitted for it

### Requirement: Store size metrics

The runnable HTTP server SHALL export, among its metrics, the on-disk size in bytes
and the record count of both the event store and the dead-trigger queue, so
operators can alert on store growth before disk or memory exhaustion. As with other
metrics, emission SHALL be a no-op when no metrics recorder is installed.

#### Scenario: Store size and count are reported

- **WHEN** a client scrapes the metrics endpoint of a running server
- **THEN** the metrics include the byte size and record count of the event store
  and the dead-trigger queue

#### Scenario: Store metrics are a no-op without a recorder

- **WHEN** the runtime runs without a metrics recorder installed
- **THEN** emitting store size metrics does nothing and does not error
