# trigger-runtime Specification

## Purpose

Defines how the Triggerlane runtime evaluates events, submits Worklane jobs, and
records trigger-side failures.
## Requirements
### Requirement: Runtime event handling

Triggerlane SHALL provide a runtime entrypoint that handles one event envelope by
evaluating registered triggers and recording a trigger trace.

#### Scenario: Event is handled

- **WHEN** `runtime.handle(event)` is called
- **THEN** the runtime evaluates enabled registered triggers against the event and records a trace for the handling run

### Requirement: Replay by id

Triggerlane SHALL provide a runtime replay entrypoint that loads a stored event
envelope by id from the retained event window and evaluates it through the current
trigger registry. Replay is scoped to the retained window: an event that has been
pruned by retention is no longer replayable.

#### Scenario: Stored event is replayed by id

- **WHEN** a retained event id is replayed
- **THEN** the runtime loads the stored `EventEnvelope`
- **AND** evaluates it through the same trigger handling path used for live
  events

#### Scenario: Replay event id is missing

- **WHEN** replay is requested for an event id that is not present in the retained
  event window
- **THEN** the runtime returns a replay not-found error without submitting a
  Worklane job

#### Scenario: Replay of a pruned event is not found

- **WHEN** replay is requested for an event id that was ingested but has since been
  removed by retention
- **THEN** the runtime returns a replay not-found error

### Requirement: Replay by timestamp range

Triggerlane SHALL provide a runtime replay entrypoint that evaluates retained
events whose timestamps fall within a requested time range. Only events still
within the retained window are eligible; events pruned by retention are not
replayed. The entrypoint SHALL accept an optional filter that further narrows the
range by event type and/or source, and SHALL provide a dry-run preview that returns
the events a replay would process without submitting any jobs.

#### Scenario: Stored events are replayed by timestamp range

- **WHEN** replay is requested for a timestamp range
- **THEN** the runtime loads retained events whose timestamps are within that
  range
- **AND** evaluates each matching event through the same trigger handling path
  used for live events

#### Scenario: Range replay preserves append order

- **WHEN** multiple retained events match a replay range
- **THEN** the runtime replays them in event store append order

#### Scenario: Range replay is narrowed by a filter

- **WHEN** a replay range is requested with an event-type or source filter
- **THEN** the runtime replays only the events in the window that also match the
  filter

#### Scenario: Dry-run previews without submitting

- **WHEN** a range replay is requested as a dry run
- **THEN** the runtime returns the matching events and submits no jobs

#### Scenario: Range replay returns per-event reports

- **WHEN** retained events are replayed by timestamp range
- **THEN** the runtime returns a replay report containing each replayed event id
  and handling report

#### Scenario: Pruned events in range are skipped

- **WHEN** a replay range covers a period whose events have been removed by
  retention
- **THEN** only the still-retained events in that range are replayed

### Requirement: Rule evaluation

Triggerlane SHALL separate trigger rule evaluation from job submission.

#### Scenario: No triggers match

- **WHEN** no enabled trigger matches an event
- **THEN** the runtime completes handling without submitting a Worklane job

### Requirement: Job submission

Triggerlane SHALL enqueue a Worklane job for each matched binding selected by
runtime evaluation.

#### Scenario: Trigger matches

- **WHEN** an enabled trigger matches and its binding creates a job
- **THEN** the runtime submits that job to the configured Worklane broker or client boundary

### Requirement: Submission retry with bounded backoff

The runtime SHALL attempt a job submission up to a configurable number of times
before recording a dead trigger, and SHALL wait a backoff delay between attempts so
a struggling broker is not retried immediately. The backoff SHALL grow between
successive attempts and SHALL be bounded by a maximum delay; a zero base SHALL
disable waiting. Retried submission is safe because each attempt reuses the same
deterministic unique key, so the broker deduplicates a job that did land.

#### Scenario: Transient submission failure is retried

- **WHEN** a submission fails transiently and another attempt remains
- **THEN** the runtime waits a backoff delay and retries with the same unique key

#### Scenario: Exhausted attempts record a dead trigger

- **WHEN** every submission attempt fails
- **THEN** the runtime records a dead trigger rather than retrying without limit

### Requirement: Failure handling

Triggerlane SHALL classify trigger evaluation and submission failures as
retryable or terminal.

#### Scenario: Retryable failure occurs

- **WHEN** runtime handling fails with a retryable trigger failure
- **THEN** Triggerlane records the failure for retry according to its retry policy

### Requirement: Dead trigger queue

Triggerlane SHALL record terminal trigger handling failures in a dead-trigger
queue. When recording a failure to a durable queue itself fails, that storage
failure SHALL be surfaced to the caller as an error rather than aborting the
process, so the inability to persist a dead trigger is itself observable.

#### Scenario: Terminal trigger failure occurs

- **WHEN** runtime handling fails terminally or retry attempts are exhausted
- **THEN** Triggerlane records the failed event, trigger context, and error in the DTQ

#### Scenario: Dead-trigger persistence failure is reported

- **WHEN** recording a dead trigger to the durable queue fails
- **THEN** runtime handling returns that storage failure as an error rather than panicking

#### Scenario: Binding produces an invalid Worklane lane

- **WHEN** a matched binding produces a job whose lane name is rejected by the
  Worklane `Lane` contract
- **THEN** Triggerlane records the failure in the dead-trigger queue without
  submitting a Worklane job and without retrying the submission

### Requirement: Idempotent submission

When the runtime submits a Worklane job for a matched trigger, it SHALL attach a
deterministic submission key derived from the event identity (its idempotency key
when present, otherwise its stable event id) and the matched trigger name, so that
a duplicate or replayed submission of the same event through the same trigger is
deduplicated by the Worklane broker while the prior job is live. The runtime SHALL
NOT implement its own job dedup store; it only supplies the key.

#### Scenario: Replaying the same event does not double-submit

- **WHEN** the same stored event is handled again (for example via replay) while
  its previously submitted job is still live
- **THEN** the runtime submits the job with the same deterministic key
- **AND** the Worklane broker deduplicates it to the existing job rather than
  creating a second one

#### Scenario: Distinct events submit distinct jobs

- **WHEN** two events with different identities match the same trigger
- **THEN** their submission keys differ and each produces its own Worklane job

### Requirement: Durable dead-trigger queue

The dead-trigger queue SHALL be pluggable behind a storage abstraction so it can
be backed durably. The shipped binary SHALL use a durable queue whose records
survive a process restart; tests and the default MAY use an in-memory queue. The
queue SHALL be inspectable, returning each recorded failed event, trigger name,
and error.

#### Scenario: Dead triggers survive a restart

- **WHEN** dead-trigger records are written to a durable queue and the process
  restarts with the same backing
- **THEN** the previously recorded dead-trigger records are still readable

#### Scenario: Dead-trigger records are inspectable

- **WHEN** an operator inspects the dead-trigger queue
- **THEN** it returns each recorded failed event, its trigger name, and the error

### Requirement: Dead-trigger retry

Triggerlane SHALL support retrying dead-trigger records by draining the
dead-trigger queue and re-handling the failed events through the normal
trigger-handling path. A drain SHALL atomically remove the records it returns
from the queue, including from the durable backing, so a retried record is not
left behind. Re-handling SHALL reuse the runtime's deterministic submission keys
so a trigger that previously succeeded for an event is deduplicated by the
Worklane broker rather than submitted twice; a trigger that fails again SHALL be
re-recorded in the dead-trigger queue. A retry pass SHALL report how many records
were drained and, of those, how many recovered versus failed again.

#### Scenario: Transient failure recovers on retry

- **WHEN** a dead-trigger record's underlying failure has been resolved and a retry pass re-handles its event
- **THEN** the record's trigger submits its job successfully and the record is not present in the queue after the pass

#### Scenario: Persistent failure is re-recorded

- **WHEN** a retry pass re-handles an event whose trigger still fails
- **THEN** the failure is recorded again in the dead-trigger queue and counted as failed again

#### Scenario: Retry does not double-submit a succeeded trigger

- **WHEN** an event had one failed trigger and one succeeded trigger, and the event is re-handled during a retry pass
- **THEN** the previously succeeded trigger's submission is deduplicated by the broker rather than creating a second job

#### Scenario: Retry pass reports its outcome

- **WHEN** a retry pass drains and re-handles the queue
- **THEN** it reports the number of records drained and, of those, the number recovered and the number that failed again

### Requirement: Dead-trigger queue retention

The durable dead-trigger queue SHALL support pruning records on operator demand,
either removing records whose event is older than a cutoff timestamp or retaining
only the most recent N records, so the append-only backing does not grow without
bound. Pruning SHALL preserve the records it retains, SHALL be atomic with
respect to the durable backing, and SHALL report how many records were removed.
Retention SHALL be an explicit operation, not automatic, so discarding
unresolved failure history is always an operator action.

#### Scenario: Prune by age keeps newer records

- **WHEN** the dead-trigger queue is pruned to remove records older than a cutoff
- **THEN** records whose event is at or after the cutoff are retained and older records are removed, and the count removed is reported

#### Scenario: Prune by count bounds the queue

- **WHEN** the dead-trigger queue is pruned to keep only the most recent N records
- **THEN** at most N records remain and the rest are removed

#### Scenario: Pruned durable queue survives reopen

- **WHEN** a durable dead-trigger queue is pruned and then reopened from the same location
- **THEN** only the retained records are present, and further records still persist

