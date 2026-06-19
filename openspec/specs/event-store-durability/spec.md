# event-store-durability Specification

## Purpose

Defines the disk-durability, crash recovery, single-writer integrity, fault
containment, delivery-state tracking, and bounded automatic retention guarantees
of the JSONL-backed event store and dead-trigger queue — the foundation of
Triggerlane's Hybrid WAL.

## Requirements

### Requirement: Durable-by-acknowledgement append

The file-backed event store and dead-trigger queue SHALL persist an appended
record to stable storage before reporting the append as successful. The append
SHALL flush the record's bytes and the file-size metadata needed to read them back
(for example via `fdatasync`/`sync_data`) prior to returning, and SHALL flush the
parent directory entry after first creating the file and after any compaction
rename, so an acknowledged record survives a power loss or OS crash.

#### Scenario: Append is durable before acknowledgement

- **WHEN** a record is appended to a file-backed store
- **THEN** the store flushes the record to stable storage before returning success

#### Scenario: Acknowledged record survives a simulated crash

- **WHEN** a record's append has returned success and the store is reopened from
  the same file without a clean shutdown
- **THEN** the reopened store contains that record

### Requirement: Crash-torn trailing line recovery

On open, the file-backed store SHALL tolerate a single torn trailing record — a
final line that fails to parse when the file does not end in a newline, which is
the signature of a crash between writing a record and completing its line — by
discarding that fragment and recovering the remaining records. A malformed record
that is not the final line, or a final malformed record whose line is newline
terminated, SHALL remain a hard error that fails the open.

#### Scenario: Torn final line is recovered

- **WHEN** a store file ends in a record that is not newline terminated and does
  not parse
- **THEN** opening the store discards the torn fragment and returns the preceding
  records

#### Scenario: Corruption in an earlier line still fails

- **WHEN** a store file contains a malformed line that is not the final line
- **THEN** opening the store returns an error identifying the offending line

### Requirement: Single-writer integrity

The file-backed store SHALL write each record's body and line terminator as a
single write, and SHALL hold an advisory exclusive lock on the store file for the
lifetime of the handle so that a second process opening the same file fails fast
rather than interleaving writes or appending into a renamed-away file.

#### Scenario: Record is written atomically

- **WHEN** a record is appended
- **THEN** its serialized body and line terminator are written in one write
  operation

#### Scenario: Concurrent opener fails fast

- **WHEN** a process opens a store file that another process already holds open
- **THEN** the second open fails with a clear error instead of corrupting the file

### Requirement: Fault containment on lock poisoning

The store's internal locks SHALL recover from poisoning rather than propagating a
panic, so a transient fault in one operation does not render every subsequent store
operation unusable for the life of the process.

#### Scenario: Store remains usable after a poisoned lock

- **WHEN** a thread panics while holding a store lock and a later operation
  acquires that lock
- **THEN** the later operation proceeds against the still-consistent data instead
  of panicking

### Requirement: Delivery-state tracking

The file-backed event store SHALL durably record, per event, whether the event has
been delivered, where delivered means every trigger matched for that event has had
its job accepted by Worklane or recorded to the dead-trigger queue. The delivery
record SHALL survive a restart, and on open the store SHALL be able to report which
retained events are not yet delivered.

#### Scenario: Event becomes delivered when its work is taken

- **WHEN** every matched trigger's job for an event is accepted by Worklane or
  recorded to the dead-trigger queue
- **THEN** the store durably marks that event as delivered

#### Scenario: Undelivered events are identifiable after restart

- **WHEN** the store is reopened after events were ingested but not all delivered
- **THEN** the store reports the not-yet-delivered events

### Requirement: Bounded automatic retention

The file-backed store SHALL support an automatic, configurable retention policy
that bounds the store by age and/or size. Automatic retention SHALL remove an event
only when it is delivered and older than a configured grace window, or when it
exceeds a configured hard age or size bound; it SHALL NOT remove an undelivered
event before its hard bound. The existing operator-driven manual prune SHALL remain
available, and retention SHALL be disable-able (unbounded) for backward-compatible
behavior.

#### Scenario: Delivered, aged event is auto-removed

- **WHEN** automatic retention runs and an event is delivered and older than the
  grace window
- **THEN** the event is removed from the store

#### Scenario: Undelivered event is retained within bounds

- **WHEN** automatic retention runs and an event is not yet delivered and within
  its hard bounds
- **THEN** the event is retained

#### Scenario: Retention can be disabled

- **WHEN** the retention policy is configured as unbounded
- **THEN** automatic retention removes nothing and the store retains all records

### Requirement: Dead-trigger queue is hard-bounded

The dead-trigger queue SHALL be subject to automatic hard-bound retention
(`max_age` / `max_count` / `max_bytes`) so that persistently failing triggers
cannot grow it without limit. The delivered-grace bound SHALL NOT apply to the
dead-trigger queue, since a dead-trigger has no delivery state. A shipped default
hard `max_count` SHALL bound both the event store and the dead-trigger queue so
neither grows without limit out of the box.

#### Scenario: Failing triggers do not grow the queue without limit

- **WHEN** the dead-trigger queue exceeds a configured hard bound
- **THEN** automatic retention removes the oldest records to satisfy the bound

#### Scenario: Grace does not remove dead-triggers

- **WHEN** only a delivered-grace bound is configured
- **THEN** automatic retention removes no dead-trigger records
