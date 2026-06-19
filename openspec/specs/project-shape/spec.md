# project-shape Specification

## Purpose

Defines Triggerlane's repository identity, workspace shape, architecture
documentation, and Worklane dependency boundary.
## Requirements
### Requirement: Triggerlane product identity

The repository SHALL identify Triggerlane as a durable, declarative event-trigger
plane — event ingestion, trigger matching, routing, and replay — layered on the
Worklane execution substrate. It SHALL NOT describe itself as a short-lived or
disposable probe.

#### Scenario: Project metadata is read

- **WHEN** a contributor reads the project README, PROJECT contract, or OpenSpec config
- **THEN** the project is identified as Triggerlane, the declarative event-trigger and replay plane that submits typed jobs to the Worklane execution substrate, and not as a short-lived probe or the starter template

### Requirement: Workspace crate layout

The Rust workspace SHALL contain crates for `triggerlane-core`,
`triggerlane-runtime`, `triggerlane-http`, `triggerlane-storage`, and
`triggerlane-cli`, plus an `examples` workspace member.

#### Scenario: Workspace is inspected

- **WHEN** a contributor inspects the workspace members
- **THEN** each v0.1 crate and the examples member are present

### Requirement: Architecture documentation

The repository SHALL document the v0.1 definitions of Event, Trigger, Binding,
and Execution.

#### Scenario: Architecture document is read

- **WHEN** a contributor reads `docs/architecture.md`
- **THEN** Event, Trigger, Binding, and Execution are defined consistently with the OpenSpec specs

### Requirement: Worklane integration boundary

Triggerlane SHALL integrate with Worklane through an explicit repository-managed
dependency boundary for job submission.

#### Scenario: Worklane dependency is inspected

- **WHEN** a contributor inspects workspace dependencies
- **THEN** Worklane integration is declared through the `worklane/` git submodule and is not hidden inside unrelated crates

### Requirement: Worklane dependency policy

Triggerlane SHALL document and centralize the policy for depending on Worklane
crates.

#### Scenario: Worklane dependency policy is inspected

- **WHEN** a contributor inspects the repository dependency guidance
- **THEN** it states that local development uses the `worklane/` git submodule
  pinned by the repository, and that non-local reproducibility follows the
  recorded submodule commit

#### Scenario: Worklane crate dependency layout is inspected

- **WHEN** a contributor inspects Triggerlane crate dependencies
- **THEN** Worklane crate paths are declared through root workspace dependencies
  rather than hidden inside member crate manifests

#### Scenario: Worklane broker implementation dependency is inspected

- **WHEN** a contributor inspects dependencies on Worklane broker
  implementations such as `worklane-memory`
- **THEN** Triggerlane runtime, HTTP, and CLI event-handling logic depends only on
  the `worklane-core` `Broker` contract, and concrete broker implementations
  appear only in tests, examples, and a single CLI broker composition seam

#### Scenario: Concrete broker is selected at one seam

- **WHEN** the CLI binary needs a concrete Worklane broker
- **THEN** the broker is constructed at a single composition seam and injected
  into broker-generic CLI logic, so the concrete-broker choice is replaceable
  without editing event-handling code

### Requirement: Core stays Worklane-agnostic

The `triggerlane-core` crate SHALL depend on neither `worklane-core` nor any
domain crate. The bridge from Triggerlane's job representation to Worklane
contract types SHALL live only in the product's runtime and HTTP crates.

#### Scenario: Core dependencies are inspected

- **WHEN** a contributor inspects `triggerlane-core`'s manifest and code
- **THEN** it depends on no `worklane-*` crate and contains no Worklane-specific
  types; its `WorklaneJob` is a plain owned record

#### Scenario: Worklane bridge location is inspected

- **WHEN** a contributor traces where a Triggerlane job becomes a Worklane job
- **THEN** the conversion to `worklane-core` types such as `Lane` and `NewJob`
  occurs only in `triggerlane-runtime` or `triggerlane-http`, not in
  `triggerlane-core`

### Requirement: Triggering, not orchestration

Triggerlane SHALL own the event-trigger plane — event ingestion and
normalization, trigger matching, typed binding, routing/fan-out, and replay — up
to submitting jobs to the Worklane substrate. Triggerlane SHALL NOT own the job
execution lifecycle (reservation, run, retry, dead-letter), which belongs to
Worklane, and SHALL NOT provide a stateful multi-step orchestration engine.
Multi-step coordination is choreographed by consumers via events, not run by a
central Triggerlane coordinator.

#### Scenario: Provided scope is inspected

- **WHEN** a contributor inspects what Triggerlane provides
- **THEN** it provides event triggering, routing, and replay primitives, and does
  not implement the job execution lifecycle or a stateful orchestration/step
  engine

#### Scenario: Multi-step coordination is needed

- **WHEN** a consumer needs multi-step coordination across jobs
- **THEN** the consumer composes it via events — a job result emitted as a new
  event re-enters trigger evaluation — rather than Triggerlane running a central
  orchestration state machine

### Requirement: Durable default broker

The shipped CLI binary SHALL default to a durable Worklane broker so jobs the
trigger plane submits survive a process restart and are drainable by a Worklane
worker, while also allowing the operator to select an alternative shipped broker
backend (such as Postgres or Redis) at the single broker composition seam. The
concrete broker SHALL be constructed at that seam and injected as an
`Arc<dyn Broker>`, leaving the rest of the CLI generic over the `worklane-core`
`Broker` contract.

#### Scenario: Default broker is durable

- **WHEN** the shipped CLI binary starts without overriding the broker
- **THEN** it opens a durable, file-backed Worklane broker rather than the
  in-memory broker

#### Scenario: Alternative durable backend is selectable

- **WHEN** the operator selects a supported network broker backend at the CLI
- **THEN** the CLI connects to that backend through the same composition seam
  without changes to event-handling code

#### Scenario: In-memory broker stays test-only

- **WHEN** a contributor inspects where the in-memory broker is used
- **THEN** it appears only in tests and examples, not as the shipped binary's
  broker

