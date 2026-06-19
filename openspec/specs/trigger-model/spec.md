# trigger-model Specification

## Purpose

Defines trigger matching, typed bindings, registry behavior, priority, and
enablement controls.
## Requirements
### Requirement: Trigger trait

Triggerlane SHALL define a trigger abstraction that determines whether a trigger
matches an `EventEnvelope`.

#### Scenario: Trigger evaluates event

- **WHEN** the runtime evaluates a trigger against an event envelope
- **THEN** the trigger returns whether it matches without submitting work itself

### Requirement: Typed binding

Triggerlane SHALL define a binding abstraction that maps matched events to typed
Worklane jobs.

#### Scenario: Matched event is bound

- **WHEN** a trigger matches an event with an associated binding
- **THEN** the binding identifies the Worklane job type to submit

### Requirement: Event to job mapping

Triggerlane SHALL support event-to-job mappings such as
`event.github.issue.created` to `CreateProjectionJob`.

#### Scenario: GitHub issue is mapped

- **WHEN** an event with type `event.github.issue.created` matches the configured trigger
- **THEN** Triggerlane prepares a `CreateProjectionJob` submission

### Requirement: Trigger registry

Triggerlane SHALL provide a registry for registering triggers and their bindings.

#### Scenario: Trigger is registered

- **WHEN** a trigger binding is registered
- **THEN** the runtime can include it in later event evaluation

### Requirement: Trigger priority

Triggerlane SHALL evaluate matched triggers according to configured priority.

#### Scenario: Multiple triggers match

- **WHEN** multiple enabled triggers match the same event
- **THEN** the runtime orders their bindings by priority before submission

### Requirement: Trigger enablement

Triggerlane SHALL allow triggers to be enabled or disabled.

#### Scenario: Disabled trigger would match

- **WHEN** a disabled trigger would otherwise match an event
- **THEN** the runtime does not submit that trigger's bound job

### Requirement: Conditional payload matching

Triggerlane SHALL support a trigger that matches on event payload content in
addition to event type. A payload-field condition SHALL match when the event
payload is JSON and the value at a configured JSON Pointer path equals a
configured value, and SHALL NOT match when the payload is not JSON, the field is
absent, or the value differs.

#### Scenario: Payload field matches the configured value

- **WHEN** an event whose JSON payload holds the configured value at the
  configured field path is evaluated against a payload-field condition
- **THEN** the condition matches

#### Scenario: Payload field differs, is absent, or payload is not JSON

- **WHEN** an event is evaluated against a payload-field condition and the field
  holds a different value, is absent, or the payload is not JSON
- **THEN** the condition does not match

### Requirement: Composed trigger conditions

Triggerlane SHALL support composing trigger conditions with all-of (logical AND)
and any-of (logical OR), so a single trigger can require, for example, an event
type together with a payload-field condition.

#### Scenario: All-of requires every condition

- **WHEN** an event is evaluated against an all-of trigger
- **THEN** it matches only if every composed condition matches

#### Scenario: Any-of requires at least one condition

- **WHEN** an event is evaluated against an any-of trigger
- **THEN** it matches if at least one composed condition matches

### Requirement: Binding payload transformation

Triggerlane SHALL allow a binding to transform the matched event into the
Worklane job payload instead of passing the event payload through unchanged. When
no transform is configured, the job payload SHALL equal the event payload, and a
transform that rejects the event SHALL produce a binding error.

#### Scenario: Default binding passes the payload through

- **WHEN** a binding with no payload transform binds a matched event
- **THEN** the job payload equals the event payload

#### Scenario: Transform shapes the job payload

- **WHEN** a binding configured with a payload transform binds a matched event
- **THEN** the job payload is the transform's output

#### Scenario: Rejecting transform yields a binding error

- **WHEN** a configured payload transform rejects the event
- **THEN** binding returns a binding error (which the runtime records as a dead
  trigger)

### Requirement: Declarative trigger configuration

Triggerlane SHALL allow the runnable binary's trigger set to be defined in a
declarative configuration source, so an operator can define `event -> job` rules
without modifying or recompiling Rust code. The configuration SHALL compose the
existing trigger conditions — event-type, payload-field-equals, and all-of /
any-of combinators — and SHALL bind a matched event to a Worklane job by lane,
kind, and maximum attempts. Each configured entry SHALL carry a name, a
priority, and an enabled flag, and SHALL feed the same registry, priority
ordering, and enablement semantics as code-registered triggers. A configured
binding SHALL pass the event payload through unchanged; code-defined payload
transforms are out of scope for the configuration source.

When the configuration source is malformed or unreadable, the binary SHALL fail
to start with a clear error rather than run with an incomplete or wrong trigger
set. When no configuration source is provided, the binary SHALL fall back to its
built-in default trigger set.

#### Scenario: Operator defines triggers in configuration

- **WHEN** an operator provides a trigger configuration that composes an event-type and a payload-field condition with a job binding
- **THEN** the binary loads those triggers into its registry and an event matching the composed condition is bound to the configured Worklane job, without any code change

#### Scenario: Priority and enablement honored from configuration

- **WHEN** a configuration defines multiple triggers with priorities and one disabled entry
- **THEN** matched triggers are ordered by their configured priority and the disabled entry does not submit its bound job

#### Scenario: Malformed configuration is rejected

- **WHEN** the configured trigger source cannot be read or parsed
- **THEN** the binary reports a configuration error and does not start with a partial trigger set

#### Scenario: No configuration falls back to the built-in default

- **WHEN** no trigger configuration source is provided
- **THEN** the binary uses its built-in default trigger set

