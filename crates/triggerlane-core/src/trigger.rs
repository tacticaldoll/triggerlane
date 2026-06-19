use std::{fmt, sync::Arc};

use thiserror::Error;

use crate::{EventEnvelope, EventType};

pub trait Trigger: Send + Sync {
    fn matches(&self, event: &EventEnvelope) -> bool;
}

pub trait Binding: Send + Sync {
    type Job;

    fn bind(&self, event: &EventEnvelope) -> Result<Self::Job, BindingError>;
}

#[derive(Debug, Error)]
pub enum BindingError {
    #[error("binding rejected event: {0}")]
    Rejected(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorklaneJob {
    pub lane: String,
    pub kind: String,
    pub payload: Vec<u8>,
    pub max_attempts: u32,
}

impl WorklaneJob {
    pub fn new(
        lane: impl Into<String>,
        kind: impl Into<String>,
        payload: impl Into<Vec<u8>>,
        max_attempts: u32,
    ) -> Self {
        Self {
            lane: lane.into(),
            kind: kind.into(),
            payload: payload.into(),
            max_attempts,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventTypeTrigger {
    event_type: EventType,
}

impl EventTypeTrigger {
    pub fn new(event_type: impl Into<EventType>) -> Self {
        Self {
            event_type: event_type.into(),
        }
    }
}

impl Trigger for EventTypeTrigger {
    fn matches(&self, event: &EventEnvelope) -> bool {
        event.event_type == self.event_type
    }
}

/// Matches when the event payload is JSON and the value at `pointer` (an
/// RFC 6901 JSON Pointer, e.g. `/issue/state`) equals `expected`. A payload that
/// is not JSON, or a pointer that resolves to nothing, does not match.
#[derive(Debug, Clone)]
pub struct PayloadFieldEquals {
    pointer: String,
    expected: serde_json::Value,
}

impl PayloadFieldEquals {
    pub fn new(pointer: impl Into<String>, expected: impl Into<serde_json::Value>) -> Self {
        Self {
            pointer: pointer.into(),
            expected: expected.into(),
        }
    }
}

impl Trigger for PayloadFieldEquals {
    fn matches(&self, event: &EventEnvelope) -> bool {
        serde_json::from_slice::<serde_json::Value>(event.payload.as_ref())
            .ok()
            .and_then(|value| value.pointer(&self.pointer).cloned())
            .is_some_and(|found| found == self.expected)
    }
}

/// Matches only when every composed trigger matches (logical AND). An empty set
/// matches, so it acts as a neutral element when composing conditions.
pub struct AllOf {
    triggers: Vec<Box<dyn Trigger>>,
}

impl AllOf {
    pub fn new(triggers: Vec<Box<dyn Trigger>>) -> Self {
        Self { triggers }
    }
}

impl Trigger for AllOf {
    fn matches(&self, event: &EventEnvelope) -> bool {
        self.triggers.iter().all(|trigger| trigger.matches(event))
    }
}

/// Matches when at least one composed trigger matches (logical OR). An empty set
/// does not match.
pub struct AnyOf {
    triggers: Vec<Box<dyn Trigger>>,
}

impl AnyOf {
    pub fn new(triggers: Vec<Box<dyn Trigger>>) -> Self {
        Self { triggers }
    }
}

impl Trigger for AnyOf {
    fn matches(&self, event: &EventEnvelope) -> bool {
        self.triggers.iter().any(|trigger| trigger.matches(event))
    }
}

/// Builds the Worklane job payload from a matched event. Returning `Err` rejects
/// the binding (recorded as a dead trigger).
type PayloadTransform = dyn Fn(&EventEnvelope) -> Result<Vec<u8>, BindingError> + Send + Sync;

#[derive(Clone)]
pub struct WorklaneJobBinding {
    lane: String,
    kind: String,
    max_attempts: u32,
    transform: Option<Arc<PayloadTransform>>,
}

impl WorklaneJobBinding {
    pub fn new(lane: impl Into<String>, kind: impl Into<String>, max_attempts: u32) -> Self {
        Self {
            lane: lane.into(),
            kind: kind.into(),
            max_attempts,
            transform: None,
        }
    }

    /// Shape the job payload from the event instead of passing the event payload
    /// through unchanged. The transform may reject the event with a
    /// [`BindingError`].
    pub fn with_payload_transform<F>(mut self, transform: F) -> Self
    where
        F: Fn(&EventEnvelope) -> Result<Vec<u8>, BindingError> + Send + Sync + 'static,
    {
        self.transform = Some(Arc::new(transform));
        self
    }
}

impl fmt::Debug for WorklaneJobBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorklaneJobBinding")
            .field("lane", &self.lane)
            .field("kind", &self.kind)
            .field("max_attempts", &self.max_attempts)
            .field("transform", &self.transform.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

impl Binding for WorklaneJobBinding {
    type Job = WorklaneJob;

    fn bind(&self, event: &EventEnvelope) -> Result<Self::Job, BindingError> {
        let payload = match &self.transform {
            Some(transform) => transform(event)?,
            None => event.payload.to_vec(),
        };
        Ok(WorklaneJob::new(
            self.lane.clone(),
            self.kind.clone(),
            payload,
            self.max_attempts,
        ))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::{EVENT_GITHUB_ISSUE_CREATED, Source};

    use super::*;

    #[test]
    fn event_type_trigger_matches_only_configured_type() {
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(b"{}"),
        );

        assert!(EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED).matches(&event));
        assert!(!EventTypeTrigger::new("event.discord.message.created").matches(&event));
    }

    #[test]
    fn worklane_binding_preserves_event_payload() {
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(br#"{"issue":1}"#),
        );
        let binding = WorklaneJobBinding::new("projection", "CreateProjectionJob", 3);

        let job = binding.bind(&event).expect("event should bind");

        assert_eq!(job.lane, "projection");
        assert_eq!(job.kind, "CreateProjectionJob");
        assert_eq!(job.payload, br#"{"issue":1}"#);
        assert_eq!(job.max_attempts, 3);
    }

    #[test]
    fn binding_payload_transform_shapes_the_job_payload() {
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(br#"{"issue":1}"#),
        );
        let binding = WorklaneJobBinding::new("projection", "CreateProjectionJob", 3)
            .with_payload_transform(|_event| Ok(b"shaped".to_vec()));

        let job = binding.bind(&event).expect("event should bind");

        assert_eq!(job.payload, b"shaped");
    }

    #[test]
    fn binding_payload_transform_can_reject_the_event() {
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(b"{}"),
        );
        let binding = WorklaneJobBinding::new("projection", "CreateProjectionJob", 3)
            .with_payload_transform(|_event| Err(BindingError::Rejected("no payload".to_owned())));

        assert!(binding.bind(&event).is_err());
    }

    fn issue_event(payload: &'static [u8]) -> EventEnvelope {
        EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(payload),
        )
    }

    #[test]
    fn payload_field_equals_matches_configured_value() {
        let event = issue_event(br#"{"label":"bug","number":7}"#);
        assert!(PayloadFieldEquals::new("/label", "bug").matches(&event));
    }

    #[test]
    fn payload_field_equals_no_match_on_different_value() {
        let event = issue_event(br#"{"label":"docs"}"#);
        assert!(!PayloadFieldEquals::new("/label", "bug").matches(&event));
    }

    #[test]
    fn payload_field_equals_no_match_when_field_absent() {
        let event = issue_event(br#"{"number":7}"#);
        assert!(!PayloadFieldEquals::new("/label", "bug").matches(&event));
    }

    #[test]
    fn payload_field_equals_no_match_on_non_json_payload() {
        let event = issue_event(b"not json");
        assert!(!PayloadFieldEquals::new("/label", "bug").matches(&event));
    }

    #[test]
    fn payload_field_equals_matches_nested_pointer() {
        let event = issue_event(br#"{"issue":{"state":"open"}}"#);
        assert!(PayloadFieldEquals::new("/issue/state", "open").matches(&event));
    }

    #[test]
    fn all_of_matches_only_when_every_condition_matches() {
        let event = issue_event(br#"{"label":"bug"}"#);
        let trigger = AllOf::new(vec![
            Box::new(EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED)),
            Box::new(PayloadFieldEquals::new("/label", "bug")),
        ]);
        assert!(trigger.matches(&event));

        let mismatched = AllOf::new(vec![
            Box::new(EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED)),
            Box::new(PayloadFieldEquals::new("/label", "docs")),
        ]);
        assert!(!mismatched.matches(&event));
    }

    #[test]
    fn any_of_matches_when_at_least_one_condition_matches() {
        let event = issue_event(br#"{"label":"bug"}"#);
        let trigger = AnyOf::new(vec![
            Box::new(PayloadFieldEquals::new("/label", "docs")),
            Box::new(PayloadFieldEquals::new("/label", "bug")),
        ]);
        assert!(trigger.matches(&event));

        let none = AnyOf::new(vec![
            Box::new(PayloadFieldEquals::new("/label", "docs")),
            Box::new(PayloadFieldEquals::new("/label", "chore")),
        ]);
        assert!(!none.matches(&event));
    }
}
