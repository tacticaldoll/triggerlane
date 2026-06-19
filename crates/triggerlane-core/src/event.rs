use std::fmt;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const EVENT_GITHUB_ISSUE_CREATED: &str = "event.github.issue.created";
pub const EVENT_GITHUB_PR_CREATED: &str = "event.github.pr.created";
pub const EVENT_DISCORD_MESSAGE_CREATED: &str = "event.discord.message.created";
pub const EVENT_CRON_DAILY: &str = "event.cron.daily";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId(Uuid);

impl EventId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for EventId {
    type Err = uuid::Error;

    /// Parse an event id from its canonical UUID string, so a stored id printed by
    /// `Display` round-trips for replay over the CLI and HTTP.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(value)?))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Source {
    Http,
    GitHub,
    Discord,
    Slack,
    Cron,
    Manual,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventType(String);

impl EventType {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for EventType {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for EventType {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventMetadata {
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub tenant_id: Option<String>,
    pub idempotency_key: Option<String>,
    /// The id of the event that directly caused this one, linking a choreography
    /// chain (a job result emitted as a new event). `None` for an originating
    /// event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub id: EventId,
    pub source: Source,
    pub event_type: EventType,
    pub payload: Bytes,
    pub timestamp: DateTime<Utc>,
    pub metadata: EventMetadata,
}

impl EventEnvelope {
    pub fn new(source: Source, event_type: impl Into<EventType>, payload: Bytes) -> Self {
        Self {
            id: EventId::new(),
            source,
            event_type: event_type.into(),
            payload,
            timestamp: Utc::now(),
            metadata: EventMetadata::default(),
        }
    }

    pub fn with_metadata(mut self, metadata: EventMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Build a follow-up event caused by `cause`, propagating the choreography
    /// chain. The follow-up records `causation_id` as the cause's id, inherits the
    /// cause's correlation id (or starts the chain from the cause's id when the
    /// cause has none), and carries the cause's tenant and trace ids. It gets a
    /// fresh event id and no idempotency key (the caller may set one). This is the
    /// trigger-plane primitive for emitting linked events; the emit transport is
    /// the ingest pipeline.
    pub fn follow_up(
        cause: &EventEnvelope,
        source: Source,
        event_type: impl Into<EventType>,
        payload: Bytes,
    ) -> Self {
        let correlation_id = cause
            .metadata
            .correlation_id
            .clone()
            .or_else(|| Some(cause.id.to_string()));

        Self::new(source, event_type, payload).with_metadata(EventMetadata {
            trace_id: cause.metadata.trace_id.clone(),
            correlation_id,
            tenant_id: cause.metadata.tenant_id.clone(),
            idempotency_key: None,
            causation_id: Some(cause.id.to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_preserves_replay_fields() {
        let metadata = EventMetadata {
            trace_id: Some("trace-1".to_owned()),
            correlation_id: Some("corr-1".to_owned()),
            tenant_id: Some("tenant-1".to_owned()),
            idempotency_key: Some("idem-1".to_owned()),
            causation_id: None,
        };

        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(br#"{"issue":1}"#),
        )
        .with_metadata(metadata.clone());

        assert_eq!(event.source, Source::GitHub);
        assert_eq!(event.event_type.as_str(), EVENT_GITHUB_ISSUE_CREATED);
        assert_eq!(event.payload, Bytes::from_static(br#"{"issue":1}"#));
        assert_eq!(event.metadata, metadata);
    }

    #[test]
    fn follow_up_links_to_cause_and_propagates_chain() {
        let cause = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(br#"{"issue":1}"#),
        )
        .with_metadata(EventMetadata {
            correlation_id: Some("corr-1".to_owned()),
            tenant_id: Some("tenant-1".to_owned()),
            ..Default::default()
        });

        let follow_up = EventEnvelope::follow_up(
            &cause,
            Source::Manual,
            "event.projection.created",
            Bytes::from_static(br#"{"projection":1}"#),
        );

        assert_eq!(follow_up.metadata.causation_id, Some(cause.id.to_string()));
        assert_eq!(follow_up.metadata.correlation_id, Some("corr-1".to_owned()));
        assert_eq!(follow_up.metadata.tenant_id, Some("tenant-1".to_owned()));
        assert_ne!(follow_up.id, cause.id);
        assert_eq!(follow_up.metadata.idempotency_key, None);
    }

    #[test]
    fn follow_up_starts_chain_from_cause_id_when_no_correlation() {
        let cause = EventEnvelope::new(Source::Cron, EVENT_CRON_DAILY, Bytes::from_static(b"{}"));

        let follow_up = EventEnvelope::follow_up(
            &cause,
            Source::Manual,
            EVENT_CRON_DAILY,
            Bytes::from_static(b"{}"),
        );

        assert_eq!(
            follow_up.metadata.correlation_id,
            Some(cause.id.to_string())
        );
        assert_eq!(follow_up.metadata.causation_id, Some(cause.id.to_string()));
    }
}
