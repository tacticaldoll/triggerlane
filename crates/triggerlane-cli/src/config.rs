//! Declarative trigger configuration.
//!
//! Loads an operator-provided JSON file into a [`TriggerRegistry`], composing
//! the existing core trigger primitives so `event -> job` rules can be defined
//! without editing Rust. Parsing lives here, at the CLI composition root, to
//! keep `triggerlane-core` a pure, serde-free set of trigger types. The builder
//! calls only existing public constructors.

use std::path::Path;

use serde::Deserialize;
use thiserror::Error;
use triggerlane_core::{
    AllOf, AnyOf, EventEnvelope, EventTypeTrigger, PayloadFieldEquals, Trigger, WorklaneJobBinding,
};
use triggerlane_runtime::{RegisteredTrigger, TriggerRegistry};

/// Top-level trigger configuration document.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TriggerFile {
    triggers: Vec<TriggerEntry>,
}

/// One configured trigger: a match condition bound to a Worklane job.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TriggerEntry {
    name: String,
    #[serde(default)]
    priority: i32,
    #[serde(default = "default_enabled")]
    enabled: bool,
    #[serde(rename = "match")]
    condition: MatchSpec,
    bind: BindSpec,
}

fn default_enabled() -> bool {
    true
}

/// Declarative match condition, mirroring the core trigger combinators. The
/// `type` tag selects the variant; `all_of` / `any_of` recurse.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MatchSpec {
    EventType {
        event_type: String,
    },
    PayloadFieldEquals {
        pointer: String,
        value: serde_json::Value,
    },
    AllOf {
        conditions: Vec<MatchSpec>,
    },
    AnyOf {
        conditions: Vec<MatchSpec>,
    },
}

/// Pass-through Worklane job binding. Payload transforms are code-defined and
/// intentionally not expressible here.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BindSpec {
    lane: String,
    kind: String,
    max_attempts: u32,
}

/// Adapts a boxed trigger trait object back into something `RegisteredTrigger`
/// accepts (`impl Trigger`), so a config-built `Box<dyn Trigger>` can be
/// registered. Kept CLI-local to avoid a forwarding impl in core.
struct BoxedTrigger(Box<dyn Trigger>);

impl Trigger for BoxedTrigger {
    fn matches(&self, event: &EventEnvelope) -> bool {
        self.0.matches(event)
    }
}

fn build_condition(spec: MatchSpec) -> Box<dyn Trigger> {
    match spec {
        MatchSpec::EventType { event_type } => Box::new(EventTypeTrigger::new(event_type)),
        MatchSpec::PayloadFieldEquals { pointer, value } => {
            Box::new(PayloadFieldEquals::new(pointer, value))
        }
        MatchSpec::AllOf { conditions } => Box::new(AllOf::new(
            conditions.into_iter().map(build_condition).collect(),
        )),
        MatchSpec::AnyOf { conditions } => Box::new(AnyOf::new(
            conditions.into_iter().map(build_condition).collect(),
        )),
    }
}

/// Read a declarative trigger configuration file and build a registry from it.
pub fn load_registry(path: impl AsRef<Path>) -> Result<TriggerRegistry, ConfigError> {
    let data = std::fs::read_to_string(path).map_err(ConfigError::Read)?;
    parse_registry(&data)
}

/// Build a registry from already-read configuration text. Separated from file
/// IO so the schema and builder are testable without touching the filesystem.
fn parse_registry(data: &str) -> Result<TriggerRegistry, ConfigError> {
    let file: TriggerFile = serde_json::from_str(data).map_err(ConfigError::Parse)?;
    let mut registry = TriggerRegistry::new();
    for entry in file.triggers {
        let trigger = BoxedTrigger(build_condition(entry.condition));
        let binding =
            WorklaneJobBinding::new(entry.bind.lane, entry.bind.kind, entry.bind.max_attempts);
        let mut registered = RegisteredTrigger::new(entry.name, entry.priority, trigger, binding);
        if !entry.enabled {
            registered = registered.disabled();
        }
        registry.register(registered);
    }
    Ok(registry)
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("reading trigger config: {0}")]
    Read(std::io::Error),
    #[error("parsing trigger config: {0}")]
    Parse(serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use triggerlane_core::{EVENT_GITHUB_ISSUE_CREATED, Source};

    fn issue_event(payload: &'static [u8]) -> EventEnvelope {
        EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(payload),
        )
    }

    #[test]
    fn composed_condition_matches_and_binds() {
        let config = r#"{
            "triggers": [
                {
                    "name": "bug-projection",
                    "priority": 10,
                    "match": {
                        "type": "all_of",
                        "conditions": [
                            { "type": "event_type", "event_type": "event.github.issue.created" },
                            { "type": "payload_field_equals", "pointer": "/label", "value": "bug" }
                        ]
                    },
                    "bind": { "lane": "projection", "kind": "CreateProjectionJob", "max_attempts": 3 }
                }
            ]
        }"#;
        let registry = parse_registry(config).expect("config should parse");

        let matched = issue_event(br#"{"label":"bug"}"#);
        let names: Vec<_> = registry.matching(&matched).map(|r| r.name()).collect();
        assert_eq!(names, vec!["bug-projection"]);

        let other = issue_event(br#"{"label":"docs"}"#);
        assert_eq!(registry.matching(&other).count(), 0);
    }

    #[test]
    fn disabled_entry_does_not_match_and_priority_orders_matches() {
        let config = r#"{
            "triggers": [
                {
                    "name": "low",
                    "priority": 1,
                    "match": { "type": "event_type", "event_type": "event.github.issue.created" },
                    "bind": { "lane": "l", "kind": "Low", "max_attempts": 1 }
                },
                {
                    "name": "high",
                    "priority": 100,
                    "match": { "type": "event_type", "event_type": "event.github.issue.created" },
                    "bind": { "lane": "l", "kind": "High", "max_attempts": 1 }
                },
                {
                    "name": "off",
                    "enabled": false,
                    "match": { "type": "event_type", "event_type": "event.github.issue.created" },
                    "bind": { "lane": "l", "kind": "Off", "max_attempts": 1 }
                }
            ]
        }"#;
        let registry = parse_registry(config).expect("config should parse");

        let event = issue_event(b"{}");
        let names: Vec<_> = registry.matching(&event).map(|r| r.name()).collect();
        assert_eq!(names, vec!["high", "low"]);
    }

    #[test]
    fn malformed_json_is_a_config_error() {
        assert!(matches!(
            parse_registry("{ not json"),
            Err(ConfigError::Parse(_))
        ));
    }

    #[test]
    fn unknown_match_type_is_a_config_error() {
        let config = r#"{
            "triggers": [
                {
                    "name": "bad",
                    "match": { "type": "regex", "pattern": ".*" },
                    "bind": { "lane": "l", "kind": "K", "max_attempts": 1 }
                }
            ]
        }"#;
        assert!(matches!(parse_registry(config), Err(ConfigError::Parse(_))));
    }
}
