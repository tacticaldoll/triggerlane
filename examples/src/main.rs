use std::sync::Arc;

use bytes::Bytes;
use triggerlane_core::{
    EVENT_GITHUB_ISSUE_CREATED, EventEnvelope, EventTypeTrigger, Source, WorklaneJobBinding,
};
use triggerlane_runtime::{EventIngest, RegisteredTrigger, TriggerRegistry, TriggerRuntime};
use triggerlane_storage::{EventStore, InMemoryEventStore};
use worklane_core::{Broker, Lane};
use worklane_memory::InMemoryBroker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker: Arc<dyn Broker> = Arc::new(InMemoryBroker::new());
    let store = Arc::new(InMemoryEventStore::new());
    let mut registry = TriggerRegistry::new();
    registry.register(RegisteredTrigger::new(
        "github-issue-projection",
        10,
        EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
        WorklaneJobBinding::new("projection", "CreateProjectionJob", 3),
    ));

    let runtime = TriggerRuntime::new(registry, Arc::clone(&broker));
    let ingest = EventIngest::new(Arc::clone(&store) as Arc<dyn EventStore>, runtime);
    let event = EventEnvelope::new(
        Source::GitHub,
        EVENT_GITHUB_ISSUE_CREATED,
        Bytes::from_static(br#"{"issue":1}"#),
    );

    let report = ingest.ingest(event).await?;
    println!(
        "ingested {} ({} stored, {} submitted)",
        report.event_id,
        store.all().len(),
        report.handle.submitted.len()
    );

    if let Some(reservation) = broker.reserve(&Lane::try_from("projection")?).await? {
        println!("submitted {}", reservation.envelope.kind);
    }

    Ok(())
}
