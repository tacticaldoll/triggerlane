//! Runtime registry and event handling for Triggerlane.

use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    io,
    sync::Arc,
    sync::Mutex,
};

use chrono::{DateTime, Utc};
use thiserror::Error;
use triggerlane_core::{
    Binding, BindingError, EventEnvelope, EventId, EventType, Source, Trigger, WorklaneJob,
};
use triggerlane_storage::{AutoRetention, DeadTriggerQueue, EventStore, InMemoryDeadTriggerQueue};
use worklane_core::{Broker, Lane, NewJob};

// `DeadTriggerRecord` now lives in `triggerlane-storage`; re-export it so its
// public path (`triggerlane_runtime::DeadTriggerRecord`) is unchanged.
pub use triggerlane_storage::DeadTriggerRecord;

pub struct RegisteredTrigger {
    name: String,
    priority: i32,
    enabled: bool,
    trigger: Arc<dyn Trigger>,
    binding: Arc<dyn Binding<Job = WorklaneJob>>,
}

impl RegisteredTrigger {
    pub fn new(
        name: impl Into<String>,
        priority: i32,
        trigger: impl Trigger + 'static,
        binding: impl Binding<Job = WorklaneJob> + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            priority,
            enabled: true,
            trigger: Arc::new(trigger),
            binding: Arc::new(binding),
        }
    }

    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Clone for RegisteredTrigger {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            priority: self.priority,
            enabled: self.enabled,
            trigger: Arc::clone(&self.trigger),
            binding: Arc::clone(&self.binding),
        }
    }
}

#[derive(Default)]
pub struct TriggerRegistry {
    registrations: Vec<RegisteredTrigger>,
}

impl TriggerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, registration: RegisteredTrigger) {
        self.registrations.push(registration);
        self.registrations
            .sort_by_key(|registration| Reverse(registration.priority));
    }

    pub fn matching<'a>(
        &'a self,
        event: &'a EventEnvelope,
    ) -> impl Iterator<Item = &'a RegisteredTrigger> {
        self.registrations
            .iter()
            .filter(|registration| registration.enabled && registration.trigger.matches(event))
    }
}

pub struct TriggerRuntime {
    registry: TriggerRegistry,
    broker: Arc<dyn Broker>,
    dead_triggers: Arc<dyn DeadTriggerQueue>,
    options: RuntimeOptions,
    trace_sink: Arc<dyn TraceSink>,
    /// Lanes this runtime has submitted to, so backlog metrics can be reported for
    /// the lanes actually in use (the registry's bindings choose lanes per event,
    /// so the in-use set is only known at runtime).
    observed_lanes: Mutex<HashSet<String>>,
}

impl TriggerRuntime {
    pub fn new(registry: TriggerRegistry, broker: Arc<dyn Broker>) -> Self {
        Self::with_options(registry, broker, RuntimeOptions::default())
    }

    pub fn with_options(
        registry: TriggerRegistry,
        broker: Arc<dyn Broker>,
        options: RuntimeOptions,
    ) -> Self {
        Self::with_options_and_trace_sink(registry, broker, options, Arc::new(NoopTraceSink))
    }

    pub fn with_trace_sink(
        registry: TriggerRegistry,
        broker: Arc<dyn Broker>,
        trace_sink: Arc<dyn TraceSink>,
    ) -> Self {
        Self::with_options_and_trace_sink(registry, broker, RuntimeOptions::default(), trace_sink)
    }

    pub fn with_options_and_trace_sink(
        registry: TriggerRegistry,
        broker: Arc<dyn Broker>,
        options: RuntimeOptions,
        trace_sink: Arc<dyn TraceSink>,
    ) -> Self {
        Self {
            registry,
            broker,
            dead_triggers: Arc::new(InMemoryDeadTriggerQueue::new()),
            options,
            trace_sink,
            observed_lanes: Mutex::new(HashSet::new()),
        }
    }

    /// Replace the dead-trigger queue (e.g. with a durable `FileDeadTriggerQueue`).
    /// Defaults to an in-memory queue.
    pub fn with_dead_trigger_queue(mut self, dead_triggers: Arc<dyn DeadTriggerQueue>) -> Self {
        self.dead_triggers = dead_triggers;
        // Publish the initial depth so a process that starts with a non-empty
        // durable queue reports it before the first event is handled.
        self.record_dead_trigger_depth();
        self
    }

    pub async fn handle(&self, event: EventEnvelope) -> Result<HandleReport, RuntimeError> {
        let started = std::time::Instant::now();
        let mut submitted = Vec::new();
        let mut failed = Vec::new();
        let mut matched_triggers = Vec::new();

        // Stable identity for the event: its idempotency key when present, else
        // its event id (which a replay reuses). Combined with the trigger name it
        // gives each submission a deterministic key, so a duplicate or replayed
        // submission deduplicates at the Worklane broker.
        let event_identity = event
            .metadata
            .idempotency_key
            .clone()
            .unwrap_or_else(|| event.id.to_string());

        for registration in self.registry.matching(&event) {
            matched_triggers.push(registration.name().to_owned());
            // Per-route metric: which trigger matched. Trigger names are a bounded
            // set (the registry), so the label cardinality is safe.
            metrics::counter!("triggerlane_trigger_matches_total", "trigger" => registration.name().to_owned())
                .increment(1);
            let unique_key = format!("{event_identity}:{}", registration.name());
            match registration.binding.bind(&event) {
                Ok(job) => match self.enqueue_with_retries(job, unique_key).await {
                    Ok(job_id) => {
                        metrics::counter!("triggerlane_jobs_submitted_total", "trigger" => registration.name().to_owned())
                            .increment(1);
                        submitted.push(job_id.to_string());
                    }
                    Err(error) => {
                        metrics::counter!("triggerlane_trigger_failures_total", "trigger" => registration.name().to_owned())
                            .increment(1);
                        let record = self.record_dead_trigger(
                            &event,
                            registration.name(),
                            error.to_string(),
                        )?;
                        failed.push(record);
                    }
                },
                Err(error) => {
                    metrics::counter!("triggerlane_trigger_failures_total", "trigger" => registration.name().to_owned())
                        .increment(1);
                    let record =
                        self.record_dead_trigger(&event, registration.name(), error.to_string())?;
                    failed.push(record);
                }
            }
        }

        let report = HandleReport { submitted, failed };

        // Per-route event counter, labelled by source and event type. Sources are a
        // fixed enum and event types are expected to be a bounded, dotted-name
        // vocabulary, so these labels do not explode Prometheus cardinality.
        // (`trigger_matches`/`jobs_submitted`/`trigger_failures` are emitted
        // per-trigger inside the loop above; sum over the label for a total.)
        metrics::counter!(
            "triggerlane_events_handled_total",
            "source" => format!("{:?}", event.source),
            "event_type" => event.event_type.as_str().to_owned(),
        )
        .increment(1);
        metrics::histogram!("triggerlane_handle_duration_seconds")
            .record(started.elapsed().as_secs_f64());
        self.record_dead_trigger_depth();

        self.trace_sink.record(TriggerTrace::from_event_report(
            &event,
            matched_triggers,
            &report,
        ));

        Ok(report)
    }

    /// Publish the current dead-trigger queue depth as a gauge. Called wherever
    /// the depth can change within the process.
    fn record_dead_trigger_depth(&self) {
        metrics::gauge!("triggerlane_dead_triggers").set(self.dead_triggers.len() as f64);
    }

    /// Apply hard-bound automatic retention to the dead-trigger queue, so a lane
    /// of persistently failing triggers cannot grow it without limit. Returns the
    /// number of records removed.
    pub fn enforce_dead_trigger_retention(
        &self,
        policy: &AutoRetention,
        now: DateTime<Utc>,
    ) -> io::Result<usize> {
        let removed = self.dead_triggers.enforce_retention(policy, now)?;
        if removed > 0 {
            self.record_dead_trigger_depth();
        }
        Ok(removed)
    }

    /// Publish event-store and dead-trigger-queue size gauges (record count and
    /// on-disk bytes), so operators can alert before disk or memory exhaustion.
    /// A no-op when no metrics recorder is installed.
    pub fn record_store_metrics(&self, store: &dyn EventStore) {
        metrics::gauge!("triggerlane_event_store_records").set(store.len() as f64);
        metrics::gauge!("triggerlane_event_store_bytes").set(store.size_bytes() as f64);
        metrics::gauge!("triggerlane_dead_triggers").set(self.dead_triggers.len() as f64);
        metrics::gauge!("triggerlane_dead_trigger_bytes")
            .set(self.dead_triggers.size_bytes() as f64);
    }

    /// Publish the Worklane broker backlog as a `triggerlane_broker_pending{lane}`
    /// gauge for each lane this runtime has submitted to — the queue depth that
    /// feeds autoscaling. Best-effort: a lane whose count cannot be read is skipped
    /// rather than failing the sweep. A no-op when no metrics recorder is installed.
    pub async fn record_broker_metrics(&self) {
        let lanes: Vec<String> = self
            .observed_lanes
            .lock()
            .expect("observed lanes mutex poisoned")
            .iter()
            .cloned()
            .collect();
        for lane in lanes {
            let Ok(parsed) = Lane::try_from(lane.as_str()) else {
                continue;
            };
            match self.broker.pending_count(&parsed).await {
                Ok(pending) => {
                    metrics::gauge!("triggerlane_broker_pending", "lane" => lane)
                        .set(pending as f64);
                }
                Err(error) => {
                    tracing::debug!(%lane, %error, "broker pending_count unavailable");
                }
            }
        }
    }

    pub async fn replay_by_id<S>(
        &self,
        store: &S,
        event_id: EventId,
    ) -> Result<HandleReport, ReplayError>
    where
        S: EventStore + ?Sized,
    {
        let event = store
            .get(event_id)
            .ok_or_else(|| ReplayError::EventNotFound(event_id.to_string()))?;

        self.handle(event).await.map_err(ReplayError::Runtime)
    }

    /// Events in `[start, end)` that pass `filter`, in append order, without
    /// handling them — the dry-run preview of what [`replay_range`] would replay.
    pub fn preview_range<S>(
        &self,
        store: &S,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        filter: &ReplayFilter,
    ) -> Vec<EventEnvelope>
    where
        S: EventStore + ?Sized,
    {
        store
            .all()
            .into_iter()
            .filter(|event| event.timestamp >= start && event.timestamp < end)
            .filter(|event| filter.matches(event))
            .collect()
    }

    pub async fn replay_range<S>(
        &self,
        store: &S,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        filter: &ReplayFilter,
    ) -> Result<ReplayRangeReport, ReplayError>
    where
        S: EventStore + ?Sized,
    {
        let mut events = Vec::new();

        for event in store
            .all()
            .into_iter()
            .filter(|event| event.timestamp >= start && event.timestamp < end)
            .filter(|event| filter.matches(event))
        {
            let event_id = event.id.to_string();
            let report = self.handle(event).await.map_err(ReplayError::Runtime)?;
            events.push(ReplayEventReport { event_id, report });
        }

        Ok(ReplayRangeReport { events })
    }

    async fn enqueue_with_retries(
        &self,
        job: WorklaneJob,
        unique_key: String,
    ) -> worklane_core::Result<worklane_core::JobId> {
        // `WorklaneJob.lane` is a plain string at the trigger-core boundary; the
        // baseline `worklane-core` contract requires a validated `Lane`. Convert
        // once: an invalid lane can never enqueue, so it fails immediately and is
        // recorded as a dead trigger rather than retried.
        let lane = Lane::try_from(job.lane.as_str()).map_err(|error| {
            worklane_core::Error::Broker(format!("invalid worklane lane {:?}: {error}", job.lane))
        })?;
        // Remember the lane so backlog metrics can be reported for it later.
        self.observed_lanes
            .lock()
            .expect("observed lanes mutex poisoned")
            .insert(job.lane.clone());

        let attempts = self.options.max_submission_attempts.max(1);
        let mut last_error = None;

        for attempt in 0..attempts {
            // Back off before a retry (never before the first attempt) so a
            // struggling broker is not hammered. Submission is idempotent via the
            // deterministic unique key, so re-enqueueing is safe.
            if attempt > 0 {
                let delay = submission_backoff_delay(attempt, self.options.submission_backoff);
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }

            let new_job = NewJob::new(
                lane.clone(),
                job.kind.clone(),
                job.payload.clone(),
                job.max_attempts,
            )
            .with_unique_key(unique_key.clone());

            match self.broker.enqueue(new_job).await {
                Ok(job_id) => return Ok(job_id),
                Err(error) => last_error = Some(error),
            }
        }

        Err(last_error.expect("at least one submission attempt should run"))
    }

    pub fn dead_triggers(&self) -> Vec<DeadTriggerRecord> {
        self.dead_triggers.all()
    }

    /// Run one retry pass over the dead-trigger queue: drain it, re-handle each
    /// distinct failed event through the normal handling path, and report the
    /// outcome. Re-handling reuses the deterministic submission key, so a trigger
    /// that previously succeeded for an event is deduplicated by the broker
    /// rather than re-submitted; a trigger that fails again is re-recorded
    /// through the normal dead-trigger path (the queue was drained first, so it
    /// only re-accumulates renewed failures).
    pub async fn retry_dead_triggers(&self) -> Result<DeadTriggerRetryReport, RuntimeError> {
        let drained = self.dead_triggers.drain()?;
        let drained_count = drained.len();

        // One event can have several failed-trigger records. Group the failed
        // trigger names by event id and re-handle each distinct event once, so
        // the event is not re-evaluated (and re-recorded) multiple times.
        let mut order = Vec::new();
        let mut events: HashMap<String, EventEnvelope> = HashMap::new();
        let mut failed_triggers: HashMap<String, HashSet<String>> = HashMap::new();
        for record in drained {
            let id = record.event.id.to_string();
            if events.insert(id.clone(), record.event).is_none() {
                order.push(id.clone());
            }
            failed_triggers
                .entry(id)
                .or_default()
                .insert(record.trigger_name);
        }

        let mut report = DeadTriggerRetryReport {
            drained: drained_count,
            ..DeadTriggerRetryReport::default()
        };
        for id in order {
            let event = events.remove(&id).expect("event recorded for id");
            let previously_failed = failed_triggers.remove(&id).unwrap_or_default();

            let result = self.handle(event).await?;
            report.events_retried += 1;
            report.submitted.extend(result.submitted.iter().cloned());

            let now_failed: HashSet<&str> = result
                .failed
                .iter()
                .map(|record| record.trigger_name.as_str())
                .collect();
            for trigger in &previously_failed {
                if now_failed.contains(trigger.as_str()) {
                    report.still_failed += 1;
                } else {
                    report.recovered += 1;
                }
            }
        }

        self.record_dead_trigger_depth();
        Ok(report)
    }

    fn record_dead_trigger(
        &self,
        event: &EventEnvelope,
        trigger_name: &str,
        error: String,
    ) -> io::Result<DeadTriggerRecord> {
        let record = DeadTriggerRecord {
            event: event.clone(),
            trigger_name: trigger_name.to_owned(),
            error,
        };
        self.dead_triggers.record(record.clone())?;
        Ok(record)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandleReport {
    pub submitted: Vec<String>,
    pub failed: Vec<DeadTriggerRecord>,
}

/// Outcome of one [`TriggerRuntime::retry_dead_triggers`] pass. `recovered` and
/// `still_failed` partition the `drained` records by whether their trigger still
/// fails; `submitted` lists job ids enqueued during the pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeadTriggerRetryReport {
    pub drained: usize,
    pub events_retried: usize,
    pub recovered: usize,
    pub still_failed: usize,
    pub submitted: Vec<String>,
}

/// Optional narrowing of a range replay/preview, on top of the time window. An
/// unset field matches everything, so the default filter narrows nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplayFilter {
    pub event_type: Option<String>,
    pub source: Option<Source>,
}

impl ReplayFilter {
    fn matches(&self, event: &EventEnvelope) -> bool {
        self.event_type
            .as_deref()
            .is_none_or(|wanted| event.event_type.as_str() == wanted)
            && self
                .source
                .as_ref()
                .is_none_or(|wanted| &event.source == wanted)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayRangeReport {
    pub events: Vec<ReplayEventReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayEventReport {
    pub event_id: String,
    pub report: HandleReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerTrace {
    pub event_id: String,
    pub event_type: EventType,
    pub source: Source,
    pub matched_triggers: Vec<String>,
    pub submitted: Vec<String>,
    pub failure_count: usize,
}

impl TriggerTrace {
    fn from_event_report(
        event: &EventEnvelope,
        matched_triggers: Vec<String>,
        report: &HandleReport,
    ) -> Self {
        Self {
            event_id: event.id.to_string(),
            event_type: event.event_type.clone(),
            source: event.source.clone(),
            matched_triggers,
            submitted: report.submitted.clone(),
            failure_count: report.failed.len(),
        }
    }
}

pub trait TraceSink: Send + Sync + 'static {
    fn record(&self, trace: TriggerTrace);
}

#[derive(Default)]
pub struct InMemoryTraceSink {
    traces: Mutex<Vec<TriggerTrace>>,
}

impl InMemoryTraceSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn traces(&self) -> Vec<TriggerTrace> {
        self.traces
            .lock()
            .expect("trace sink mutex poisoned")
            .clone()
    }
}

impl TraceSink for InMemoryTraceSink {
    fn record(&self, trace: TriggerTrace) {
        self.traces
            .lock()
            .expect("trace sink mutex poisoned")
            .push(trace);
    }
}

struct NoopTraceSink;

impl TraceSink for NoopTraceSink {
    fn record(&self, _trace: TriggerTrace) {}
}

/// A [`TraceSink`] that writes each trigger trace to the `tracing` log as a
/// structured record. Events with trigger-side failures are logged at `warn`,
/// clean events at `info`. Emitting only happens when the binary has installed
/// a subscriber; without one the records go nowhere (so tests and embedders are
/// unaffected).
#[derive(Debug, Default, Clone, Copy)]
pub struct LoggingTraceSink;

impl TraceSink for LoggingTraceSink {
    fn record(&self, trace: TriggerTrace) {
        if trace.failure_count > 0 {
            tracing::warn!(
                event_id = %trace.event_id,
                event_type = trace.event_type.as_str(),
                source = ?trace.source,
                matched = trace.matched_triggers.len(),
                submitted = trace.submitted.len(),
                failure_count = trace.failure_count,
                "event handled with trigger failures"
            );
        } else {
            tracing::info!(
                event_id = %trace.event_id,
                event_type = trace.event_type.as_str(),
                source = ?trace.source,
                matched = trace.matched_triggers.len(),
                submitted = trace.submitted.len(),
                failure_count = trace.failure_count,
                "event handled"
            );
        }
    }
}

/// Upper bound on a single inter-attempt backoff delay, so exponential growth can
/// never produce an absurd sleep no matter how the base/attempts are configured.
const MAX_SUBMISSION_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeOptions {
    /// How many times to attempt a job submission before recording a dead trigger.
    pub max_submission_attempts: u32,
    /// Base delay before the first retry, doubled each subsequent retry and capped
    /// at [`MAX_SUBMISSION_BACKOFF`]. `Duration::ZERO` disables backoff. Only
    /// engages when `max_submission_attempts > 1`.
    pub submission_backoff: std::time::Duration,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            max_submission_attempts: 1,
            submission_backoff: std::time::Duration::from_millis(100),
        }
    }
}

/// Exponential backoff before retry `retry_index` (1 = the first retry): `base`
/// doubled `retry_index - 1` times, capped at [`MAX_SUBMISSION_BACKOFF`]. A zero
/// base (or index 0) yields no delay. Pure, so the schedule is unit-testable
/// without sleeping.
fn submission_backoff_delay(retry_index: u32, base: std::time::Duration) -> std::time::Duration {
    if base.is_zero() || retry_index == 0 {
        return std::time::Duration::ZERO;
    }
    // `1 << (retry_index - 1)` = 2^(retry_index-1); saturate to u32::MAX once the
    // shift would overflow, so the cap (not arithmetic) bounds the delay.
    let factor = 1u32.checked_shl(retry_index - 1).unwrap_or(u32::MAX);
    base.saturating_mul(factor).min(MAX_SUBMISSION_BACKOFF)
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Binding(#[from] BindingError),
    #[error("durable storage error: {0}")]
    Storage(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum ReplayError {
    #[error("event {0} was not found for replay")]
    EventNotFound(String),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
}

/// Source-agnostic ingest pipeline: persist an accepted event, then handle it.
///
/// The event is appended to the store before handling so it remains replayable
/// regardless of the handling outcome.
pub struct EventIngest {
    store: Arc<dyn EventStore>,
    runtime: TriggerRuntime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestReport {
    pub event_id: String,
    pub handle: HandleReport,
    /// True when the event was recognized as an already-ingested idempotency key
    /// and therefore neither stored again nor re-evaluated.
    pub deduplicated: bool,
}

/// Outcome of [`EventIngest::accept`]: the event was durably stored but not yet
/// handled. `deduplicated` is true when an earlier copy already existed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptReport {
    pub event_id: String,
    pub deduplicated: bool,
}

impl EventIngest {
    pub fn new(store: Arc<dyn EventStore>, runtime: TriggerRuntime) -> Self {
        Self { store, runtime }
    }

    pub async fn ingest(&self, event: EventEnvelope) -> Result<IngestReport, RuntimeError> {
        // Deduplication is scoped to the retained window: `find_by_idempotency_key`
        // sees only events the store still holds, so once an earlier copy is pruned
        // by retention a later event with the same key is treated as new.
        if let Some(key) = event.metadata.idempotency_key.as_deref()
            && let Some(existing) = self.store.find_by_idempotency_key(key)
        {
            return Ok(IngestReport {
                event_id: existing.id.to_string(),
                handle: HandleReport {
                    submitted: Vec::new(),
                    failed: Vec::new(),
                },
                deduplicated: true,
            });
        }

        let event_id = event.id.to_string();
        let id = event.id;
        self.store.append(event.clone())?;
        let handle = self.runtime.handle(event).await?;
        // A successful `handle` means every matched trigger was either submitted to
        // Worklane or recorded to the dead-trigger queue, so the event is delivered
        // and eligible for grace-based retention. On a handling error we leave it
        // undelivered so a restart replays it.
        self.store.mark_delivered(id)?;
        Ok(IngestReport {
            event_id,
            handle,
            deduplicated: false,
        })
    }

    /// Durably accept an event without handling it: deduplicate (scoped to the
    /// retained window) and append, then return. The event is left *undelivered* so
    /// a dispatcher (or, after a crash, [`recover_undelivered`](Self::recover_undelivered))
    /// handles it. This is the fast half of an asynchronous ingestion path — accept
    /// is durable-by-ack while submission happens off the request path.
    pub async fn accept(&self, event: EventEnvelope) -> Result<AcceptReport, RuntimeError> {
        if let Some(key) = event.metadata.idempotency_key.as_deref()
            && let Some(existing) = self.store.find_by_idempotency_key(key)
        {
            return Ok(AcceptReport {
                event_id: existing.id.to_string(),
                deduplicated: true,
            });
        }
        let event_id = event.id.to_string();
        self.store.append(event)?;
        Ok(AcceptReport {
            event_id,
            deduplicated: false,
        })
    }

    /// Handle an already-accepted (appended) event and mark it delivered on
    /// success — the dispatch half of the asynchronous path. On a handling error
    /// the event is left undelivered so a restart's recovery retries it.
    pub async fn dispatch(&self, event: EventEnvelope) -> Result<HandleReport, RuntimeError> {
        let id = event.id;
        let handle = self.runtime.handle(event).await?;
        self.store.mark_delivered(id)?;
        Ok(handle)
    }

    /// Replay events that are durably stored but not yet delivered — the in-flight
    /// set interrupted by a crash — through the normal handling path, marking each
    /// delivered on success. Re-submission is safe: `handle` submits with a
    /// deterministic unique key, so Worklane deduplicates a job that did land
    /// before the crash. Returns the number of events recovered.
    pub async fn recover_undelivered(&self) -> Result<usize, RuntimeError> {
        let pending = self.store.undelivered();
        let mut recovered = 0;
        for event in pending {
            let id = event.id;
            self.runtime.handle(event).await?;
            self.store.mark_delivered(id)?;
            recovered += 1;
        }
        Ok(recovered)
    }

    pub fn runtime(&self) -> &TriggerRuntime {
        &self.runtime
    }

    pub fn store(&self) -> &Arc<dyn EventStore> {
        &self.store
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::{Duration as ChronoDuration, Utc};
    use triggerlane_core::{
        Binding, BindingError, EVENT_GITHUB_ISSUE_CREATED, EventEnvelope, EventMetadata,
        EventTypeTrigger, Source, WorklaneJob, WorklaneJobBinding,
    };
    use triggerlane_storage::{
        DeadTriggerQueue, EventStore, InMemoryDeadTriggerQueue, InMemoryEventStore,
    };
    use worklane_core::Broker;
    use worklane_core::{
        DeadLetter, Error as WorklaneError, JobId, JobState, Lane, Reservation, ReservationReceipt,
        Result as WorklaneResult,
    };
    use worklane_memory::InMemoryBroker;

    use super::*;

    #[test]
    fn logging_trace_sink_records_without_a_subscriber() {
        // With no subscriber installed the record is a no-op sink for the log,
        // but it must not panic and must accept both clean and failed traces.
        let sink = LoggingTraceSink;
        sink.record(TriggerTrace {
            event_id: "evt-1".to_owned(),
            event_type: EventType::new(EVENT_GITHUB_ISSUE_CREATED),
            source: Source::GitHub,
            matched_triggers: vec!["t".to_owned()],
            submitted: vec!["job-1".to_owned()],
            failure_count: 0,
        });
        sink.record(TriggerTrace {
            event_id: "evt-2".to_owned(),
            event_type: EventType::new(EVENT_GITHUB_ISSUE_CREATED),
            source: Source::GitHub,
            matched_triggers: vec!["t".to_owned()],
            submitted: Vec::new(),
            failure_count: 1,
        });
    }

    #[tokio::test]
    async fn matching_trigger_submits_worklane_job() {
        let broker = Arc::new(InMemoryBroker::new());
        let mut registry = TriggerRegistry::new();
        registry.register(RegisteredTrigger::new(
            "github-issue",
            10,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            WorklaneJobBinding::new("projection", "CreateProjectionJob", 3),
        ));
        let runtime = TriggerRuntime::new(registry, Arc::clone(&broker) as Arc<dyn Broker>);
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(br#"{"issue":1}"#),
        );

        let report = runtime.handle(event).await.expect("event should handle");

        assert_eq!(report.submitted.len(), 1);
        assert_eq!(broker.len(), 1);
        let reserved = broker
            .reserve(&Lane::try_from("projection").unwrap())
            .await
            .expect("reserve should succeed")
            .expect("job should exist");
        assert_eq!(reserved.envelope.kind, "CreateProjectionJob");
        assert_eq!(reserved.envelope.payload, br#"{"issue":1}"#);
    }

    #[tokio::test]
    async fn dead_triggers_are_recorded_into_the_injected_queue() {
        let broker = Arc::new(InMemoryBroker::new());
        let queue = Arc::new(InMemoryDeadTriggerQueue::new());
        let mut registry = TriggerRegistry::new();
        // A whitespace lane is rejected by the Worklane `Lane` contract, so the
        // submission fails terminally and is recorded as a dead trigger.
        registry.register(RegisteredTrigger::new(
            "bad-lane",
            10,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            WorklaneJobBinding::new("  ", "CreateProjectionJob", 3),
        ));
        let runtime = TriggerRuntime::new(registry, broker)
            .with_dead_trigger_queue(Arc::clone(&queue) as Arc<dyn DeadTriggerQueue>);
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(b"{}"),
        );

        let report = runtime.handle(event).await.expect("event should handle");

        assert_eq!(report.submitted.len(), 0);
        assert_eq!(report.failed.len(), 1);
        assert_eq!(queue.all().len(), 1);
        assert_eq!(runtime.dead_triggers().len(), 1);
    }

    #[tokio::test]
    async fn dead_trigger_record_failure_surfaces_as_error() {
        // A durable dead-trigger write failure must surface as a runtime error,
        // not panic the handling path (Gate 3 / finding 3).
        struct FailingQueue;
        impl DeadTriggerQueue for FailingQueue {
            fn record(&self, _record: DeadTriggerRecord) -> std::io::Result<()> {
                Err(std::io::Error::other("dead-trigger write failed"))
            }
            fn all(&self) -> Vec<DeadTriggerRecord> {
                Vec::new()
            }
            fn drain(&self) -> std::io::Result<Vec<DeadTriggerRecord>> {
                Ok(Vec::new())
            }
            fn prune(
                &self,
                _policy: &triggerlane_storage::ManualRetention,
            ) -> std::io::Result<usize> {
                Ok(0)
            }
            fn len(&self) -> usize {
                0
            }
        }

        let broker = Arc::new(InMemoryBroker::new());
        let mut registry = TriggerRegistry::new();
        // Whitespace lane fails to enqueue, so handling tries to record a dead
        // trigger — which here fails durably.
        registry.register(RegisteredTrigger::new(
            "bad-lane",
            10,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            WorklaneJobBinding::new("  ", "CreateProjectionJob", 3),
        ));
        let runtime = TriggerRuntime::new(registry, broker)
            .with_dead_trigger_queue(Arc::new(FailingQueue) as Arc<dyn DeadTriggerQueue>);
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(b"{}"),
        );

        let error = runtime
            .handle(event)
            .await
            .expect_err("dead-trigger write failure should surface");
        assert!(matches!(error, RuntimeError::Storage(_)));
    }

    #[tokio::test]
    async fn event_store_append_failure_surfaces_through_ingest() {
        // A durable event-store write failure must surface through ingest as a
        // runtime error rather than aborting the process (Gate 3 / finding 1).
        struct FailingStore;
        impl EventStore for FailingStore {
            fn append(&self, _event: EventEnvelope) -> std::io::Result<()> {
                Err(std::io::Error::other("event store write failed"))
            }
            fn get(&self, _id: EventId) -> Option<EventEnvelope> {
                None
            }
            fn all(&self) -> Vec<EventEnvelope> {
                Vec::new()
            }
            fn prune(
                &self,
                _policy: &triggerlane_storage::ManualRetention,
            ) -> std::io::Result<usize> {
                Ok(0)
            }
        }

        let broker = Arc::new(InMemoryBroker::new());
        let runtime = TriggerRuntime::new(TriggerRegistry::new(), broker);
        let ingest = EventIngest::new(Arc::new(FailingStore) as Arc<dyn EventStore>, runtime);
        let event = EventEnvelope::new(
            Source::Manual,
            EventType::new("event.manual.test"),
            Bytes::from_static(b"{}"),
        );

        let error = ingest
            .ingest(event)
            .await
            .expect_err("append failure should surface");
        assert!(matches!(error, RuntimeError::Storage(_)));
    }

    #[tokio::test]
    async fn retry_recovers_when_failure_is_resolved() {
        // The registry now has a valid trigger; a dead-trigger record left over
        // from an earlier failure re-handles successfully and leaves the queue.
        let broker = Arc::new(InMemoryBroker::new());
        let queue = Arc::new(InMemoryDeadTriggerQueue::new());
        let mut registry = TriggerRegistry::new();
        registry.register(RegisteredTrigger::new(
            "github-issue",
            10,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            WorklaneJobBinding::new("projection", "CreateProjectionJob", 3),
        ));
        let runtime = TriggerRuntime::new(registry, Arc::clone(&broker) as Arc<dyn Broker>)
            .with_dead_trigger_queue(Arc::clone(&queue) as Arc<dyn DeadTriggerQueue>);
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(br#"{"issue":1}"#),
        );
        queue
            .record(DeadTriggerRecord {
                event,
                trigger_name: "github-issue".to_owned(),
                error: "broker was down".to_owned(),
            })
            .unwrap();

        let report = runtime
            .retry_dead_triggers()
            .await
            .expect("retry should run");

        assert_eq!(report.drained, 1);
        assert_eq!(report.recovered, 1);
        assert_eq!(report.still_failed, 0);
        assert_eq!(report.submitted.len(), 1);
        assert!(queue.all().is_empty());
        assert_eq!(broker.len(), 1);
    }

    #[tokio::test]
    async fn retry_re_records_persistent_failure() {
        let broker = Arc::new(InMemoryBroker::new());
        let queue = Arc::new(InMemoryDeadTriggerQueue::new());
        let mut registry = TriggerRegistry::new();
        // Whitespace lane is always rejected, so the failure persists.
        registry.register(RegisteredTrigger::new(
            "bad-lane",
            10,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            WorklaneJobBinding::new("  ", "CreateProjectionJob", 3),
        ));
        let runtime = TriggerRuntime::new(registry, broker)
            .with_dead_trigger_queue(Arc::clone(&queue) as Arc<dyn DeadTriggerQueue>);
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(b"{}"),
        );
        runtime
            .handle(event)
            .await
            .expect("handle should record a dead trigger");
        assert_eq!(queue.all().len(), 1);

        let report = runtime
            .retry_dead_triggers()
            .await
            .expect("retry should run");

        assert_eq!(report.drained, 1);
        assert_eq!(report.recovered, 0);
        assert_eq!(report.still_failed, 1);
        // The renewed failure is re-recorded (queue drained first, then re-added).
        assert_eq!(queue.all().len(), 1);
    }

    #[tokio::test]
    async fn retry_does_not_double_submit_a_succeeded_trigger() {
        // One event, two triggers: one valid (submits), one invalid (dead). On
        // retry the whole event is re-handled; the valid trigger must dedup at
        // the broker rather than create a second job.
        let broker = Arc::new(InMemoryBroker::new());
        let queue = Arc::new(InMemoryDeadTriggerQueue::new());
        let mut registry = TriggerRegistry::new();
        registry.register(RegisteredTrigger::new(
            "ok",
            20,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            WorklaneJobBinding::new("projection", "CreateProjectionJob", 3),
        ));
        registry.register(RegisteredTrigger::new(
            "bad-lane",
            10,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            WorklaneJobBinding::new("  ", "CreateProjectionJob", 3),
        ));
        let runtime = TriggerRuntime::new(registry, Arc::clone(&broker) as Arc<dyn Broker>)
            .with_dead_trigger_queue(Arc::clone(&queue) as Arc<dyn DeadTriggerQueue>);
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(br#"{"issue":1}"#),
        );
        runtime.handle(event).await.expect("handle should run");
        assert_eq!(broker.len(), 1);
        assert_eq!(queue.all().len(), 1);

        let report = runtime
            .retry_dead_triggers()
            .await
            .expect("retry should run");

        assert_eq!(report.drained, 1);
        assert_eq!(report.still_failed, 1);
        assert_eq!(report.recovered, 0);
        // The valid trigger deduplicated rather than creating a second job.
        assert_eq!(broker.len(), 1);
    }

    #[tokio::test]
    async fn duplicate_submission_dedups_via_unique_key() {
        let broker = Arc::new(InMemoryBroker::new());
        let mut registry = TriggerRegistry::new();
        registry.register(RegisteredTrigger::new(
            "github-issue",
            10,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            WorklaneJobBinding::new("projection", "CreateProjectionJob", 3),
        ));
        let runtime = TriggerRuntime::new(registry, Arc::clone(&broker) as Arc<dyn Broker>);
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(br#"{"issue":1}"#),
        )
        .with_metadata(EventMetadata {
            idempotency_key: Some("issue-1".to_owned()),
            ..Default::default()
        });

        // Handling the same event twice (e.g. duplicate delivery or replay) while
        // the job is still live must produce a single Worklane job.
        runtime
            .handle(event.clone())
            .await
            .expect("first handle should succeed");
        runtime
            .handle(event)
            .await
            .expect("second handle should succeed");

        assert_eq!(broker.len(), 1);
    }

    #[tokio::test]
    async fn duplicate_idempotency_key_is_deduplicated_on_ingest() {
        let broker = Arc::new(InMemoryBroker::new());
        let store = Arc::new(InMemoryEventStore::new());
        let mut registry = TriggerRegistry::new();
        registry.register(RegisteredTrigger::new(
            "github-issue",
            10,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            WorklaneJobBinding::new("projection", "CreateProjectionJob", 3),
        ));
        let runtime = TriggerRuntime::new(registry, Arc::clone(&broker) as Arc<dyn Broker>);
        let ingest = EventIngest::new(Arc::clone(&store) as Arc<dyn EventStore>, runtime);

        let event = || {
            EventEnvelope::new(
                Source::GitHub,
                EVENT_GITHUB_ISSUE_CREATED,
                Bytes::from_static(br#"{"issue":1}"#),
            )
            .with_metadata(EventMetadata {
                idempotency_key: Some("dup-1".to_owned()),
                ..Default::default()
            })
        };

        let first = ingest.ingest(event()).await.expect("first ingest");
        let second = ingest.ingest(event()).await.expect("second ingest");

        assert!(!first.deduplicated);
        assert!(second.deduplicated);
        assert_eq!(store.all().len(), 1, "duplicate event must not be stored");
        assert_eq!(broker.len(), 1, "duplicate event must not submit again");
    }

    fn github_issue_ingest() -> (Arc<InMemoryBroker>, Arc<InMemoryEventStore>, EventIngest) {
        let broker = Arc::new(InMemoryBroker::new());
        let store = Arc::new(InMemoryEventStore::new());
        let mut registry = TriggerRegistry::new();
        registry.register(RegisteredTrigger::new(
            "github-issue",
            10,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            WorklaneJobBinding::new("projection", "CreateProjectionJob", 3),
        ));
        let runtime = TriggerRuntime::new(registry, Arc::clone(&broker) as Arc<dyn Broker>);
        let ingest = EventIngest::new(Arc::clone(&store) as Arc<dyn EventStore>, runtime);
        (broker, store, ingest)
    }

    fn github_issue_event() -> EventEnvelope {
        EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(br#"{"issue":1}"#),
        )
    }

    #[test]
    fn record_store_metrics_is_noop_without_recorder() {
        let (_broker, store, ingest) = github_issue_ingest();
        store.append(github_issue_event()).unwrap();
        // With no metrics recorder installed, emitting store gauges must not panic.
        ingest.runtime().record_store_metrics(store.as_ref());
    }

    #[tokio::test]
    async fn record_broker_metrics_tracks_submitted_lane_and_is_noop_without_recorder() {
        let (_broker, _store, ingest) = github_issue_ingest();
        // No lanes observed yet → nothing to query, must not panic.
        ingest.runtime().record_broker_metrics().await;
        // After a submission, the lane is observed and its backlog is queried from
        // the broker; emitting the gauge with no recorder installed must not panic.
        ingest.ingest(github_issue_event()).await.expect("ingest");
        ingest.runtime().record_broker_metrics().await;
    }

    #[tokio::test]
    async fn accept_stores_without_handling_then_dispatch_handles() {
        let (broker, store, ingest) = github_issue_ingest();
        let event = github_issue_event();
        let id = event.id;

        // Accept durably stores the event but does NOT handle it: nothing submitted,
        // and the event is left undelivered for the dispatcher.
        let report = ingest.accept(event.clone()).await.expect("accept");
        assert!(!report.deduplicated);
        assert!(store.get(id).is_some());
        assert!(broker.is_empty(), "accept must not submit");
        assert_eq!(store.undelivered().len(), 1);

        // Dispatch handles the accepted event and marks it delivered.
        ingest.dispatch(event).await.expect("dispatch");
        assert_eq!(broker.len(), 1);
        assert!(store.undelivered().is_empty());
    }

    #[tokio::test]
    async fn accept_deduplicates_on_idempotency_key() {
        let (_broker, store, ingest) = github_issue_ingest();
        let event = || {
            github_issue_event().with_metadata(EventMetadata {
                idempotency_key: Some("dup".to_owned()),
                ..Default::default()
            })
        };
        assert!(!ingest.accept(event()).await.expect("first").deduplicated);
        assert!(ingest.accept(event()).await.expect("second").deduplicated);
        assert_eq!(store.all().len(), 1, "duplicate must not be stored again");
    }

    #[tokio::test]
    async fn ingest_marks_event_delivered_once_work_is_taken() {
        let (broker, store, ingest) = github_issue_ingest();

        let report = ingest.ingest(github_issue_event()).await.expect("ingest");

        assert_eq!(report.handle.submitted.len(), 1);
        assert_eq!(broker.len(), 1);
        // The event was submitted, so it is delivered and no longer in-flight.
        assert!(
            store.undelivered().is_empty(),
            "a fully-handled event must be marked delivered"
        );
    }

    #[tokio::test]
    async fn recover_undelivered_replays_only_in_flight_events() {
        let (broker, store, ingest) = github_issue_ingest();

        // One event already delivered (e.g. handled before a restart) and one
        // in-flight event that was stored but never delivered.
        let delivered = github_issue_event();
        store.append(delivered.clone()).unwrap();
        store.mark_delivered(delivered.id).unwrap();
        let in_flight = github_issue_event();
        store.append(in_flight.clone()).unwrap();

        let recovered = ingest.recover_undelivered().await.expect("recover");

        assert_eq!(recovered, 1, "only the in-flight event is replayed");
        assert_eq!(broker.len(), 1, "the delivered event is not re-submitted");
        assert!(store.undelivered().is_empty());
    }

    #[tokio::test]
    async fn dedup_is_bounded_by_retention() {
        let (_broker, store, ingest) = github_issue_ingest();
        let event = || {
            github_issue_event().with_metadata(EventMetadata {
                idempotency_key: Some("key-1".to_owned()),
                ..Default::default()
            })
        };

        let first = ingest.ingest(event()).await.expect("first ingest");
        assert!(!first.deduplicated);

        // Drop the stored copy from the retained window (as retention would).
        store
            .prune(&triggerlane_storage::ManualRetention::KeepMostRecent(0))
            .unwrap();

        // With the earlier copy gone, the same key is treated as new, not deduped.
        let second = ingest.ingest(event()).await.expect("second ingest");
        assert!(
            !second.deduplicated,
            "dedup must not see events pruned from the retained window"
        );
        assert_eq!(store.all().len(), 1);
    }

    #[tokio::test]
    async fn message_created_submits_process_message_job() {
        let broker = Arc::new(InMemoryBroker::new());
        let mut registry = TriggerRegistry::new();
        registry.register(RegisteredTrigger::new(
            "message-created",
            10,
            EventTypeTrigger::new("message.created"),
            WorklaneJobBinding::new("messaging", "messaging.process_message", 3),
        ));
        let runtime = TriggerRuntime::new(registry, Arc::clone(&broker) as Arc<dyn Broker>);
        let event = EventEnvelope::new(
            Source::Http,
            "message.created",
            Bytes::from_static(br#"{"message_id":"msg-1"}"#),
        );

        let report = runtime.handle(event).await.expect("event should handle");

        assert_eq!(report.submitted.len(), 1);
        let reserved = broker
            .reserve(&Lane::try_from("messaging").unwrap())
            .await
            .expect("reserve should succeed")
            .expect("job should exist");
        assert_eq!(reserved.envelope.kind, "messaging.process_message");
        assert_eq!(reserved.envelope.payload, br#"{"message_id":"msg-1"}"#);
    }

    #[tokio::test]
    async fn follow_up_event_preserves_metadata_and_trace() {
        let broker = Arc::new(InMemoryBroker::new());
        let store = Arc::new(InMemoryEventStore::new());
        let trace_sink = Arc::new(InMemoryTraceSink::new());
        let mut registry = TriggerRegistry::new();
        registry.register(RegisteredTrigger::new(
            "projection-created",
            10,
            EventTypeTrigger::new("projection.created"),
            WorklaneJobBinding::new("projection", "projection.build_scope", 3),
        ));
        let runtime = TriggerRuntime::with_trace_sink(
            registry,
            Arc::clone(&broker) as Arc<dyn Broker>,
            Arc::clone(&trace_sink) as Arc<dyn TraceSink>,
        );
        let ingest = EventIngest::new(Arc::clone(&store) as Arc<dyn EventStore>, runtime);
        let event = EventEnvelope::new(
            Source::Http,
            "projection.created",
            Bytes::from_static(br#"{"message_id":"msg-1","projection_id":"proj-1"}"#),
        )
        .with_metadata(EventMetadata {
            trace_id: Some("trace-1".to_owned()),
            correlation_id: Some("msg-1".to_owned()),
            tenant_id: Some("tenant-1".to_owned()),
            idempotency_key: Some("projection.created:proj-1".to_owned()),
            causation_id: None,
        });
        let event_id = event.id;

        let report = ingest.ingest(event).await.expect("event should ingest");

        assert_eq!(report.handle.submitted.len(), 1);
        let stored = store.get(event_id).expect("event should be stored");
        assert_eq!(stored.metadata.correlation_id.as_deref(), Some("msg-1"));
        assert_eq!(
            stored.metadata.idempotency_key.as_deref(),
            Some("projection.created:proj-1")
        );

        let traces = trace_sink.traces();
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].event_type.as_str(), "projection.created");
        assert_eq!(traces[0].matched_triggers, ["projection-created"]);

        let reserved = broker
            .reserve(&Lane::try_from("projection").unwrap())
            .await
            .expect("reserve should succeed")
            .expect("job should exist");
        assert_eq!(reserved.envelope.kind, "projection.build_scope");
        assert_eq!(
            reserved.envelope.payload,
            br#"{"message_id":"msg-1","projection_id":"proj-1"}"#
        );
    }

    #[tokio::test]
    async fn successful_handling_records_trigger_trace() {
        let broker = Arc::new(InMemoryBroker::new());
        let trace_sink = Arc::new(InMemoryTraceSink::new());
        let mut registry = TriggerRegistry::new();
        registry.register(RegisteredTrigger::new(
            "github-issue",
            10,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            WorklaneJobBinding::new("projection", "CreateProjectionJob", 3),
        ));
        let runtime = TriggerRuntime::with_trace_sink(
            registry,
            Arc::clone(&broker) as Arc<dyn Broker>,
            Arc::clone(&trace_sink) as Arc<dyn TraceSink>,
        );
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(br#"{"issue":1}"#),
        );
        let event_id = event.id.to_string();

        let report = runtime.handle(event).await.expect("event should handle");

        assert_eq!(report.submitted.len(), 1);
        let traces = trace_sink.traces();
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].event_id, event_id);
        assert_eq!(traces[0].event_type.as_str(), EVENT_GITHUB_ISSUE_CREATED);
        assert_eq!(traces[0].source, Source::GitHub);
        assert_eq!(traces[0].matched_triggers, ["github-issue"]);
        assert_eq!(traces[0].submitted, report.submitted);
        assert_eq!(traces[0].failure_count, 0);
    }

    #[tokio::test]
    async fn replay_by_id_handles_stored_event() {
        let broker = Arc::new(InMemoryBroker::new());
        let store = InMemoryEventStore::new();
        let mut registry = TriggerRegistry::new();
        registry.register(RegisteredTrigger::new(
            "github-issue",
            10,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            WorklaneJobBinding::new("projection", "CreateProjectionJob", 3),
        ));
        let runtime = TriggerRuntime::new(registry, Arc::clone(&broker) as Arc<dyn Broker>);
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(br#"{"issue":1}"#),
        );
        let event_id = event.id;
        store.append(event).unwrap();

        let report = runtime
            .replay_by_id(&store, event_id)
            .await
            .expect("stored event should replay");

        assert_eq!(report.submitted.len(), 1);
        assert_eq!(broker.len(), 1);
    }

    #[tokio::test]
    async fn replay_by_id_returns_not_found_without_submitting() {
        let broker = Arc::new(InMemoryBroker::new());
        let store = InMemoryEventStore::new();
        let runtime = TriggerRuntime::new(
            TriggerRegistry::new(),
            Arc::clone(&broker) as Arc<dyn Broker>,
        );

        let error = runtime
            .replay_by_id(
                &store,
                EventEnvelope::new(
                    Source::Manual,
                    "event.manual.missing",
                    Bytes::from_static(b"{}"),
                )
                .id,
            )
            .await
            .expect_err("missing event should not replay");

        assert!(matches!(error, ReplayError::EventNotFound(_)));
        assert!(broker.is_empty());
    }

    #[tokio::test]
    async fn replay_range_handles_events_inside_time_window() {
        let broker = Arc::new(InMemoryBroker::new());
        let store = InMemoryEventStore::new();
        let runtime = TriggerRuntime::new(
            github_issue_registry(),
            Arc::clone(&broker) as Arc<dyn Broker>,
        );
        let base = Utc::now();
        let before = event_at(base - ChronoDuration::minutes(5), b"before");
        let first = event_at(base + ChronoDuration::minutes(1), b"first");
        let second = event_at(base + ChronoDuration::minutes(2), b"second");
        let after = event_at(base + ChronoDuration::minutes(5), b"after");
        let first_id = first.id.to_string();
        let second_id = second.id.to_string();
        store.append(before).unwrap();
        store.append(first).unwrap();
        store.append(second).unwrap();
        store.append(after).unwrap();

        let report = runtime
            .replay_range(
                &store,
                base,
                base + ChronoDuration::minutes(3),
                &ReplayFilter::default(),
            )
            .await
            .expect("range should replay");

        let event_ids: Vec<_> = report
            .events
            .iter()
            .map(|event| event.event_id.as_str())
            .collect();
        assert_eq!(event_ids, [first_id, second_id]);
        assert_eq!(broker.len(), 2);
    }

    #[tokio::test]
    async fn replay_range_preserves_append_order() {
        let broker = Arc::new(InMemoryBroker::new());
        let store = InMemoryEventStore::new();
        let runtime = TriggerRuntime::new(
            github_issue_registry(),
            Arc::clone(&broker) as Arc<dyn Broker>,
        );
        let base = Utc::now();
        let later_appended_first = event_at(base + ChronoDuration::minutes(2), b"first");
        let earlier_appended_second = event_at(base + ChronoDuration::minutes(1), b"second");
        let first_id = later_appended_first.id.to_string();
        let second_id = earlier_appended_second.id.to_string();
        store.append(later_appended_first).unwrap();
        store.append(earlier_appended_second).unwrap();

        let report = runtime
            .replay_range(
                &store,
                base,
                base + ChronoDuration::minutes(3),
                &ReplayFilter::default(),
            )
            .await
            .expect("range should replay");

        let event_ids: Vec<_> = report
            .events
            .iter()
            .map(|event| event.event_id.as_str())
            .collect();
        assert_eq!(event_ids, [first_id, second_id]);
        assert_eq!(report.events.len(), 2);
    }

    #[tokio::test]
    async fn replay_range_filters_by_event_type_and_preview_does_not_submit() {
        let broker = Arc::new(InMemoryBroker::new());
        let store = InMemoryEventStore::new();
        let runtime = TriggerRuntime::new(
            github_issue_registry(),
            Arc::clone(&broker) as Arc<dyn Broker>,
        );
        let base = Utc::now();
        let matching = event_at(base + ChronoDuration::minutes(1), b"match");
        let mut other = event_at(base + ChronoDuration::minutes(2), b"other");
        other.event_type = EventType::new("event.other.kind");
        store.append(matching.clone()).unwrap();
        store.append(other).unwrap();

        let filter = ReplayFilter {
            event_type: Some(EVENT_GITHUB_ISSUE_CREATED.to_owned()),
            source: None,
        };
        let window_end = base + ChronoDuration::minutes(5);

        // Dry-run preview returns only the matching event and submits nothing.
        let preview = runtime.preview_range(&store, base, window_end, &filter);
        assert_eq!(preview.len(), 1);
        assert_eq!(preview[0].id, matching.id);
        assert!(broker.is_empty(), "dry-run must not submit");

        // A real filtered replay handles only the matching event.
        let report = runtime
            .replay_range(&store, base, window_end, &filter)
            .await
            .expect("filtered replay should run");
        assert_eq!(report.events.len(), 1);
        assert_eq!(broker.len(), 1);
    }

    #[tokio::test]
    async fn ingest_persists_event_and_handles_it() {
        let broker = Arc::new(InMemoryBroker::new());
        let store = Arc::new(InMemoryEventStore::new());
        let runtime = TriggerRuntime::new(
            github_issue_registry(),
            Arc::clone(&broker) as Arc<dyn Broker>,
        );
        let ingest = EventIngest::new(Arc::clone(&store) as Arc<dyn EventStore>, runtime);
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(br#"{"issue":1}"#),
        );
        let event_id = event.id;

        let report = ingest.ingest(event).await.expect("event should ingest");

        assert_eq!(report.event_id, event_id.to_string());
        assert_eq!(report.handle.submitted.len(), 1);
        assert_eq!(broker.len(), 1);
        assert!(store.get(event_id).is_some());
    }

    #[tokio::test]
    async fn ingest_persists_event_even_when_handling_fails() {
        struct FailingBinding;

        impl Binding for FailingBinding {
            type Job = WorklaneJob;

            fn bind(&self, _event: &EventEnvelope) -> Result<Self::Job, BindingError> {
                Err(BindingError::Rejected("bad event".to_owned()))
            }
        }

        let broker = Arc::new(InMemoryBroker::new());
        let store = Arc::new(InMemoryEventStore::new());
        let mut registry = TriggerRegistry::new();
        registry.register(RegisteredTrigger::new(
            "failing",
            10,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            FailingBinding,
        ));
        let runtime = TriggerRuntime::new(registry, broker);
        let ingest = EventIngest::new(Arc::clone(&store) as Arc<dyn EventStore>, runtime);
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(b"{}"),
        );
        let event_id = event.id;

        let report = ingest.ingest(event).await.expect("event should ingest");

        assert_eq!(report.handle.failed.len(), 1);
        assert!(store.get(event_id).is_some());
    }

    #[tokio::test]
    async fn no_match_handling_records_empty_trace() {
        let broker = Arc::new(InMemoryBroker::new());
        let trace_sink = Arc::new(InMemoryTraceSink::new());
        let registry = TriggerRegistry::new();
        let runtime = TriggerRuntime::with_trace_sink(
            registry,
            broker,
            Arc::clone(&trace_sink) as Arc<dyn TraceSink>,
        );
        let event = EventEnvelope::new(
            Source::Manual,
            "event.manual.unmatched",
            Bytes::from_static(b"{}"),
        );

        let report = runtime.handle(event).await.expect("event should handle");

        assert!(report.submitted.is_empty());
        let traces = trace_sink.traces();
        assert_eq!(traces.len(), 1);
        assert!(traces[0].matched_triggers.is_empty());
        assert!(traces[0].submitted.is_empty());
        assert_eq!(traces[0].failure_count, 0);
    }

    #[tokio::test]
    async fn disabled_trigger_does_not_submit() {
        let broker = Arc::new(InMemoryBroker::new());
        let mut registry = TriggerRegistry::new();
        registry.register(
            RegisteredTrigger::new(
                "disabled",
                10,
                EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
                WorklaneJobBinding::new("projection", "CreateProjectionJob", 3),
            )
            .disabled(),
        );
        let runtime = TriggerRuntime::new(registry, Arc::clone(&broker) as Arc<dyn Broker>);
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(b"{}"),
        );

        let report = runtime.handle(event).await.expect("event should handle");

        assert!(report.submitted.is_empty());
        assert!(broker.is_empty());
    }

    #[tokio::test]
    async fn binding_failure_records_dead_trigger() {
        struct FailingBinding;

        impl Binding for FailingBinding {
            type Job = WorklaneJob;

            fn bind(&self, _event: &EventEnvelope) -> Result<Self::Job, BindingError> {
                Err(BindingError::Rejected("bad event".to_owned()))
            }
        }

        let broker = Arc::new(InMemoryBroker::new());
        let mut registry = TriggerRegistry::new();
        registry.register(RegisteredTrigger::new(
            "failing",
            10,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            FailingBinding,
        ));
        let runtime = TriggerRuntime::new(registry, broker);
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(b"{}"),
        );

        let report = runtime.handle(event).await.expect("event should handle");

        assert_eq!(report.failed.len(), 1);
        assert_eq!(runtime.dead_triggers().len(), 1);
        assert_eq!(runtime.dead_triggers()[0].trigger_name, "failing");
    }

    #[tokio::test]
    async fn failed_trigger_records_failure_count_in_trace() {
        struct FailingBinding;

        impl Binding for FailingBinding {
            type Job = WorklaneJob;

            fn bind(&self, _event: &EventEnvelope) -> Result<Self::Job, BindingError> {
                Err(BindingError::Rejected("bad event".to_owned()))
            }
        }

        let broker = Arc::new(InMemoryBroker::new());
        let trace_sink = Arc::new(InMemoryTraceSink::new());
        let mut registry = TriggerRegistry::new();
        registry.register(RegisteredTrigger::new(
            "failing",
            10,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            FailingBinding,
        ));
        let runtime = TriggerRuntime::with_trace_sink(
            registry,
            broker,
            Arc::clone(&trace_sink) as Arc<dyn TraceSink>,
        );
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(b"{}"),
        );

        let report = runtime.handle(event).await.expect("event should handle");

        assert_eq!(report.failed.len(), 1);
        let traces = trace_sink.traces();
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].matched_triggers, ["failing"]);
        assert_eq!(traces[0].failure_count, 1);
    }

    #[tokio::test]
    async fn retryable_submission_failure_can_succeed() {
        let broker = Arc::new(FlakyBroker::new(1));
        let mut registry = TriggerRegistry::new();
        registry.register(RegisteredTrigger::new(
            "flaky",
            10,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            WorklaneJobBinding::new("projection", "CreateProjectionJob", 3),
        ));
        let runtime = TriggerRuntime::with_options(
            registry,
            Arc::clone(&broker) as Arc<dyn Broker>,
            RuntimeOptions {
                max_submission_attempts: 2,
                // No real sleeping in tests; the backoff schedule is covered by a
                // dedicated unit test.
                submission_backoff: Duration::ZERO,
            },
        );
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(b"{}"),
        );

        let report = runtime.handle(event).await.expect("event should handle");

        assert_eq!(report.submitted.len(), 1);
        assert!(report.failed.is_empty());
        assert_eq!(broker.enqueue_calls(), 2);
    }

    #[tokio::test]
    async fn exhausted_submission_retry_records_dead_trigger() {
        let broker = Arc::new(FlakyBroker::new(3));
        let mut registry = TriggerRegistry::new();
        registry.register(RegisteredTrigger::new(
            "always-failing",
            10,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            WorklaneJobBinding::new("projection", "CreateProjectionJob", 3),
        ));
        let runtime = TriggerRuntime::with_options(
            registry,
            Arc::clone(&broker) as Arc<dyn Broker>,
            RuntimeOptions {
                max_submission_attempts: 2,
                // No real sleeping in tests; the backoff schedule is covered by a
                // dedicated unit test.
                submission_backoff: Duration::ZERO,
            },
        );
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(b"{}"),
        );

        let report = runtime.handle(event).await.expect("event should handle");

        assert!(report.submitted.is_empty());
        assert_eq!(report.failed.len(), 1);
        assert_eq!(runtime.dead_triggers().len(), 1);
        assert_eq!(broker.enqueue_calls(), 2);
    }

    #[test]
    fn registry_orders_by_priority() {
        let mut registry = TriggerRegistry::new();
        registry.register(RegisteredTrigger::new(
            "low",
            1,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            WorklaneJobBinding::new("lane", "LowJob", 1),
        ));
        registry.register(RegisteredTrigger::new(
            "high",
            9,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            WorklaneJobBinding::new("lane", "HighJob", 1),
        ));
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(b"{}"),
        );

        let names: Vec<_> = registry
            .matching(&event)
            .map(RegisteredTrigger::name)
            .collect();

        assert_eq!(names, ["high", "low"]);
    }

    #[test]
    fn submission_backoff_doubles_and_caps() {
        let base = Duration::from_millis(100);
        // No delay before the first attempt, then exponential growth from the base.
        assert_eq!(submission_backoff_delay(0, base), Duration::ZERO);
        assert_eq!(
            submission_backoff_delay(1, base),
            Duration::from_millis(100)
        );
        assert_eq!(
            submission_backoff_delay(2, base),
            Duration::from_millis(200)
        );
        assert_eq!(
            submission_backoff_delay(3, base),
            Duration::from_millis(400)
        );
        // A zero base disables backoff regardless of attempt.
        assert_eq!(submission_backoff_delay(5, Duration::ZERO), Duration::ZERO);
        // A huge attempt index saturates to the cap rather than overflowing.
        assert_eq!(submission_backoff_delay(1000, base), MAX_SUBMISSION_BACKOFF);
    }

    fn github_issue_registry() -> TriggerRegistry {
        let mut registry = TriggerRegistry::new();
        registry.register(RegisteredTrigger::new(
            "github-issue",
            10,
            EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
            WorklaneJobBinding::new("projection", "CreateProjectionJob", 3),
        ));
        registry
    }

    fn event_at(timestamp: chrono::DateTime<Utc>, payload: &'static [u8]) -> EventEnvelope {
        let mut event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(payload),
        );
        event.timestamp = timestamp;
        event
    }

    struct FlakyBroker {
        failures_remaining: Mutex<u32>,
        enqueue_calls: Mutex<u32>,
    }

    impl FlakyBroker {
        fn new(failures_remaining: u32) -> Self {
            Self {
                failures_remaining: Mutex::new(failures_remaining),
                enqueue_calls: Mutex::new(0),
            }
        }

        fn enqueue_calls(&self) -> u32 {
            *self
                .enqueue_calls
                .lock()
                .expect("enqueue calls mutex poisoned")
        }
    }

    #[async_trait]
    impl Broker for FlakyBroker {
        async fn enqueue(&self, _job: NewJob) -> WorklaneResult<JobId> {
            *self
                .enqueue_calls
                .lock()
                .expect("enqueue calls mutex poisoned") += 1;
            let mut failures = self
                .failures_remaining
                .lock()
                .expect("failures mutex poisoned");

            if *failures > 0 {
                *failures -= 1;
                Err(WorklaneError::Broker("temporary failure".to_owned()))
            } else {
                Ok(JobId::new())
            }
        }

        async fn enqueue_batch(&self, jobs: Vec<NewJob>) -> WorklaneResult<Vec<JobId>> {
            let mut ids = Vec::with_capacity(jobs.len());
            for job in jobs {
                ids.push(self.enqueue(job).await?);
            }
            Ok(ids)
        }

        async fn reserve(&self, _lane: &Lane) -> WorklaneResult<Option<Reservation>> {
            Ok(None)
        }

        async fn ack(&self, _receipt: ReservationReceipt) -> WorklaneResult<()> {
            Ok(())
        }

        async fn retry(
            &self,
            _receipt: ReservationReceipt,
            _delay: Duration,
        ) -> WorklaneResult<()> {
            Ok(())
        }

        async fn defer(
            &self,
            _receipt: ReservationReceipt,
            _delay: Duration,
        ) -> WorklaneResult<()> {
            Ok(())
        }

        async fn extend(&self, _receipt: ReservationReceipt) -> WorklaneResult<()> {
            Ok(())
        }

        async fn fail(&self, _receipt: ReservationReceipt, _error: String) -> WorklaneResult<()> {
            Ok(())
        }

        async fn read_dead_letters(
            &self,
            _lane: &Lane,
            _limit: usize,
        ) -> WorklaneResult<Vec<DeadLetter>> {
            Ok(Vec::new())
        }

        async fn count_dead_letters(&self, _lane: &Lane) -> WorklaneResult<u64> {
            Ok(0)
        }

        async fn pending_count(&self, _lane: &Lane) -> WorklaneResult<u64> {
            Ok(0)
        }

        async fn classify(&self, _id: JobId) -> WorklaneResult<JobState> {
            Ok(JobState::CompletedOrUnknown)
        }

        async fn requeue(&self, id: JobId) -> WorklaneResult<()> {
            Err(WorklaneError::Broker(format!(
                "no dead-letter record for job {id}"
            )))
        }

        async fn purge_dead_letters(&self, _lane: &Lane) -> WorklaneResult<u64> {
            Ok(0)
        }
    }
}
