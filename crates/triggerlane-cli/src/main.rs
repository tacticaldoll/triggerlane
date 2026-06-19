//! Triggerlane command-line interface: event injection, file loading, the HTTP
//! serve entrypoint, dead-trigger retry, and durable-store pruning.

use std::{env, fs, future::Future, net::SocketAddr, path::Path, sync::Arc, time::Duration};

use bytes::Bytes;
use clap::{Parser, Subcommand};
use serde::Deserialize;
use thiserror::Error;
use tokio::net::TcpListener;
use triggerlane_core::{
    EVENT_GITHUB_ISSUE_CREATED, EventEnvelope, EventId, EventMetadata, EventType, EventTypeTrigger,
    Source, WorklaneJobBinding,
};
use triggerlane_http::{
    AcceptEventHandler, IngestEventHandler, ReadAuth, Readiness, WebhookVerifier,
};
use triggerlane_runtime::{
    EventIngest, LoggingTraceSink, RegisteredTrigger, ReplayFilter, RuntimeError, RuntimeOptions,
    TriggerRegistry, TriggerRuntime,
};
use triggerlane_storage::{
    AutoRetention, DeadTriggerQueue, EventStore, FileDeadTriggerQueue, FileEventStore,
    ManualRetention,
};
use worklane_core::{Broker, Lane, redact_credentials};
use worklane_postgres::PostgresBroker;
use worklane_redis::RedisBroker;
use worklane_sqlite::SqliteBroker;

mod config;

/// Default durable event store path; override with `TRIGGERLANE_STORE`.
const DEFAULT_STORE_PATH: &str = "triggerlane-events.jsonl";

/// Default durable Worklane broker database path; override with
/// `TRIGGERLANE_WORKLANE_DB`.
const DEFAULT_BROKER_DB: &str = "triggerlane-jobs.sqlite3";

/// Optional per-broker isolation *above* the lane level, so several Triggerlane
/// deployments can share one database/server: a Postgres schema (via
/// `connect_with_schema`) or a Redis key namespace (via `connect_with_namespace`).
/// Unset/empty keeps Worklane's default (`public` / `worklane`). Inert for sqlite,
/// which isolates by file path (`TRIGGERLANE_WORKLANE_DB`).
const BROKER_SCHEMA_ENV: &str = "TRIGGERLANE_WORKLANE_SCHEMA";
const BROKER_NAMESPACE_ENV: &str = "TRIGGERLANE_WORKLANE_NAMESPACE";

/// Default durable dead-trigger queue path; override with `TRIGGERLANE_DTQ`.
const DEFAULT_DTQ_PATH: &str = "triggerlane-dead-triggers.jsonl";

/// Path to a declarative trigger configuration file; when set, it replaces the
/// built-in `default_registry()`.
const TRIGGERS_ENV: &str = "TRIGGERLANE_TRIGGERS";

/// Per-request handling timeout in seconds; `0` disables it. Defaults to
/// `DEFAULT_REQUEST_TIMEOUT_SECS`.
const REQUEST_TIMEOUT_ENV: &str = "TRIGGERLANE_REQUEST_TIMEOUT_SECS";

/// Default per-request handling timeout when `TRIGGERLANE_REQUEST_TIMEOUT_SECS`
/// is unset.
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;

/// Selects the log output format; `json` for aggregators, anything else (or
/// unset) for the human-readable default.
const LOG_FORMAT_ENV: &str = "TRIGGERLANE_LOG_FORMAT";

/// Automatic-retention settings for `serve`. The delivered-grace bound is on by
/// default (Hybrid WAL: clean up work Worklane has already taken); the hard bounds
/// are opt-in safety valves. Set a bound to `0`/`off` to disable it.
const RETENTION_GRACE_ENV: &str = "TRIGGERLANE_RETENTION_DELIVERED_GRACE";
const RETENTION_MAX_AGE_ENV: &str = "TRIGGERLANE_RETENTION_MAX_AGE";
const RETENTION_MAX_COUNT_ENV: &str = "TRIGGERLANE_RETENTION_MAX_COUNT";
const RETENTION_MAX_BYTES_ENV: &str = "TRIGGERLANE_RETENTION_MAX_BYTES";
const RETENTION_INTERVAL_ENV: &str = "TRIGGERLANE_RETENTION_INTERVAL";

/// Default delivered-grace window: clean up delivered events older than this.
const DEFAULT_RETENTION_GRACE: &str = "24h";

/// Default interval between automatic-retention sweeps in `serve`.
const DEFAULT_RETENTION_INTERVAL: &str = "5m";

/// Default hard cap on retained records — a structural ceiling so the event store
/// and dead-trigger queue cannot grow without limit even if delivery stalls or
/// triggers keep failing. Operators with large payloads should also set
/// `TRIGGERLANE_RETENTION_MAX_BYTES`.
const DEFAULT_RETENTION_MAX_COUNT: &str = "1000000";

/// Job-submission resilience: how many times to attempt enqueuing a matched job
/// before recording a dead trigger, and the base backoff (milliseconds) doubled
/// between attempts. Submission is idempotent (deterministic unique key), so the
/// shipped default retries a few times to ride out a transient broker hiccup.
const SUBMISSION_ATTEMPTS_ENV: &str = "TRIGGERLANE_SUBMISSION_ATTEMPTS";
const SUBMISSION_BACKOFF_MS_ENV: &str = "TRIGGERLANE_SUBMISSION_BACKOFF_MS";
const DEFAULT_SUBMISSION_ATTEMPTS: &str = "3";
const DEFAULT_SUBMISSION_BACKOFF_MS: &str = "100";

/// Opt-in asynchronous ingestion. When enabled, `POST /events` durably accepts an
/// event (append + dedup) and returns `202 Accepted`, while a background dispatcher
/// submits it off the request path. Default off: ingestion is synchronous and the
/// response carries submission results. The capacity bounds in-flight accepted
/// events — once full, accepts wait (backpressure) rather than growing memory.
const ASYNC_DISPATCH_ENV: &str = "TRIGGERLANE_ASYNC_DISPATCH";
const ASYNC_DISPATCH_CAPACITY_ENV: &str = "TRIGGERLANE_ASYNC_DISPATCH_CAPACITY";
const DEFAULT_ASYNC_DISPATCH_CAPACITY: usize = 1024;

/// Triggerlane: the declarative event-trigger and replay plane over Worklane.
#[derive(Parser)]
#[command(name = "triggerlane", version, about)]
struct Cli {
    /// Worklane broker backend: sqlite (default), postgres, or redis.
    #[arg(
        long,
        global = true,
        env = "TRIGGERLANE_WORKLANE_BROKER",
        default_value = "sqlite"
    )]
    broker: String,
    /// Connection URL for a network broker; takes precedence over environment.
    #[arg(long, global = true)]
    url: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Inject a manual event and handle it.
    Inject { event_type: String, payload: String },
    /// Ingest every event from a JSON array file.
    LoadFile { path: String },
    /// Run the HTTP ingest server.
    Serve {
        /// Bind address.
        #[arg(default_value = "127.0.0.1:8080")]
        addr: String,
    },
    /// Drain and retry dead-trigger records.
    RetryDeadTriggers,
    /// Prune the durable stores.
    Prune {
        #[command(subcommand)]
        policy: PrunePolicy,
    },
    /// Inspect and replay stored events.
    Events {
        #[command(subcommand)]
        command: EventsCommand,
    },
    /// Inspect dead-trigger records.
    DeadTriggers {
        #[command(subcommand)]
        command: DeadTriggersCommand,
    },
    /// Inspect the Worklane broker's job-side dead-letter store, per lane.
    WorklaneDlq {
        #[command(subcommand)]
        command: WorklaneDlqCommand,
    },
}

#[derive(Subcommand)]
enum PrunePolicy {
    /// Keep records at or newer than the given age (e.g. 30d).
    OlderThan { duration: String },
    /// Keep at most the given number of most-recent records.
    Keep { count: usize },
}

#[derive(Subcommand)]
enum EventsCommand {
    /// List retained events as JSONL.
    List,
    /// Print a stored event by id.
    Get { id: String },
    /// Replay a stored event by id.
    Replay { id: String },
    /// Replay events whose timestamps fall in an RFC3339 range, optionally
    /// narrowed by event type / source, with a dry-run that previews matches.
    ReplayRange {
        start: String,
        end: String,
        /// Only replay events of this exact event type.
        #[arg(long)]
        event_type: Option<String>,
        /// Only replay events from this source (e.g. `GitHub`).
        #[arg(long)]
        source: Option<String>,
        /// Preview the matching events without submitting any jobs.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
enum DeadTriggersCommand {
    /// List dead-trigger records as JSONL.
    List,
    /// Drain and retry dead-trigger records.
    Retry,
}

/// Operator inspection of Worklane's job-side dead-letter store (jobs that failed
/// after execution), distinct from Triggerlane's trigger-side dead-trigger queue.
/// Read-only inspection plus a manual purge; retention itself is the worker's.
#[derive(Subcommand)]
enum WorklaneDlqCommand {
    /// List dead-letter records for a lane as JSONL.
    List {
        lane: String,
        #[arg(long, default_value = "50")]
        limit: usize,
    },
    /// Count dead-letter records for a lane.
    Count { lane: String },
    /// Purge all dead-letter records for a lane.
    Purge { lane: String },
}

#[tokio::main]
async fn main() {
    init_logging();
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

/// Install the structured-logging subscriber. Level comes from `RUST_LOG`
/// (default `info`); `TRIGGERLANE_LOG_FORMAT=json` selects machine-readable
/// JSON, otherwise a human-readable format. Logs go to stderr so stdout stays
/// reserved for command results.
fn init_logging() {
    use tracing_subscriber::{EnvFilter, fmt};

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let json = env::var(LOG_FORMAT_ENV).is_ok_and(|value| value.eq_ignore_ascii_case("json"));
    let builder = fmt().with_env_filter(filter).with_writer(std::io::stderr);
    if json {
        builder.json().init();
    } else {
        builder.init();
    }
}

async fn run(cli: Cli) -> Result<(), CliError> {
    let Cli {
        broker,
        url,
        command,
    } = cli;
    match command {
        Command::Inject {
            event_type,
            payload,
        } => {
            let ingest = build_ingest(broker_from_cli(&broker, url.as_deref()).await?)?;
            let event = inject(&event_type, payload.into_bytes());
            let report = ingest.ingest(event).await?;
            println!(
                "ingested {} submitted {}",
                report.event_id,
                report.handle.submitted.len()
            );
            Ok(())
        }
        Command::LoadFile { path } => {
            let ingest = build_ingest(broker_from_cli(&broker, url.as_deref()).await?)?;
            let events = load_events_file(&path)?;
            let count = events.len();
            let mut submitted = 0;
            for event in events {
                let report = ingest.ingest(event).await?;
                submitted += report.handle.submitted.len();
            }
            println!("loaded {count} submitted {submitted}");
            Ok(())
        }
        Command::Serve { addr } => serve(&addr, &broker, url.as_deref()).await,
        Command::RetryDeadTriggers => {
            let ingest = build_ingest(broker_from_cli(&broker, url.as_deref()).await?)?;
            let report = ingest.runtime().retry_dead_triggers().await?;
            println!(
                "retried {} drained {} recovered {} still-failed {} submitted {}",
                report.events_retried,
                report.drained,
                report.recovered,
                report.still_failed,
                report.submitted.len()
            );
            Ok(())
        }
        Command::Prune { policy } => {
            let policy = resolve_prune_policy(policy)?;
            let events = open_event_store()?.prune(&policy)?;
            let dead_triggers = open_dead_trigger_queue()?.prune(&policy)?;
            println!("pruned events {events} dead-triggers {dead_triggers}");
            Ok(())
        }
        Command::Events { command } => run_events(command, &broker, url.as_deref()).await,
        Command::DeadTriggers { command } => {
            run_dead_triggers(command, &broker, url.as_deref()).await
        }
        Command::WorklaneDlq { command } => {
            run_worklane_dlq(command, &broker, url.as_deref()).await
        }
    }
}

/// Inspect or purge the Worklane broker's job-side dead-letter store for a lane.
async fn run_worklane_dlq(
    command: WorklaneDlqCommand,
    broker: &str,
    url: Option<&str>,
) -> Result<(), CliError> {
    let broker = broker_from_cli(broker, url).await?;
    let parse_lane = |lane: &str| {
        Lane::try_from(lane)
            .map_err(|error| CliError::Broker(format!("invalid lane {lane:?}: {error}")))
    };
    match command {
        WorklaneDlqCommand::List { lane, limit } => {
            let lane = parse_lane(&lane)?;
            let records = broker
                .read_dead_letters(&lane, limit)
                .await
                .map_err(|error| CliError::Broker(error.to_string()))?;
            // `DeadLetter` is not `Serialize`, but its `JobEnvelope` is; emit a
            // JSONL line of the envelope plus the retained error.
            for record in records {
                let line = serde_json::json!({
                    "envelope": serde_json::to_value(&record.envelope)?,
                    "error": record.error,
                });
                println!("{line}");
            }
            Ok(())
        }
        WorklaneDlqCommand::Count { lane } => {
            let lane = parse_lane(&lane)?;
            let count = broker
                .count_dead_letters(&lane)
                .await
                .map_err(|error| CliError::Broker(error.to_string()))?;
            println!("{count}");
            Ok(())
        }
        WorklaneDlqCommand::Purge { lane } => {
            let lane = parse_lane(&lane)?;
            let purged = broker
                .purge_dead_letters(&lane)
                .await
                .map_err(|error| CliError::Broker(error.to_string()))?;
            println!("purged {purged}");
            Ok(())
        }
    }
}

/// Print stored events or replay them, per the `events` subcommand. Listing and
/// getting need only the store; replay drives the runtime through a broker.
async fn run_events(
    command: EventsCommand,
    broker: &str,
    url: Option<&str>,
) -> Result<(), CliError> {
    match command {
        EventsCommand::List => {
            for event in open_event_store()?.all() {
                println!("{}", serde_json::to_string(&event)?);
            }
            Ok(())
        }
        EventsCommand::Get { id } => {
            let id = parse_event_id(&id)?;
            match open_event_store()?.get(id) {
                Some(event) => {
                    println!("{}", serde_json::to_string(&event)?);
                    Ok(())
                }
                None => Err(CliError::Events(format!("event {id} not found"))),
            }
        }
        EventsCommand::Replay { id } => {
            let id = parse_event_id(&id)?;
            let ingest = build_ingest(broker_from_cli(broker, url).await?)?;
            let report = ingest
                .runtime()
                .replay_by_id(ingest.store().as_ref(), id)
                .await
                .map_err(|error| CliError::Events(error.to_string()))?;
            println!("replayed {id} submitted {}", report.submitted.len());
            Ok(())
        }
        EventsCommand::ReplayRange {
            start,
            end,
            event_type,
            source,
            dry_run,
        } => {
            let start = parse_timestamp(&start)?;
            let end = parse_timestamp(&end)?;
            let filter = ReplayFilter {
                event_type,
                source: source.map(|raw| parse_source(&raw)).transpose()?,
            };
            let ingest = build_ingest(broker_from_cli(broker, url).await?)?;
            if dry_run {
                let matched =
                    ingest
                        .runtime()
                        .preview_range(ingest.store().as_ref(), start, end, &filter);
                println!("would replay {} (dry-run)", matched.len());
                return Ok(());
            }
            let report = ingest
                .runtime()
                .replay_range(ingest.store().as_ref(), start, end, &filter)
                .await
                .map_err(|error| CliError::Events(error.to_string()))?;
            println!("replayed {}", report.events.len());
            Ok(())
        }
    }
}

/// Parse a [`Source`] from its name via its serde representation (`GitHub`, `Http`,
/// …), rejecting an unknown one rather than silently ignoring it.
fn parse_source(raw: &str) -> Result<Source, CliError> {
    serde_json::from_value(serde_json::Value::String(raw.to_owned()))
        .map_err(|_| CliError::Events(format!("unknown source {raw:?}")))
}

/// List or retry dead-trigger records, per the `dead-triggers` subcommand.
async fn run_dead_triggers(
    command: DeadTriggersCommand,
    broker: &str,
    url: Option<&str>,
) -> Result<(), CliError> {
    match command {
        DeadTriggersCommand::List => {
            for record in open_dead_trigger_queue()?.all() {
                println!("{}", serde_json::to_string(&record)?);
            }
            Ok(())
        }
        DeadTriggersCommand::Retry => {
            let ingest = build_ingest(broker_from_cli(broker, url).await?)?;
            let report = ingest.runtime().retry_dead_triggers().await?;
            println!(
                "retried {} drained {} recovered {} still-failed {} submitted {}",
                report.events_retried,
                report.drained,
                report.recovered,
                report.still_failed,
                report.submitted.len()
            );
            Ok(())
        }
    }
}

/// Connect to the broker selected by the global CLI flags (or their env fallback).
async fn broker_from_cli(backend: &str, url: Option<&str>) -> Result<Arc<dyn Broker>, CliError> {
    connect_broker(parse_backend(backend)?, url).await
}

/// Resolve a `prune` subcommand into a manual retention policy.
fn resolve_prune_policy(policy: PrunePolicy) -> Result<ManualRetention, CliError> {
    match policy {
        PrunePolicy::OlderThan { duration } => {
            let duration = parse_duration(&duration)
                .ok_or_else(|| CliError::Retention(format!("invalid duration {duration:?}")))?;
            Ok(ManualRetention::OlderThan(chrono::Utc::now() - duration))
        }
        PrunePolicy::Keep { count } => Ok(ManualRetention::KeepMostRecent(count)),
    }
}

fn parse_event_id(raw: &str) -> Result<EventId, CliError> {
    raw.parse()
        .map_err(|error| CliError::Events(format!("invalid event id {raw:?}: {error}")))
}

fn parse_timestamp(raw: &str) -> Result<chrono::DateTime<chrono::Utc>, CliError> {
    raw.parse()
        .map_err(|error| CliError::Events(format!("invalid timestamp {raw:?}: {error}")))
}

/// The Worklane broker backends the shipped CLI can target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BrokerBackend {
    Sqlite,
    Postgres,
    Redis,
}

/// Parse a broker-backend name. The default (`sqlite`) is a durable, embedded
/// broker; `postgres`/`redis` target a shared Worklane cluster.
fn parse_backend(name: &str) -> Result<BrokerBackend, CliError> {
    match name {
        "sqlite" => Ok(BrokerBackend::Sqlite),
        "postgres" => Ok(BrokerBackend::Postgres),
        "redis" => Ok(BrokerBackend::Redis),
        other => Err(CliError::Broker(format!(
            "unknown broker {other:?} (expected sqlite, postgres, or redis)"
        ))),
    }
}

/// Resolve a network broker URL with one documented precedence — an explicit
/// `--url`, then `$TRIGGERLANE_WORKLANE_URL`, then the backend's conventional variable —
/// returning the URL and a human-readable source label for the announcement. The
/// URL itself is never logged (it may carry a password). Pure, for testing.
fn resolve_broker_url(
    url_flag: Option<&str>,
    triggerlane_url: Option<&str>,
    fallback: Option<&str>,
    fallback_var: &str,
    backend: &str,
) -> Result<(String, String), CliError> {
    if let Some(url) = url_flag {
        Ok((url.to_owned(), "--url".to_owned()))
    } else if let Some(url) = triggerlane_url {
        Ok((url.to_owned(), "$TRIGGERLANE_WORKLANE_URL".to_owned()))
    } else if let Some(url) = fallback {
        Ok((url.to_owned(), format!("${fallback_var}")))
    } else {
        Err(CliError::Broker(format!(
            "--url, $TRIGGERLANE_WORKLANE_URL, or ${fallback_var} is required for --broker {backend}"
        )))
    }
}

/// Read an environment variable, treating both unset and empty as absent. Used
/// for optional knobs where an empty value should mean "use the default", not
/// "use the empty string".
fn nonempty_env(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.is_empty())
}

/// Connect to the selected Worklane broker as an `Arc<dyn Broker>`, the single
/// composition seam that keeps the rest of the CLI generic over the
/// `worklane-core` `Broker` contract. Connection errors are redacted of
/// credentials. A Postgres schema / Redis namespace, when configured, isolates
/// this deployment from others sharing the same database/server.
async fn connect_broker(
    backend: BrokerBackend,
    url_flag: Option<&str>,
) -> Result<Arc<dyn Broker>, CliError> {
    match backend {
        BrokerBackend::Sqlite => {
            let path = env::var("TRIGGERLANE_WORKLANE_DB")
                .unwrap_or_else(|_| DEFAULT_BROKER_DB.to_owned());
            let broker = SqliteBroker::open(&path).map_err(|error| {
                CliError::Broker(format!("sqlite: failed to open {path:?}: {error}"))
            })?;
            Ok(Arc::new(broker))
        }
        BrokerBackend::Postgres => {
            let triggerlane_url = env::var("TRIGGERLANE_WORKLANE_URL").ok();
            let fallback = env::var("DATABASE_URL").ok();
            let (url, source) = resolve_broker_url(
                url_flag,
                triggerlane_url.as_deref(),
                fallback.as_deref(),
                "DATABASE_URL",
                "postgres",
            )?;
            let schema = nonempty_env(BROKER_SCHEMA_ENV);
            match schema.as_deref() {
                Some(schema) => eprintln!("postgres: using {source} (schema {schema})"),
                None => eprintln!("postgres: using {source}"),
            }
            let connect = async {
                match schema.as_deref() {
                    Some(schema) => PostgresBroker::connect_with_schema(&url, schema).await,
                    None => PostgresBroker::connect(&url).await,
                }
            };
            let broker = connect.await.map_err(|error| {
                CliError::Broker(format!(
                    "postgres: connection failed: {}",
                    redact_credentials(&error.to_string())
                ))
            })?;
            Ok(Arc::new(broker))
        }
        BrokerBackend::Redis => {
            let triggerlane_url = env::var("TRIGGERLANE_WORKLANE_URL").ok();
            let fallback = env::var("REDIS_URL").ok();
            let (url, source) = resolve_broker_url(
                url_flag,
                triggerlane_url.as_deref(),
                fallback.as_deref(),
                "REDIS_URL",
                "redis",
            )?;
            let namespace = nonempty_env(BROKER_NAMESPACE_ENV);
            match namespace.as_deref() {
                Some(namespace) => eprintln!("redis: using {source} (namespace {namespace})"),
                None => eprintln!("redis: using {source}"),
            }
            let connect = async {
                match namespace.as_deref() {
                    Some(namespace) => RedisBroker::connect_with_namespace(&url, namespace).await,
                    None => RedisBroker::connect(&url).await,
                }
            };
            let broker = connect.await.map_err(|error| {
                CliError::Broker(format!(
                    "redis: connection failed: {}",
                    redact_credentials(&error.to_string())
                ))
            })?;
            Ok(Arc::new(broker))
        }
    }
}

/// Connect to the broker selected by the environment, defaulting to the durable
fn open_event_store() -> Result<FileEventStore, CliError> {
    let path = env::var("TRIGGERLANE_STORE").unwrap_or_else(|_| DEFAULT_STORE_PATH.to_owned());
    Ok(FileEventStore::open(path)?)
}

fn open_dead_trigger_queue() -> Result<FileDeadTriggerQueue, CliError> {
    let path = env::var("TRIGGERLANE_DTQ").unwrap_or_else(|_| DEFAULT_DTQ_PATH.to_owned());
    Ok(FileDeadTriggerQueue::open(path)?)
}

fn build_ingest(broker: Arc<dyn Broker>) -> Result<Arc<EventIngest>, CliError> {
    let store = Arc::new(open_event_store()?);
    let dead_triggers = Arc::new(open_dead_trigger_queue()?);
    let runtime = TriggerRuntime::with_options_and_trace_sink(
        load_registry()?,
        broker,
        runtime_options_from_env()?,
        Arc::new(LoggingTraceSink),
    )
    .with_dead_trigger_queue(dead_triggers);
    Ok(Arc::new(EventIngest::new(
        store as Arc<dyn EventStore>,
        runtime,
    )))
}

/// Resolve job-submission resilience options from the environment: how many
/// attempts before a dead trigger, and the base retry backoff in milliseconds.
fn runtime_options_from_env() -> Result<RuntimeOptions, CliError> {
    parse_runtime_options(
        env::var(SUBMISSION_ATTEMPTS_ENV).ok().as_deref(),
        env::var(SUBMISSION_BACKOFF_MS_ENV).ok().as_deref(),
    )
}

/// Build [`RuntimeOptions`] from raw settings, applying the shipped defaults when
/// unset. Attempts are floored at 1; a non-numeric value is an error rather than
/// silently ignored. Pure, so it is testable without mutating process environment.
fn parse_runtime_options(
    attempts: Option<&str>,
    backoff_ms: Option<&str>,
) -> Result<RuntimeOptions, CliError> {
    let attempts = attempts.unwrap_or(DEFAULT_SUBMISSION_ATTEMPTS);
    let attempts = attempts
        .trim()
        .parse::<u32>()
        .map_err(|_| CliError::Submission(format!("invalid submission attempts {attempts:?}")))?
        .max(1);
    let backoff_ms = backoff_ms.unwrap_or(DEFAULT_SUBMISSION_BACKOFF_MS);
    let backoff_ms = backoff_ms
        .trim()
        .parse::<u64>()
        .map_err(|_| CliError::Submission(format!("invalid submission backoff {backoff_ms:?}")))?;
    Ok(RuntimeOptions {
        max_submission_attempts: attempts,
        submission_backoff: Duration::from_millis(backoff_ms),
    })
}

async fn serve(addr: &str, broker: &str, url: Option<&str>) -> Result<(), CliError> {
    let addr: SocketAddr = addr.parse()?;
    let request_timeout = request_timeout()?;
    let metrics = install_metrics_recorder()?;
    let ingest = build_ingest(broker_from_cli(broker, url).await?)?;
    // Recover work interrupted by a crash: replay events that are durably stored
    // but not yet delivered before accepting new traffic.
    let recovered = ingest.recover_undelivered().await?;
    if recovered > 0 {
        tracing::info!(recovered, "replayed undelivered events on startup");
    }
    // Publish store-size metrics and bound the durable store automatically:
    // clean up delivered events past the grace window, plus any opt-in hard bounds.
    spawn_maintenance(
        Arc::clone(&ingest),
        auto_retention_from_env()?,
        retention_interval_from_env()?,
    );
    // Enable webhook signature verification when a secret is configured; without
    // one the receiver is open (trusted-network deployments).
    let verifier = env::var("TRIGGERLANE_WEBHOOK_SECRET")
        .ok()
        // An empty secret is treated as unset, not as a verifier keyed on "":
        // otherwise setting the variable empty (to "turn it off") would enable
        // verification with a guessable key. Mirrors the read-token handling.
        .filter(|secret| !secret.is_empty())
        .map(|secret| Arc::new(WebhookVerifier::github(secret)));
    // When set, the read/replay endpoints require `Authorization: Bearer <token>`;
    // unset leaves them open for trusted-network deployments.
    let read_auth = env::var("TRIGGERLANE_READ_TOKEN")
        .ok()
        .filter(|token| !token.is_empty())
        .map(|token| Arc::new(ReadAuth::new(token)));
    // Opt-in asynchronous ingestion: accept durably and dispatch off the request
    // path. Default off keeps `POST /events` synchronous with submission results.
    let dispatch_tx = if env_flag_enabled(ASYNC_DISPATCH_ENV) {
        let capacity = async_dispatch_capacity()?;
        let (tx, rx) = tokio::sync::mpsc::channel::<EventEnvelope>(capacity);
        spawn_dispatcher(Arc::clone(&ingest), rx);
        tracing::info!(capacity, "asynchronous dispatch enabled");
        Some(tx)
    } else {
        None
    };
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(addr = %listener.local_addr()?, "listening");

    // Mark readiness as draining the moment a shutdown signal arrives, before
    // graceful shutdown stops accepting connections, so a load balancer polling
    // `/readyz` can stop routing new traffic while in-flight requests drain.
    let readiness = Readiness::ready();
    let draining = readiness.clone();
    let shutdown = async move {
        shutdown_signal().await;
        tracing::info!("shutdown signal received, draining");
        draining.set_ready(false);
    };
    serve_ingest(
        ServeConfig {
            listener,
            ingest,
            verifier,
            read_auth,
            readiness,
            metrics,
            request_timeout,
            dispatch_tx,
        },
        shutdown,
    )
    .await
}

/// Whether an environment flag is set to a truthy value (`1`/`true`/`yes`/`on`).
fn env_flag_enabled(key: &str) -> bool {
    env::var(key).is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// Resolve the asynchronous-dispatch channel capacity (default
/// [`DEFAULT_ASYNC_DISPATCH_CAPACITY`]); must be a positive integer.
fn async_dispatch_capacity() -> Result<usize, CliError> {
    match nonempty_env(ASYNC_DISPATCH_CAPACITY_ENV) {
        None => Ok(DEFAULT_ASYNC_DISPATCH_CAPACITY),
        Some(raw) => {
            let capacity = raw.trim().parse::<usize>().map_err(|_| {
                CliError::Submission(format!("invalid async dispatch capacity {raw:?}"))
            })?;
            if capacity == 0 {
                return Err(CliError::Submission(
                    "async dispatch capacity must be greater than 0".to_owned(),
                ));
            }
            Ok(capacity)
        }
    }
}

/// Spawn the background dispatcher for asynchronous ingestion: handle each accepted
/// event off the request path and mark it delivered. A handling error leaves the
/// event undelivered so restart recovery retries it. Runs until the channel closes.
fn spawn_dispatcher(ingest: Arc<EventIngest>, mut rx: tokio::sync::mpsc::Receiver<EventEnvelope>) {
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            if let Err(error) = ingest.dispatch(event).await {
                tracing::warn!(%error, "async dispatch failed; event left for restart recovery");
            }
        }
    });
}

/// Install the Prometheus metrics recorder and return a render closure for the
/// `/metrics` endpoint. Installing fails only if a recorder is already set.
fn install_metrics_recorder() -> Result<triggerlane_http::MetricsRender, CliError> {
    let handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .map_err(|error| CliError::Metrics(error.to_string()))?;
    Ok(Arc::new(move || handle.render()))
}

/// Resolve the per-request handling timeout from the environment.
fn request_timeout() -> Result<Option<Duration>, CliError> {
    parse_request_timeout(env::var(REQUEST_TIMEOUT_ENV).ok().as_deref())
}

/// Parse a request-timeout setting. `None` (unset) → the default; `0` → disabled
/// (`None`); a positive integer → that many seconds; a non-numeric value is an
/// error rather than silently ignored. Pure, so it is testable without mutating
/// process environment.
fn parse_request_timeout(raw: Option<&str>) -> Result<Option<Duration>, CliError> {
    let secs = match raw {
        Some(raw) => raw
            .parse::<u64>()
            .map_err(|_| CliError::RequestTimeout(raw.to_owned()))?,
        None => DEFAULT_REQUEST_TIMEOUT_SECS,
    };
    Ok((secs > 0).then(|| Duration::from_secs(secs)))
}

/// Parse a compact duration such as `30d`, `12h`, `90m`, or `3600s`. Returns
/// `None` on an empty input, unknown/missing suffix, non-numeric amount, or
/// overflow.
fn parse_duration(raw: &str) -> Option<chrono::Duration> {
    let unit = raw.chars().last()?;
    let amount: i64 = raw[..raw.len() - unit.len_utf8()].parse().ok()?;
    if amount < 0 {
        return None;
    }
    let seconds = match unit {
        's' => amount,
        'm' => amount.checked_mul(60)?,
        'h' => amount.checked_mul(3_600)?,
        'd' => amount.checked_mul(86_400)?,
        _ => return None,
    };
    chrono::Duration::try_seconds(seconds)
}

/// Resolve the automatic-retention policy for `serve` from the environment.
/// Delivered events are cleaned up after a grace window (default `24h`), and a
/// default hard `max_count` caps the store (and dead-trigger queue) even if
/// delivery stalls. Set any bound to `0`/`off` to disable it.
fn auto_retention_from_env() -> Result<AutoRetention, CliError> {
    let grace = env::var(RETENTION_GRACE_ENV).ok();
    let grace = grace.as_deref().or(Some(DEFAULT_RETENTION_GRACE));
    let max_count = env::var(RETENTION_MAX_COUNT_ENV).ok();
    let max_count = max_count.as_deref().or(Some(DEFAULT_RETENTION_MAX_COUNT));
    parse_auto_retention(
        grace,
        env::var(RETENTION_MAX_AGE_ENV).ok().as_deref(),
        max_count,
        env::var(RETENTION_MAX_BYTES_ENV).ok().as_deref(),
    )
}

/// Build an [`AutoRetention`] from raw settings. A duration of `0`/`off`/empty is
/// "no bound"; a present value must parse. Pure, so it is testable without
/// mutating the process environment.
fn parse_auto_retention(
    grace: Option<&str>,
    max_age: Option<&str>,
    max_count: Option<&str>,
    max_bytes: Option<&str>,
) -> Result<AutoRetention, CliError> {
    Ok(AutoRetention {
        delivered_grace: parse_bound_duration(grace)?,
        max_age: parse_bound_duration(max_age)?,
        max_count: parse_bound_number(max_count, "max-count")?.map(|count| count as usize),
        max_bytes: parse_bound_number(max_bytes, "max-bytes")?,
    })
}

/// Parse an optional retention duration bound. `None`, empty, `0`, or `off` mean
/// "no bound"; anything else must be a valid compact duration.
fn parse_bound_duration(raw: Option<&str>) -> Result<Option<chrono::Duration>, CliError> {
    match raw.map(str::trim) {
        None | Some("") | Some("0") | Some("off") => Ok(None),
        Some(value) => parse_duration(value)
            .ok_or_else(|| CliError::Retention(format!("invalid retention duration {value:?}")))
            .map(Some),
    }
}

/// Parse an optional retention numeric bound. `None`, empty, `0`, or `off` mean
/// "no bound"; anything else must be a non-negative integer.
fn parse_bound_number(raw: Option<&str>, label: &str) -> Result<Option<u64>, CliError> {
    match raw.map(str::trim) {
        None | Some("") | Some("0") | Some("off") => Ok(None),
        Some(value) => value
            .parse::<u64>()
            .map_err(|_| CliError::Retention(format!("invalid retention {label} {value:?}")))
            .map(Some),
    }
}

/// Resolve the automatic-retention sweep interval (default `5m`).
fn retention_interval_from_env() -> Result<Duration, CliError> {
    let raw = env::var(RETENTION_INTERVAL_ENV).ok();
    let raw = raw.as_deref().unwrap_or(DEFAULT_RETENTION_INTERVAL);
    parse_duration(raw)
        .and_then(|duration| duration.to_std().ok())
        .ok_or_else(|| CliError::Retention(format!("invalid retention interval {raw:?}")))
}

/// Spawn the background maintenance task: every tick it publishes store-size
/// metrics and, when the policy is bounded, enforces automatic retention. Runs
/// until the process exits.
fn spawn_maintenance(ingest: Arc<EventIngest>, policy: AutoRetention, interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            // Broker backlog is an async broker call (per observed lane); do it on
            // the async worker before the blocking store work below.
            ingest.runtime().record_broker_metrics().await;
            let ingest = Arc::clone(&ingest);
            // Metrics read file metadata and retention does blocking fsync I/O;
            // keep both off the async workers.
            let result = tokio::task::spawn_blocking(move || -> std::io::Result<usize> {
                ingest
                    .runtime()
                    .record_store_metrics(ingest.store().as_ref());
                if policy.is_unbounded() {
                    return Ok(0);
                }
                let now = chrono::Utc::now();
                // Bound both the event store and the dead-trigger queue.
                let events = ingest.store().enforce_retention(&policy, now)?;
                let dead = ingest
                    .runtime()
                    .enforce_dead_trigger_retention(&policy, now)?;
                Ok(events + dead)
            })
            .await;
            match result {
                Ok(Ok(0)) => {}
                Ok(Ok(removed)) => tracing::info!(removed, "auto-retention removed records"),
                Ok(Err(error)) => tracing::warn!(%error, "auto-retention failed"),
                Err(error) => tracing::warn!(%error, "maintenance task panicked"),
            }
        }
    });
}

/// Wrap the served app in a request-handling timeout when one is configured.
/// `tower-http`'s layer returns `408 Request Timeout` on expiry (vs
/// `tower::timeout`, which errors and breaks the infallible serve).
fn with_request_timeout(app: axum::Router, timeout: Option<Duration>) -> axum::Router {
    match timeout {
        Some(timeout) => app.layer(tower_http::timeout::TimeoutLayer::with_status_code(
            axum::http::StatusCode::REQUEST_TIMEOUT,
            timeout,
        )),
        None => app,
    }
}

/// Everything needed to wire and run the HTTP server, bundled so `serve_ingest`
/// takes the listener, this config, and a shutdown future rather than a long
/// positional argument list.
struct ServeConfig {
    listener: TcpListener,
    ingest: Arc<EventIngest>,
    verifier: Option<Arc<WebhookVerifier>>,
    read_auth: Option<Arc<ReadAuth>>,
    readiness: Readiness,
    metrics: triggerlane_http::MetricsRender,
    request_timeout: Option<Duration>,
    /// When set, `POST /events` accepts asynchronously and hands events to the
    /// background dispatcher over this channel; when `None`, handling is inline.
    dispatch_tx: Option<tokio::sync::mpsc::Sender<EventEnvelope>>,
}

async fn serve_ingest(
    config: ServeConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), CliError> {
    let ServeConfig {
        listener,
        ingest,
        verifier,
        read_auth,
        readiness,
        metrics,
        request_timeout,
        dispatch_tx,
    } = config;
    let reads = triggerlane_http::read_router(Arc::clone(&ingest), read_auth);
    // Asynchronous accept-and-dispatch when a channel is configured, otherwise
    // synchronous inline handling. Both yield a plain events `Router`.
    let events = match dispatch_tx {
        Some(dispatch_tx) => {
            let handler = Arc::new(AcceptEventHandler::new(Arc::clone(&ingest), dispatch_tx));
            match verifier {
                Some(verifier) => triggerlane_http::router_with_verifier(handler, verifier),
                None => triggerlane_http::router(handler),
            }
        }
        None => {
            let handler = Arc::new(IngestEventHandler::new(Arc::clone(&ingest)));
            match verifier {
                Some(verifier) => triggerlane_http::router_with_verifier(handler, verifier),
                None => triggerlane_http::router(handler),
            }
        }
    };
    let app = events
        .merge(reads)
        .merge(triggerlane_http::health_router(readiness))
        .merge(triggerlane_http::metrics_router(metrics));
    // Bound request handling so a slow or stalled client cannot hold a
    // connection open indefinitely.
    let app = with_request_timeout(app, request_timeout);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

/// Resolve when the process is asked to stop. Composes SIGINT (Ctrl-C) with,
/// on Unix, SIGTERM — the signal Kubernetes, Docker, and systemd send to stop a
/// service before escalating to an uncatchable SIGKILL. Driving Axum's graceful
/// shutdown from this future lets in-flight `POST /events` requests drain
/// instead of being dropped on a supervisor stop. On non-Unix targets, where
/// SIGTERM does not exist, this falls back to SIGINT alone.
async fn shutdown_signal() {
    let interrupt = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut term) => {
                term.recv().await;
            }
            // If the SIGTERM handler cannot be installed, never resolve on this
            // arm so SIGINT still drives shutdown.
            Err(_) => std::future::pending::<()>().await,
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    wait_for_first(interrupt, terminate).await;
}

/// Resolve as soon as either shutdown source fires. Extracted so the
/// signal-composition logic is unit-testable without sending real OS signals.
async fn wait_for_first(a: impl Future<Output = ()>, b: impl Future<Output = ()>) {
    tokio::select! {
        _ = a => {},
        _ = b => {},
    }
}

/// Build the trigger registry the runtime evaluates. When `TRIGGERLANE_TRIGGERS`
/// names a config file, the registry is loaded from it (a read/parse failure is
/// a CLI error rather than a silent partial rule set); otherwise the built-in
/// `default_registry()` is used so existing behavior is unchanged.
fn load_registry() -> Result<TriggerRegistry, CliError> {
    match env::var(TRIGGERS_ENV) {
        Ok(path) => Ok(config::load_registry(path)?),
        Err(_) => Ok(default_registry()),
    }
}

fn default_registry() -> TriggerRegistry {
    let mut registry = TriggerRegistry::new();
    registry.register(RegisteredTrigger::new(
        "github-issue-projection",
        10,
        EventTypeTrigger::new(EVENT_GITHUB_ISSUE_CREATED),
        WorklaneJobBinding::new("projection", "CreateProjectionJob", 3),
    ));
    registry
}

fn inject(event_type: &str, payload: Vec<u8>) -> EventEnvelope {
    EventEnvelope::new(
        Source::Manual,
        EventType::new(event_type),
        Bytes::from(payload),
    )
}

fn load_events_file(path: impl AsRef<Path>) -> Result<Vec<EventEnvelope>, CliError> {
    let data = fs::read_to_string(path)?;
    let inputs: Vec<FileEvent> = serde_json::from_str(&data)?;
    Ok(inputs
        .into_iter()
        .map(|input| {
            EventEnvelope::new(
                Source::Manual,
                EventType::new(input.event_type),
                Bytes::from(input.payload),
            )
            .with_metadata(input.metadata)
        })
        .collect())
}

#[derive(Debug, Deserialize)]
struct FileEvent {
    event_type: String,
    #[serde(default)]
    payload: Vec<u8>,
    #[serde(default)]
    metadata: EventMetadata,
}

#[derive(Debug, Error)]
enum CliError {
    #[error("invalid bind address: {0}")]
    BindAddr(#[from] std::net::AddrParseError),
    #[error("events: {0}")]
    Events(String),
    #[error("worklane broker error: {0}")]
    Broker(String),
    #[error(
        "invalid {REQUEST_TIMEOUT_ENV}: {0:?} (expected a non-negative integer number of seconds)"
    )]
    RequestTimeout(String),
    #[error("prune: {0}")]
    Retention(String),
    #[error("submission: {0}")]
    Submission(String),
    #[error("metrics recorder: {0}")]
    Metrics(String),
    #[error(transparent)]
    Config(#[from] config::ConfigError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inject_creates_manual_event() {
        let event = inject("event.manual.test", b"{}".to_vec());

        assert_eq!(event.source, Source::Manual);
        assert_eq!(event.event_type.as_str(), "event.manual.test");
        assert_eq!(event.payload, Bytes::from_static(b"{}"));
    }

    #[test]
    fn parse_request_timeout_handles_default_value_zero_and_invalid() {
        // Unset → default.
        assert_eq!(
            parse_request_timeout(None).expect("default parses"),
            Some(Duration::from_secs(DEFAULT_REQUEST_TIMEOUT_SECS))
        );
        // Explicit positive value.
        assert_eq!(
            parse_request_timeout(Some("5")).expect("value parses"),
            Some(Duration::from_secs(5))
        );
        // Zero disables the timeout.
        assert_eq!(parse_request_timeout(Some("0")).expect("zero parses"), None);
        // Non-numeric is an error, not silently ignored.
        assert!(matches!(
            parse_request_timeout(Some("nope")),
            Err(CliError::RequestTimeout(_))
        ));
    }

    #[test]
    fn parse_backend_selects_or_rejects() {
        assert_eq!(parse_backend("sqlite").unwrap(), BrokerBackend::Sqlite);
        assert_eq!(parse_backend("postgres").unwrap(), BrokerBackend::Postgres);
        assert_eq!(parse_backend("redis").unwrap(), BrokerBackend::Redis);
        // The shipped default is the durable sqlite backend.
        assert_eq!(parse_backend("sqlite").unwrap(), BrokerBackend::Sqlite);
        assert!(matches!(parse_backend("mysql"), Err(CliError::Broker(_))));
    }

    #[test]
    fn resolve_broker_url_applies_precedence_and_requires_a_source() {
        // Flag wins over both environment sources.
        let (url, source) = resolve_broker_url(
            Some("flag"),
            Some("tl"),
            Some("db"),
            "DATABASE_URL",
            "postgres",
        )
        .expect("flag wins");
        assert_eq!(url, "flag");
        assert_eq!(source, "--url");

        // Then $TRIGGERLANE_WORKLANE_URL over the backend variable.
        let (url, source) =
            resolve_broker_url(None, Some("tl"), Some("db"), "DATABASE_URL", "postgres")
                .expect("triggerlane url wins");
        assert_eq!(url, "tl");
        assert_eq!(source, "$TRIGGERLANE_WORKLANE_URL");

        // Then the backend's conventional variable.
        let (url, source) = resolve_broker_url(None, None, Some("db"), "DATABASE_URL", "postgres")
            .expect("fallback used");
        assert_eq!(url, "db");
        assert_eq!(source, "$DATABASE_URL");

        // No source at all is an error naming what it looked for.
        assert!(matches!(
            resolve_broker_url(None, None, None, "REDIS_URL", "redis"),
            Err(CliError::Broker(_))
        ));
    }

    #[test]
    fn parse_auto_retention_reads_bounds_and_disables() {
        // Disabled bounds (None / "off" / "0") yield an unbounded policy.
        let unbounded = parse_auto_retention(Some("off"), None, Some("0"), None)
            .expect("disabled bounds parse");
        assert!(unbounded.is_unbounded());

        // A grace plus hard bounds populate the policy.
        let policy = parse_auto_retention(Some("24h"), Some("30d"), Some("1000"), Some("4096"))
            .expect("bounds parse");
        assert_eq!(policy.delivered_grace, Some(chrono::Duration::hours(24)));
        assert_eq!(policy.max_age, Some(chrono::Duration::days(30)));
        assert_eq!(policy.max_count, Some(1000));
        assert_eq!(policy.max_bytes, Some(4096));

        // An invalid value is a retention error, not silently ignored.
        assert!(matches!(
            parse_auto_retention(Some("nope"), None, None, None),
            Err(CliError::Retention(_))
        ));

        // The default hard cap is a valid bound, so the shipped policy is never
        // unbounded by accident.
        let defaulted = parse_auto_retention(
            Some(DEFAULT_RETENTION_GRACE),
            None,
            Some(DEFAULT_RETENTION_MAX_COUNT),
            None,
        )
        .expect("default bounds parse");
        assert_eq!(defaulted.max_count, Some(1_000_000));
        assert!(!defaulted.is_unbounded());
    }

    #[test]
    fn parse_duration_handles_suffixes_and_rejects_bad_input() {
        assert_eq!(parse_duration("30s"), Some(chrono::Duration::seconds(30)));
        assert_eq!(parse_duration("15m"), Some(chrono::Duration::minutes(15)));
        assert_eq!(parse_duration("12h"), Some(chrono::Duration::hours(12)));
        assert_eq!(parse_duration("7d"), Some(chrono::Duration::days(7)));
        assert_eq!(parse_duration(""), None);
        assert_eq!(parse_duration("30"), None); // no suffix
        assert_eq!(parse_duration("xd"), None); // non-numeric amount
        assert_eq!(parse_duration("5y"), None); // unknown unit
        assert_eq!(parse_duration("-5d"), None); // negative
    }

    #[test]
    fn parse_runtime_options_applies_defaults_floors_and_rejects() {
        // Unset → shipped defaults (retry a few times with a base backoff).
        let defaulted = parse_runtime_options(None, None).expect("defaults parse");
        assert_eq!(defaulted.max_submission_attempts, 3);
        assert_eq!(defaulted.submission_backoff, Duration::from_millis(100));

        // Explicit values are honored; attempts are floored at 1.
        let explicit = parse_runtime_options(Some("5"), Some("250")).expect("values parse");
        assert_eq!(explicit.max_submission_attempts, 5);
        assert_eq!(explicit.submission_backoff, Duration::from_millis(250));
        assert_eq!(
            parse_runtime_options(Some("0"), Some("0"))
                .expect("zero parses")
                .max_submission_attempts,
            1,
            "attempts floor at 1"
        );

        // Non-numeric is an error, not silently ignored.
        assert!(matches!(
            parse_runtime_options(Some("nope"), None),
            Err(CliError::Submission(_))
        ));
        assert!(matches!(
            parse_runtime_options(None, Some("soon")),
            Err(CliError::Submission(_))
        ));
    }

    #[test]
    fn resolve_prune_policy_maps_subcommands_and_rejects_bad_input() {
        assert!(matches!(
            resolve_prune_policy(PrunePolicy::Keep { count: 100 }),
            Ok(ManualRetention::KeepMostRecent(100))
        ));
        assert!(matches!(
            resolve_prune_policy(PrunePolicy::OlderThan {
                duration: "30d".to_owned()
            }),
            Ok(ManualRetention::OlderThan(_))
        ));
        assert!(matches!(
            resolve_prune_policy(PrunePolicy::OlderThan {
                duration: "nope".to_owned()
            }),
            Err(CliError::Retention(_))
        ));
    }

    #[test]
    fn cli_parses_global_broker_flag_and_subcommands() {
        // The clap grammar accepts the global broker flag and a subcommand.
        let cli = Cli::try_parse_from(["triggerlane", "--broker", "redis", "events", "list"])
            .expect("events list parses");
        assert_eq!(cli.broker, "redis");
        assert!(matches!(
            cli.command,
            Command::Events {
                command: EventsCommand::List
            }
        ));
        // An unknown subcommand is rejected by clap.
        assert!(Cli::try_parse_from(["triggerlane", "bogus"]).is_err());
    }

    #[test]
    fn cli_parses_worklane_dlq_subcommands() {
        let cli = Cli::try_parse_from(["triggerlane", "worklane-dlq", "count", "projection"])
            .expect("worklane-dlq count parses");
        assert!(matches!(
            cli.command,
            Command::WorklaneDlq {
                command: WorklaneDlqCommand::Count { lane }
            } if lane == "projection"
        ));
        // List takes an optional --limit.
        let cli = Cli::try_parse_from([
            "triggerlane",
            "worklane-dlq",
            "list",
            "projection",
            "--limit",
            "10",
        ])
        .expect("worklane-dlq list parses");
        assert!(matches!(
            cli.command,
            Command::WorklaneDlq {
                command: WorklaneDlqCommand::List { limit: 10, .. }
            }
        ));
    }

    #[tokio::test]
    async fn request_exceeding_timeout_returns_408() {
        use std::future::pending;

        use axum::{Router, http::StatusCode, routing::get};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        // A handler that never completes, behind a tiny timeout, must be cut off
        // with 408 rather than hang — proving `with_request_timeout` is wired.
        let app = with_request_timeout(
            Router::new().route(
                "/slow",
                get(|| async {
                    pending::<()>().await;
                    StatusCode::OK
                }),
            ),
            Some(Duration::from_millis(50)),
        );

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should report addr");
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let mut stream = TcpStream::connect(addr)
            .await
            .expect("client should connect");
        stream
            .write_all(b"GET /slow HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("request should send");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .await
            .expect("response should read");

        assert!(
            response.starts_with("HTTP/1.1 408"),
            "expected 408, got: {response}"
        );

        server.abort();
    }

    #[tokio::test]
    async fn shutdown_resolves_when_terminate_source_fires() {
        use std::future::{pending, ready};

        // The interrupt (SIGINT) arm never fires; the terminate (SIGTERM) arm is
        // ready. Proves a SIGTERM-side signal alone resolves the shutdown wait.
        wait_for_first(pending::<()>(), ready(())).await;
    }

    #[tokio::test]
    async fn shutdown_resolves_when_interrupt_source_fires() {
        use std::future::{pending, ready};

        wait_for_first(ready(()), pending::<()>()).await;
    }

    #[tokio::test]
    async fn serve_handles_posted_event_over_socket() {
        use std::future::pending;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;
        use triggerlane_storage::InMemoryEventStore;
        use worklane_memory::InMemoryBroker;

        let store = Arc::new(InMemoryEventStore::new());
        let broker = Arc::new(InMemoryBroker::new());
        let runtime = TriggerRuntime::new(default_registry(), broker);
        let ingest = Arc::new(EventIngest::new(
            Arc::clone(&store) as Arc<dyn EventStore>,
            runtime,
        ));

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should report addr");
        let metrics: triggerlane_http::MetricsRender = Arc::new(String::new);
        let server = tokio::spawn(serve_ingest(
            ServeConfig {
                listener,
                ingest,
                verifier: None,
                read_auth: None,
                readiness: Readiness::ready(),
                metrics,
                request_timeout: Some(Duration::from_secs(30)),
                dispatch_tx: None,
            },
            pending(),
        ));

        let body = r#"{"event_type":"event.manual.test","payload":[123,125]}"#;
        let request = format!(
            "POST /events HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );

        let mut stream = TcpStream::connect(addr)
            .await
            .expect("client should connect");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("request should send");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .await
            .expect("response should read");

        assert!(
            response.starts_with("HTTP/1.1 200"),
            "unexpected response: {response}"
        );
        assert_eq!(store.all().len(), 1);

        server.abort();
    }
}
