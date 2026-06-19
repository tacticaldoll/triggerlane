//! Event source normalization and HTTP routing for event inputs.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{HeaderMap, HeaderName, StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use thiserror::Error;
use tower_http::trace::{DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::Level;
use triggerlane_core::{
    EVENT_GITHUB_ISSUE_CREATED, EVENT_GITHUB_PR_CREATED, EventEnvelope, EventId, EventMetadata,
    EventType, Source,
};
use triggerlane_runtime::{DeadTriggerRecord, EventIngest, ReplayError, ReplayFilter};

pub const EVENTS_PATH: &str = "/events";

/// Read endpoint listing dead-trigger records.
pub const DEAD_TRIGGERS_PATH: &str = "/dead-triggers";

/// Liveness probe path: succeeds while the server is serving.
pub const HEALTHZ_PATH: &str = "/healthz";

/// Readiness probe path: succeeds while accepting traffic, fails while draining.
pub const READYZ_PATH: &str = "/readyz";

const GITHUB_SIGNATURE_HEADER: &str = "x-hub-signature-256";

/// Maximum accepted `POST /events` body size. Set explicitly rather than relying
/// on the framework default so a dependency bump cannot silently change it.
const MAX_EVENT_BODY_BYTES: usize = 2 * 1024 * 1024;

/// Default page size for `GET /events` when the caller gives no `limit`, so an
/// unbounded retained window cannot turn one list call into an arbitrarily large
/// response (and allocation). A caller may request more deliberately.
const DEFAULT_EVENT_LIST_LIMIT: usize = 1000;

/// Shared readiness state for the `/readyz` probe. Cloneable: the readiness
/// router holds one handle as state, and the server holds another to flip it to
/// draining when graceful shutdown begins. Backed by an atomic so the probe and
/// the shutdown path share one flag without locking.
#[derive(Clone)]
pub struct Readiness(Arc<AtomicBool>);

impl Readiness {
    /// A handle that starts ready to accept traffic.
    pub fn ready() -> Self {
        Self(Arc::new(AtomicBool::new(true)))
    }

    /// Mark whether the service should receive traffic. Set `false` to drain.
    pub fn set_ready(&self, ready: bool) {
        self.0.store(ready, Ordering::SeqCst);
    }

    /// Whether the service is currently accepting traffic.
    pub fn is_ready(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

impl Default for Readiness {
    fn default() -> Self {
        Self::ready()
    }
}

/// Build the liveness/readiness probe router. Mounts `GET /healthz` (always
/// succeeds while serving) and `GET /readyz` (succeeds while `readiness` is
/// ready, fails while draining). Kept separate from the events router so it can
/// be merged in without touching the `POST /events` constructors.
pub fn health_router(readiness: Readiness) -> Router {
    Router::new()
        .route(HEALTHZ_PATH, get(healthz_route))
        .route(READYZ_PATH, get(readyz_route))
        .with_state(readiness)
}

async fn healthz_route() -> StatusCode {
    StatusCode::OK
}

async fn readyz_route(State(readiness): State<Readiness>) -> StatusCode {
    if readiness.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Metrics scrape path.
pub const METRICS_PATH: &str = "/metrics";

/// Renders the current metrics in Prometheus text format. Injected by the binary
/// so this crate does not depend on the metrics exporter; the closure typically
/// wraps a Prometheus recorder handle.
pub type MetricsRender = Arc<dyn Fn() -> String + Send + Sync>;

/// Build the metrics router mounting `GET /metrics`. Kept separate from the
/// events router (and merged without a trace layer) so scrape traffic is not
/// request-logged.
pub fn metrics_router(render: MetricsRender) -> Router {
    Router::new()
        .route(METRICS_PATH, get(metrics_route))
        .with_state(render)
}

async fn metrics_route(State(render): State<MetricsRender>) -> impl IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        render(),
    )
}

type HmacSha256 = Hmac<Sha256>;

/// Verifies an HMAC-SHA256 signature over the raw request body against a shared
/// secret, read from a configured header. The header value may be bare hex or
/// `sha256=`-prefixed. Used to authenticate webhook sources before ingest.
pub struct WebhookVerifier {
    secret: Vec<u8>,
    header: HeaderName,
}

impl WebhookVerifier {
    pub fn new(secret: impl Into<Vec<u8>>, header: HeaderName) -> Self {
        Self {
            secret: secret.into(),
            header,
        }
    }

    /// A GitHub-style verifier that reads the `X-Hub-Signature-256` header.
    pub fn github(secret: impl Into<Vec<u8>>) -> Self {
        Self::new(secret, HeaderName::from_static(GITHUB_SIGNATURE_HEADER))
    }

    /// Whether `body` carries a valid signature in the configured header. Returns
    /// false on a missing header, non-hex value, or mismatch. The comparison is
    /// constant-time (`Mac::verify_slice`).
    pub fn verify(&self, headers: &HeaderMap, body: &[u8]) -> bool {
        let Some(value) = headers
            .get(&self.header)
            .and_then(|value| value.to_str().ok())
        else {
            return false;
        };
        let hex_signature = value.strip_prefix("sha256=").unwrap_or(value);
        let Ok(expected) = hex::decode(hex_signature) else {
            return false;
        };
        let Ok(mut mac) = HmacSha256::new_from_slice(&self.secret) else {
            return false;
        };
        mac.update(body);
        mac.verify_slice(&expected).is_ok()
    }
}

struct AppState<H> {
    handler: Arc<H>,
    verifier: Option<Arc<WebhookVerifier>>,
}

impl<H> Clone for AppState<H> {
    fn clone(&self) -> Self {
        Self {
            handler: Arc::clone(&self.handler),
            verifier: self.verifier.clone(),
        }
    }
}

/// Build the events router with no signature verification (suitable for trusted
/// networks).
pub fn router<H>(handler: Arc<H>) -> Router
where
    H: EventHandler,
{
    build_router(AppState {
        handler,
        verifier: None,
    })
}

/// Build the events router that rejects requests failing `verifier`.
pub fn router_with_verifier<H>(handler: Arc<H>, verifier: Arc<WebhookVerifier>) -> Router
where
    H: EventHandler,
{
    build_router(AppState {
        handler,
        verifier: Some(verifier),
    })
}

fn build_router<H>(state: AppState<H>) -> Router
where
    H: EventHandler,
{
    Router::new()
        .route(EVENTS_PATH, post(post_events_route::<H>))
        // Cap the request body explicitly so a large or unbounded upload cannot
        // exhaust memory before JSON parsing.
        .layer(DefaultBodyLimit::max(MAX_EVENT_BODY_BYTES))
        // Log each event request (method, path, status, latency) at INFO so it
        // surfaces under the default filter. Mounted on the events router only —
        // the health/readiness router is intentionally left unlogged so probe
        // traffic does not flood the log.
        .layer(
            TraceLayer::new_for_http()
                .on_request(DefaultOnRequest::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .with_state(state)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncomingEvent {
    pub event_type: String,
    #[serde(default)]
    pub payload: Vec<u8>,
    #[serde(default)]
    pub metadata: EventMetadata,
}

pub fn post_events(input: IncomingEvent) -> EventEnvelope {
    EventEnvelope::new(
        Source::Http,
        EventType::new(input.event_type),
        Bytes::from(input.payload),
    )
    .with_metadata(input.metadata)
}

async fn post_events_route<H>(
    State(state): State<AppState<H>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<EventResponse>), HttpRouteError>
where
    H: EventHandler,
{
    if let Some(verifier) = &state.verifier
        && !verifier.verify(&headers, &body)
    {
        return Err(HttpRouteError::Unauthorized);
    }

    let input: IncomingEvent = serde_json::from_slice(&body)
        .map_err(|error| HttpRouteError::BadRequest(error.to_string()))?;
    let event = post_events(input);
    let event_id = event.id.to_string();
    let report = state
        .handler
        .handle_event(event)
        .await
        .map_err(HttpRouteError::Handler)?;

    // 202 Accepted when the event was queued for asynchronous dispatch; 200 OK when
    // it was handled inline and submission results are known.
    let status = if report.accepted {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    Ok((
        status,
        Json(EventResponse {
            event_id,
            submitted: report.submitted,
            failure_count: report.failure_count,
            accepted: report.accepted,
        }),
    ))
}

/// Bearer-token guard for the operator read/replay endpoints. When configured,
/// every read-router request must present `Authorization: Bearer <token>`. The
/// read endpoints expose stored payloads and the replay routes submit jobs, so
/// they must not rely on the inbound webhook secret (which signs request bodies
/// and does not apply to reads).
pub struct ReadAuth {
    token: Vec<u8>,
}

impl ReadAuth {
    pub fn new(token: impl Into<Vec<u8>>) -> Self {
        Self {
            token: token.into(),
        }
    }

    /// Whether the request carries the expected bearer token, compared in
    /// constant time so a wrong token cannot be guessed byte-by-byte via timing.
    fn authorized(&self, headers: &HeaderMap) -> bool {
        let Some(presented) = headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
        else {
            return false;
        };
        constant_time_eq(presented.as_bytes(), &self.token)
    }
}

/// Constant-time byte equality (the lengths may differ, which is not secret).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Reject read-router requests that fail bearer-token authentication.
async fn require_read_auth(
    State(auth): State<Arc<ReadAuth>>,
    request: Request,
    next: Next,
) -> Response {
    if auth.authorized(request.headers()) {
        next.run(request).await
    } else {
        HttpRouteError::Unauthorized.into_response()
    }
}

/// Build the operator/consumer read router: list and get retained events, replay
/// by id and by time range, and list dead-triggers. Backed by the runtime ingest
/// pipeline; introduces no result store (job results live in Worklane). When
/// `auth` is set, every endpoint requires a bearer token; when `None`, the router
/// is open (trusted-network deployments), mirroring the webhook-secret default.
pub fn read_router(ingest: Arc<EventIngest>, auth: Option<Arc<ReadAuth>>) -> Router {
    let mut router = Router::new()
        .route(EVENTS_PATH, get(list_events_route))
        .route(&format!("{EVENTS_PATH}/replay"), post(replay_range_route))
        .route(&format!("{EVENTS_PATH}/{{id}}"), get(get_event_route))
        .route(
            &format!("{EVENTS_PATH}/{{id}}/replay"),
            post(replay_by_id_route),
        )
        .route(DEAD_TRIGGERS_PATH, get(list_dead_triggers_route));
    if let Some(auth) = auth {
        router = router.layer(middleware::from_fn_with_state(auth, require_read_auth));
    }
    router
        .layer(
            TraceLayer::new_for_http()
                .on_request(DefaultOnRequest::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .with_state(ingest)
}

/// Time window for a range replay, as RFC3339 `start`/`end` query parameters, with
/// optional narrowing by `event_type` / `source` and a `dry_run` that previews the
/// matching events without submitting any jobs.
#[derive(Debug, Deserialize)]
pub struct ReplayRangeQuery {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    #[serde(default)]
    pub event_type: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
}

/// Summary of a range replay: how many retained events in the window matched. When
/// `dry_run` is set the events were previewed only — `matched` counts them but no
/// jobs were submitted.
#[derive(Debug, Serialize)]
pub struct ReplayRangeResponse {
    pub matched: usize,
    pub dry_run: bool,
}

/// Optional paging for `GET /events`, in append order: skip `offset` records then
/// return at most `limit` (default [`DEFAULT_EVENT_LIST_LIMIT`]).
#[derive(Debug, Default, Deserialize)]
pub struct ListEventsQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

async fn list_events_route(
    State(ingest): State<Arc<EventIngest>>,
    Query(page): Query<ListEventsQuery>,
) -> Json<Vec<EventEnvelope>> {
    let offset = page.offset.unwrap_or(0);
    let limit = page.limit.unwrap_or(DEFAULT_EVENT_LIST_LIMIT);
    let events = ingest
        .store()
        .all()
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect();
    Json(events)
}

async fn get_event_route(
    State(ingest): State<Arc<EventIngest>>,
    Path(id): Path<String>,
) -> Result<Json<EventEnvelope>, HttpRouteError> {
    let id = parse_event_id(&id)?;
    ingest
        .store()
        .get(id)
        .map(Json)
        .ok_or_else(|| HttpRouteError::NotFound(format!("event {id} not found")))
}

async fn replay_by_id_route(
    State(ingest): State<Arc<EventIngest>>,
    Path(id): Path<String>,
) -> Result<Json<EventResponse>, HttpRouteError> {
    let id = parse_event_id(&id)?;
    let report = ingest
        .runtime()
        .replay_by_id(ingest.store().as_ref(), id)
        .await
        .map_err(replay_to_http)?;
    Ok(Json(EventResponse {
        event_id: id.to_string(),
        submitted: report.submitted,
        failure_count: report.failed.len(),
        // Replay handles inline, so the result is known (never an async accept).
        accepted: false,
    }))
}

async fn replay_range_route(
    State(ingest): State<Arc<EventIngest>>,
    Query(range): Query<ReplayRangeQuery>,
) -> Result<Json<ReplayRangeResponse>, HttpRouteError> {
    let filter = replay_filter(range.event_type, range.source.as_deref())?;
    if range.dry_run {
        let matched = ingest.runtime().preview_range(
            ingest.store().as_ref(),
            range.start,
            range.end,
            &filter,
        );
        return Ok(Json(ReplayRangeResponse {
            matched: matched.len(),
            dry_run: true,
        }));
    }
    let report = ingest
        .runtime()
        .replay_range(ingest.store().as_ref(), range.start, range.end, &filter)
        .await
        .map_err(replay_to_http)?;
    Ok(Json(ReplayRangeResponse {
        matched: report.events.len(),
        dry_run: false,
    }))
}

/// Build a [`ReplayFilter`] from query strings, parsing the optional source name
/// (e.g. `GitHub`) and rejecting an unknown one as a bad request.
fn replay_filter(
    event_type: Option<String>,
    source: Option<&str>,
) -> Result<ReplayFilter, HttpRouteError> {
    let source = match source {
        Some(raw) => Some(parse_source(raw)?),
        None => None,
    };
    Ok(ReplayFilter { event_type, source })
}

/// Parse a [`Source`] from its name using its serde representation (`"GitHub"`,
/// `"Http"`, …), so the accepted spellings track the type itself.
fn parse_source(raw: &str) -> Result<Source, HttpRouteError> {
    serde_json::from_value(serde_json::Value::String(raw.to_owned()))
        .map_err(|_| HttpRouteError::BadRequest(format!("unknown source {raw:?}")))
}

async fn list_dead_triggers_route(
    State(ingest): State<Arc<EventIngest>>,
) -> Json<Vec<DeadTriggerRecord>> {
    Json(ingest.runtime().dead_triggers())
}

fn parse_event_id(raw: &str) -> Result<EventId, HttpRouteError> {
    raw.parse()
        .map_err(|error| HttpRouteError::BadRequest(format!("invalid event id: {error}")))
}

/// Map a replay error to its HTTP status: a missing/pruned event is 404, any
/// other failure is a server error.
fn replay_to_http(error: ReplayError) -> HttpRouteError {
    match error {
        ReplayError::EventNotFound(message) => HttpRouteError::NotFound(message),
        other => HttpRouteError::Handler(HandlerError::new(other.to_string())),
    }
}

#[async_trait]
pub trait EventHandler: Send + Sync + 'static {
    async fn handle_event(&self, event: EventEnvelope)
    -> Result<EventHandlingReport, HandlerError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventHandlingReport {
    pub submitted: Vec<String>,
    pub failure_count: usize,
    /// True when the event was durably accepted for asynchronous dispatch rather
    /// than handled inline; `submitted`/`failure_count` are then not yet known.
    pub accepted: bool,
}

impl EventHandlingReport {
    /// A report for an event handled inline (the synchronous path).
    pub fn new(submitted: Vec<String>, failure_count: usize) -> Self {
        Self {
            submitted,
            failure_count,
            accepted: false,
        }
    }

    /// A report for an event durably accepted for asynchronous dispatch.
    pub fn accepted() -> Self {
        Self {
            submitted: Vec::new(),
            failure_count: 0,
            accepted: true,
        }
    }
}

/// `EventHandler` backed by the runtime ingest pipeline: persists the accepted
/// event and handles it inline through the trigger runtime (synchronous path).
pub struct IngestEventHandler {
    ingest: Arc<EventIngest>,
}

impl IngestEventHandler {
    pub fn new(ingest: Arc<EventIngest>) -> Self {
        Self { ingest }
    }
}

#[async_trait]
impl EventHandler for IngestEventHandler {
    async fn handle_event(
        &self,
        event: EventEnvelope,
    ) -> Result<EventHandlingReport, HandlerError> {
        let report = self
            .ingest
            .ingest(event)
            .await
            .map_err(|error| HandlerError::new(error.to_string()))?;

        Ok(EventHandlingReport::new(
            report.handle.submitted,
            report.handle.failed.len(),
        ))
    }
}

/// `EventHandler` for the asynchronous ingestion path: durably accept the event
/// (append + dedup) and hand it to a background dispatcher over `dispatch`, instead
/// of handling it inline. The response reports acceptance, not submission results.
/// Backpressure is the channel's: if the dispatcher falls behind, `send` waits.
pub struct AcceptEventHandler {
    ingest: Arc<EventIngest>,
    dispatch: tokio::sync::mpsc::Sender<EventEnvelope>,
}

impl AcceptEventHandler {
    pub fn new(
        ingest: Arc<EventIngest>,
        dispatch: tokio::sync::mpsc::Sender<EventEnvelope>,
    ) -> Self {
        Self { ingest, dispatch }
    }
}

#[async_trait]
impl EventHandler for AcceptEventHandler {
    async fn handle_event(
        &self,
        event: EventEnvelope,
    ) -> Result<EventHandlingReport, HandlerError> {
        let report = self
            .ingest
            .accept(event.clone())
            .await
            .map_err(|error| HandlerError::new(error.to_string()))?;
        // A duplicate is already stored (and was dispatched on its first accept), so
        // it does not need re-dispatching; only hand a fresh event to the dispatcher.
        if !report.deduplicated && self.dispatch.send(event).await.is_err() {
            return Err(HandlerError::new("dispatch channel closed"));
        }
        Ok(EventHandlingReport::accepted())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EventResponse {
    pub event_id: String,
    pub submitted: Vec<String>,
    pub failure_count: usize,
    /// True when the event was accepted for asynchronous dispatch (HTTP 202) rather
    /// than handled inline (HTTP 200).
    pub accepted: bool,
}

#[derive(Debug, Error)]
#[error("event handler failed: {0}")]
pub struct HandlerError(String);

impl HandlerError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

#[derive(Debug, Error)]
pub enum HttpRouteError {
    #[error("unauthorized: webhook signature verification failed")]
    Unauthorized,
    #[error("invalid event json: {0}")]
    BadRequest(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error(transparent)]
    Handler(HandlerError),
}

impl IntoResponse for HttpRouteError {
    fn into_response(self) -> Response {
        let status = match self {
            HttpRouteError::Unauthorized => StatusCode::UNAUTHORIZED,
            HttpRouteError::BadRequest(_) => StatusCode::BAD_REQUEST,
            HttpRouteError::NotFound(_) => StatusCode::NOT_FOUND,
            HttpRouteError::Handler(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        // The trace layer logs the status; log the cause too. A handler failure
        // is a server-side error; auth/bad-request/not-found are client errors.
        match &self {
            HttpRouteError::Handler(_) => {
                tracing::error!(%status, error = %self, "event request failed")
            }
            // The bad-request detail is a JSON parse error that can quote a
            // fragment of the (possibly sensitive) request body, so it is not
            // logged — the caller still receives it in the response.
            HttpRouteError::BadRequest(_) => {
                tracing::warn!(%status, "event request rejected: malformed json")
            }
            HttpRouteError::Unauthorized | HttpRouteError::NotFound(_) => {
                tracing::warn!(%status, error = %self, "event request rejected")
            }
        }

        let body = Json(serde_json::json!({
            "error": self.to_string(),
        }));

        (status, body).into_response()
    }
}

pub fn github_webhook(
    event_name: &str,
    action: Option<&str>,
    payload: Bytes,
) -> Result<EventEnvelope, SourceError> {
    let event_type = match (event_name, action) {
        ("issues", Some("opened" | "created")) => EVENT_GITHUB_ISSUE_CREATED,
        ("pull_request", Some("opened" | "created")) => EVENT_GITHUB_PR_CREATED,
        _ => return Err(SourceError::UnsupportedGitHubEvent(event_name.to_owned())),
    };

    Ok(EventEnvelope::new(Source::GitHub, event_type, payload))
}

pub fn discord_message_created(payload: Bytes) -> EventEnvelope {
    EventEnvelope::new(
        Source::Discord,
        triggerlane_core::EVENT_DISCORD_MESSAGE_CREATED,
        payload,
    )
}

pub fn slack_event(payload: Bytes, event_type: impl Into<EventType>) -> EventEnvelope {
    EventEnvelope::new(Source::Slack, event_type, payload)
}

#[derive(Debug, Error)]
pub enum SourceError {
    #[error("unsupported github event: {0}")]
    UnsupportedGitHubEvent(String),
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn healthz_is_ok_while_serving() {
        let app = health_router(Readiness::ready());
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(HEALTHZ_PATH)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("route should respond");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_reflects_readiness_state() {
        let readiness = Readiness::ready();
        let ready = health_router(readiness.clone())
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(READYZ_PATH)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("route should respond");
        assert_eq!(ready.status(), StatusCode::OK);

        readiness.set_ready(false);
        let draining = health_router(readiness)
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(READYZ_PATH)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("route should respond");
        assert_eq!(draining.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn metrics_endpoint_serves_rendered_text() {
        let render: MetricsRender = Arc::new(|| "triggerlane_events_handled_total 7\n".to_owned());
        let app = metrics_router(render);
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(METRICS_PATH)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("route should respond");

        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("text/plain")),
            "expected prometheus content type"
        );
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        assert_eq!(&body[..], b"triggerlane_events_handled_total 7\n");
    }

    #[test]
    fn post_events_normalizes_http_source() {
        let event = post_events(IncomingEvent {
            event_type: "event.manual.test".to_owned(),
            payload: b"{}".to_vec(),
            metadata: EventMetadata {
                trace_id: Some("trace-1".to_owned()),
                correlation_id: None,
                tenant_id: None,
                idempotency_key: Some("idem-1".to_owned()),
                causation_id: None,
            },
        });

        assert_eq!(event.source, Source::Http);
        assert_eq!(event.event_type.as_str(), "event.manual.test");
        assert_eq!(event.payload, Bytes::from_static(b"{}"));
        assert_eq!(event.metadata.trace_id.as_deref(), Some("trace-1"));
        assert_eq!(event.metadata.idempotency_key.as_deref(), Some("idem-1"));
    }

    #[test]
    fn github_webhook_normalizes_issue_opened() {
        let event = github_webhook("issues", Some("opened"), Bytes::from_static(b"{}"))
            .expect("webhook should normalize");

        assert_eq!(event.source, Source::GitHub);
        assert_eq!(event.event_type.as_str(), EVENT_GITHUB_ISSUE_CREATED);
    }

    #[tokio::test]
    async fn post_events_route_dispatches_to_handler() {
        let handler = Arc::new(RecordingHandler::new(EventHandlingReport::new(
            vec!["job-1".to_owned()],
            0,
        )));
        let app = router(Arc::clone(&handler));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(EVENTS_PATH)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"event_type":"event.manual.test","payload":[123,125]}"#,
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("route should respond");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(handler.events().len(), 1);
        assert_eq!(handler.events()[0].source, Source::Http);
        assert_eq!(handler.events()[0].event_type.as_str(), "event.manual.test");

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let value: serde_json::Value =
            serde_json::from_slice(&body).expect("response should be json");

        assert!(value["event_id"].as_str().is_some());
        assert_eq!(value["submitted"][0], "job-1");
        assert_eq!(value["failure_count"], 0);
    }

    #[tokio::test]
    async fn post_events_through_ingest_persists_event() {
        use triggerlane_runtime::{TriggerRegistry, TriggerRuntime};
        use triggerlane_storage::{EventStore, InMemoryEventStore};
        use worklane_memory::InMemoryBroker;

        let broker = Arc::new(InMemoryBroker::new());
        let store = Arc::new(InMemoryEventStore::new());
        let runtime = TriggerRuntime::new(TriggerRegistry::new(), broker);
        let ingest = Arc::new(EventIngest::new(
            Arc::clone(&store) as Arc<dyn EventStore>,
            runtime,
        ));
        let handler = Arc::new(IngestEventHandler::new(ingest));
        let app = router(handler);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(EVENTS_PATH)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"event_type":"event.manual.test","payload":[123,125]}"#,
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("route should respond");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let value: serde_json::Value =
            serde_json::from_slice(&body).expect("response should be json");
        let event_id = value["event_id"].as_str().expect("event id should be set");

        let stored = store.all();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].id.to_string(), event_id);
        assert_eq!(stored[0].source, Source::Http);
    }

    #[tokio::test]
    async fn async_accept_returns_202_durably_stores_and_queues_for_dispatch() {
        use triggerlane_runtime::{TriggerRegistry, TriggerRuntime};
        use triggerlane_storage::{EventStore, InMemoryEventStore};
        use worklane_memory::InMemoryBroker;

        let broker = Arc::new(InMemoryBroker::new());
        let store = Arc::new(InMemoryEventStore::new());
        let runtime = TriggerRuntime::new(TriggerRegistry::new(), broker);
        let ingest = Arc::new(EventIngest::new(
            Arc::clone(&store) as Arc<dyn EventStore>,
            runtime,
        ));
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let app = router(Arc::new(AcceptEventHandler::new(ingest, tx)));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(EVENTS_PATH)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"event_type":"event.manual.test","payload":[123,125]}"#,
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("route should respond");

        // Async ingestion answers 202 Accepted with `accepted: true`.
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let value: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(value["accepted"], serde_json::Value::Bool(true));

        // The event is durably stored and handed to the dispatcher channel.
        assert_eq!(store.all().len(), 1);
        let queued = rx.recv().await.expect("event queued for dispatch");
        assert_eq!(queued.source, Source::Http);
    }

    #[tokio::test]
    async fn invalid_json_is_rejected_before_handler() {
        let handler = Arc::new(RecordingHandler::new(EventHandlingReport::new(
            Vec::new(),
            0,
        )));
        let app = router(Arc::clone(&handler));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(EVENTS_PATH)
                    .header("content-type", "application/json")
                    .body(Body::from("{not-json"))
                    .expect("request should build"),
            )
            .await
            .expect("route should respond");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(handler.events().is_empty());
    }

    #[tokio::test]
    async fn read_router_gets_event_and_404s_unknown() {
        use triggerlane_runtime::{TriggerRegistry, TriggerRuntime};
        use triggerlane_storage::{EventStore, InMemoryEventStore};
        use worklane_memory::InMemoryBroker;

        let broker = Arc::new(InMemoryBroker::new());
        let store = Arc::new(InMemoryEventStore::new());
        let event = EventEnvelope::new(
            Source::Http,
            EventType::new("event.manual.test"),
            Bytes::from_static(b"{}"),
        );
        let id = event.id;
        store.append(event).expect("append");
        let runtime = TriggerRuntime::new(TriggerRegistry::new(), broker);
        let ingest = Arc::new(EventIngest::new(
            Arc::clone(&store) as Arc<dyn EventStore>,
            runtime,
        ));
        let app = read_router(ingest, None);

        // A retained event is returned by id.
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("{EVENTS_PATH}/{id}"))
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("route should respond");
        assert_eq!(response.status(), StatusCode::OK);

        // An unknown id is a 404, not a 500.
        let missing = uuid_like_unknown();
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("{EVENTS_PATH}/{missing}"))
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("route should respond");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn read_router_lists_events_and_dead_triggers() {
        use triggerlane_runtime::{TriggerRegistry, TriggerRuntime};
        use triggerlane_storage::{EventStore, InMemoryEventStore};
        use worklane_memory::InMemoryBroker;

        let broker = Arc::new(InMemoryBroker::new());
        let store = Arc::new(InMemoryEventStore::new());
        store
            .append(EventEnvelope::new(
                Source::Http,
                EventType::new("event.manual.test"),
                Bytes::from_static(b"{}"),
            ))
            .expect("append");
        let runtime = TriggerRuntime::new(TriggerRegistry::new(), broker);
        let ingest = Arc::new(EventIngest::new(
            Arc::clone(&store) as Arc<dyn EventStore>,
            runtime,
        ));
        let app = read_router(ingest, None);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(EVENTS_PATH)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("route should respond");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let value: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(value.as_array().expect("array").len(), 1);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(DEAD_TRIGGERS_PATH)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("route should respond");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn read_router_enforces_bearer_token_when_configured() {
        use triggerlane_runtime::{TriggerRegistry, TriggerRuntime};
        use triggerlane_storage::{EventStore, InMemoryEventStore};
        use worklane_memory::InMemoryBroker;

        let broker = Arc::new(InMemoryBroker::new());
        let store = Arc::new(InMemoryEventStore::new());
        let runtime = TriggerRuntime::new(TriggerRegistry::new(), broker);
        let ingest = Arc::new(EventIngest::new(
            Arc::clone(&store) as Arc<dyn EventStore>,
            runtime,
        ));
        let app = read_router(ingest, Some(Arc::new(ReadAuth::new("s3cret"))));

        let request = |auth: Option<&str>| {
            let mut builder = Request::builder().uri(EVENTS_PATH);
            if let Some(auth) = auth {
                builder = builder.header("authorization", auth);
            }
            builder.body(Body::empty()).expect("request should build")
        };

        // No token → 401.
        let response = app
            .clone()
            .oneshot(request(None))
            .await
            .expect("route should respond");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        // Wrong token → 401.
        let response = app
            .clone()
            .oneshot(request(Some("Bearer nope")))
            .await
            .expect("route should respond");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        // Correct token → 200.
        let response = app
            .oneshot(request(Some("Bearer s3cret")))
            .await
            .expect("route should respond");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn read_router_list_events_honors_limit_and_offset() {
        use triggerlane_runtime::{TriggerRegistry, TriggerRuntime};
        use triggerlane_storage::{EventStore, InMemoryEventStore};
        use worklane_memory::InMemoryBroker;

        let broker = Arc::new(InMemoryBroker::new());
        let store = Arc::new(InMemoryEventStore::new());
        for _ in 0..3 {
            store
                .append(EventEnvelope::new(
                    Source::Http,
                    EventType::new("event.manual.test"),
                    Bytes::from_static(b"{}"),
                ))
                .expect("append");
        }
        let runtime = TriggerRuntime::new(TriggerRegistry::new(), broker);
        let ingest = Arc::new(EventIngest::new(
            Arc::clone(&store) as Arc<dyn EventStore>,
            runtime,
        ));
        let app = read_router(ingest, None);

        let count = |query: &str| {
            let app = app.clone();
            let uri = format!("{EVENTS_PATH}{query}");
            async move {
                let response = app
                    .oneshot(
                        Request::builder()
                            .uri(uri)
                            .body(Body::empty())
                            .expect("request should build"),
                    )
                    .await
                    .expect("route should respond");
                assert_eq!(response.status(), StatusCode::OK);
                let body = to_bytes(response.into_body(), usize::MAX)
                    .await
                    .expect("body should read");
                let value: serde_json::Value = serde_json::from_slice(&body).expect("json");
                value.as_array().expect("array").len()
            }
        };

        // limit caps the page; offset skips from the front; together they page.
        assert_eq!(count("?limit=2").await, 2);
        assert_eq!(count("?offset=2").await, 1);
        assert_eq!(count("?offset=1&limit=1").await, 1);
        assert_eq!(count("?offset=5").await, 0);
    }

    /// A well-formed UUID that is not in the store.
    fn uuid_like_unknown() -> &'static str {
        "00000000-0000-4000-8000-000000000000"
    }

    fn sign(secret: &[u8], body: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(secret).expect("hmac key");
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    #[test]
    fn webhook_verifier_accepts_valid_and_rejects_tampered_or_missing() {
        let secret = b"topsecret";
        let body = br#"{"event_type":"x"}"#;
        let verifier = WebhookVerifier::github(secret.to_vec());

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-hub-signature-256"),
            sign(secret, body).parse().expect("header value"),
        );

        assert!(verifier.verify(&headers, body));
        assert!(!verifier.verify(&headers, b"tampered body"));
        assert!(!verifier.verify(&HeaderMap::new(), body));
    }

    #[tokio::test]
    async fn signed_request_accepted_unsigned_rejected() {
        let secret = b"shh";
        let body = r#"{"event_type":"event.manual.test","payload":[]}"#;
        let handler = Arc::new(RecordingHandler::new(EventHandlingReport::new(
            vec!["job-1".to_owned()],
            0,
        )));
        let verifier = Arc::new(WebhookVerifier::github(secret.to_vec()));

        let signed = router_with_verifier(Arc::clone(&handler), Arc::clone(&verifier))
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(EVENTS_PATH)
                    .header("content-type", "application/json")
                    .header("x-hub-signature-256", sign(secret, body.as_bytes()))
                    .body(Body::from(body))
                    .expect("request should build"),
            )
            .await
            .expect("route should respond");
        assert_eq!(signed.status(), StatusCode::OK);

        let unsigned = router_with_verifier(Arc::clone(&handler), verifier)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(EVENTS_PATH)
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .expect("request should build"),
            )
            .await
            .expect("route should respond");
        assert_eq!(unsigned.status(), StatusCode::UNAUTHORIZED);

        // Only the signed request reached the handler.
        assert_eq!(handler.events().len(), 1);
    }

    struct RecordingHandler {
        report: EventHandlingReport,
        events: Mutex<Vec<EventEnvelope>>,
    }

    impl RecordingHandler {
        fn new(report: EventHandlingReport) -> Self {
            Self {
                report,
                events: Mutex::new(Vec::new()),
            }
        }

        fn events(&self) -> Vec<EventEnvelope> {
            self.events
                .lock()
                .expect("recording handler mutex poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl EventHandler for RecordingHandler {
        async fn handle_event(
            &self,
            event: EventEnvelope,
        ) -> Result<EventHandlingReport, HandlerError> {
            self.events
                .lock()
                .expect("recording handler mutex poisoned")
                .push(event);
            Ok(self.report.clone())
        }
    }
}
