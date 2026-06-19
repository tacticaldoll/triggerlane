//! Reference Worklane consumer: a worker that runs opaque `worklane-core`
//! reservations through an external invocation policy. Kept as an example
//! outside the trigger-plane product — the job execution lifecycle belongs to
//! the Worklane substrate, not Triggerlane.

use std::{collections::HashMap, process::Stdio, str::FromStr, sync::Arc, time::Duration};

use async_trait::async_trait;
use http_body_util::{BodyExt, Full};
use hyper::{
    Method, Request, StatusCode, Uri,
    body::Bytes,
    header::{HeaderName, HeaderValue},
};
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use thiserror::Error;
use tokio::time::{Instant, Sleep, sleep};
use tokio::{io::AsyncWriteExt, process::Command};
use worklane_core::{
    Broker, DeadLetter, Error as WorklaneError, JobId, Lane, Reservation, RetryPolicy,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvocationJob {
    pub id: JobId,
    pub kind: String,
    pub payload: Vec<u8>,
    pub attempts: u32,
    pub max_attempts: u32,
}

impl InvocationJob {
    fn from_reservation(reservation: &Reservation) -> Self {
        let envelope = &reservation.envelope;
        Self {
            id: envelope.id,
            kind: envelope.kind.clone(),
            payload: envelope.payload.clone(),
            attempts: envelope.attempts,
            max_attempts: envelope.max_attempts,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadInvocationJob {
    pub id: JobId,
    pub lane: String,
    pub kind: String,
    pub payload: Vec<u8>,
    pub attempts: u32,
    pub max_attempts: u32,
    pub error: String,
}

impl DeadInvocationJob {
    fn from_dead_letter(dead_letter: DeadLetter) -> Self {
        let envelope = dead_letter.envelope;
        Self {
            id: envelope.id,
            lane: envelope.lane.to_string(),
            kind: envelope.kind,
            payload: envelope.payload,
            attempts: envelope.attempts,
            max_attempts: envelope.max_attempts,
            error: dead_letter.error,
        }
    }
}

#[async_trait]
pub trait ExternalInvoker: Send + Sync + 'static {
    async fn invoke(&self, job: InvocationJob) -> InvocationResult;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvocationResult {
    Success,
    RetryableFailure(String),
    TerminalFailure(String),
}

#[derive(Debug, Clone)]
pub struct HttpEndpoint {
    pub url: Uri,
    pub method: Method,
    pub headers: Vec<(HeaderName, HeaderValue)>,
    pub terminal_status_codes: Vec<StatusCode>,
}

impl HttpEndpoint {
    pub fn new(url: impl Into<String>) -> Result<Self, HttpEndpointError> {
        let url = Uri::from_str(&url.into()).map_err(HttpEndpointError::InvalidUri)?;
        Ok(Self {
            url,
            method: Method::POST,
            headers: Vec::new(),
            terminal_status_codes: Vec::new(),
        })
    }

    pub fn method(mut self, method: Method) -> Self {
        self.method = method;
        self
    }

    pub fn header(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, HttpEndpointError> {
        let name =
            HeaderName::from_str(&name.into()).map_err(HttpEndpointError::InvalidHeaderName)?;
        let value =
            HeaderValue::from_str(&value.into()).map_err(HttpEndpointError::InvalidHeaderValue)?;
        self.headers.push((name, value));
        Ok(self)
    }

    pub fn terminal_status_code(mut self, status: StatusCode) -> Self {
        self.terminal_status_codes.push(status);
        self
    }
}

#[derive(Debug, Error)]
pub enum HttpEndpointError {
    #[error("invalid URI: {0}")]
    InvalidUri(#[from] hyper::http::uri::InvalidUri),
    #[error("invalid header name: {0}")]
    InvalidHeaderName(#[from] hyper::http::header::InvalidHeaderName),
    #[error("invalid header value: {0}")]
    InvalidHeaderValue(#[from] hyper::http::header::InvalidHeaderValue),
}

#[derive(Clone)]
pub struct HttpInvoker {
    endpoints: HashMap<String, HttpEndpoint>,
    transport: Arc<dyn HttpTransport>,
}

impl HttpInvoker {
    pub fn new() -> Self {
        Self {
            endpoints: HashMap::new(),
            transport: Arc::new(HyperHttpTransport),
        }
    }

    #[cfg(test)]
    fn with_transport(transport: impl HttpTransport + 'static) -> Self {
        Self {
            endpoints: HashMap::new(),
            transport: Arc::new(transport),
        }
    }

    pub fn register(mut self, kind: impl Into<String>, endpoint: HttpEndpoint) -> Self {
        self.endpoints.insert(kind.into(), endpoint);
        self
    }
}

impl Default for HttpInvoker {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExternalInvoker for HttpInvoker {
    async fn invoke(&self, job: InvocationJob) -> InvocationResult {
        let Some(endpoint) = self.endpoints.get(&job.kind) else {
            return InvocationResult::TerminalFailure(format!(
                "no HTTP endpoint configured for kind {}",
                job.kind
            ));
        };

        match self.transport.send(endpoint, job.payload).await {
            Ok(status) if status.is_success() => InvocationResult::Success,
            Ok(status) if endpoint.terminal_status_codes.contains(&status) => {
                InvocationResult::TerminalFailure(format!(
                    "HTTP endpoint returned terminal status {status}"
                ))
            }
            Ok(status) => InvocationResult::RetryableFailure(format!(
                "HTTP endpoint returned status {status}"
            )),
            Err(error) => InvocationResult::RetryableFailure(error),
        }
    }
}

#[async_trait]
trait HttpTransport: Send + Sync {
    async fn send(&self, endpoint: &HttpEndpoint, payload: Vec<u8>) -> Result<StatusCode, String>;
}

struct HyperHttpTransport;

#[async_trait]
impl HttpTransport for HyperHttpTransport {
    async fn send(&self, endpoint: &HttpEndpoint, payload: Vec<u8>) -> Result<StatusCode, String> {
        send_http_request(endpoint, payload)
            .await
            .map_err(|error| error.to_string())
    }
}

async fn send_http_request(
    endpoint: &HttpEndpoint,
    payload: Vec<u8>,
) -> Result<StatusCode, HttpInvocationError> {
    let client = Client::builder(TokioExecutor::new()).build_http();
    let mut builder = Request::builder()
        .method(endpoint.method.clone())
        .uri(endpoint.url.clone());

    for (name, value) in &endpoint.headers {
        builder = builder.header(name, value);
    }

    let request = builder
        .body(Full::new(Bytes::from(payload)))
        .map_err(HttpInvocationError::RequestBuild)?;
    let response = client
        .request(request)
        .await
        .map_err(HttpInvocationError::Transport)?;
    let status = response.status();
    response
        .into_body()
        .collect()
        .await
        .map_err(HttpInvocationError::Body)?;
    Ok(status)
}

#[derive(Debug, Error)]
enum HttpInvocationError {
    #[error("failed to build HTTP request: {0}")]
    RequestBuild(#[from] hyper::http::Error),
    #[error("HTTP transport failed: {0}")]
    Transport(#[from] hyper_util::client::legacy::Error),
    #[error("failed to read HTTP response body: {0}")]
    Body(#[from] hyper::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubprocessCommand {
    pub program: String,
    pub args: Vec<String>,
    pub terminal_exit_codes: Vec<i32>,
}

impl SubprocessCommand {
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            terminal_exit_codes: Vec::new(),
        }
    }

    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn terminal_exit_code(mut self, code: i32) -> Self {
        self.terminal_exit_codes.push(code);
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct SubprocessInvoker {
    commands: HashMap<String, SubprocessCommand>,
}

impl SubprocessInvoker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(mut self, kind: impl Into<String>, command: SubprocessCommand) -> Self {
        self.commands.insert(kind.into(), command);
        self
    }
}

#[async_trait]
impl ExternalInvoker for SubprocessInvoker {
    async fn invoke(&self, job: InvocationJob) -> InvocationResult {
        let Some(command) = self.commands.get(&job.kind) else {
            return InvocationResult::TerminalFailure(format!(
                "no subprocess configured for kind {}",
                job.kind
            ));
        };

        match run_subprocess(command, &job.payload).await {
            Ok(output) if output.status.success() => InvocationResult::Success,
            Ok(output) => {
                let code = output.status.code().unwrap_or(-1);
                let message = subprocess_failure_message(code, &output.stderr);
                if command.terminal_exit_codes.contains(&code) {
                    InvocationResult::TerminalFailure(message)
                } else {
                    InvocationResult::RetryableFailure(message)
                }
            }
            Err(error) => InvocationResult::RetryableFailure(error.to_string()),
        }
    }
}

async fn run_subprocess(
    command: &SubprocessCommand,
    payload: &[u8],
) -> std::io::Result<std::process::Output> {
    let mut child = Command::new(&command.program)
        .args(&command.args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(payload).await?;
    }

    child.wait_with_output().await
}

fn subprocess_failure_message(code: i32, stderr: &[u8]) -> String {
    let stderr = String::from_utf8_lossy(stderr);
    let stderr = stderr.trim();

    if stderr.is_empty() {
        format!("subprocess exited with code {code}")
    } else {
        format!("subprocess exited with code {code}: {stderr}")
    }
}

#[derive(Debug, Clone)]
pub struct InvokerConfig {
    pub lane: Lane,
    pub deadline: Duration,
    pub retry_policy: RetryPolicy,
}

impl InvokerConfig {
    pub fn new(lane: Lane, deadline: Duration) -> Self {
        Self {
            lane,
            deadline,
            retry_policy: RetryPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessOutcome {
    Idle,
    Acked,
    Retried { delay: Duration },
    Failed { error: String },
    StaleResolution,
}

pub struct ExternalInvokerRuntime<B, I> {
    broker: Arc<B>,
    invoker: Arc<I>,
    config: InvokerConfig,
}

impl<B, I> ExternalInvokerRuntime<B, I>
where
    B: Broker,
    I: ExternalInvoker,
{
    pub fn new(broker: Arc<B>, invoker: Arc<I>, config: InvokerConfig) -> Self {
        Self {
            broker,
            invoker,
            config,
        }
    }

    pub async fn process_once(&self) -> Result<ProcessOutcome, InvokerError> {
        let Some(reservation) = self.broker.reserve(&self.config.lane).await? else {
            return Ok(ProcessOutcome::Idle);
        };

        self.process_reservation(reservation).await
    }

    pub async fn list_dead_letters(
        &self,
        lane: &Lane,
        limit: usize,
    ) -> Result<Vec<DeadInvocationJob>, InvokerError> {
        Ok(self
            .broker
            .read_dead_letters(lane, limit)
            .await?
            .into_iter()
            .map(DeadInvocationJob::from_dead_letter)
            .collect())
    }

    pub async fn requeue_dead_letter(&self, id: JobId) -> Result<(), InvokerError> {
        self.broker.requeue(id).await?;
        Ok(())
    }

    async fn process_reservation(
        &self,
        reservation: Reservation,
    ) -> Result<ProcessOutcome, InvokerError> {
        let invocation = self
            .run_bounded(&reservation, InvocationJob::from_reservation(&reservation))
            .await;
        self.resolve(reservation, invocation).await
    }

    async fn run_bounded(
        &self,
        reservation: &Reservation,
        job: InvocationJob,
    ) -> BoundedInvocation {
        let invocation = self.invoker.invoke(job);
        tokio::pin!(invocation);

        let deadline = sleep(self.config.deadline);
        tokio::pin!(deadline);

        let heartbeat_delay = heartbeat_delay(reservation.lease);
        let heartbeat = sleep(heartbeat_delay);
        tokio::pin!(heartbeat);

        let mut heartbeat_enabled = true;

        loop {
            if heartbeat_enabled {
                tokio::select! {
                    result = &mut invocation => return BoundedInvocation::Completed(result),
                    () = &mut deadline => return BoundedInvocation::TimedOut,
                    () = &mut heartbeat => {
                        match self.broker.extend(reservation.receipt).await {
                            Ok(()) => reset_sleep(&mut heartbeat, heartbeat_delay),
                            Err(WorklaneError::StaleReservation(_)) => heartbeat_enabled = false,
                            Err(error) => return BoundedInvocation::HeartbeatFailed(error.to_string()),
                        }
                    }
                }
            } else {
                tokio::select! {
                    result = &mut invocation => return BoundedInvocation::Completed(result),
                    () = &mut deadline => return BoundedInvocation::TimedOut,
                }
            }
        }
    }

    async fn resolve(
        &self,
        reservation: Reservation,
        invocation: BoundedInvocation,
    ) -> Result<ProcessOutcome, InvokerError> {
        match invocation {
            BoundedInvocation::Completed(InvocationResult::Success) => {
                resolve_ack(self.broker.ack(reservation.receipt).await)
            }
            BoundedInvocation::Completed(InvocationResult::RetryableFailure(error)) => {
                self.retry_or_fail(reservation, error).await
            }
            BoundedInvocation::TimedOut => {
                self.retry_or_fail(reservation, "invocation timed out".to_owned())
                    .await
            }
            BoundedInvocation::Completed(InvocationResult::TerminalFailure(error))
            | BoundedInvocation::HeartbeatFailed(error) => resolve_fail(
                self.broker.fail(reservation.receipt, error.clone()).await,
                error,
            ),
        }
    }

    async fn retry_or_fail(
        &self,
        reservation: Reservation,
        error: String,
    ) -> Result<ProcessOutcome, InvokerError> {
        let completed_attempts = reservation.envelope.attempts + 1;
        if completed_attempts < reservation.envelope.max_attempts {
            let delay = self
                .config
                .retry_policy
                .delay_for(reservation.envelope.attempts);
            resolve_retry(self.broker.retry(reservation.receipt, delay).await, delay)
        } else {
            resolve_fail(
                self.broker.fail(reservation.receipt, error.clone()).await,
                error,
            )
        }
    }
}

#[derive(Debug)]
enum BoundedInvocation {
    Completed(InvocationResult),
    TimedOut,
    HeartbeatFailed(String),
}

fn heartbeat_delay(lease: Duration) -> Duration {
    (lease / 2).max(Duration::from_millis(1))
}

fn reset_sleep(sleep: &mut std::pin::Pin<&mut Sleep>, delay: Duration) {
    sleep.as_mut().reset(Instant::now() + delay);
}

fn resolve_ack(result: worklane_core::Result<()>) -> Result<ProcessOutcome, InvokerError> {
    match result {
        Ok(()) => Ok(ProcessOutcome::Acked),
        Err(WorklaneError::StaleReservation(_)) => Ok(ProcessOutcome::StaleResolution),
        Err(error) => Err(InvokerError::Broker(error)),
    }
}

fn resolve_retry(
    result: worklane_core::Result<()>,
    delay: Duration,
) -> Result<ProcessOutcome, InvokerError> {
    match result {
        Ok(()) => Ok(ProcessOutcome::Retried { delay }),
        Err(WorklaneError::StaleReservation(_)) => Ok(ProcessOutcome::StaleResolution),
        Err(error) => Err(InvokerError::Broker(error)),
    }
}

fn resolve_fail(
    result: worklane_core::Result<()>,
    error_message: String,
) -> Result<ProcessOutcome, InvokerError> {
    match result {
        Ok(()) => Ok(ProcessOutcome::Failed {
            error: error_message,
        }),
        Err(WorklaneError::StaleReservation(_)) => Ok(ProcessOutcome::StaleResolution),
        Err(error) => Err(InvokerError::Broker(error)),
    }
}

#[derive(Debug, Error)]
pub enum InvokerError {
    #[error(transparent)]
    Broker(#[from] WorklaneError),
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use hyper::StatusCode;
    use worklane_core::{
        Broker, DeadLetter, Error as WorklaneError, JobEnvelope, JobId, JobState, NewJob,
        Reservation, ReservationReceipt, Result as WorklaneResult,
    };
    use worklane_memory::InMemoryBroker;

    use super::*;

    /// Build a validated `Lane` from a literal known to be well-formed.
    fn lane(name: &str) -> Lane {
        Lane::try_from(name).expect("test lane should be valid")
    }

    #[tokio::test]
    async fn success_acks_real_broker_reservation() {
        let broker = Arc::new(InMemoryBroker::new());
        broker
            .enqueue(NewJob::new(lane("external"), "echo", b"opaque".to_vec(), 3))
            .await
            .expect("enqueue should succeed");
        let runtime = runtime(
            Arc::clone(&broker),
            StaticInvoker::new(InvocationResult::Success),
            Duration::from_millis(100),
        );

        let outcome = runtime.process_once().await.expect("process should run");

        assert_eq!(outcome, ProcessOutcome::Acked);
        assert!(broker.is_empty());
    }

    #[tokio::test]
    async fn retryable_failure_retries_real_broker_reservation() {
        let broker = Arc::new(InMemoryBroker::new());
        broker
            .enqueue(NewJob::new(lane("external"), "http", b"opaque".to_vec(), 3))
            .await
            .expect("enqueue should succeed");
        let runtime = runtime(
            Arc::clone(&broker),
            StaticInvoker::new(InvocationResult::RetryableFailure("temporary".to_owned())),
            Duration::from_millis(100),
        );

        let outcome = runtime.process_once().await.expect("process should run");

        assert_eq!(
            outcome,
            ProcessOutcome::Retried {
                delay: Duration::from_secs(1)
            }
        );
        assert_eq!(broker.len(), 1);
    }

    #[tokio::test]
    async fn terminal_failure_dead_letters_real_broker_reservation() {
        let broker = Arc::new(InMemoryBroker::new());
        broker
            .enqueue(NewJob::new(lane("external"), "http", b"opaque".to_vec(), 3))
            .await
            .expect("enqueue should succeed");
        let runtime = runtime(
            Arc::clone(&broker),
            StaticInvoker::new(InvocationResult::TerminalFailure("bad request".to_owned())),
            Duration::from_millis(100),
        );

        let outcome = runtime.process_once().await.expect("process should run");

        assert_eq!(
            outcome,
            ProcessOutcome::Failed {
                error: "bad request".to_owned()
            }
        );
        assert_eq!(broker.dead_letters().len(), 1);
    }

    #[tokio::test]
    async fn list_dead_letters_preserves_opaque_envelope_and_error() {
        let broker = Arc::new(InMemoryBroker::new());
        let job_id = broker
            .enqueue(NewJob::new(
                lane("external"),
                "http",
                b"\x00opaque".to_vec(),
                3,
            ))
            .await
            .expect("enqueue should succeed");
        let runtime = runtime(
            Arc::clone(&broker),
            StaticInvoker::new(InvocationResult::TerminalFailure("bad request".to_owned())),
            Duration::from_millis(100),
        );
        runtime.process_once().await.expect("process should run");

        let dead = runtime
            .list_dead_letters(&lane("external"), 10)
            .await
            .expect("dead letters should list");

        assert_eq!(
            dead,
            [DeadInvocationJob {
                id: job_id,
                lane: "external".to_owned(),
                kind: "http".to_owned(),
                payload: b"\x00opaque".to_vec(),
                attempts: 0,
                max_attempts: 3,
                error: "bad request".to_owned(),
            }]
        );
    }

    #[tokio::test]
    async fn list_dead_letters_is_lane_scoped_and_bounded() {
        let broker = Arc::new(InMemoryBroker::new());
        broker
            .enqueue(NewJob::new(lane("external"), "http", b"one".to_vec(), 3))
            .await
            .expect("enqueue should succeed");
        broker
            .enqueue(NewJob::new(lane("external"), "http", b"two".to_vec(), 3))
            .await
            .expect("enqueue should succeed");
        broker
            .enqueue(NewJob::new(lane("other"), "http", b"other".to_vec(), 3))
            .await
            .expect("enqueue should succeed");

        runtime(
            Arc::clone(&broker),
            StaticInvoker::new(InvocationResult::TerminalFailure("bad request".to_owned())),
            Duration::from_millis(100),
        )
        .process_once()
        .await
        .expect("first external job should fail");
        runtime(
            Arc::clone(&broker),
            StaticInvoker::new(InvocationResult::TerminalFailure("bad request".to_owned())),
            Duration::from_millis(100),
        )
        .process_once()
        .await
        .expect("second external job should fail");
        let other_runtime = ExternalInvokerRuntime::new(
            Arc::clone(&broker),
            Arc::new(StaticInvoker::new(InvocationResult::TerminalFailure(
                "other failure".to_owned(),
            ))),
            InvokerConfig::new(lane("other"), Duration::from_millis(100)),
        );
        other_runtime
            .process_once()
            .await
            .expect("other lane job should fail");

        let dead = other_runtime
            .list_dead_letters(&lane("external"), 1)
            .await
            .expect("dead letters should list");

        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].lane, "external");
    }

    #[tokio::test]
    async fn requeue_dead_letter_makes_job_reservable_on_original_lane() {
        let broker = Arc::new(InMemoryBroker::new());
        let job_id = broker
            .enqueue(NewJob::new(lane("external"), "http", b"opaque".to_vec(), 3))
            .await
            .expect("enqueue should succeed");
        let runtime = runtime(
            Arc::clone(&broker),
            StaticInvoker::new(InvocationResult::TerminalFailure("bad request".to_owned())),
            Duration::from_millis(100),
        );
        runtime.process_once().await.expect("process should run");

        runtime
            .requeue_dead_letter(job_id)
            .await
            .expect("requeue should succeed");

        assert!(
            runtime
                .list_dead_letters(&lane("external"), 10)
                .await
                .expect("dead letters should list")
                .is_empty()
        );
        let reservation = broker
            .reserve(&lane("external"))
            .await
            .expect("reserve should succeed")
            .expect("requeued job should be reservable");
        assert_eq!(reservation.envelope.id, job_id);
        assert_eq!(reservation.envelope.lane, "external");
        assert_eq!(reservation.envelope.kind, "http");
        assert_eq!(reservation.envelope.payload, b"opaque");
    }

    #[tokio::test]
    async fn requeue_unknown_dead_letter_returns_broker_error() {
        let broker = Arc::new(InMemoryBroker::new());
        let runtime = runtime(
            Arc::clone(&broker),
            StaticInvoker::new(InvocationResult::Success),
            Duration::from_millis(100),
        );

        let error = runtime
            .requeue_dead_letter(JobId::new())
            .await
            .expect_err("unknown dead letter should fail");

        assert!(matches!(
            error,
            InvokerError::Broker(WorklaneError::Broker(_))
        ));
        assert!(broker.is_empty());
    }

    #[tokio::test]
    async fn timeout_retries_until_attempts_are_exhausted() {
        let broker = Arc::new(InMemoryBroker::new());
        broker
            .enqueue(NewJob::new(lane("external"), "slow", b"opaque".to_vec(), 2))
            .await
            .expect("enqueue should succeed");
        let runtime = runtime(
            Arc::clone(&broker),
            SleepingInvoker::new(Duration::from_millis(50), InvocationResult::Success),
            Duration::from_millis(5),
        );

        let outcome = runtime.process_once().await.expect("process should run");

        assert_eq!(
            outcome,
            ProcessOutcome::Retried {
                delay: Duration::from_secs(1)
            }
        );
    }

    #[tokio::test]
    async fn heartbeat_keeps_real_broker_reservation_alive() {
        let broker = Arc::new(InMemoryBroker::new().with_lease(Duration::from_millis(10)));
        broker
            .enqueue(NewJob::new(lane("external"), "slow", b"opaque".to_vec(), 3))
            .await
            .expect("enqueue should succeed");
        let runtime = runtime(
            Arc::clone(&broker),
            SleepingInvoker::new(Duration::from_millis(30), InvocationResult::Success),
            Duration::from_millis(100),
        );

        let outcome = runtime.process_once().await.expect("process should run");

        assert_eq!(outcome, ProcessOutcome::Acked);
        assert!(broker.is_empty());
    }

    #[tokio::test]
    async fn stale_resolution_is_non_fatal_outcome() {
        let broker = Arc::new(StaleAckBroker::new());
        let runtime = runtime(
            Arc::clone(&broker),
            StaticInvoker::new(InvocationResult::Success),
            Duration::from_millis(100),
        );

        let outcome = runtime.process_once().await.expect("stale is non-fatal");

        assert_eq!(outcome, ProcessOutcome::StaleResolution);
        assert_eq!(broker.ack_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn subprocess_success_returns_success() {
        let invoker = SubprocessInvoker::new().register(
            "cat",
            SubprocessCommand::new("sh").arg("-c").arg("cat >/dev/null"),
        );

        let result = invoker.invoke(job("cat", b"opaque")).await;

        assert_eq!(result, InvocationResult::Success);
    }

    #[tokio::test]
    async fn subprocess_receives_payload_on_stdin() {
        let invoker = SubprocessInvoker::new().register(
            "check-payload",
            SubprocessCommand::new("sh")
                .arg("-c")
                .arg(r#"test "$(cat)" = "hello""#),
        );

        let result = invoker.invoke(job("check-payload", b"hello")).await;

        assert_eq!(result, InvocationResult::Success);
    }

    #[tokio::test]
    async fn subprocess_terminal_exit_code_returns_terminal_failure() {
        let invoker = SubprocessInvoker::new().register(
            "bad-request",
            SubprocessCommand::new("false").terminal_exit_code(1),
        );

        let result = invoker.invoke(job("bad-request", b"opaque")).await;

        assert!(matches!(result, InvocationResult::TerminalFailure(_)));
    }

    #[tokio::test]
    async fn subprocess_non_terminal_exit_returns_retryable_failure() {
        let invoker = SubprocessInvoker::new().register(
            "temporary",
            SubprocessCommand::new("sh").arg("-c").arg("exit 2"),
        );

        let result = invoker.invoke(job("temporary", b"opaque")).await;

        assert!(matches!(result, InvocationResult::RetryableFailure(_)));
    }

    #[tokio::test]
    async fn subprocess_unknown_kind_returns_terminal_failure() {
        let invoker = SubprocessInvoker::new();

        let result = invoker.invoke(job("missing", b"opaque")).await;

        assert!(matches!(result, InvocationResult::TerminalFailure(_)));
    }

    #[tokio::test]
    async fn http_success_returns_success() {
        let transport = FakeHttpTransport::status(StatusCode::OK);
        let payloads = Arc::clone(&transport.payloads);
        let endpoint =
            HttpEndpoint::new("http://example.test/invoke").expect("endpoint should parse");
        let invoker = HttpInvoker::with_transport(transport).register("webhook", endpoint);

        let result = invoker.invoke(job("webhook", b"opaque")).await;

        assert_eq!(result, InvocationResult::Success);
        assert_eq!(
            payloads.lock().expect("payload mutex poisoned").as_slice(),
            [b"opaque".to_vec()]
        );
    }

    #[tokio::test]
    async fn http_receives_payload_as_body() {
        let transport = FakeHttpTransport::status(StatusCode::OK);
        let payloads = Arc::clone(&transport.payloads);
        let endpoint =
            HttpEndpoint::new("http://example.test/invoke").expect("endpoint should parse");
        let invoker = HttpInvoker::with_transport(transport).register("webhook", endpoint);

        let result = invoker.invoke(job("webhook", b"hello")).await;

        assert_eq!(result, InvocationResult::Success);
        assert_eq!(
            payloads.lock().expect("payload mutex poisoned").as_slice(),
            [b"hello".to_vec()]
        );
    }

    #[tokio::test]
    async fn http_terminal_status_returns_terminal_failure() {
        let endpoint = HttpEndpoint::new("http://example.test/invoke")
            .expect("endpoint should parse")
            .terminal_status_code(StatusCode::BAD_REQUEST);
        let invoker =
            HttpInvoker::with_transport(FakeHttpTransport::status(StatusCode::BAD_REQUEST))
                .register("webhook", endpoint);

        let result = invoker.invoke(job("webhook", b"opaque")).await;

        assert!(matches!(result, InvocationResult::TerminalFailure(_)));
    }

    #[tokio::test]
    async fn http_non_terminal_status_returns_retryable_failure() {
        let endpoint =
            HttpEndpoint::new("http://example.test/invoke").expect("endpoint should parse");
        let invoker =
            HttpInvoker::with_transport(FakeHttpTransport::status(StatusCode::SERVICE_UNAVAILABLE))
                .register("webhook", endpoint);

        let result = invoker.invoke(job("webhook", b"opaque")).await;

        assert!(matches!(result, InvocationResult::RetryableFailure(_)));
    }

    #[tokio::test]
    async fn http_transport_error_returns_retryable_failure() {
        let endpoint =
            HttpEndpoint::new("http://example.test/invoke").expect("endpoint should parse");
        let invoker = HttpInvoker::with_transport(FakeHttpTransport::error("connection refused"))
            .register("webhook", endpoint);

        let result = invoker.invoke(job("webhook", b"opaque")).await;

        assert!(matches!(result, InvocationResult::RetryableFailure(_)));
    }

    #[tokio::test]
    async fn http_unknown_kind_returns_terminal_failure() {
        let invoker = HttpInvoker::new();

        let result = invoker.invoke(job("missing", b"opaque")).await;

        assert!(matches!(result, InvocationResult::TerminalFailure(_)));
    }

    fn runtime<B, I>(broker: Arc<B>, invoker: I, deadline: Duration) -> ExternalInvokerRuntime<B, I>
    where
        B: Broker,
        I: ExternalInvoker,
    {
        ExternalInvokerRuntime::new(
            broker,
            Arc::new(invoker),
            InvokerConfig::new(lane("external"), deadline),
        )
    }

    fn job(kind: &str, payload: &[u8]) -> InvocationJob {
        InvocationJob {
            id: JobId::new(),
            kind: kind.to_owned(),
            payload: payload.to_vec(),
            attempts: 0,
            max_attempts: 3,
        }
    }

    struct StaticInvoker {
        result: InvocationResult,
        seen_payloads: Mutex<Vec<Vec<u8>>>,
    }

    impl StaticInvoker {
        fn new(result: InvocationResult) -> Self {
            Self {
                result,
                seen_payloads: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl ExternalInvoker for StaticInvoker {
        async fn invoke(&self, job: InvocationJob) -> InvocationResult {
            self.seen_payloads
                .lock()
                .expect("static invoker mutex poisoned")
                .push(job.payload);
            self.result.clone()
        }
    }

    struct SleepingInvoker {
        duration: Duration,
        result: InvocationResult,
    }

    impl SleepingInvoker {
        fn new(duration: Duration, result: InvocationResult) -> Self {
            Self { duration, result }
        }
    }

    #[async_trait]
    impl ExternalInvoker for SleepingInvoker {
        async fn invoke(&self, _job: InvocationJob) -> InvocationResult {
            tokio::time::sleep(self.duration).await;
            self.result.clone()
        }
    }

    struct FakeHttpTransport {
        result: Result<StatusCode, String>,
        payloads: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl FakeHttpTransport {
        fn status(status: StatusCode) -> Self {
            Self {
                result: Ok(status),
                payloads: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn error(error: impl Into<String>) -> Self {
            Self {
                result: Err(error.into()),
                payloads: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl HttpTransport for FakeHttpTransport {
        async fn send(
            &self,
            _endpoint: &HttpEndpoint,
            payload: Vec<u8>,
        ) -> Result<StatusCode, String> {
            self.payloads
                .lock()
                .expect("payload mutex poisoned")
                .push(payload);
            self.result.clone()
        }
    }

    struct StaleAckBroker {
        ack_calls: AtomicUsize,
        receipt: ReservationReceipt,
    }

    impl StaleAckBroker {
        fn new() -> Self {
            Self {
                ack_calls: AtomicUsize::new(0),
                receipt: ReservationReceipt::new(),
            }
        }
    }

    #[async_trait]
    impl Broker for StaleAckBroker {
        async fn enqueue(&self, _job: NewJob) -> WorklaneResult<JobId> {
            Ok(JobId::new())
        }

        async fn enqueue_batch(&self, jobs: Vec<NewJob>) -> WorklaneResult<Vec<JobId>> {
            let mut ids = Vec::with_capacity(jobs.len());
            for job in jobs {
                ids.push(self.enqueue(job).await?);
            }
            Ok(ids)
        }

        async fn reserve(&self, lane: &Lane) -> WorklaneResult<Option<Reservation>> {
            Ok(Some(Reservation::new(
                JobEnvelope::new(
                    JobId::new(),
                    lane.clone(),
                    "stale",
                    b"opaque".to_vec(),
                    3,
                    0,
                    None,
                ),
                self.receipt,
                Duration::from_millis(100),
            )))
        }

        async fn ack(&self, _receipt: ReservationReceipt) -> WorklaneResult<()> {
            self.ack_calls.fetch_add(1, Ordering::SeqCst);
            Err(WorklaneError::StaleReservation("stale".to_owned()))
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
