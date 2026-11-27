use crate::snapshot::Store;
#[cfg(test)]
#[path = "../tests/unit/queue.rs"]
mod tests;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

pub(crate) const LEASE_DURATION: Duration = Duration::from_secs(100);
pub(crate) const MAX_OUTPUT_BYTES: usize = 64 * 1024;
const MAX_ATTEMPTS: u8 = 3;
const ACTIVE_LIMIT: usize = 2_048;
const HISTORY_LIMIT: usize = 128;

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub struct Request {
    pub prompt: String,
    pub prompt_tokens: u32,
    pub max_tokens: u32,
}

impl Request {
    fn valid(&self) -> bool {
        !self.prompt.trim().is_empty() && self.prompt.len() <= 64 * 1024
            && (1..=32_768).contains(&self.prompt_tokens)
            && (1..=4_096).contains(&self.max_tokens)
    }

    fn token_budget(&self) -> u64 {
        u64::from(self.prompt_tokens) + u64::from(self.max_tokens)
    }
}

#[derive(Deserialize, Serialize)]
pub struct Lease {
    pub job_id: u64,
    pub lease_id: u64,
    pub request: Request,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Outcome {
    Done { output: String },
    Retry,
}

#[derive(Deserialize, Serialize)]
pub struct Finish {
    pub lease_id: u64,
    pub outcome: Outcome,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Phase {
    Pending,
    Running {
        lease_id: u64,
        #[serde(skip, default = "Instant::now")]
        expires: Instant,
    },
    Done { output: String },
    Failed,
}

#[derive(Clone, Deserialize, Serialize)]
struct Job {
    request: Request,
    phase: Phase,
    attempts: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_finish: Option<FinishReceipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    idempotency_key: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
struct FinishReceipt {
    lease_id: u64,
    done: bool,
}

impl Job {
    fn acknowledges(&self, finish: &Finish) -> bool {
        let Some(receipt) = &self.last_finish else {
            return false;
        };
        if receipt.lease_id != finish.lease_id {
            return false;
        }
        match (&self.phase, &finish.outcome) {
            (Phase::Done { output }, Outcome::Done { output: requested }) => {
                receipt.done && output == requested
            }
            (Phase::Pending | Phase::Failed, Outcome::Retry) => !receipt.done,
            _ => false,
        }
    }
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub(crate) struct Queue {
    jobs: HashMap<u64, Job>,
    pending: VecDeque<u64>,
    history: VecDeque<u64>,
    job_id: u64,
    lease_id: u64,
    completed: u64,
    failed: u64,
}

impl Queue {
    fn expired(&self, now: Instant) -> impl Iterator<Item = u64> + '_ {
        self.jobs.iter().filter_map(move |(&id, job)| {
            matches!(&job.phase, Phase::Running { expires, .. } if *expires <= now).then_some(id)
        })
    }

    fn expire(&mut self, now: Instant) -> bool {
        let mut expired: Vec<u64> = self.expired(now).collect();
        expired.sort_unstable();
        let changed = !expired.is_empty();
        for id in expired {
            self.retry(id);
        }
        changed
    }

    pub(crate) fn validate_snapshot(&self) -> io::Result<()> {
        let invalid = || io::Error::new(io::ErrorKind::InvalidData, "inconsistent queue snapshot");
        let pending: HashSet<_> = self.pending.iter().copied().collect();
        let history: HashSet<_> = self.history.iter().copied().collect();
        if pending.len() != self.pending.len() || history.len() != self.history.len()
            || !pending.is_disjoint(&history) || history.len() > HISTORY_LIMIT
            || pending.union(&history).any(|id| !self.jobs.contains_key(id))
            || self.jobs.len() - history.len() > ACTIVE_LIMIT
            || self.completed.saturating_add(self.failed) > self.job_id
        {
            return Err(invalid());
        }
        let mut leases = HashSet::new();
        let mut idempotency_keys = HashSet::new();
        for (&id, job) in &self.jobs {
            if id == 0 || id > self.job_id || !job.request.valid() || job.attempts > MAX_ATTEMPTS {
                return Err(invalid());
            }
            let valid = match &job.phase {
                Phase::Pending => pending.contains(&id) && job.attempts < MAX_ATTEMPTS,
                Phase::Running { lease_id, .. } => !pending.contains(&id) && !history.contains(&id)
                    && job.attempts > 0 && *lease_id > 0 && *lease_id <= self.lease_id
                    && leases.insert(*lease_id),
                Phase::Done { output } => history.contains(&id) && output.len() <= MAX_OUTPUT_BYTES
                    && job.attempts > 0,
                Phase::Failed => history.contains(&id) && job.attempts == MAX_ATTEMPTS,
            };
            let receipt_valid = job.last_finish.as_ref().is_none_or(|receipt| {
                job.attempts > 0
                    && receipt.lease_id > 0 && receipt.lease_id <= self.lease_id
                    && leases.insert(receipt.lease_id)
                    && match job.phase {
                        Phase::Done { .. } => receipt.done,
                        Phase::Pending | Phase::Failed => !receipt.done,
                        Phase::Running { .. } => false,
                    }
            });
            let key_valid = job.idempotency_key.as_deref().is_none_or(|key| {
                valid_idempotency_key(key) && idempotency_keys.insert(key)
            });
            if !valid || !receipt_valid || !key_valid {
                return Err(invalid());
            }
        }
        Ok(())
    }

    fn retain_result(&mut self, id: u64) {
        self.history.push_back(id);
        if self.history.len() > HISTORY_LIMIT {
            let oldest = self.history.pop_front().expect("retained history");
            self.jobs.remove(&oldest);
        }
    }

    fn retry(&mut self, id: u64) {
        let job = self.jobs.get_mut(&id).expect("leased job exists");
        if job.attempts >= MAX_ATTEMPTS {
            job.phase = Phase::Failed;
            self.failed += 1;
            self.retain_result(id);
        } else {
            job.phase = Phase::Pending;
            self.pending.push_back(id);
        }
    }
}

struct Broker {
    queue: Queue,
    store: Option<Store>,
    sync_failed: bool,
}

impl Broker {
    fn open(path: Option<PathBuf>) -> io::Result<Self> {
        let Some(path) = path else {
            return Ok(Self { queue: Queue::default(), store: None, sync_failed: false });
        };
        let (store, mut queue) = Store::open(path)?;
        if queue.expire(Instant::now()) {
            store.save(&queue).map_err(|error| error.source)?;
        }
        Ok(Self { queue, store: Some(store), sync_failed: false })
    }

    fn available(&self) -> io::Result<()> {
        if self.sync_failed {
            return Err(io::Error::other("queue state sync failed after replacement, restart required"));
        }
        Ok(())
    }

    fn update<T>(&mut self, op: impl FnOnce(&mut Queue) -> T) -> Result<T, QueueError> {
        let Some(store) = &self.store else {
            return Ok(op(&mut self.queue));
        };
        let mut next = self.queue.clone();
        let response = op(&mut next);
        if let Err(error) = store.save(&next) {
            if error.replaced {
                // The rename is visible, but its persistence is uncertain. Stop
                // serving until restart to avoid diverging from the snapshot.
                self.sync_failed = true;
                return Err(QueueError::CommitOutcomeUnknown(error.source));
            }
            return Err(error.source.into());
        }
        self.queue = next;
        Ok(response)
    }

    fn expire(&mut self) -> Result<(), QueueError> {
        let now = Instant::now();
        if self.queue.expired(now).next().is_some() {
            self.update(|queue| queue.expire(now))?;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Shared {
    broker: Arc<Mutex<Broker>>,
    admission: Option<Arc<Semaphore>>,
}

enum QueueError {
    Busy,
    Storage(io::Error),
    CommitOutcomeUnknown(io::Error),
}

impl From<io::Error> for QueueError {
    fn from(error: io::Error) -> Self {
        Self::Storage(error)
    }
}

impl IntoResponse for QueueError {
    fn into_response(self) -> Response {
        match self {
            Self::Busy => (
                StatusCode::SERVICE_UNAVAILABLE,
                [(header::RETRY_AFTER, "1")],
                "queue storage busy",
            ).into_response(),
            Self::Storage(error) => {
                eprintln!("queue operation failed: {error}");
                (StatusCode::SERVICE_UNAVAILABLE, "queue storage unavailable").into_response()
            }
            Self::CommitOutcomeUnknown(error) => {
                eprintln!("snapshot replaced but directory sync failed: {error}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": "queue_commit_outcome_unknown",
                        "message": "The queue change may have been applied. Do not retry blindly."
                    })),
                ).into_response()
            }
        }
    }
}

async fn access<T: Send + 'static>(
    shared: Shared,
    op: impl FnOnce(&mut Broker) -> Result<T, QueueError> + Send + 'static,
) -> Result<T, QueueError> {
    let Shared { broker, admission } = shared;
    let op = move || {
        let mut broker = broker.lock().map_err(|_| io::Error::other("queue lock poisoned"))?;
        broker.available()?;
        op(&mut broker)
    };
    if let Some(admission) = admission {
        let permit = admission.try_acquire_owned().map_err(|_| QueueError::Busy)?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            op()
        }).await.map_err(io::Error::other)?
    } else {
        op()
    }
}

pub fn router(path: Option<PathBuf>) -> io::Result<Router> {
    let admission = path.is_some().then(|| Arc::new(Semaphore::new(1)));
    let broker = Broker::open(path.clone()).map_err(|error| {
        if let Some(path) = path {
            return io::Error::new(error.kind(), format!("open queue state {}: {error}", path.display()));
        }
        error
    })?;
    let state = Shared { broker: Arc::new(Mutex::new(broker)), admission };
    Ok(Router::new()
        .route("/healthz", get(health))
        .route("/metrics", get(metrics))
        .route("/jobs", post(submit))
        .route("/jobs/{id}", get(status))
        .route("/claim", post(claim))
        .route("/jobs/{id}/finish", post(finish))
        .with_state(state))
}

async fn health(State(shared): State<Shared>) -> Result<StatusCode, QueueError> {
    access(shared, |_| Ok(StatusCode::OK)).await
}

async fn submit(
    State(shared): State<Shared>,
    headers: HeaderMap,
    Json(request): Json<Request>,
) -> Result<Response, QueueError> {
    if !request.valid() {
        return Ok((StatusCode::BAD_REQUEST, "invalid prompt or token budget").into_response());
    }
    let mut keys = headers.get_all("idempotency-key").iter();
    let idempotency_key = match (keys.next().map(|header| header.to_str()), keys.next()) {
        (None, _) => None,
        (Some(Ok(key)), None) if valid_idempotency_key(key) => Some(key.to_owned()),
        _ => return Ok((StatusCode::BAD_REQUEST, "invalid Idempotency-Key").into_response()),
    };
    access(shared, move |broker| {
        if let Some(key) = idempotency_key.as_ref() {
            if let Some((&id, job)) = broker.queue.jobs.iter().find(|(_, job)| {
                job.idempotency_key.as_ref() == Some(key)
            }) {
                if job.request != request {
                    return Ok((StatusCode::CONFLICT, "Idempotency-Key is already assigned to another request").into_response());
                }
                return Ok(accepted_job(id));
            }
        }
        if broker.queue.jobs.len() - broker.queue.history.len() >= ACTIVE_LIMIT {
            return Ok((StatusCode::TOO_MANY_REQUESTS, "queue full").into_response());
        }
        let id = broker.queue.job_id.checked_add(1)
            .ok_or_else(|| io::Error::other("job IDs exhausted"))?;
        broker.update(|queue| {
            queue.job_id = id;
            queue.jobs.insert(id, Job {
                request,
                phase: Phase::Pending,
                attempts: 0,
                last_finish: None,
                idempotency_key,
            });
            queue.pending.push_back(id);
        })?;
        Ok(accepted_job(id))
    }).await
}

fn accepted_job(id: u64) -> Response {
    (StatusCode::ACCEPTED, Json(serde_json::json!({ "job_id": id }))).into_response()
}

fn valid_idempotency_key(key: &str) -> bool {
    (1..=128).contains(&key.len())
        && key.bytes().all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.'))
}

async fn status(
    State(shared): State<Shared>, Path(id): Path<u64>,
) -> Result<Response, QueueError> {
    access(shared, move |broker| {
        broker.expire()?;
        let response = match broker.queue.jobs.get(&id).map(|job| &job.phase) {
            Some(Phase::Running { .. }) => Json(serde_json::json!({ "status": "running" })).into_response(),
            Some(phase) => Json(phase).into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        };
        Ok(response)
    }).await
}

async fn claim(State(shared): State<Shared>) -> Result<Json<Option<Lease>>, QueueError> {
    access(shared, |broker| {
        broker.expire()?;
        let Some(&job_id) = broker.queue.pending.front() else {
            return Ok(Json(None));
        };
        let lease_id = broker.queue.lease_id.checked_add(1)
            .ok_or_else(|| io::Error::other("lease IDs exhausted"))?;
        let lease = broker.update(|queue| {
            queue.pending.pop_front();
            queue.lease_id = lease_id;
            let job = queue.jobs.get_mut(&job_id).expect("pending job exists");
            job.attempts += 1;
            job.last_finish = None;
            job.phase = Phase::Running { lease_id, expires: Instant::now() + LEASE_DURATION };
            Lease { job_id, lease_id, request: job.request.clone() }
        })?;
        Ok(Json(Some(lease)))
    }).await
}

async fn finish(
    State(shared): State<Shared>,
    Path(id): Path<u64>,
    Json(finish): Json<Finish>,
) -> Result<StatusCode, QueueError> {
    access(shared, move |broker| {
        broker.expire()?;
        let Some(job) = broker.queue.jobs.get(&id) else {
            return Ok(StatusCode::NOT_FOUND);
        };
        if job.acknowledges(&finish) {
            return Ok(StatusCode::NO_CONTENT);
        }
        if !matches!(&job.phase, Phase::Running { lease_id, .. } if *lease_id == finish.lease_id) {
            return Ok(StatusCode::CONFLICT);
        }
        if matches!(&finish.outcome, Outcome::Done { output } if output.len() > MAX_OUTPUT_BYTES) {
            return Ok(StatusCode::UNPROCESSABLE_ENTITY);
        }
        broker.update(|queue| {
            let job = queue.jobs.get_mut(&id).expect("leased job exists");
            job.last_finish = Some(FinishReceipt {
                lease_id: finish.lease_id,
                done: matches!(finish.outcome, Outcome::Done { .. }),
            });
            match finish.outcome {
                Outcome::Done { output } => {
                    job.phase = Phase::Done { output };
                    queue.completed += 1;
                    queue.retain_result(id);
                }
                Outcome::Retry => queue.retry(id),
            }
        })?;
        Ok(StatusCode::NO_CONTENT)
    }).await
}

async fn metrics(State(shared): State<Shared>) -> Result<Response, QueueError> {
    access(shared, |broker| {
        broker.expire()?;
        Ok(render_metrics(&broker.queue).into_response())
    }).await
}

fn render_metrics(queue: &Queue) -> impl IntoResponse {
    let mut pending = 0;
    let mut inflight = 0;
    for job in queue.jobs.values() {
        match job.phase {
            Phase::Pending => pending += job.request.token_budget(),
            Phase::Running { .. } => inflight += job.request.token_budget(),
            _ => {}
        }
    }
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        format!(
            "# TYPE inference_pending_tokens gauge\ninference_pending_tokens {pending}\n\
             # TYPE inference_inflight_tokens gauge\ninference_inflight_tokens {inflight}\n\
             # TYPE inference_completed_total counter\ninference_completed_total {}\n\
             # TYPE inference_failed_total counter\ninference_failed_total {}\n",
            queue.completed, queue.failed
        ),
    )
}
