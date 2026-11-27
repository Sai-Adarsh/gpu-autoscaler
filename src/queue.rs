use axum::{
    Json, Router,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const LEASE_DURATION: Duration = Duration::from_secs(100);
const MAX_ATTEMPTS: u8 = 3;
const ACTIVE_LIMIT: usize = 2_048;
const HISTORY_LIMIT: usize = 128;

#[derive(Clone, Deserialize, Serialize)]
pub struct Request {
    pub prompt: String,
    pub prompt_tokens: u32,
    pub max_tokens: u32,
}

impl Request {
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

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Phase {
    Pending,
    Running {
        #[serde(skip)]
        lease_id: u64,
        #[serde(skip)]
        expires: Instant,
    },
    Done { output: String },
    Failed,
}

struct Job {
    request: Request,
    phase: Phase,
    attempts: u8,
}

#[derive(Default)]
struct Queue {
    jobs: HashMap<u64, Job>,
    pending: VecDeque<u64>,
    history: VecDeque<u64>,
    job_id: u64,
    lease_id: u64,
    completed: u64,
    failed: u64,
}

impl Queue {
    fn expire(&mut self) {
        let now = Instant::now();
        let expired: Vec<u64> = self.jobs.iter().filter_map(|(&id, job)| {
            matches!(&job.phase, Phase::Running { expires, .. } if *expires <= now).then_some(id)
        }).collect();
        for id in expired {
            self.retry(id);
        }
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

type Shared = Arc<Mutex<Queue>>;

pub fn router() -> Router {
    Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/metrics", get(metrics))
        .route("/jobs", post(submit))
        .route("/jobs/{id}", get(status))
        .route("/claim", post(claim))
        .route("/jobs/{id}/finish", post(finish))
        .with_state(Shared::default())
}

async fn submit(State(shared): State<Shared>, Json(request): Json<Request>) -> Response {
    if request.prompt.trim().is_empty() || request.prompt.len() > 64 * 1024
        || !(1..=32_768).contains(&request.prompt_tokens)
        || !(1..=4_096).contains(&request.max_tokens)
    {
        return (StatusCode::BAD_REQUEST, "invalid prompt or token budget").into_response();
    }
    let mut queue = shared.lock().expect("queue lock");
    if queue.jobs.len() - queue.history.len() >= ACTIVE_LIMIT {
        return (StatusCode::TOO_MANY_REQUESTS, "queue full").into_response();
    }
    queue.job_id += 1;
    let id = queue.job_id;
    queue.jobs.insert(id, Job { request, phase: Phase::Pending, attempts: 0 });
    queue.pending.push_back(id);
    (StatusCode::ACCEPTED, Json(serde_json::json!({ "job_id": id }))).into_response()
}

async fn status(State(shared): State<Shared>, Path(id): Path<u64>) -> Response {
    let mut queue = shared.lock().expect("queue lock");
    queue.expire();
    match queue.jobs.get(&id) {
        Some(job) => Json(&job.phase).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn claim(State(shared): State<Shared>) -> Json<Option<Lease>> {
    let mut queue = shared.lock().expect("queue lock");
    queue.expire();
    let Some(job_id) = queue.pending.pop_front() else {
        return Json(None);
    };
    queue.lease_id += 1;
    let lease_id = queue.lease_id;
    let job = queue.jobs.get_mut(&job_id).expect("pending job exists");
    job.attempts += 1;
    job.phase = Phase::Running { lease_id, expires: Instant::now() + LEASE_DURATION };
    Json(Some(Lease { job_id, lease_id, request: job.request.clone() }))
}

async fn finish(
    State(shared): State<Shared>,
    Path(id): Path<u64>,
    Json(finish): Json<Finish>,
) -> StatusCode {
    let mut queue = shared.lock().expect("queue lock");
    queue.expire();
    let Some(job) = queue.jobs.get_mut(&id) else {
        return StatusCode::NOT_FOUND;
    };
    if !matches!(&job.phase, Phase::Running { lease_id, .. } if *lease_id == finish.lease_id) {
        return StatusCode::CONFLICT;
    }
    match finish.outcome {
        Outcome::Done { output } if output.len() <= 64 * 1024 => {
            job.phase = Phase::Done { output };
            queue.completed += 1;
            queue.retain_result(id);
        }
        _ => queue.retry(id),
    }
    StatusCode::NO_CONTENT
}

async fn metrics(State(shared): State<Shared>) -> impl IntoResponse {
    let mut queue = shared.lock().expect("queue lock");
    queue.expire();
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
