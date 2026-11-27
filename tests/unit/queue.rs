use super::*;
use axum::body::to_bytes;
use std::{
    fs,
    sync::atomic::{AtomicUsize, Ordering},
};

struct StateDir(PathBuf);

impl StateDir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "autoscaler-state-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        Self(dir)
    }

    fn snapshot(&self) -> PathBuf {
        self.0.join("queue.json")
    }
}

impl Drop for StateDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn request() -> Request {
    Request {
        prompt: "describe the fixture".into(),
        prompt_tokens: 7,
        max_tokens: 11,
    }
}

fn running(lease_id: u64, attempts: u8, expires: Instant) -> Job {
    Job {
        request: request(),
        phase: Phase::Running { lease_id, expires },
        attempts,
        last_finish: None,
        idempotency_key: None,
    }
}

fn shared(broker: Broker, admission: Option<Arc<Semaphore>>) -> Shared {
    Shared {
        broker: Arc::new(Mutex::new(broker)),
        admission,
    }
}

#[test]
fn expires_jobs_in_stable_order_and_retains_only_the_recent_terminal_history() {
    let now = Instant::now();
    let mut queue = Queue::default();
    queue.jobs.insert(2, running(2, 1, now));
    queue.jobs.insert(1, running(1, 1, now));
    queue.jobs.insert(3, running(3, MAX_ATTEMPTS, now));
    assert!(queue.expire(now));
    assert_eq!(queue.pending, [1, 2]);
    assert_eq!(queue.history, [3]);
    assert_eq!(queue.failed, 1);
    assert!(!queue.expire(now));

    for id in 4..(4 + HISTORY_LIMIT as u64) {
        queue.jobs.insert(id, running(id, MAX_ATTEMPTS, now));
        queue.retry(id);
    }
    assert_eq!(queue.history.len(), HISTORY_LIMIT);
    assert!(!queue.jobs.contains_key(&3));
    assert_eq!(queue.failed, 1 + HISTORY_LIMIT as u64);
}

#[cfg(unix)]
#[test]
fn rejects_snapshots_with_inconsistent_job_counters() {
    let dir = StateDir::new();
    let snapshot = dir.snapshot();
    let mut job = running(1, 1, Instant::now());
    job.phase = Phase::Done { output: "fixture".into() };
    let mut queue = Queue {
        jobs: [(1, job)].into(),
        history: [1].into(),
        job_id: 1,
        lease_id: 1,
        completed: 1,
        ..Queue::default()
    };
    fs::write(&snapshot, serde_json::json!({"version": 1, "queue": queue}).to_string()).unwrap();
    assert!(Broker::open(Some(snapshot.clone())).is_ok());

    for (job_id, completed, failed) in [(1, 0, 1), (2, 1, 0), (u64::MAX, u64::MAX, 1)] {
        queue.job_id = job_id;
        queue.completed = completed;
        queue.failed = failed;
        fs::write(&snapshot, serde_json::json!({"version": 1, "queue": queue}).to_string()).unwrap();
        let error = Broker::open(Some(snapshot.clone())).err()
            .expect("inconsistent counters must prevent startup");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}

#[test]
fn rejects_malformed_and_incompatible_persisted_state() {
    let dir = StateDir::new();
    let snapshot = dir.snapshot();
    fs::write(&snapshot, "{").unwrap();
    assert_eq!(
        Broker::open(Some(snapshot.clone())).err().unwrap().kind(),
        io::ErrorKind::InvalidData
    );
    let queue = Queue::default();
    fs::write(
        &snapshot,
        serde_json::json!({"version": 10, "queue": queue}).to_string(),
    )
    .unwrap();
    let error = Broker::open(Some(snapshot.clone())).err().unwrap();
    assert!(
        error
            .to_string()
            .contains("unsupported queue snapshot version 10")
    );
    let mut queue = Queue {
        pending: [1].into(),
        ..Queue::default()
    };
    fs::write(
        &snapshot,
        serde_json::json!({"version": 1, "queue": queue}).to_string(),
    )
    .unwrap();
    let error = Broker::open(Some(snapshot.clone())).err().unwrap();
    assert!(error.to_string().contains("inconsistent queue snapshot"));
    queue.pending.clear();
    queue.job_id = 1;
    queue.lease_id = 2;
    let mut job = running(1, 1, Instant::now());
    job.last_finish = Some(FinishReceipt {
        lease_id: 2,
        done: false,
    });
    queue.jobs.insert(1, job);
    assert!(queue.validate_snapshot().is_err());
    queue.jobs.get_mut(&1).unwrap().attempts = MAX_ATTEMPTS + 1;
    assert!(queue.validate_snapshot().is_err());

    fs::remove_file(&snapshot).unwrap();
    fs::create_dir(&snapshot).unwrap();
    let error = Broker::open(Some(snapshot)).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}

#[cfg(unix)]
#[test]
fn second_broker_cannot_open_state_until_the_first_releases_its_lock() {
    let dir = StateDir::new();
    let snapshot = dir.snapshot();
    fs::write(dir.0.join("queue.json.tmp"), "old staged file").unwrap();
    let first = Broker::open(Some(snapshot.clone())).unwrap();
    assert!(!dir.0.join("queue.json.tmp").exists());
    let error = Broker::open(Some(snapshot.clone())).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    drop(first);
    assert!(Broker::open(Some(snapshot)).is_ok());
    assert!(Store::open(PathBuf::from("/")).is_err());
    fs::create_dir(dir.0.join("queue.json.tmp")).unwrap();
    assert!(Broker::open(Some(dir.snapshot())).is_err());
}

#[tokio::test]
async fn claim_requeues_an_expired_job_under_a_fresh_lease() {
    let mut broker = Broker::open(None).unwrap();
    broker.queue.job_id = 1;
    broker.queue.lease_id = 1;
    broker.queue.jobs.insert(1, running(1, 1, Instant::now()));
    let state = shared(broker, None);
    let Json(result) = claim(State(state.clone())).await.ok().unwrap();
    let new = result.unwrap();
    assert_eq!(new.job_id, 1);
    assert_eq!(new.lease_id, 2);
    let old = Finish {
        lease_id: 1,
        outcome: Outcome::Done {
            output: "stale".into(),
        },
    };
    assert_eq!(
        finish(State(state), Path(1), Json(old)).await.ok(),
        Some(StatusCode::CONFLICT)
    );
}

#[cfg(unix)]
#[tokio::test]
async fn directory_sync_failure_after_rename_fences_broker_until_restart() {
    use std::os::{fd::OwnedFd, unix::net::UnixStream};

    let dir = StateDir::new();
    let snapshot = dir.snapshot();
    let mut broker = Broker::open(Some(snapshot.clone())).unwrap();
    let (socket, _peer) = UnixStream::pair().unwrap();
    let replacement = OwnedFd::from(socket);
    crate::snapshot::tests::replace_directory_sync_handle(
        broker.store.as_mut().unwrap(),
        replacement.into(),
    );
    let state = shared(broker, None);
    let failure = submit(State(state.clone()), HeaderMap::new(), Json(request()))
        .await
        .err()
        .unwrap()
        .into_response();
    assert_eq!(failure.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = to_bytes(failure.into_body(), 4096).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"],
        "queue_commit_outcome_unknown"
    );
    let unavailable = health(State(state.clone()))
        .await
        .err()
        .unwrap()
        .into_response();
    assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(state.broker.lock().unwrap().queue.jobs.is_empty());
    drop(state);
    let reopened = Broker::open(Some(snapshot)).unwrap();
    assert!(reopened.queue.jobs.contains_key(&1));
}

#[tokio::test]
async fn unavailable_storage_preserves_mutation_and_semaphore_backpressure() {
    let dir = StateDir::new();
    let snapshot = dir.snapshot();
    let broker = Broker::open(Some(snapshot.clone())).unwrap();
    fs::create_dir(&snapshot).unwrap();
    let gate = Arc::new(Semaphore::new(1));
    let state = shared(broker, Some(gate.clone()));
    let permit = gate.try_acquire().unwrap();
    let blocked = health(State(state.clone()))
        .await
        .err()
        .unwrap()
        .into_response();
    assert_eq!(blocked.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(blocked.headers()[header::RETRY_AFTER], "1");
    drop(permit);

    let result = submit(State(state.clone()), HeaderMap::new(), Json(request()))
        .await
        .err()
        .unwrap()
        .into_response();
    assert_eq!(result.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(state.broker.lock().unwrap().queue.jobs.is_empty());
    assert!(!dir.0.join("queue.json.tmp").exists());
    fs::remove_dir(&snapshot).unwrap();
    let result = submit(State(state), HeaderMap::new(), Json(request()))
        .await
        .ok()
        .unwrap();
    assert_eq!(result.status(), StatusCode::ACCEPTED);
    let body = to_bytes(result.into_body(), 4096).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["job_id"],
        1
    );
}

#[tokio::test]
async fn validates_finish_before_mutating_active_job_and_caps_queue_admission() {
    let mut broker = Broker::open(None).unwrap();
    broker
        .queue
        .jobs
        .insert(1, running(1, 1, Instant::now() + LEASE_DURATION));
    let state = shared(broker, None);
    let too_big = Finish {
        lease_id: 1,
        outcome: Outcome::Done {
            output: "x".repeat(MAX_OUTPUT_BYTES + 1),
        },
    };
    assert_eq!(
        finish(State(state.clone()), Path(1), Json(too_big))
            .await
            .ok(),
        Some(StatusCode::UNPROCESSABLE_ENTITY)
    );
    let retry = Finish {
        lease_id: 999,
        outcome: Outcome::Retry,
    };
    assert_eq!(
        finish(State(state.clone()), Path(999), Json(retry))
            .await
            .ok(),
        Some(StatusCode::NOT_FOUND)
    );
    {
        let mut broker = state.broker.lock().unwrap();
        assert!(matches!(broker.queue.jobs[&1].phase, Phase::Running { .. }));
        for id in 2..=ACTIVE_LIMIT as u64 {
            broker.queue.jobs.insert(
                id,
                Job {
                    request: request(),
                    phase: Phase::Pending,
                    attempts: 0,
                    last_finish: None,
                    idempotency_key: None,
                },
            );
        }
    }
    let response = submit(State(state), HeaderMap::new(), Json(request()))
        .await
        .ok()
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
}
