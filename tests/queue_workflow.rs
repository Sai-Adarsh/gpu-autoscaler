#![cfg(unix)]

use axum::{
    Json, Router,
    extract::Path,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
};
use serde_json::{Value, json};
use std::{
    fs,
    net::TcpListener as StdListener,
    path::PathBuf,
    process::{Child, Command},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    time::{sleep, timeout},
};

const BIN: &str = env!("CARGO_BIN_EXE_gpu-autoscaler");

struct ChildProcess(Child);

impl ChildProcess {
    fn start(mode: &str, env: &[(&str, &str)]) -> Self {
        let child = Command::new(BIN)
            .arg(mode)
            .env_remove("QUEUE_STATE_FILE")
            .env_remove("INFERENCE_API_KEY")
            .env("TOKIO_WORKER_THREADS", "2")
            .envs(env.iter().copied())
            .spawn()
            .unwrap();
        Self(child)
    }
}

impl Drop for ChildProcess {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .args(["-TERM", &self.0.id().to_string()])
            .status();
        for _ in 0..300 {
            if self.0.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Files(PathBuf);

impl Files {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "gpu-autoscaler-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Files {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn cli_rejects_unknown_modes_and_corrupt_queue_state() {
    let invalid = Command::new(BIN).arg("invalid-mode").output().unwrap();
    assert!(!invalid.status.success());
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("usage:"));
    let dir = Files::new();
    let state = dir.0.join("corrupt.json");
    fs::write(&state, "{").unwrap();
    let invalid = Command::new(BIN)
        .arg("queue")
        .env("QUEUE_STATE_FILE", state)
        .output()
        .unwrap();
    assert!(!invalid.status.success());
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("open queue state"));
}

struct Broker {
    addr: String,
    base: String,
    client: reqwest::Client,
}

impl Broker {
    fn new() -> Self {
        let reservation = StdListener::bind("127.0.0.1:0").unwrap();
        let addr = reservation.local_addr().unwrap().to_string();
        Self {
            base: format!("http://{addr}"),
            addr,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
        }
    }

    async fn start(&self, mode: &str, state: Option<&str>) -> ChildProcess {
        let mut env = vec![("QUEUE_LISTEN", self.addr.as_str())];
        if let Some(state) = state {
            env.push(("QUEUE_STATE_FILE", state));
        }
        let mut child = ChildProcess::start(mode, &env);
        timeout(Duration::from_secs(10), async {
            loop {
                assert!(
                    child.0.try_wait().unwrap().is_none(),
                    "queue exited before listening"
                );
                if self
                    .client
                    .get(format!("{}/healthz", self.base))
                    .send()
                    .await
                    .is_ok_and(|response| response.status() == StatusCode::OK)
                {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("queue failed to start");
        child
    }

    async fn post(&self, path: &str, body: Value) -> reqwest::Response {
        self.client
            .post(format!("{}{path}", self.base))
            .json(&body)
            .send()
            .await
            .unwrap()
    }

    async fn get(&self, path: &str) -> reqwest::Response {
        self.client
            .get(format!("{}{path}", self.base))
            .send()
            .await
            .unwrap()
    }

    async fn submit(&self, prompt: &str) -> u64 {
        let response = self
            .post(
                "/jobs",
                json!({
                    "prompt": prompt, "prompt_tokens": 9, "max_tokens": 15
                }),
            )
            .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        response.json::<Value>().await.unwrap()["job_id"]
            .as_u64()
            .unwrap()
    }
}

#[tokio::test]
async fn restarts_recover_jobs_and_fence_old_leases() {
    let files = Files::new();
    let snapshot = files.0.join("state.json");
    let state = snapshot.to_str().unwrap();
    let broker = Broker::new();
    let first = broker.start("queue", Some(state)).await;
    let id = broker.submit("retry").await;
    let metrics = broker.get("/metrics").await.text().await.unwrap();
    assert!(metrics.contains("inference_pending_tokens 24"));
    assert!(metrics.contains("inference_inflight_tokens 0"));
    let path = format!("/jobs/{id}/finish");
    let claim = broker
        .post("/claim", Value::Null)
        .await
        .json::<Value>()
        .await
        .unwrap();
    let lease = claim["lease_id"].as_u64().unwrap();
    assert_eq!(claim["job_id"], id);
    let metrics = broker.get("/metrics").await.text().await.unwrap();
    assert!(metrics.contains("inference_pending_tokens 0"));
    assert!(metrics.contains("inference_inflight_tokens 24"));
    let retry = json!({"lease_id": lease, "outcome": {"status": "retry"}});
    assert_eq!(
        broker.post(&path, retry.clone()).await.status(),
        StatusCode::NO_CONTENT
    );
    drop(first);
    let second = broker.start("queue", Some(state)).await;
    assert_eq!(
        broker.post(&path, retry.clone()).await.status(),
        StatusCode::NO_CONTENT
    );
    let claim = broker
        .post("/claim", Value::Null)
        .await
        .json::<Value>()
        .await
        .unwrap();
    assert!(claim["lease_id"].as_u64().unwrap() > lease);
    assert_eq!(
        broker.post(&path, retry).await.status(),
        StatusCode::CONFLICT
    );
    drop(second);
    let third = broker.start("queue", Some(state)).await;
    let claim = broker
        .post("/claim", Value::Null)
        .await
        .json::<Value>()
        .await
        .unwrap();
    let last_lease = claim["lease_id"].as_u64().unwrap();
    let last = json!({"lease_id": last_lease, "outcome": {"status": "retry"}});
    assert_eq!(
        broker.post(&path, last.clone()).await.status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        broker.post(&path, last).await.status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        broker
            .get(&format!("/jobs/{id}"))
            .await
            .json::<Value>()
            .await
            .unwrap()["status"],
        "failed"
    );
    assert!(
        broker
            .get("/metrics")
            .await
            .text()
            .await
            .unwrap()
            .contains("inference_failed_total 1")
    );
    drop(third);
    let _restarted = broker.start("queue", Some(state)).await;
    assert_eq!(
        broker
            .get(&format!("/jobs/{id}"))
            .await
            .json::<Value>()
            .await
            .unwrap()["status"],
        "failed"
    );
}

#[tokio::test]
async fn replays_submission_after_restart_without_queuing_twice() {
    let files = Files::new();
    let snapshot = files.0.join("state.json");
    let state = snapshot.to_str().unwrap();
    let broker = Broker::new();
    let queue = broker.start("queue", Some(state)).await;
    let request = json!({"prompt":"complete once", "prompt_tokens":9, "max_tokens":15});
    let submit = |key: &'static str, body: Value| {
        broker
            .client
            .post(format!("{}/jobs", broker.base))
            .header("idempotency-key", key)
            .json(&body)
            .send()
    };
    let response = submit("checkout-123", request.clone()).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let first = response.json::<Value>().await.unwrap();
    drop(queue);

    let _restarted = broker.start("queue", Some(state)).await;
    let response = submit("checkout-123", request.clone()).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(response.json::<Value>().await.unwrap(), first);
    let mut different = request.clone();
    different["max_tokens"] = json!(16);
    assert_eq!(
        submit("checkout-123", different).await.unwrap().status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        submit("contains space", request.clone())
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );

    let lease = broker
        .post("/claim", Value::Null)
        .await
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(lease["job_id"], first["job_id"]);
    assert!(
        broker
            .post("/claim", Value::Null)
            .await
            .json::<Value>()
            .await
            .unwrap()
            .is_null()
    );
    let path = format!("/jobs/{}/finish", first["job_id"]);
    let done = json!({"lease_id":lease["lease_id"],"outcome":{"status":"done","output":"once"}});
    assert_eq!(
        broker.post(&path, done).await.status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        submit("checkout-123", request)
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap(),
        first
    );
    assert!(
        broker
            .get("/metrics")
            .await
            .text()
            .await
            .unwrap()
            .contains("inference_completed_total 1")
    );
}

#[tokio::test]
async fn worker_retries_one_output_through_busy_and_uncertain_acknowledgments() {
    let files = Files::new();
    let snapshot = files.0.join("state.json");
    let state = snapshot.to_str().unwrap();
    let broker = Broker::new();
    let queue = broker.start("queue", Some(state)).await;
    let id = broker.submit("complete").await;
    let generation_calls = Arc::new(AtomicUsize::new(0));
    let claims = Arc::new(AtomicUsize::new(0));
    let finishes = Arc::new(Mutex::new(Vec::<Value>::new()));
    let counts = generation_calls.clone();
    let claim_count = claims.clone();
    let history = finishes.clone();
    let broker_base = broker.base.clone();
    let claim_url = format!("{broker_base}/claim");
    let proxy = Router::new()
        .route(
            "/claim",
            post(move || {
                let url = claim_url.clone();
                let claims = claim_count.clone();
                async move {
                    let response = reqwest::Client::new()
                        .post(url)
                        .send()
                        .await
                        .unwrap()
                        .json::<Value>()
                        .await
                        .unwrap();
                    claims.fetch_add(1, Ordering::Relaxed);
                    Json(response)
                }
            }),
        )
        .route(
            "/jobs/{id}/finish",
            post(move |Path(id): Path<u64>, Json(body): Json<Value>| {
                let url = format!("{broker_base}/jobs/{id}/finish");
                let history = history.clone();
                async move {
                    let attempt = {
                        let mut history = history.lock().unwrap();
                        history.push(body.clone());
                        history.len()
                    };
                    if attempt == 1 {
                        return (StatusCode::SERVICE_UNAVAILABLE, [("Retry-After", "1")])
                            .into_response();
                    }
                    let status = reqwest::Client::new()
                        .post(url)
                        .json(&body)
                        .send()
                        .await
                        .unwrap()
                        .status();
                    assert_eq!(status, StatusCode::NO_CONTENT);
                    if attempt == 2 {
                        return StatusCode::BAD_GATEWAY.into_response();
                    }
                    status.into_response()
                }
            }),
        )
        .route(
            "/generate",
            post(move |headers: HeaderMap, Json(body): Json<Value>| {
                let counts = counts.clone();
                async move {
                    assert_eq!(headers["authorization"], "Bearer local-fixture");
                    assert_eq!(body["messages"][0]["content"], "complete");
                    counts.fetch_add(1, Ordering::Relaxed);
                    Json(json!({"choices": [{"message": {"content": "answer"}}]}))
                }
            }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("http://{}", listener.local_addr().unwrap());
    let proxy = tokio::spawn(async move { axum::serve(listener, proxy).await.unwrap() });
    let inference = format!("{proxy_url}/generate");
    let worker = ChildProcess::start(
        "worker",
        &[
            ("QUEUE_URL", &proxy_url),
            ("INFERENCE_URL", &inference),
            ("INFERENCE_MODEL", "fixture"),
            ("INFERENCE_API_KEY", "local-fixture"),
        ],
    );
    timeout(Duration::from_secs(8), async {
        while claims.load(Ordering::Relaxed) < 2 {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let replay = {
        let payloads = finishes.lock().unwrap();
        assert_eq!(payloads.len(), 3);
        assert!(payloads.iter().all(|payload| *payload == payloads[0]));
        payloads[0].clone()
    };
    assert_eq!(generation_calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        broker
            .get(&format!("/jobs/{id}"))
            .await
            .json::<Value>()
            .await
            .unwrap(),
        json!({"status": "done", "output": "answer"})
    );
    assert!(
        broker
            .get("/metrics")
            .await
            .text()
            .await
            .unwrap()
            .contains("inference_completed_total 1")
    );

    // Closing the subprocesses must leave the same receipt in the replacement broker.
    drop(worker);
    drop(queue);
    let restarted = broker.start("queue", Some(state)).await;
    let path = format!("/jobs/{id}/finish");
    assert_eq!(
        broker.post(&path, replay.clone()).await.status(),
        StatusCode::NO_CONTENT
    );
    let mut wrong_lease = replay.clone();
    wrong_lease["lease_id"] = json!(999);
    assert_eq!(
        broker.post(&path, wrong_lease).await.status(),
        StatusCode::CONFLICT
    );
    let mut wrong_outcome = replay.clone();
    wrong_outcome["outcome"] = json!({"status": "retry"});
    assert_eq!(
        broker.post(&path, wrong_outcome).await.status(),
        StatusCode::CONFLICT
    );
    let mut changed = replay;
    changed["outcome"]["output"] = json!("different");
    assert_eq!(
        broker.post(&path, changed).await.status(),
        StatusCode::CONFLICT
    );
    drop(restarted);
    proxy.abort();
}

#[tokio::test]
async fn demo_rejects_invalid_requests_and_finishes_valid_jobs() {
    let broker = Broker::new();
    let _queue = broker.start("demo", None).await;
    assert_eq!(
        broker.get("/jobs/123").await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        broker
            .post(
                "/jobs",
                json!({"prompt":"", "prompt_tokens":9, "max_tokens":15})
            )
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
    let id = broker.submit("demo").await;
    let path = format!("/jobs/{id}");
    timeout(Duration::from_secs(5), async {
        loop {
            let result = broker.get(&path).await.json::<Value>().await.unwrap();
            if result["status"] == "done" {
                assert_eq!(result["output"], "Local fixture completed the request.");
                break;
            }
            sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn worker_retries_when_upstream_response_exceeds_the_body_limit() {
    let broker = Broker::new();
    let _queue = broker.start("queue", None).await;
    let id = broker.submit("complete").await;
    let calls = Arc::new(AtomicUsize::new(0));
    let upstream = Router::new().route(
        "/generate",
        post({
            let calls = calls.clone();
            move || {
                let calls = calls.clone();
                async move {
                    if calls.fetch_add(1, Ordering::Relaxed) == 0 {
                        return Json(json!({
                            "choices": [{"message": {"content": "oversized response"}}],
                            "provider_metadata": "x".repeat(1024 * 1024),
                        }));
                    }
                    Json(json!({"choices": [{"message": {"content": "accepted answer"}}]}))
                }
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let inference_url = format!("http://{}/generate", listener.local_addr().unwrap());
    let upstream = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
    let _worker = ChildProcess::start(
        "worker",
        &[
            ("QUEUE_URL", &broker.base),
            ("INFERENCE_URL", &inference_url),
            ("INFERENCE_MODEL", "fixture"),
        ],
    );
    let result = timeout(Duration::from_secs(8), async {
        loop {
            let result: Value = broker.get(&format!("/jobs/{id}")).await.json().await.unwrap();
            if result["status"] == "done" || result["status"] == "failed" {
                break result;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(result, json!({"status": "done", "output": "accepted answer"}));
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    upstream.abort();
}
