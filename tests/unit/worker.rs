use super::*;
use axum::{Json, Router, response::IntoResponse, routing::post};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::net::TcpListener;

async fn serve(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (base, task)
}

#[tokio::test]
async fn stops_retrying_stale_permanent_and_past_deadline_finishes() {
    let finish = Finish {
        lease_id: 1,
        outcome: Outcome::Retry,
    };
    let client = reqwest::Client::new();
    for status in [
        StatusCode::CONFLICT,
        StatusCode::BAD_REQUEST,
        StatusCode::SERVICE_UNAVAILABLE,
    ] {
        let count = Arc::new(AtomicUsize::new(0));
        let calls = count.clone();
        let router = Router::new().route(
            "/jobs/1/finish",
            post(move || {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::Relaxed);
                    (status, [("retry-after", "9")])
                }
            }),
        );
        let (base, task) = serve(router).await;
        acknowledge(
            &client,
            &base,
            1,
            &finish,
            Instant::now() + Duration::from_secs(1),
        )
        .await;
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        task.abort();
    }
    let router = Router::new().route(
        "/jobs/1/finish",
        post(|| async { std::future::pending::<StatusCode>().await }),
    );
    let (base, task) = serve(router).await;
    acknowledge(
        &client,
        &base,
        1,
        &finish,
        Instant::now() + Duration::from_millis(100),
    )
    .await;
    task.abort();
    task.await.unwrap_err();
    acknowledge(
        &client,
        &base,
        1,
        &finish,
        Instant::now() + Duration::from_millis(100),
    )
    .await;
}

#[tokio::test]
async fn returns_lease_when_draining_and_retries_large_or_invalid_model_responses() {
    for (draining, oversized) in [(true, false), (false, true), (false, false)] {
        let (stop, receiver) = watch::channel(false);
        let stop = Arc::new(stop);
        let claim_stop = stop.clone();
        let completed = Arc::new(Mutex::new(Vec::<Value>::new()));
        let captured = completed.clone();
        let requests = Arc::new(AtomicUsize::new(0));
        let calls = requests.clone();
        let router = Router::new()
            .route("/claim", post(move || {
                let stop = claim_stop.clone();
                async move {
                    if draining {
                        stop.send(true).unwrap();
                    }
                    Json(json!({
                        "job_id":1, "lease_id":9,
                        "request":{"prompt":"fixture", "prompt_tokens":9, "max_tokens":30}
                    }))
                }
            }))
            .route("/jobs/1/finish", post(move |Json(body): Json<Value>| {
                let completed = completed.clone();
                let stop = stop.clone();
                async move {
                    completed.lock().unwrap().push(body);
                    let _ = stop.send(true);
                    StatusCode::NO_CONTENT
                }
            }))
            .route("/generate", post(move || {
                let requests = requests.clone();
                async move {
                    requests.fetch_add(1, Ordering::Relaxed);
                    if oversized {
                        Json(json!({"choices":[{"message":{"content":"x".repeat(MAX_OUTPUT_BYTES + 1)}}]})).into_response()
                    } else {
                        Json(json!({"choices":[]})).into_response()
                    }
                }
            }));
        let (base, task) = serve(router).await;
        let runner = Runner::new(format!("{base}/generate"), "fixture".into()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), run(base, Some(runner), receiver))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            *captured.lock().unwrap(),
            [json!({"lease_id":9, "outcome":{"status":"retry"}})]
        );
        assert_eq!(calls.load(Ordering::Relaxed), usize::from(!draining));
        task.abort();
    }
}

#[tokio::test]
async fn failed_claim_does_not_create_a_finish_during_shutdown() {
    let (stop, receiver) = watch::channel(false);
    let stop = Arc::new(stop);
    let count = Arc::new(AtomicUsize::new(0));
    let finishes = count.clone();
    let router = Router::new()
        .route(
            "/claim",
            post(move || {
                let stop = stop.clone();
                async move {
                    stop.send(true).unwrap();
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }),
        )
        .route(
            "/jobs/{id}/finish",
            post(move || {
                let finishes = finishes.clone();
                async move {
                    finishes.fetch_add(1, Ordering::Relaxed);
                    StatusCode::NO_CONTENT
                }
            }),
        );
    let (base, task) = serve(router).await;
    tokio::time::timeout(Duration::from_secs(2), run(base, None, receiver))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count.load(Ordering::Relaxed), 0);
    task.abort();
}
