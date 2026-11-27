use crate::{Result, queue::{Finish, LEASE_DURATION, Lease, MAX_OUTPUT_BYTES, Outcome, Request}};
use reqwest::{StatusCode, header::RETRY_AFTER};
use serde::Deserialize;
use std::time::Duration;
use tokio::{sync::watch, time::{Instant, sleep, timeout_at}};

const FINISH_RETRY_WINDOW: Duration = Duration::from_secs(20);

pub struct Runner {
    client: reqwest::Client,
    endpoint: String,
    model: String,
}

#[derive(Deserialize)]
struct Completion {
    choices: Vec<Choice>,
}

#[derive(Deserialize)]
struct Choice {
    message: Message,
}

#[derive(Deserialize)]
struct Message {
    content: String,
}

impl Runner {
    pub fn new(endpoint: String, model: String) -> Result<Self> {
        Ok(Self {
            endpoint,
            model,
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(75))
                .build()?,
        })
    }

    async fn generate(&self, request: &Request) -> Result<String> {
        let mut call = self.client.post(&self.endpoint).json(&serde_json::json!({
            "model": self.model,
            "messages": [{ "role": "user", "content": request.prompt }],
            "max_tokens": request.max_tokens,
            "stream": false
        }));
        if let Ok(key) = std::env::var("INFERENCE_API_KEY") {
            call = call.bearer_auth(key);
        }
        let result: Completion = call.send().await?.error_for_status()?.json().await?;
        Ok(result.choices.into_iter().next().ok_or("no choices")?.message.content)
    }
}

pub async fn run(base: String, runner: Option<Runner>, mut stop: watch::Receiver<bool>) -> Result<()> {
    let base = base.trim_end_matches('/');
    let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?;
    while !*stop.borrow() {
        let claim_started = Instant::now();
        let result = async {
            client.post(format!("{base}/claim")).send().await?.error_for_status()?
                .json::<Option<Lease>>().await
        }.await;
        let lease = match result {
            Ok(Some(lease)) => lease,
            empty_or_error => {
                if let Err(error) = empty_or_error {
                    eprintln!("claim failed: {error}");
                }
                tokio::select! {
                    _ = sleep(Duration::from_secs(1)) => {},
                    _ = stop.changed() => {},
                }
                continue;
            }
        };

        let draining = *stop.borrow();
        let outcome = if draining {
            Outcome::Retry
        } else if let Some(runner) = &runner {
            match runner.generate(&lease.request).await {
                Ok(output) if output.len() <= MAX_OUTPUT_BYTES => Outcome::Done { output },
                Ok(_) => {
                    eprintln!("inference exceeded the output limit for job {}", lease.job_id);
                    Outcome::Retry
                }
                Err(error) => {
                    eprintln!("inference failed for job {}: {error}", lease.job_id);
                    Outcome::Retry
                }
            }
        } else {
            sleep(Duration::from_millis(150)).await;
            Outcome::Done { output: "Local fixture completed the request.".to_owned() }
        };

        let finish = Finish { lease_id: lease.lease_id, outcome };
        // A claim starts its lease no earlier than receipt at the broker. Using
        // the request start is conservative and avoids cross-machine clocks.
        let deadline = (claim_started + LEASE_DURATION).min(Instant::now() + FINISH_RETRY_WINDOW);
        acknowledge(&client, base, lease.job_id, &finish, deadline).await;
    }
    Ok(())
}

async fn acknowledge(
    client: &reqwest::Client,
    base: &str,
    job_id: u64,
    finish: &Finish,
    deadline: Instant,
) {
    let url = format!("{base}/jobs/{job_id}/finish");
    let mut backoff = Duration::from_millis(100);
    let stagger = Duration::from_millis(finish.lease_id % 151);
    while Instant::now() < deadline {
        let request = client.post(&url).json(finish).send();
        let (reason, retry_after) = match timeout_at(deadline, request).await {
            Ok(Ok(response)) if response.status() == StatusCode::NO_CONTENT => return,
            Ok(Ok(response)) if response.status() == StatusCode::CONFLICT => {
                eprintln!("finish rejected for stale lease {} on job {job_id}", finish.lease_id);
                return;
            }
            Ok(Ok(response)) if matches!(response.status(),
                StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
                | StatusCode::INTERNAL_SERVER_ERROR | StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
            ) => {
                let after = response.headers().get(RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok())
                    .map(Duration::from_secs);
                (format!("HTTP {}", response.status().as_u16()), after)
            }
            Ok(Ok(response)) => {
                eprintln!("finish rejected for job {job_id}: HTTP {}", response.status().as_u16());
                return;
            }
            Ok(Err(_)) => ("transport failure".to_owned(), None),
            Err(_) => break,
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        let delay = retry_after.unwrap_or_default().max(backoff).saturating_add(stagger);
        if delay >= remaining {
            break;
        }
        eprintln!("retrying finish for job {job_id} lease {} after {reason}", finish.lease_id);
        sleep(delay).await;
        backoff = (backoff * 2).min(Duration::from_secs(1));
    }
    eprintln!("finish outcome unknown for job {job_id} lease {}: retry window expired", finish.lease_id);
}
