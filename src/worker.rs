use crate::{Result, queue::{Finish, Lease, Outcome, Request}};
use serde::Deserialize;
use std::time::Duration;
use tokio::{sync::watch, time::sleep};

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
                Ok(output) => Outcome::Done { output },
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
        let response = client
            .post(format!("{base}/jobs/{}/finish", lease.job_id))
            .json(&finish)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status);
        if let Err(error) = response {
            eprintln!("could not acknowledge job {}: {error}", lease.job_id);
        }
    }
    Ok(())
}
