mod queue;
mod snapshot;
mod worker;

use std::{error::Error, net::SocketAddr};
use tokio::{net::TcpListener, sync::watch};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

pub fn setting(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "demo".to_owned());
    match mode.as_str() {
        "demo" | "queue" => {
            let app = queue::router(std::env::var_os("QUEUE_STATE_FILE").map(Into::into))?;
            let listen: SocketAddr = setting("QUEUE_LISTEN", "127.0.0.1:8080").parse()?;
            let listener = TcpListener::bind(listen).await?;
            let bound = listener.local_addr()?;
            eprintln!("queue listening on {bound}");
            let (stop, stop_rx) = watch::channel(false);
            let demo = (mode == "demo").then(|| {
                let url = format!("http://127.0.0.1:{}", bound.port());
                tokio::spawn(worker::run(url, None, stop_rx))
            });
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    signal().await;
                    let _ = stop.send(true);
                    if let Some(worker) = demo {
                        match worker.await {
                            Ok(Ok(())) => {},
                            Ok(Err(error)) => eprintln!("demo worker ended: {error}"),
                            Err(error) => eprintln!("demo worker task ended: {error}"),
                        }
                    }
                })
                .await?;
        }
        "worker" => {
            let endpoint = std::env::var("INFERENCE_URL")?;
            let runner = worker::Runner::new(endpoint, std::env::var("INFERENCE_MODEL")?)?;
            let (stop, stop_rx) = watch::channel(false);
            tokio::spawn(async move {
                signal().await;
                let _ = stop.send(true);
            });
            worker::run(setting("QUEUE_URL", "http://127.0.0.1:8080"), Some(runner), stop_rx)
                .await?;
        }
        _ => return Err("usage: gpu-autoscaler [demo|queue|worker]".into()),
    }
    Ok(())
}

async fn signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut term) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = term.recv() => {},
                _ = tokio::signal::ctrl_c() => {},
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
