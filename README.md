# Queue based GPU autoscaler

## What it is

Scales Kubernetes GPU workers using the token budget of queued and leased jobs.

## How to run

Start the local queue and demo worker from the repo root.

```sh
cargo run --locked -- demo
```

In another terminal, submit a job, then poll its status. Replace `1` with the returned `job_id`.

```sh
curl -sS http://127.0.0.1:8080/jobs -H 'Content-Type: application/json' \
  -d '{"prompt":"Explain continuous batching","prompt_tokens":4,"max_tokens":128}'
curl -sS http://127.0.0.1:8080/jobs/1
```

## Other things

[Deployment and queue reference](docs/reference.md)
