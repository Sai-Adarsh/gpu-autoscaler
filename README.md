# Queue based GPU autoscaler

The Rust broker measures queued and leased work in estimated prompt tokens plus the requested generation budget. Each worker leases one job at a time and calls an OpenAI-compatible inference endpoint. Prometheus scrapes the token count and KEDA scales the worker Deployment. On AWS, the Karpenter NodePool provisions Spot GPUs for those pods and consolidates empty nodes.

## Local queue and worker

```sh
cargo run --locked -- demo
```

Submit a job from another terminal. Use its returned ID to look up the result.

```sh
curl -sS http://127.0.0.1:8080/jobs -H 'Content-Type: application/json' \
  -d '{"prompt":"Explain continuous batching","prompt_tokens":4,"max_tokens":128}'
curl -sS http://127.0.0.1:8080/jobs/1
curl -sS http://127.0.0.1:8080/metrics
```

The demo worker returns a fixture response. For live inference, start the broker with `cargo run --locked -- queue` and run a separate worker with the full chat completions URL and model name:

```sh
QUEUE_URL=http://127.0.0.1:8080 \
  INFERENCE_URL=http://127.0.0.1:8081/v1/chat/completions INFERENCE_MODEL=worker-model \
  cargo run --locked -- worker
```

Set `INFERENCE_API_KEY` if the upstream requires authentication. On SIGTERM the worker stops taking leases, finishes its active job, and sends the result to the queue. If the worker dies, the lease expires after 100 seconds and the broker requeues the job for up to three total attempts. The broker requires the active lease ID when accepting a result, so a worker with an expired lease cannot complete a reassigned job.

During drain and normal operation, the worker retries the same finish payload on connection failures and transient HTTP responses, including a busy `503`. It honors `Retry-After` in seconds and spends at most 20 seconds acknowledging a result, bounded by its remaining lease time. A `409` ends the acknowledgement attempt. When a finish response is lost after the broker applied the result, replaying the same lease and outcome returns `204` without changing job state or counters. A different outcome or output conflicts. Retry receipts are cleared on reassignment, and completion receipts last as long as the retained result (the latest 128 terminal jobs).

## Persist the queue

On macOS or Linux, set `QUEUE_STATE_FILE` to keep queue state across broker restarts. Its parent directory must already exist and be writable by the broker.

```sh
mkdir -p "$HOME/.local/state/gpu-autoscaler"
QUEUE_STATE_FILE="$HOME/.local/state/gpu-autoscaler/queue.json" cargo run --locked -- queue
```

The broker syncs a replacement snapshot before acknowledging each change. A restart preserves queued jobs, retained results, metrics counters, and the next IDs. It expires outstanding leases and requeues those jobs under the existing retry limit. Delivery is at least once. A worker may still finish inference for a job that another worker receives after restart. The broker returns HTTP 409 for an acknowledgement under the old lease while it retains that job.

Only one broker can open a state file. Keep the directory private because snapshots contain prompts and completions. The broker reserves the sibling files with `.lock` and `.tmp` suffixes and removes any leftover temporary file after acquiring the lock. Use a filesystem that supports advisory locks, atomic renames, and directory sync. The broker refuses to start with an unreadable or inconsistent snapshot.

The durable broker admits one request at a time. Requests rejected while it is busy return HTTP 503 with `Retry-After: 1` before touching the queue. A write failure before rename returns HTTP 503 and leaves the committed state intact.

If directory sync fails after rename, the request returns HTTP 500 with the error code `queue_commit_outcome_unknown`. The change may already be in the snapshot. The broker stops processing queue requests until restart. A client timeout can also leave the outcome unknown if the broker has started the mutation. Finish receipts survive restart, so a worker can replay a committed acknowledgement without running inference again. Reconcile unknown submit outcomes before resubmitting because submissions do not carry idempotency keys.

## Kubernetes deployment

Use Kubernetes 1.33 or later, native sidecars, KEDA, and the NVIDIA device plugin. On EKS, create an EC2NodeClass named `gpu-nvidia` with an NVIDIA-enabled image, IAM role, subnets, and security groups. Configure the device plugin to tolerate the GPU pool taint. The NodePool restricts workers to Spot instances. [Kubernetes sidecars](https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/) · [KEDA Prometheus scaler](https://keda.sh/docs/2.19/scalers/prometheus/) · [Karpenter NodePools](https://karpenter.sh/docs/concepts/nodepools/)

In the `inference` namespace, provision a shared read-only PVC named `inference-models` containing an inference-ready `model.gguf` that fits the GPU. Pin the llama.cpp image for repeatable runs. Build and push the Rust image using the Dockerfile, then replace both `portfolio.invalid/inference/gpu-autoscaler:0.1.0` references in `k8s/workloads.yaml` with your image.

```sh
kubectl apply -f k8s/spot-nodepool.yaml
kubectl apply -f k8s/workloads.yaml
kubectl apply -f k8s/prometheus.yaml
kubectl apply -f k8s/scaler.yaml
kubectl -n inference port-forward svc/token-queue 8080:8080
```

Submit jobs through the forwarded queue. Watch `kubectl -n inference get scaledobject,hpa,pods -w` and `kubectl get nodeclaims`. Estimate prompt size with the target tokenizer. Tune the 10,000-token target per replica against measured GPU throughput and acceptable queue wait.

Leased tokens keep the metric above zero until the last job completes. The GPU sidecar stays up while its Rust worker drains. Spot termination can interrupt the pod before its grace period ends, in which case the broker retries when the lease expires.

The manifests default to one broker replica with in-memory state. To opt into persistence, mount a dedicated writable PVC on the broker Deployment and set `QUEUE_STATE_FILE` to a file on that volume. Keep the `Recreate` strategy and a single broker replica. This snapshot store rewrites the bounded queue on each change and does not provide high availability. Use authenticated ingress for external access. Cluster scaling and billing remain unmeasured.
