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

The broker keeps jobs and measurements in memory on a single replica. Use authenticated ingress for external access and durable storage if jobs must survive broker restart. Cluster scaling and billing remain unmeasured.
