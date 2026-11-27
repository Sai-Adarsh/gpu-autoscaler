FROM rust:1.87-bookworm AS builder
WORKDIR /source
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /source/target/release/gpu-autoscaler /usr/local/bin/gpu-autoscaler
USER 65532:65532
ENTRYPOINT ["gpu-autoscaler"]
