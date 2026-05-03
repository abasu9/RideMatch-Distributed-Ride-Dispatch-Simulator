RideMatch is a small but end-to-end **distributed ride-dispatch simulator** written in Go. Three gRPC microservices cooperate with Redis, PostgreSQL, and a Kafka-compatible event stream while exposing Prometheus metrics, structured JSON logs, and OpenTelemetry trace propagation suitable for recruiter-style code review.

## Architecture

```text
                 +---------------+            +------------------+
                 | Rider Service |  gRPC    | Matching Service  |
 riders --------> | RequestRide    | -------> | FindNearestDriver |
                 | GetRideStatus  |          +---------+---------+
                 +--------+-------+                    |
                           | Postgres (rides)           | Redis (drivers by H3 cell)
                           v                            v
                     +--------------+           +---------------+
                     | PostgreSQL    |           | Redis          |
                     +--------------+           +-------+-------+
                                                       ^
 drivers updates                                      |
       +----------------------------------------------+
       |
+------+-------------------+
| Driver Service           |
| UpdateLocation(Register) |
+----+---------+-----------+
     | Kafka   |
     v         v
+----------+ +-+----------------------+
| Redis    | | Redpanda (Kafka API)    |
| (H7 ZSET)| | topic driver-locations  |
+----------+ +--------------------------+

Prometheus scrapes `/metrics` on each service.

Jaeger OTLP ingestion (`4317`) receives traces propagated across Rider -> Matching RPCs.
```

## Tech stack

- Go microservices (`cmd/driver`, `cmd/rider`, `cmd/matching`)
- **gRPC** + dynamic protobuf linkage via `protocompile` + `dynamicpb`
- **H3 hex-grid indexing (`h3-go` v4, resolution = 7)**
- Redis (`go-redis`) for active drivers + cell membership ZSET
- PostgreSQL (`pgx`) for ride persistence
- Redpanda (`kafka-go` producer) publishing `driver-locations`
- Prometheus (`prometheus/client_golang`) + **OTLP -> Jaeger** (`otelgrpc`)
- Zerolog structured JSON logs with `service` + `request_id`

## Prerequisites

- Docker Desktop (Compose v2 bundled) for the full stack
- [`k6`](https://grafana.com/docs/k6/latest/) for load generation

### macOS tip: PATH for user-local installs

If you do not want to rely on Homebrew (or `/opt/homebrew` is not writable), this repo can use tools under `$HOME`:

- `$HOME/sdk/go/bin` — Go toolchain (offline-friendly install uses `dl.google.com/go`)
- `$HOME/bin` — `k6` binary

Activate for the current shell:

```bash
source scripts/env.sh
```

Optional local development tooling:

```bash
source scripts/env.sh
go mod tidy

# Keep embedded schemas in sync with canonical protos:
make proto-sync
```

## Run the full stack

```bash
docker compose up --build
```

Useful URLs (host mapped ports):

- Rider gRPC: `localhost:50052`
- Driver gRPC: `localhost:50051`
- Matching gRPC: `localhost:50053`
- Prometheus UI: http://localhost:9090/
- Jaeger UI: http://localhost:16686/

## Observability knobs

Services accept:

- `OTEL_EXPORTER_OTLP_ENDPOINT` (example: `jaeger:4317` inside Compose)
- `LOG_LEVEL` (`info` by default)

## Load tests (k6)

With Compose running locally (ports published):

```bash
cd "$(git rev-parse --show-toplevel)"
k6 run k6/load_test.js \
  --env DRIVER_GRPC_ADDR=127.0.0.1:50051 \
  --env RIDER_GRPC_ADDR=127.0.0.1:50052 \
  --env RIDEMATCH_REPO_ROOT="$(pwd)"
```

Notes:

- The script performs **real unary gRPC** calls (`RegisterDriver`, `UpdateLocation`, `RequestRide`) using protos from `proto/`.
- Drivers are synthesized around a Manhattan-ish coordinate pinch point so Matching can reliably find neighbors.

### Protocol buffers (`buf`/`protoc` optional)

RideMatch parses `proto/*.proto` at startup (see `internal/proto`). If you want generated `*_pb.go` instead, compile with normal `protoc` plugins and swap the wiring in `grpcx`/services.

## Operational notes / resume alignment

Matching literally performs **H3 cell indexing**, expands a **k=1 disk neighborhood**, scans Redis cells, then reranks surviving drivers via **Haversine** distance. Drivers publish each movement to **`driver-locations`** on Kafka/Redpanda and mirror state in Redis. Rider persists each attempt in PostgreSQL regardless of assignment outcome.
