FROM golang:1.23-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc6-dev \
    ca-certificates \
    git \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY go.mod go.sum* ./

RUN go mod download

COPY . .

ARG SERVICE
RUN test -n "$SERVICE"
RUN CGO_ENABLED=1 GOOS=linux go build -trimpath -ldflags="-s -w" -o /service ./cmd/$SERVICE

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder --chown=65532:65532 --chmod=0555 /service /service

USER 65532:65532

ENTRYPOINT ["/service"]
