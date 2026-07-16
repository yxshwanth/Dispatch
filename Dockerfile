# Multi-stage Dockerfile for api and consumer (same image, different args/entrypoint).
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/api ./cmd/api && CGO_ENABLED=0 go build -o /out/consumer ./cmd/consumer

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/api /usr/local/bin/api
COPY --from=build /out/consumer /usr/local/bin/consumer
ENTRYPOINT ["/usr/local/bin/api"]
