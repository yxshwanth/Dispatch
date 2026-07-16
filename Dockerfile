# Multi-stage Dockerfile for api and consumer (same image, different command).
FROM golang:1.26-bookworm AS build
WORKDIR /src
ENV GOTOOLCHAIN=local
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# -p 1 avoids intermittent ICE in this toolchain/image combo under parallel compile.
RUN CGO_ENABLED=0 go build -p 1 -o /out/api ./cmd/api && CGO_ENABLED=0 go build -p 1 -o /out/consumer ./cmd/consumer

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/api /usr/local/bin/api
COPY --from=build /out/consumer /usr/local/bin/consumer
CMD ["/usr/local/bin/api"]
