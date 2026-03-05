FROM golang:1.26 AS builder
RUN go install go.opentelemetry.io/collector/cmd/builder@latest
WORKDIR /build/lgtmexporter
COPY go.mod go.sum builder-config.yaml ./
COPY *.go ./
COPY internal/ ./internal/
RUN builder --config builder-config.yaml

FROM alpine:latest
COPY --from=builder /build/lgtmexporter/dist/otelcol-lgtm /otelcol-lgtm
ENTRYPOINT ["/otelcol-lgtm"]
CMD ["--config", "/etc/otelcol/config.yaml"]