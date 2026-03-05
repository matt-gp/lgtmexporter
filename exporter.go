// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package lgtmexporter

import (
	"context"
	"time"

	"github.com/mattgp/lgtmexporter/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type exporterTelemetry struct {
	telemetryBuilder *metadata.TelemetryBuilder
	otelAttrs        []attribute.KeyValue
}

func newExporterTelemetry(set exporter.Settings) (*exporterTelemetry, error) {
	telemetryBuilder, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	return &exporterTelemetry{
		telemetryBuilder: telemetryBuilder,
		otelAttrs: []attribute.KeyValue{
			attribute.String("exporter", set.ID.String()),
		},
	}, nil
}

// recordLGTMSentSamples records the number of samples sent to LGTM
// split by HTTP status code, signal type, and tenant.
func (e *exporterTelemetry) recordLGTMSentSamples(ctx context.Context, numSamples int, httpStatusCode int, otelcolSignal, otelcolSignalTenant string) {
	e.telemetryBuilder.ExporterLGTMSentSamples.Add(
		ctx,
		int64(numSamples),
		metric.WithAttributes(e.otelAttrs...),
		metric.WithAttributes(attribute.Int("http_status_code", httpStatusCode)),
		metric.WithAttributes(attribute.String("otelcol_signal", otelcolSignal)),
		metric.WithAttributes(attribute.String("otelcol_signal_tenant", otelcolSignalTenant)),
	)
}

// recordLGTMSentRequests records the number of requests sent to LGTM
// split by HTTP status code, signal type, and tenant.
func (e *exporterTelemetry) recordLGTMSentRequests(ctx context.Context, numRequests int, httpStatusCode int, otelcolSignal, otelcolSignalTenant string) {
	e.telemetryBuilder.ExporterLGTMSentRequests.Add(
		ctx,
		int64(numRequests),
		metric.WithAttributes(e.otelAttrs...),
		metric.WithAttributes(attribute.Int("http_status_code", httpStatusCode)),
		metric.WithAttributes(attribute.String("otelcol_signal", otelcolSignal)),
		metric.WithAttributes(attribute.String("otelcol_signal_tenant", otelcolSignalTenant)),
	)
}

// recordLGTMWriteLatency records the latency of requests sent to LGTM
// split by HTTP status code and URL.
func (e *exporterTelemetry) recordLGTMWriteLatency(ctx context.Context, latency time.Duration, httpStatusCode int, url string) {
	e.telemetryBuilder.ExporterLGTMWriteLatency.Record(
		ctx,
		latency.Milliseconds(),
		metric.WithAttributes(e.otelAttrs...),
		metric.WithAttributes(attribute.Int("http_status_code", httpStatusCode)),
		metric.WithAttributes(attribute.String("http_url", url)),
	)
}

// lgtmExporter is the main struct for the LGTM exporter, contains the processor for logs, metrics and traces.
type lgtmExporter[T ResourceData] struct {
	ctx               context.Context
	cfg               Config
	clientConfig      confighttp.ClientConfig
	set               exporter.Settings
	telemetrySettings component.TelemetrySettings
	otelcolSignal     string
	processor         *Processor[T]
	getResource       func(T) pcommon.Resource
	marshal           func([]T) ([]byte, error)
}

// newLGTMLogsExporter creates a new instance of lgtmExporter specifically for logs
func newLGTMLogsExporter(ctx context.Context, set exporter.Settings, cfg Config) (*lgtmExporter[plog.ResourceLogs], error) {

	return &lgtmExporter[plog.ResourceLogs]{
		ctx:               ctx,
		cfg:               cfg,
		clientConfig:      cfg.Logs,
		set:               set,
		telemetrySettings: set.TelemetrySettings,
		otelcolSignal:     "logs",
		getResource: func(rl plog.ResourceLogs) pcommon.Resource {
			return rl.Resource()
		},
		marshal: func(rls []plog.ResourceLogs) ([]byte, error) {
			data := plog.NewLogs()
			data.ResourceLogs().EnsureCapacity(len(rls))
			for _, rl := range rls {
				rl.CopyTo(data.ResourceLogs().AppendEmpty())
			}
			marshaler := plog.ProtoMarshaler{}
			return marshaler.MarshalLogs(data)
		},
	}, nil
}

// newLGTMMetricsExporter creates a new instance of lgtmExporter specifically for metrics
func newLGTMMetricsExporter(ctx context.Context, set exporter.Settings, cfg Config) (*lgtmExporter[pmetric.ResourceMetrics], error) {

	return &lgtmExporter[pmetric.ResourceMetrics]{
		ctx:               ctx,
		cfg:               cfg,
		clientConfig:      cfg.Metrics,
		set:               set,
		telemetrySettings: set.TelemetrySettings,
		otelcolSignal:     "metrics",
		getResource: func(rm pmetric.ResourceMetrics) pcommon.Resource {
			return rm.Resource()
		},
		marshal: func(rms []pmetric.ResourceMetrics) ([]byte, error) {
			data := pmetric.NewMetrics()
			data.ResourceMetrics().EnsureCapacity(len(rms))
			for _, rm := range rms {
				rm.CopyTo(data.ResourceMetrics().AppendEmpty())
			}
			marshaler := pmetric.ProtoMarshaler{}
			return marshaler.MarshalMetrics(data)
		},
	}, nil
}

// newLGTMTracesExporter creates a new instance of lgtmExporter specifically for traces
func newLGTMTracesExporter(ctx context.Context, set exporter.Settings, cfg Config) (*lgtmExporter[ptrace.ResourceSpans], error) {

	return &lgtmExporter[ptrace.ResourceSpans]{
		ctx:               ctx,
		cfg:               cfg,
		clientConfig:      cfg.Traces,
		set:               set,
		telemetrySettings: set.TelemetrySettings,
		otelcolSignal:     "traces",
		getResource: func(rs ptrace.ResourceSpans) pcommon.Resource {
			return rs.Resource()
		},
		marshal: func(rss []ptrace.ResourceSpans) ([]byte, error) {
			data := ptrace.NewTraces()
			data.ResourceSpans().EnsureCapacity(len(rss))
			for _, rs := range rss {
				rs.CopyTo(data.ResourceSpans().AppendEmpty())
			}
			marshaler := ptrace.ProtoMarshaler{}
			return marshaler.MarshalTraces(data)
		},
	}, nil
}

// pushLogsData processes and sends log data to the configured endpoint,
func (e *lgtmExporter[T]) pushLogsData(ctx context.Context, ld plog.Logs) error {
	var resourceLogs []T
	for _, rl := range ld.ResourceLogs().All() {
		resourceLogs = append(resourceLogs, any(rl).(T))
	}

	return e.processor.dispatch(ctx, e.processor.partition(resourceLogs))
}

// pushMetricsData processes and sends metric data to the configured endpoint,
func (e *lgtmExporter[T]) pushMetricsData(ctx context.Context, md pmetric.Metrics) error {
	var resourceMetrics []T
	for _, rm := range md.ResourceMetrics().All() {
		resourceMetrics = append(resourceMetrics, any(rm).(T))
	}

	return e.processor.dispatch(ctx, e.processor.partition(resourceMetrics))
}

// pushTracesData processes and sends trace data to the configured endpoint,
func (e *lgtmExporter[T]) pushTracesData(ctx context.Context, td ptrace.Traces) error {
	var resourceSpans []T
	for _, rs := range td.ResourceSpans().All() {
		resourceSpans = append(resourceSpans, any(rs).(T))
	}

	return e.processor.dispatch(ctx, e.processor.partition(resourceSpans))
}

func (e *lgtmExporter[T]) start(ctx context.Context, host component.Host) (err error) {
	e.telemetrySettings.Logger.Info("starting LGTM exporter telemetry")

	// Create exporter telemetry for recording metrics about the exporter
	exporterTelemetry, err := newExporterTelemetry(e.set)
	if err != nil {
		e.telemetrySettings.Logger.Error("failed to create exporter telemetry", zap.Error(err))
		return err
	}

	// Create processor with host for proper auth configuration
	e.processor, err = NewProcessor(
		ctx,
		e.cfg.Tenant,
		e.clientConfig,
		host,
		exporterTelemetry,
		e.telemetrySettings,
		e.otelcolSignal,
		e.getResource,
		e.marshal,
	)
	if err != nil {
		e.telemetrySettings.Logger.Error("failed to create processor", zap.Error(err))
		return err
	}

	return nil
}

func (e *lgtmExporter[T]) shutdown(_ context.Context) (err error) {
	e.telemetrySettings.Logger.Info("shutting down LGTM exporter")
	return nil
}
