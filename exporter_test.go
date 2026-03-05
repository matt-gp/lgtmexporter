// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package lgtmexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestNewExporterTelemetry(t *testing.T) {
	set := exportertest.NewNopSettings(component.MustNewType("lgtm"))
	telemetry, err := newExporterTelemetry(set)
	assert.NoError(t, err)
	assert.NotNil(t, telemetry)
	assert.NotNil(t, telemetry.telemetryBuilder)
	assert.NotEmpty(t, telemetry.otelAttrs)
	assert.Len(t, telemetry.otelAttrs, 1)
}

func TestNewLGTMLogsExporter(t *testing.T) {
	cfg := &Config{
		Logs: confighttp.ClientConfig{
			Endpoint: "http://localhost:3100/otlp/v1/logs",
		},
	}

	set := exportertest.NewNopSettings(component.MustNewType("lgtm"))
	exp, err := newLGTMLogsExporter(context.Background(), set, *cfg)
	assert.NoError(t, err)
	assert.NotNil(t, exp)
	assert.Equal(t, "logs", exp.otelcolSignal)
	assert.Equal(t, cfg.Logs.Endpoint, exp.clientConfig.Endpoint)
	assert.NotNil(t, exp.getResource)
	assert.NotNil(t, exp.marshal)
}

func TestNewLGTMMetricsExporter(t *testing.T) {
	cfg := &Config{
		Metrics: confighttp.ClientConfig{
			Endpoint: "http://localhost:9009/otlp/v1/metrics",
		},
	}

	set := exportertest.NewNopSettings(component.MustNewType("lgtm"))
	exp, err := newLGTMMetricsExporter(context.Background(), set, *cfg)
	assert.NoError(t, err)
	assert.NotNil(t, exp)
	assert.Equal(t, "metrics", exp.otelcolSignal)
	assert.Equal(t, cfg.Metrics.Endpoint, exp.clientConfig.Endpoint)
	assert.NotNil(t, exp.getResource)
	assert.NotNil(t, exp.marshal)
}

func TestNewLGTMTracesExporter(t *testing.T) {
	cfg := &Config{
		Traces: confighttp.ClientConfig{
			Endpoint: "http://localhost:4317/v1/traces",
		},
	}

	set := exportertest.NewNopSettings(component.MustNewType("lgtm"))
	exp, err := newLGTMTracesExporter(context.Background(), set, *cfg)
	assert.NoError(t, err)
	assert.NotNil(t, exp)
	assert.Equal(t, "traces", exp.otelcolSignal)
	assert.Equal(t, cfg.Traces.Endpoint, exp.clientConfig.Endpoint)
	assert.NotNil(t, exp.getResource)
	assert.NotNil(t, exp.marshal)
}

func TestStart(t *testing.T) {

	type test[T ResourceData] struct {
		name         string
		cfg          *Config
		exporterFunc func(context.Context, exporter.Settings, Config) (*lgtmExporter[T], error)
		wantErr      bool
		errMsg       string
	}

	t.Run("logs", func(t *testing.T) {
		tests := []test[plog.ResourceLogs]{
			{
				name: "valid logs endpoint",
				cfg: &Config{
					Logs: confighttp.ClientConfig{
						Endpoint: "http://localhost:3100/otlp/v1/logs",
					},
				},
				exporterFunc: newLGTMLogsExporter,
				wantErr:      false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				set := exportertest.NewNopSettings(component.MustNewType("lgtm"))
				exp, err := tt.exporterFunc(context.Background(), set, *tt.cfg)
				require.NoError(t, err)

				err = exp.start(context.Background(), componenttest.NewNopHost())
				if tt.wantErr {
					require.Error(t, err)
					if tt.errMsg != "" {
						assert.Contains(t, err.Error(), tt.errMsg)
					}
				} else {
					require.NoError(t, err)
					assert.NotNil(t, exp.processor)
				}
			})
		}
	})

	t.Run("metrics", func(t *testing.T) {
		tests := []test[pmetric.ResourceMetrics]{
			{
				name: "valid metrics endpoint",
				cfg: &Config{
					Metrics: confighttp.ClientConfig{
						Endpoint: "http://localhost:9009/otlp/v1/metrics",
					},
				},
				exporterFunc: newLGTMMetricsExporter,
				wantErr:      false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				set := exportertest.NewNopSettings(component.MustNewType("lgtm"))
				exp, err := tt.exporterFunc(context.Background(), set, *tt.cfg)
				require.NoError(t, err)

				err = exp.start(context.Background(), componenttest.NewNopHost())
				if tt.wantErr {
					require.Error(t, err)
					if tt.errMsg != "" {
						assert.Contains(t, err.Error(), tt.errMsg)
					}
				} else {
					require.NoError(t, err)
					assert.NotNil(t, exp.processor)
				}
			})
		}
	})

	t.Run("traces", func(t *testing.T) {
		tests := []test[ptrace.ResourceSpans]{
			{
				name: "valid traces endpoint",
				cfg: &Config{
					Traces: confighttp.ClientConfig{
						Endpoint: "http://localhost:4317/v1/traces",
					},
				},
				exporterFunc: newLGTMTracesExporter,
				wantErr:      false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				set := exportertest.NewNopSettings(component.MustNewType("lgtm"))
				exp, err := tt.exporterFunc(context.Background(), set, *tt.cfg)
				require.NoError(t, err)

				err = exp.start(context.Background(), componenttest.NewNopHost())
				if tt.wantErr {
					require.Error(t, err)
					if tt.errMsg != "" {
						assert.Contains(t, err.Error(), tt.errMsg)
					}
				} else {
					require.NoError(t, err)
					assert.NotNil(t, exp.processor)
				}
			})
		}
	})
}

func TestShutdown(t *testing.T) {

	type test[T ResourceData] struct {
		name         string
		cfg          *Config
		exporterFunc func(context.Context, exporter.Settings, Config) (*lgtmExporter[T], error)
		wantErr      bool
		errMsg       string
	}

	t.Run("logs", func(t *testing.T) {
		tests := []test[plog.ResourceLogs]{
			{
				name: "successful shutdown",
				cfg: &Config{
					Logs: confighttp.ClientConfig{
						Endpoint: "http://localhost:3100/otlp/v1/logs",
					},
				},
				exporterFunc: newLGTMLogsExporter,
				wantErr:      false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				set := exportertest.NewNopSettings(component.MustNewType("lgtm"))
				exp, err := tt.exporterFunc(context.Background(), set, *tt.cfg)
				require.NoError(t, err)

				err = exp.start(context.Background(), componenttest.NewNopHost())
				require.NoError(t, err)

				err = exp.shutdown(context.Background())
				if tt.wantErr {
					require.Error(t, err)
					if tt.errMsg != "" {
						assert.Contains(t, err.Error(), tt.errMsg)
					}
				} else {
					assert.NoError(t, err)
				}
			})
		}
	})

	t.Run("metrics", func(t *testing.T) {
		tests := []test[pmetric.ResourceMetrics]{
			{
				name: "successful shutdown",
				cfg: &Config{
					Metrics: confighttp.ClientConfig{
						Endpoint: "http://localhost:9009/otlp/v1/metrics",
					},
				},
				exporterFunc: newLGTMMetricsExporter,
				wantErr:      false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				set := exportertest.NewNopSettings(component.MustNewType("lgtm"))
				exp, err := tt.exporterFunc(context.Background(), set, *tt.cfg)
				require.NoError(t, err)

				err = exp.start(context.Background(), componenttest.NewNopHost())
				require.NoError(t, err)

				err = exp.shutdown(context.Background())
				if tt.wantErr {
					require.Error(t, err)
					if tt.errMsg != "" {
						assert.Contains(t, err.Error(), tt.errMsg)
					}
				} else {
					assert.NoError(t, err)
				}
			})
		}
	})

	t.Run("traces", func(t *testing.T) {
		tests := []test[ptrace.ResourceSpans]{
			{
				name: "successful shutdown",
				cfg: &Config{
					Traces: confighttp.ClientConfig{
						Endpoint: "http://localhost:4317/v1/traces",
					},
				},
				exporterFunc: newLGTMTracesExporter,
				wantErr:      false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				set := exportertest.NewNopSettings(component.MustNewType("lgtm"))
				exp, err := tt.exporterFunc(context.Background(), set, *tt.cfg)
				require.NoError(t, err)

				err = exp.start(context.Background(), componenttest.NewNopHost())
				require.NoError(t, err)

				err = exp.shutdown(context.Background())
				if tt.wantErr {
					require.Error(t, err)
					if tt.errMsg != "" {
						assert.Contains(t, err.Error(), tt.errMsg)
					}
				} else {
					assert.NoError(t, err)
				}
			})
		}
	})
}

func TestPushLogsData(t *testing.T) {
	cfg := &Config{
		Logs: confighttp.ClientConfig{
			Endpoint: "http://localhost:3100/otlp/v1/logs",
		},
	}

	set := exportertest.NewNopSettings(component.MustNewType("lgtm"))
	exp, err := newLGTMLogsExporter(context.Background(), set, *cfg)
	require.NoError(t, err)

	err = exp.start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Test with empty logs
	logs := plog.NewLogs()
	err = exp.pushLogsData(context.Background(), logs)
	assert.NoError(t, err)

	// Test with logs data
	logs = plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("tenant.id", "test-tenant")
	sl := rl.ScopeLogs().AppendEmpty()
	logRecord := sl.LogRecords().AppendEmpty()
	logRecord.Body().SetStr("test log message")

	// Push data - will be queued/retried by the exporter helper
	err = exp.pushLogsData(context.Background(), logs)
	assert.NoError(t, err)
}

func TestPushMetricsData(t *testing.T) {
	cfg := &Config{
		Metrics: confighttp.ClientConfig{
			Endpoint: "http://localhost:9009/otlp/v1/metrics",
		},
	}

	set := exportertest.NewNopSettings(component.MustNewType("lgtm"))
	exp, err := newLGTMMetricsExporter(context.Background(), set, *cfg)
	require.NoError(t, err)

	err = exp.start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Test with empty metrics
	metrics := pmetric.NewMetrics()
	err = exp.pushMetricsData(context.Background(), metrics)
	assert.NoError(t, err)

	// Test with metrics data
	metrics = pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("tenant.id", "test-tenant")
	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("test.metric")
	metric.SetEmptyGauge()
	dp := metric.Gauge().DataPoints().AppendEmpty()
	dp.SetIntValue(42)

	// Push data - will be queued/retried by the exporter helper
	err = exp.pushMetricsData(context.Background(), metrics)
	assert.NoError(t, err)
}

func TestPushTracesData(t *testing.T) {
	cfg := &Config{
		Traces: confighttp.ClientConfig{
			Endpoint: "http://localhost:4317/v1/traces",
		},
	}

	set := exportertest.NewNopSettings(component.MustNewType("lgtm"))
	exp, err := newLGTMTracesExporter(context.Background(), set, *cfg)
	require.NoError(t, err)

	err = exp.start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Test with empty traces
	traces := ptrace.NewTraces()
	err = exp.pushTracesData(context.Background(), traces)
	assert.NoError(t, err)

	// Test with traces data
	traces = ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("tenant.id", "test-tenant")
	ss := rs.ScopeSpans().AppendEmpty()
	span := ss.Spans().AppendEmpty()
	span.SetName("test-span")
	span.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	span.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})

	// Push data - will be queued/retried by the exporter helper
	err = exp.pushTracesData(context.Background(), traces)
	assert.NoError(t, err)
}
