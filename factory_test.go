// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package lgtmexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"

	"github.com/mattgp/lgtmexporter/internal/metadata"
)

func TestNewFactory(t *testing.T) {
	factory := NewFactory()
	require.NotNil(t, factory)
	assert.Equal(t, metadata.Type, factory.Type())
	assert.Equal(t, factory.CreateDefaultConfig(), createDefaultConfig())
	assert.Equal(t, metadata.TracesStability, factory.TracesStability())
	assert.Equal(t, metadata.MetricsStability, factory.MetricsStability())
	assert.Equal(t, metadata.LogsStability, factory.LogsStability())
}

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg, "failed to create default config")
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))

	expectedCfg := &Config{
		QueueBatchConfig: configoptional.Some(exporterhelper.NewDefaultQueueConfig()),
		Tenant: TenantConfig{
			Label:   "tenant.id",
			Labels:  []string{},
			Format:  "%s",
			Header:  "X-Scope-OrgID",
			Default: "default",
		},
	}
	assert.Equal(t, expectedCfg, cfg)
}

func TestCreateLogsExporter(t *testing.T) {
	tests := []struct {
		name       string
		configFunc func(*Config)
		wantErr    bool
		errMsg     string
	}{
		{
			name: "valid logs endpoint",
			configFunc: func(cfg *Config) {
				cfg.Logs.Endpoint = "http://localhost:3100/loki/api/v1/push"
			},
			wantErr: false,
		},
		{
			name: "valid logs endpoint with https",
			configFunc: func(cfg *Config) {
				cfg.Logs.Endpoint = "https://logs.example.com/loki/api/v1/push"
			},
			wantErr: false,
		},
		{
			name: "logs endpoint with metrics also configured",
			configFunc: func(cfg *Config) {
				cfg.Logs.Endpoint = "http://localhost:3100/loki/api/v1/push"
				cfg.Metrics.Endpoint = "http://localhost:9009/api/v1/push"
			},
			wantErr: false,
		},
		{
			name:       "no endpoints configured",
			configFunc: func(cfg *Config) {},
			wantErr:    true,
			errMsg:     "at least one of logs, metrics, or traces endpoint must be configured",
		},
		{
			name: "empty logs endpoint",
			configFunc: func(cfg *Config) {
				cfg.Logs.Endpoint = ""
			},
			wantErr: true,
			errMsg:  "at least one of logs, metrics, or traces endpoint must be configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig().(*Config)
			tt.configFunc(cfg)

			params := exportertest.NewNopSettings(metadata.Type)
			exporter, err := factory.CreateLogs(t.Context(), params, cfg)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, exporter)
				require.NoError(t, exporter.Shutdown(t.Context()))
			}
		})
	}
}

func TestCreateMetricsExporter(t *testing.T) {
	tests := []struct {
		name       string
		configFunc func(*Config)
		wantErr    bool
		errMsg     string
	}{
		{
			name: "valid metrics endpoint",
			configFunc: func(cfg *Config) {
				cfg.Metrics.Endpoint = "http://localhost:9009/api/v1/push"
			},
			wantErr: false,
		},
		{
			name: "valid metrics endpoint with https",
			configFunc: func(cfg *Config) {
				cfg.Metrics.Endpoint = "https://metrics.example.com/api/v1/push"
			},
			wantErr: false,
		},
		{
			name: "metrics endpoint with traces also configured",
			configFunc: func(cfg *Config) {
				cfg.Metrics.Endpoint = "http://localhost:9009/api/v1/push"
				cfg.Traces.Endpoint = "http://localhost:4318/v1/traces"
			},
			wantErr: false,
		},
		{
			name:       "no endpoints configured",
			configFunc: func(cfg *Config) {},
			wantErr:    true,
			errMsg:     "at least one of logs, metrics, or traces endpoint must be configured",
		},
		{
			name: "empty metrics endpoint",
			configFunc: func(cfg *Config) {
				cfg.Metrics.Endpoint = ""
			},
			wantErr: true,
			errMsg:  "at least one of logs, metrics, or traces endpoint must be configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig().(*Config)
			tt.configFunc(cfg)

			params := exportertest.NewNopSettings(metadata.Type)
			exporter, err := factory.CreateMetrics(t.Context(), params, cfg)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, exporter)
				require.NoError(t, exporter.Shutdown(t.Context()))
			}
		})
	}
}

func TestCreateTracesExporter(t *testing.T) {
	tests := []struct {
		name       string
		configFunc func(*Config)
		wantErr    bool
		errMsg     string
	}{
		{
			name: "valid traces endpoint",
			configFunc: func(cfg *Config) {
				cfg.Traces.Endpoint = "http://localhost:4318/v1/traces"
			},
			wantErr: false,
		},
		{
			name: "valid traces endpoint with https",
			configFunc: func(cfg *Config) {
				cfg.Traces.Endpoint = "https://traces.example.com/v1/traces"
			},
			wantErr: false,
		},
		{
			name: "traces endpoint with logs also configured",
			configFunc: func(cfg *Config) {
				cfg.Traces.Endpoint = "http://localhost:4318/v1/traces"
				cfg.Logs.Endpoint = "http://localhost:3100/loki/api/v1/push"
			},
			wantErr: false,
		},
		{
			name:       "no endpoints configured",
			configFunc: func(cfg *Config) {},
			wantErr:    true,
			errMsg:     "at least one of logs, metrics, or traces endpoint must be configured",
		},
		{
			name: "empty traces endpoint",
			configFunc: func(cfg *Config) {
				cfg.Traces.Endpoint = ""
			},
			wantErr: true,
			errMsg:  "at least one of logs, metrics, or traces endpoint must be configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig().(*Config)
			tt.configFunc(cfg)

			params := exportertest.NewNopSettings(metadata.Type)
			exporter, err := factory.CreateTraces(t.Context(), params, cfg)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, exporter)
				require.NoError(t, exporter.Shutdown(t.Context()))
			}
		})
	}
}
