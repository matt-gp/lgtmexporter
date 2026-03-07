// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package lgtmexporter

import (
	"path/filepath"
	"testing"

	"github.com/mattgp/lgtmexporter/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config with logs endpoint",
			config: &Config{
				Logs: confighttp.ClientConfig{
					Endpoint: "http://localhost:3100/loki/api/v1/push",
				},
			},
			wantErr: false,
		},
		{
			name: "valid config with metrics endpoint",
			config: &Config{
				Metrics: confighttp.ClientConfig{
					Endpoint: "http://localhost:9009/api/v1/push",
				},
			},
			wantErr: false,
		},
		{
			name: "valid config with traces endpoint",
			config: &Config{
				Traces: confighttp.ClientConfig{
					Endpoint: "http://localhost:4318/v1/traces",
				},
			},
			wantErr: false,
		},
		{
			name: "valid config with all endpoints",
			config: &Config{
				Logs: confighttp.ClientConfig{
					Endpoint: "http://localhost:3100/loki/api/v1/push",
				},
				Metrics: confighttp.ClientConfig{
					Endpoint: "http://localhost:9009/api/v1/push",
				},
				Traces: confighttp.ClientConfig{
					Endpoint: "http://localhost:4318/v1/traces",
				},
			},
			wantErr: false,
		},
		{
			name:    "invalid config with no endpoints",
			config:  &Config{},
			wantErr: true,
			errMsg:  "at least one of logs, metrics, or traces endpoint must be configured",
		},
		{
			name: "invalid config with empty string endpoints",
			config: &Config{
				Logs:    confighttp.ClientConfig{Endpoint: ""},
				Metrics: confighttp.ClientConfig{Endpoint: ""},
				Traces:  confighttp.ClientConfig{Endpoint: ""},
			},
			wantErr: true,
			errMsg:  "at least one of logs, metrics, or traces endpoint must be configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfig(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	tests := []struct {
		id       component.ID
		validate func(*testing.T, *Config)
	}{
		{
			id: component.NewIDWithName(metadata.Type, ""),
			validate: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "http://localhost:3100/otlp/v1/logs", cfg.Logs.Endpoint)
				assert.Equal(t, "http://localhost:8080/otlp/v1/metrics", cfg.Metrics.Endpoint)
				assert.Equal(t, "http://localhost:3201/v1/traces", cfg.Traces.Endpoint)
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "logs_only"),
			validate: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "http://loki:3100/otlp/v1/logs", cfg.Logs.Endpoint)
				assert.Empty(t, cfg.Metrics.Endpoint)
				assert.Empty(t, cfg.Traces.Endpoint)
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "metrics_only"),
			validate: func(t *testing.T, cfg *Config) {
				assert.Empty(t, cfg.Logs.Endpoint)
				assert.Equal(t, "http://mimir:8080/otlp/v1/metrics", cfg.Metrics.Endpoint)
				assert.Empty(t, cfg.Traces.Endpoint)
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "traces_only"),
			validate: func(t *testing.T, cfg *Config) {
				assert.Empty(t, cfg.Logs.Endpoint)
				assert.Empty(t, cfg.Metrics.Endpoint)
				assert.Equal(t, "http://tempo:3201/v1/traces", cfg.Traces.Endpoint)
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "with_tls"),
			validate: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "https://loki:3100/otlp/v1/logs", cfg.Logs.Endpoint)
				assert.Equal(t, "/certs/client.crt", cfg.Logs.TLS.CertFile)
				assert.Equal(t, "/certs/client.key", cfg.Logs.TLS.KeyFile)
				assert.Equal(t, "/certs/ca.crt", cfg.Logs.TLS.CAFile)
				assert.False(t, cfg.Logs.TLS.InsecureSkipVerify, "Should verify TLS certificates")
				
				assert.Equal(t, "https://mimir:8080/otlp/v1/metrics", cfg.Metrics.Endpoint)
				assert.Equal(t, "/certs/client.crt", cfg.Metrics.TLS.CertFile)
				assert.False(t, cfg.Metrics.TLS.InsecureSkipVerify)
				
				assert.Equal(t, "https://tempo:3201/v1/traces", cfg.Traces.Endpoint)
				assert.Equal(t, "/certs/client.crt", cfg.Traces.TLS.CertFile)
				assert.False(t, cfg.Traces.TLS.InsecureSkipVerify)
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "insecure_tls"),
			validate: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "https://loki:3100/otlp/v1/logs", cfg.Logs.Endpoint)
				assert.True(t, cfg.Logs.TLS.InsecureSkipVerify, "Should skip TLS certificate verification")
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "with_auth"),
			validate: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "http://loki:3100/otlp/v1/logs", cfg.Logs.Endpoint)
				logsAuth := cfg.Logs.Auth.Get()
				assert.Equal(t, "basicauth", logsAuth.AuthenticatorID.String())
				
				assert.Equal(t, "http://mimir:8080/otlp/v1/metrics", cfg.Metrics.Endpoint)
				metricsAuth := cfg.Metrics.Auth.Get()
				assert.Equal(t, "bearertokenauth", metricsAuth.AuthenticatorID.String())
				
				assert.Equal(t, "http://tempo:3201/v1/traces", cfg.Traces.Endpoint)
				tracesAuth := cfg.Traces.Auth.Get()
				assert.Equal(t, "oauth2client", tracesAuth.AuthenticatorID.String())
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "custom_headers"),
			validate: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "http://loki:3100/otlp/v1/logs", cfg.Logs.Endpoint)
				assert.Len(t, cfg.Logs.Headers, 2)
				assert.Equal(t, "X-Scope-OrgID", cfg.Logs.Headers[0].Name)
				assert.Equal(t, "X-Custom-Header", cfg.Logs.Headers[1].Name)
				
				assert.Equal(t, "http://mimir:8080/otlp/v1/metrics", cfg.Metrics.Endpoint)
				assert.Len(t, cfg.Metrics.Headers, 1)
				assert.Equal(t, "X-Scope-OrgID", cfg.Metrics.Headers[0].Name)
				
				assert.Equal(t, "http://tempo:3201/v1/traces", cfg.Traces.Endpoint)
				assert.Len(t, cfg.Traces.Headers, 1)
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "with_timeout_retry"),
			validate: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "http://loki:3100/otlp/v1/logs", cfg.Logs.Endpoint)
				assert.Equal(t, 30000000000, int(cfg.Logs.Timeout))
				assert.Equal(t, 1024, cfg.Logs.ReadBufferSize)
				assert.Equal(t, 1024, cfg.Logs.WriteBufferSize)
				
				assert.Equal(t, "http://mimir:8080/otlp/v1/metrics", cfg.Metrics.Endpoint)
				assert.Equal(t, 30000000000, int(cfg.Metrics.Timeout))
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "with_compression"),
			validate: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "http://loki:3100/otlp/v1/logs", cfg.Logs.Endpoint)
				assert.Equal(t, "gzip", string(cfg.Logs.Compression))
				
				assert.Equal(t, "http://mimir:8080/otlp/v1/metrics", cfg.Metrics.Endpoint)
				assert.Equal(t, "gzip", string(cfg.Metrics.Compression))
				
				assert.Equal(t, "http://tempo:3201/v1/traces", cfg.Traces.Endpoint)
				assert.Equal(t, "gzip", string(cfg.Traces.Compression))
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "connection_settings"),
			validate: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "http://loki:3100/otlp/v1/logs", cfg.Logs.Endpoint)
				assert.Equal(t, 100, cfg.Logs.MaxIdleConns)
				assert.Equal(t, 10, cfg.Logs.MaxIdleConnsPerHost)
				assert.Equal(t, 50, cfg.Logs.MaxConnsPerHost)
				assert.Equal(t, 90000000000, int(cfg.Logs.IdleConnTimeout))
				
				assert.Equal(t, "http://mimir:8080/otlp/v1/metrics", cfg.Metrics.Endpoint)
				assert.Equal(t, 100, cfg.Metrics.MaxIdleConns)
				assert.Equal(t, 10, cfg.Metrics.MaxIdleConnsPerHost)
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "with_proxy"),
			validate: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "http://loki:3100/otlp/v1/logs", cfg.Logs.Endpoint)
				assert.Equal(t, "http://proxy:8080", cfg.Logs.ProxyURL)
				
				assert.Equal(t, "http://mimir:8080/otlp/v1/metrics", cfg.Metrics.Endpoint)
				assert.Equal(t, "http://proxy:8080", cfg.Metrics.ProxyURL)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))
			
			lgtmCfg, ok := cfg.(*Config)
			require.True(t, ok)
			
			// Run the specific validation for this test case
			tt.validate(t, lgtmCfg)
		})
	}
}
