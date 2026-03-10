// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package lgtmexporter

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/mattgp/lgtmexporter/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

// Helper functions to create test data
func createTestResourceLogs() plog.ResourceLogs {
	rl := plog.NewResourceLogs()
	rl.Resource().Attributes().PutStr("tenant.id", "tenant-1")
	rl.Resource().Attributes().PutStr("service.name", "test-service")
	return rl
}

func createTestResourceMetrics() pmetric.ResourceMetrics {
	rm := pmetric.NewResourceMetrics()
	rm.Resource().Attributes().PutStr("tenant.id", "tenant-1")
	rm.Resource().Attributes().PutStr("service.name", "test-service")
	return rm
}

func createTestResourceSpans() ptrace.ResourceSpans {
	rs := ptrace.NewResourceSpans()
	rs.Resource().Attributes().PutStr("tenant.id", "tenant-1")
	rs.Resource().Attributes().PutStr("service.name", "test-service")
	return rs
}

func TestNewProcessor(t *testing.T) {

	type testCase[T ResourceData] struct {
		name        string
		signalType  string
		endpoint    string
		getResource func(T) pcommon.Resource
		marshal     func([]T, contentType) ([]byte, error)
	}

	t.Run("logs", func(t *testing.T) {
		logsTestsCases := []testCase[plog.ResourceLogs]{
			{
				name:       "create logs processor",
				signalType: "logs",
				endpoint:   "http://localhost:3100/loki/api/v1/push",
				getResource: func(rd plog.ResourceLogs) pcommon.Resource {
					return rd.Resource()
				},
				marshal: func(rds []plog.ResourceLogs, ct contentType) ([]byte, error) {
					return []byte("test"), nil
				},
			},
		}

		for _, tc := range logsTestsCases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				tenantConfig := TenantConfig{
					Label:   "tenant.id",
					Format:  "%s",
					Header:  "X-Scope-OrgID",
					Default: "default",
				}
				clientConfig := confighttp.ClientConfig{
					Endpoint: tc.endpoint,
				}
				host := componenttest.NewNopHost()
				telemetrySettings := componenttest.NewNopTelemetrySettings()

				tb, _ := metadata.NewTelemetryBuilder(telemetrySettings)
				exporterTelemetry := &exporterTelemetry{
					telemetryBuilder: tb,
					otelAttrs:        []attribute.KeyValue{},
				}

				processor, err := NewProcessor(
					ctx,
					tenantConfig,
					clientConfig,
					host,
					exporterTelemetry,
					telemetrySettings,
					tc.signalType,
					contentTypeProtobuf,
					tc.getResource,
					tc.marshal,
				)

				require.NoError(t, err)
				assert.NotNil(t, processor)
				assert.Equal(t, tenantConfig, processor.tenantConfig)
				assert.Equal(t, clientConfig, processor.clientConfig)
				assert.Equal(t, tc.signalType, processor.otelcolSignal)
			})
		}
	})

	t.Run("metrics", func(t *testing.T) {
		metricsTestsCases := []testCase[pmetric.ResourceMetrics]{
			{
				name:       "create metrics processor",
				signalType: "metrics",
				endpoint:   "http://localhost:3100/mimir/api/v1/push",
				getResource: func(rd pmetric.ResourceMetrics) pcommon.Resource {
					return rd.Resource()
				},
				marshal: func(rds []pmetric.ResourceMetrics, ct contentType) ([]byte, error) {
					return []byte("test"), nil
				},
			},
		}

		for _, tc := range metricsTestsCases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				tenantConfig := TenantConfig{
					Label:   "tenant.id",
					Format:  "%s",
					Header:  "X-Scope-OrgID",
					Default: "default",
				}
				clientConfig := confighttp.ClientConfig{
					Endpoint: tc.endpoint,
				}
				host := componenttest.NewNopHost()
				telemetrySettings := componenttest.NewNopTelemetrySettings()

				tb, _ := metadata.NewTelemetryBuilder(telemetrySettings)
				exporterTelemetry := &exporterTelemetry{
					telemetryBuilder: tb,
					otelAttrs:        []attribute.KeyValue{},
				}

				processor, err := NewProcessor(
					ctx,
					tenantConfig,
					clientConfig,
					host,
					exporterTelemetry,
					telemetrySettings,
					tc.signalType,
					contentTypeProtobuf,
					tc.getResource,
					tc.marshal,
				)

				require.NoError(t, err)
				assert.NotNil(t, processor)
				assert.Equal(t, tenantConfig, processor.tenantConfig)
				assert.Equal(t, clientConfig, processor.clientConfig)
				assert.Equal(t, tc.signalType, processor.otelcolSignal)
			})
		}
	})

	t.Run("traces", func(t *testing.T) {
		tracesTestsCases := []testCase[ptrace.ResourceSpans]{
			{
				name:       "create traces processor",
				signalType: "traces",
				endpoint:   "http://localhost:4318/v1/traces",
				getResource: func(rd ptrace.ResourceSpans) pcommon.Resource {
					return rd.Resource()
				},
				marshal: func(rds []ptrace.ResourceSpans, ct contentType) ([]byte, error) {
					return []byte("test"), nil
				},
			},
		}

		for _, tc := range tracesTestsCases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				tenantConfig := TenantConfig{
					Label:   "tenant.id",
					Format:  "%s",
					Header:  "X-Scope-OrgID",
					Default: "default",
				}
				clientConfig := confighttp.ClientConfig{
					Endpoint: tc.endpoint,
				}
				host := componenttest.NewNopHost()
				telemetrySettings := componenttest.NewNopTelemetrySettings()

				tb, _ := metadata.NewTelemetryBuilder(telemetrySettings)
				exporterTelemetry := &exporterTelemetry{
					telemetryBuilder: tb,
					otelAttrs:        []attribute.KeyValue{},
				}

				processor, err := NewProcessor(
					ctx,
					tenantConfig,
					clientConfig,
					host,
					exporterTelemetry,
					telemetrySettings,
					tc.signalType,
					contentTypeProtobuf,
					tc.getResource,
					tc.marshal,
				)

				require.NoError(t, err)
				assert.NotNil(t, processor)
				assert.Equal(t, tenantConfig, processor.tenantConfig)
				assert.Equal(t, clientConfig, processor.clientConfig)
				assert.Equal(t, tc.signalType, processor.otelcolSignal)
			})
		}
	})
}

func TestPartition(t *testing.T) {

	t.Run("logs", func(t *testing.T) {
		processor := &Processor[plog.ResourceLogs]{
			tenantConfig: TenantConfig{
				Label:   "tenant.id",
				Format:  "%s",
				Header:  "X-Scope-OrgID",
				Default: "default",
			},
			getResource: func(rl plog.ResourceLogs) pcommon.Resource {
				return rl.Resource()
			},
			telemetrySettings: component.TelemetrySettings{
				Logger: zap.NewNop(),
			},
		}

		// Create test data with multiple tenants and multiple resources per tenant
		rl1 := plog.NewResourceLogs()
		rl1.Resource().Attributes().PutStr("tenant.id", "tenant-1")
		rl2 := plog.NewResourceLogs()
		rl2.Resource().Attributes().PutStr("tenant.id", "tenant-2")
		rl3 := plog.NewResourceLogs()
		rl3.Resource().Attributes().PutStr("tenant.id", "tenant-1")
		rl4 := plog.NewResourceLogs()
		rl4.Resource().Attributes().PutStr("tenant.id", "tenant-3")

		resources := []plog.ResourceLogs{rl1, rl2, rl3, rl4}
		result := processor.partition(resources)

		// Verify correct number of tenants
		assert.Equal(t, 3, len(result))

		// Verify resources are grouped by tenant
		assert.Len(t, result["tenant-1"], 2)
		assert.Len(t, result["tenant-2"], 1)
		assert.Len(t, result["tenant-3"], 1)
	})

	t.Run("metrics", func(t *testing.T) {
		processor := &Processor[pmetric.ResourceMetrics]{
			tenantConfig: TenantConfig{
				Label:   "tenant.id",
				Format:  "%s",
				Header:  "X-Scope-OrgID",
				Default: "default",
			},
			getResource: func(rm pmetric.ResourceMetrics) pcommon.Resource {
				return rm.Resource()
			},
			telemetrySettings: component.TelemetrySettings{
				Logger: zap.NewNop(),
			},
		}

		// Create test data with multiple tenants and multiple resources per tenant
		rm1 := pmetric.NewResourceMetrics()
		rm1.Resource().Attributes().PutStr("tenant.id", "tenant-1")
		rm2 := pmetric.NewResourceMetrics()
		rm2.Resource().Attributes().PutStr("tenant.id", "tenant-2")
		rm3 := pmetric.NewResourceMetrics()
		rm3.Resource().Attributes().PutStr("tenant.id", "tenant-1")
		rm4 := pmetric.NewResourceMetrics()
		rm4.Resource().Attributes().PutStr("tenant.id", "tenant-3")

		resources := []pmetric.ResourceMetrics{rm1, rm2, rm3, rm4}
		result := processor.partition(resources)

		// Verify correct number of tenants
		assert.Equal(t, 3, len(result))

		// Verify resources are grouped by tenant
		assert.Len(t, result["tenant-1"], 2)
		assert.Len(t, result["tenant-2"], 1)
		assert.Len(t, result["tenant-3"], 1)
	})

	t.Run("traces", func(t *testing.T) {
		processor := &Processor[ptrace.ResourceSpans]{
			tenantConfig: TenantConfig{
				Label:   "tenant.id",
				Format:  "%s",
				Header:  "X-Scope-OrgID",
				Default: "default",
			},
			getResource: func(rs ptrace.ResourceSpans) pcommon.Resource {
				return rs.Resource()
			},
			telemetrySettings: component.TelemetrySettings{
				Logger: zap.NewNop(),
			},
		}

		// Create test data with multiple tenants and multiple resources per tenant
		rs1 := ptrace.NewResourceSpans()
		rs1.Resource().Attributes().PutStr("tenant.id", "tenant-1")
		rs2 := ptrace.NewResourceSpans()
		rs2.Resource().Attributes().PutStr("tenant.id", "tenant-2")
		rs3 := ptrace.NewResourceSpans()
		rs3.Resource().Attributes().PutStr("tenant.id", "tenant-1")
		rs4 := ptrace.NewResourceSpans()
		rs4.Resource().Attributes().PutStr("tenant.id", "tenant-3")

		resources := []ptrace.ResourceSpans{rs1, rs2, rs3, rs4}
		result := processor.partition(resources)

		// Verify correct number of tenants
		assert.Equal(t, 3, len(result))

		// Verify resources are grouped by tenant
		assert.Len(t, result["tenant-1"], 2)
		assert.Len(t, result["tenant-2"], 1)
		assert.Len(t, result["tenant-3"], 1)
	})
}

func TestExtractTenantFromResource(t *testing.T) {

	type testCase struct {
		name           string
		tenantConfig   TenantConfig
		setupResource  func() pcommon.Resource
		expectedTenant string
	}

	tests := []testCase{
		{
			name: "extract tenant from primary label",
			tenantConfig: TenantConfig{
				Label:   "tenant.id",
				Format:  "%s",
				Header:  "X-Scope-OrgID",
				Default: "default",
			},
			setupResource: func() pcommon.Resource {
				r := pcommon.NewResource()
				r.Attributes().PutStr("tenant.id", "tenant-1")
				r.Attributes().PutStr("service.name", "test-service")
				return r
			},
			expectedTenant: "tenant-1",
		},
		{
			name: "extract tenant from labels list when primary label not found",
			tenantConfig: TenantConfig{
				Label:   "tenant.id",
				Labels:  []string{"org.id", "namespace"},
				Format:  "%s",
				Header:  "X-Scope-OrgID",
				Default: "default",
			},
			setupResource: func() pcommon.Resource {
				r := pcommon.NewResource()
				r.Attributes().PutStr("org.id", "org-123")
				r.Attributes().PutStr("service.name", "test-service")
				return r
			},
			expectedTenant: "org-123",
		},
		{
			name: "extract tenant from second label in labels list",
			tenantConfig: TenantConfig{
				Label:   "tenant.id",
				Labels:  []string{"org.id", "namespace", "customer.id"},
				Format:  "%s",
				Header:  "X-Scope-OrgID",
				Default: "default",
			},
			setupResource: func() pcommon.Resource {
				r := pcommon.NewResource()
				r.Attributes().PutStr("namespace", "prod-namespace")
				r.Attributes().PutStr("customer.id", "customer-456")
				r.Attributes().PutStr("service.name", "test-service")
				return r
			},
			expectedTenant: "prod-namespace",
		},
		{
			name: "use default tenant when no labels match",
			tenantConfig: TenantConfig{
				Label:   "tenant.id",
				Labels:  []string{"org.id", "namespace"},
				Format:  "%s",
				Header:  "X-Scope-OrgID",
				Default: "default-tenant",
			},
			setupResource: func() pcommon.Resource {
				r := pcommon.NewResource()
				r.Attributes().PutStr("service.name", "test-service")
				r.Attributes().PutStr("other.label", "other-value")
				return r
			},
			expectedTenant: "default-tenant",
		},
		{
			name: "return empty string when no labels match and no default",
			tenantConfig: TenantConfig{
				Label:  "tenant.id",
				Labels: []string{"org.id", "namespace"},
				Format: "%s",
				Header: "X-Scope-OrgID",
			},
			setupResource: func() pcommon.Resource {
				r := pcommon.NewResource()
				r.Attributes().PutStr("service.name", "test-service")
				return r
			},
			expectedTenant: "",
		},
		{
			name: "primary label takes precedence over labels list",
			tenantConfig: TenantConfig{
				Label:   "tenant.id",
				Labels:  []string{"org.id", "namespace"},
				Format:  "%s",
				Header:  "X-Scope-OrgID",
				Default: "default",
			},
			setupResource: func() pcommon.Resource {
				r := pcommon.NewResource()
				r.Attributes().PutStr("tenant.id", "primary-tenant")
				r.Attributes().PutStr("org.id", "org-fallback")
				r.Attributes().PutStr("namespace", "namespace-fallback")
				return r
			},
			expectedTenant: "primary-tenant",
		},
		{
			name: "handle empty tenant label value falls back to default",
			tenantConfig: TenantConfig{
				Label:   "tenant.id",
				Format:  "%s",
				Header:  "X-Scope-OrgID",
				Default: "default-tenant",
			},
			setupResource: func() pcommon.Resource {
				r := pcommon.NewResource()
				r.Attributes().PutStr("tenant.id", "")
				r.Attributes().PutStr("service.name", "test-service")
				return r
			},
			expectedTenant: "default-tenant",
		},
		{
			name: "empty primary label falls back to labels list",
			tenantConfig: TenantConfig{
				Label:   "tenant.id",
				Labels:  []string{"org.id", "namespace"},
				Format:  "%s",
				Header:  "X-Scope-OrgID",
				Default: "default-tenant",
			},
			setupResource: func() pcommon.Resource {
				r := pcommon.NewResource()
				r.Attributes().PutStr("tenant.id", "")
				r.Attributes().PutStr("org.id", "fallback-org")
				r.Attributes().PutStr("service.name", "test-service")
				return r
			},
			expectedTenant: "fallback-org",
		},
		{
			name: "extract tenant with no labels configured, only default",
			tenantConfig: TenantConfig{
				Format:  "%s",
				Header:  "X-Scope-OrgID",
				Default: "default-only",
			},
			setupResource: func() pcommon.Resource {
				r := pcommon.NewResource()
				r.Attributes().PutStr("service.name", "test-service")
				r.Attributes().PutStr("any.attribute", "any-value")
				return r
			},
			expectedTenant: "default-only",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor := &Processor[plog.ResourceLogs]{
				tenantConfig: tt.tenantConfig,
				telemetrySettings: component.TelemetrySettings{
					Logger: zap.NewNop(),
				},
			}

			resource := tt.setupResource()
			result := processor.extractTenantFromResource(resource)

			assert.Equal(t, tt.expectedTenant, result)
		})
	}
}

func TestSend(t *testing.T) {

	type testCase struct {
		name          string
		tenant        string
		tenantFormat  string
		contentType   contentType
		statusCode    int
		wantErr       bool
		customHeaders configopaque.MapList
		checkRequest  func(t *testing.T, req *http.Request)
	}

	t.Run("logs", func(t *testing.T) {
		tests := []testCase{
			{
				name:         "successful send",
				tenant:       "tenant-1",
				tenantFormat: "%s",
				contentType:  contentTypeProtobuf,
				statusCode:   http.StatusOK,
				wantErr:      false,
				checkRequest: func(t *testing.T, req *http.Request) {
					assert.Equal(t, http.MethodPost, req.Method)
					assert.Equal(t, "application/x-protobuf", req.Header.Get("Content-Type"))
					assert.Equal(t, "tenant-1", req.Header.Get("X-Scope-OrgID"))
				},
			},
			{
				name:         "send with json content type",
				tenant:       "tenant-1",
				tenantFormat: "%s",
				contentType:  contentTypeJSON,
				statusCode:   http.StatusOK,
				wantErr:      false,
				checkRequest: func(t *testing.T, req *http.Request) {
					assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
				},
			},
			{
				name:         "send with unsupported content type",
				tenant:       "tenant-1",
				tenantFormat: "%s",
				contentType:  "unsupported",
				statusCode:   http.StatusOK,
				wantErr:      true,
			},
			{
				name:         "send with custom header format",
				tenant:       "tenant-2",
				tenantFormat: "prefix-%s",
				contentType:  contentTypeProtobuf,
				statusCode:   http.StatusOK,
				wantErr:      false,
				checkRequest: func(t *testing.T, req *http.Request) {
					assert.Equal(t, "prefix-tenant-2", req.Header.Get("X-Scope-OrgID"))
				},
			},
			{
				name:         "send with custom headers",
				tenant:       "tenant-3",
				tenantFormat: "%s",
				contentType:  contentTypeProtobuf,
				statusCode:   http.StatusOK,
				wantErr:      false,
				customHeaders: configopaque.MapList{
					{Name: "X-Custom-Header", Value: "test-value"},
					{Name: "X-Another-Header", Value: "another-value"},
				},
				checkRequest: func(t *testing.T, req *http.Request) {
					assert.Equal(t, "test-value", req.Header.Get("X-Custom-Header"))
					assert.Equal(t, "another-value", req.Header.Get("X-Another-Header"))
				},
			},
			{
				name:         "send with error status code",
				tenant:       "tenant-4",
				tenantFormat: "%s",
				contentType:  contentTypeProtobuf,
				statusCode:   http.StatusBadRequest,
				wantErr:      false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				mockClient := NewMockHTTPClient(ctrl)

				if !tt.wantErr {
					mockClient.EXPECT().
						Do(gomock.Any()).
						DoAndReturn(func(req *http.Request) (*http.Response, error) {
							if tt.checkRequest != nil {
								tt.checkRequest(t, req)
							}
							return &http.Response{
								StatusCode: tt.statusCode,
								Body:       io.NopCloser(bytes.NewBufferString("OK")),
							}, nil
						}).
						Times(1)
				}

				processor := &Processor[plog.ResourceLogs]{
					tenantConfig: TenantConfig{
						Label:   "tenant.id",
						Format:  tt.tenantFormat,
						Header:  "X-Scope-OrgID",
						Default: "default",
					},
					contentType: tt.contentType,
					clientConfig: confighttp.ClientConfig{
						Endpoint: "http://localhost:3100/loki/api/v1/push",
						Headers:  tt.customHeaders,
					},
					httpClient: mockClient,
					marshalResources: func(rls []plog.ResourceLogs, ct contentType) ([]byte, error) {
						return []byte("test-data"), nil
					},
					exporterTelemetry: func() *exporterTelemetry {
						tb, _ := metadata.NewTelemetryBuilder(component.TelemetrySettings{
							MeterProvider: noop.NewMeterProvider(),
						})
						return &exporterTelemetry{
							telemetryBuilder: tb,
							otelAttrs:        []attribute.KeyValue{},
						}
					}(),
				}

				ctx := context.Background()
				resources := []plog.ResourceLogs{createTestResourceLogs()}

				resp, err := processor.send(ctx, tt.tenant, resources)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.statusCode, resp.StatusCode)
				}
			})
		}
	})

	t.Run("metrics", func(t *testing.T) {
		tests := []testCase{
			{
				name:         "successful send",
				tenant:       "tenant-1",
				tenantFormat: "%s",
				contentType:  contentTypeProtobuf,
				statusCode:   http.StatusOK,
				wantErr:      false,
				checkRequest: func(t *testing.T, req *http.Request) {
					assert.Equal(t, http.MethodPost, req.Method)
					assert.Equal(t, "application/x-protobuf", req.Header.Get("Content-Type"))
					assert.Equal(t, "tenant-1", req.Header.Get("X-Scope-OrgID"))
				},
			},
			{
				name:         "send with json content type",
				tenant:       "tenant-1",
				tenantFormat: "%s",
				contentType:  contentTypeJSON,
				statusCode:   http.StatusOK,
				wantErr:      false,
				checkRequest: func(t *testing.T, req *http.Request) {
					assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
				},
			},
			{
				name:         "send with unsupported content type",
				tenant:       "tenant-1",
				tenantFormat: "%s",
				contentType:  "unsupported",
				statusCode:   http.StatusOK,
				wantErr:      true,
			},
			{
				name:         "send with custom header format",
				tenant:       "tenant-2",
				tenantFormat: "prefix-%s",
				contentType:  contentTypeProtobuf,
				statusCode:   http.StatusOK,
				wantErr:      false,
				checkRequest: func(t *testing.T, req *http.Request) {
					assert.Equal(t, "prefix-tenant-2", req.Header.Get("X-Scope-OrgID"))
				},
			},
			{
				name:         "send with custom headers",
				tenant:       "tenant-3",
				tenantFormat: "%s",
				contentType:  contentTypeProtobuf,
				statusCode:   http.StatusOK,
				wantErr:      false,
				customHeaders: configopaque.MapList{
					{Name: "X-Custom-Header", Value: "test-value"},
					{Name: "X-Another-Header", Value: "another-value"},
				},
				checkRequest: func(t *testing.T, req *http.Request) {
					assert.Equal(t, "test-value", req.Header.Get("X-Custom-Header"))
					assert.Equal(t, "another-value", req.Header.Get("X-Another-Header"))
				},
			},
			{
				name:         "send with error status code",
				tenant:       "tenant-4",
				tenantFormat: "%s",
				contentType:  contentTypeProtobuf,
				statusCode:   http.StatusBadRequest,
				wantErr:      false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				mockClient := NewMockHTTPClient(ctrl)

				if !tt.wantErr {
					mockClient.EXPECT().
						Do(gomock.Any()).
						DoAndReturn(func(req *http.Request) (*http.Response, error) {
							if tt.checkRequest != nil {
								tt.checkRequest(t, req)
							}
							return &http.Response{
								StatusCode: tt.statusCode,
								Body:       io.NopCloser(bytes.NewBufferString("OK")),
							}, nil
						}).
						Times(1)
				}

				processor := &Processor[pmetric.ResourceMetrics]{
					tenantConfig: TenantConfig{
						Label:   "tenant.id",
						Format:  tt.tenantFormat,
						Header:  "X-Scope-OrgID",
						Default: "default",
					},
					contentType: tt.contentType,
					clientConfig: confighttp.ClientConfig{
						Endpoint: "http://localhost:3100/mimir/api/v1/push",
						Headers:  tt.customHeaders,
					},
					httpClient: mockClient,
					marshalResources: func(rms []pmetric.ResourceMetrics, ct contentType) ([]byte, error) {
						return []byte("test-data"), nil
					},
					exporterTelemetry: func() *exporterTelemetry {
						tb, _ := metadata.NewTelemetryBuilder(component.TelemetrySettings{
							MeterProvider: noop.NewMeterProvider(),
						})
						return &exporterTelemetry{
							telemetryBuilder: tb,
							otelAttrs:        []attribute.KeyValue{},
						}
					}(),
				}

				ctx := context.Background()
				resources := []pmetric.ResourceMetrics{createTestResourceMetrics()}

				resp, err := processor.send(ctx, tt.tenant, resources)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.statusCode, resp.StatusCode)
				}
			})
		}
	})

	t.Run("traces", func(t *testing.T) {
		tests := []testCase{
			{
				name:         "successful send",
				tenant:       "tenant-1",
				tenantFormat: "%s",
				contentType:  contentTypeProtobuf,
				statusCode:   http.StatusOK,
				wantErr:      false,
				checkRequest: func(t *testing.T, req *http.Request) {
					assert.Equal(t, http.MethodPost, req.Method)
					assert.Equal(t, "application/x-protobuf", req.Header.Get("Content-Type"))
					assert.Equal(t, "tenant-1", req.Header.Get("X-Scope-OrgID"))
				},
			},
			{
				name:         "send with json content type",
				tenant:       "tenant-1",
				tenantFormat: "%s",
				contentType:  contentTypeJSON,
				statusCode:   http.StatusOK,
				wantErr:      false,
				checkRequest: func(t *testing.T, req *http.Request) {
					assert.Equal(t, http.MethodPost, req.Method)
					assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
					assert.Equal(t, "tenant-1", req.Header.Get("X-Scope-OrgID"))
				},
			},
			{
				name:         "send with unsupported content type",
				tenant:       "tenant-1",
				tenantFormat: "%s",
				contentType:  "unsupported",
				statusCode:   http.StatusOK,
				wantErr:      true,
			},
			{
				name:         "send with custom header format",
				tenant:       "tenant-2",
				tenantFormat: "prefix-%s",
				contentType:  contentTypeProtobuf,
				statusCode:   http.StatusOK,
				wantErr:      false,
				checkRequest: func(t *testing.T, req *http.Request) {
					assert.Equal(t, "prefix-tenant-2", req.Header.Get("X-Scope-OrgID"))
				},
			},
			{
				name:         "send with custom headers",
				tenant:       "tenant-3",
				tenantFormat: "%s",
				contentType:  contentTypeProtobuf,
				statusCode:   http.StatusOK,
				wantErr:      false,
				customHeaders: configopaque.MapList{
					{Name: "X-Custom-Header", Value: "test-value"},
					{Name: "X-Another-Header", Value: "another-value"},
				},
				checkRequest: func(t *testing.T, req *http.Request) {
					assert.Equal(t, "test-value", req.Header.Get("X-Custom-Header"))
					assert.Equal(t, "another-value", req.Header.Get("X-Another-Header"))
				},
			},
			{
				name:         "send with error status code",
				tenant:       "tenant-4",
				tenantFormat: "%s",
				contentType:  contentTypeProtobuf,
				statusCode:   http.StatusBadRequest,
				wantErr:      false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				mockClient := NewMockHTTPClient(ctrl)

				if !tt.wantErr {
					mockClient.EXPECT().
						Do(gomock.Any()).
						DoAndReturn(func(req *http.Request) (*http.Response, error) {
							if tt.checkRequest != nil {
								tt.checkRequest(t, req)
							}
							return &http.Response{
								StatusCode: tt.statusCode,
								Body:       io.NopCloser(bytes.NewBufferString("OK")),
							}, nil
						}).
						Times(1)
				}

				processor := &Processor[ptrace.ResourceSpans]{
					tenantConfig: TenantConfig{
						Label:   "tenant.id",
						Format:  tt.tenantFormat,
						Header:  "X-Scope-OrgID",
						Default: "default",
					},
					contentType: tt.contentType,
					clientConfig: confighttp.ClientConfig{
						Endpoint: "http://localhost:4318/v1/traces",
						Headers:  tt.customHeaders,
					},
					httpClient: mockClient,
					marshalResources: func(rss []ptrace.ResourceSpans, ct contentType) ([]byte, error) {
						return []byte("test-data"), nil
					},
					exporterTelemetry: func() *exporterTelemetry {
						tb, _ := metadata.NewTelemetryBuilder(component.TelemetrySettings{
							MeterProvider: noop.NewMeterProvider(),
						})
						return &exporterTelemetry{
							telemetryBuilder: tb,
							otelAttrs:        []attribute.KeyValue{},
						}
					}(),
				}

				ctx := context.Background()
				resources := []ptrace.ResourceSpans{createTestResourceSpans()}

				resp, err := processor.send(ctx, tt.tenant, resources)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.statusCode, resp.StatusCode)
				}
			})
		}
	})
}
func TestDispatch(t *testing.T) {
	type testCase[T ResourceData] struct {
		name             string
		tenantMap        map[string][]T
		statusCode       int
		wantErr          bool
		expectedRequests int
	}

	t.Run("logs", func(t *testing.T) {
		tests := []testCase[plog.ResourceLogs]{
			{
				name: "dispatch to single tenant",
				tenantMap: map[string][]plog.ResourceLogs{
					"tenant-1": {createTestResourceLogs()},
				},
				statusCode:       http.StatusOK,
				wantErr:          false,
				expectedRequests: 1,
			},
			{
				name: "dispatch to multiple tenants",
				tenantMap: map[string][]plog.ResourceLogs{
					"tenant-1": {createTestResourceLogs()},
					"tenant-2": {createTestResourceLogs()},
					"tenant-3": {createTestResourceLogs()},
				},
				statusCode:       http.StatusOK,
				wantErr:          false,
				expectedRequests: 3,
			},
			{
				name: "dispatch with error response",
				tenantMap: map[string][]plog.ResourceLogs{
					"tenant-1": {createTestResourceLogs()},
				},
				statusCode:       http.StatusBadRequest,
				wantErr:          true,
				expectedRequests: 1,
			},
			{
				name: "dispatch with multiple resources per tenant",
				tenantMap: map[string][]plog.ResourceLogs{
					"tenant-1": {createTestResourceLogs(), createTestResourceLogs(), createTestResourceLogs()},
					"tenant-2": {createTestResourceLogs(), createTestResourceLogs()},
				},
				statusCode:       http.StatusOK,
				wantErr:          false,
				expectedRequests: 2,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				mockClient := NewMockHTTPClient(ctrl)

				mockClient.EXPECT().
					Do(gomock.Any()).
					DoAndReturn(func(req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: tt.statusCode,
							Body:       io.NopCloser(bytes.NewBufferString("OK")),
						}, nil
					}).
					Times(tt.expectedRequests)

				processor := &Processor[plog.ResourceLogs]{
					tenantConfig: TenantConfig{
						Label:   "tenant.id",
						Format:  "%s",
						Header:  "X-Scope-OrgID",
						Default: "default",
					},
					contentType: contentTypeProtobuf,
					clientConfig: confighttp.ClientConfig{
						Endpoint: "http://localhost:3100/loki/api/v1/push",
					},
					httpClient: mockClient,
					marshalResources: func(rls []plog.ResourceLogs, ct contentType) ([]byte, error) {
						return []byte("test-data"), nil
					},
					exporterTelemetry: func() *exporterTelemetry {
						tb, _ := metadata.NewTelemetryBuilder(component.TelemetrySettings{
							MeterProvider: noop.NewMeterProvider(),
						})
						return &exporterTelemetry{
							telemetryBuilder: tb,
							otelAttrs:        []attribute.KeyValue{},
						}
					}(),
					telemetrySettings: component.TelemetrySettings{
						Logger: zap.NewNop(),
					},
					otelcolSignal: "logs",
				}

				ctx := context.Background()
				err := processor.dispatch(ctx, tt.tenantMap)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
			})
		}
	})

	t.Run("metrics", func(t *testing.T) {
		tests := []testCase[pmetric.ResourceMetrics]{
			{
				name: "dispatch to single tenant",
				tenantMap: map[string][]pmetric.ResourceMetrics{
					"tenant-1": {createTestResourceMetrics()},
				},
				statusCode:       http.StatusOK,
				wantErr:          false,
				expectedRequests: 1,
			},
			{
				name: "dispatch to multiple tenants",
				tenantMap: map[string][]pmetric.ResourceMetrics{
					"tenant-1": {createTestResourceMetrics()},
					"tenant-2": {createTestResourceMetrics()},
					"tenant-3": {createTestResourceMetrics()},
				},
				statusCode:       http.StatusOK,
				wantErr:          false,
				expectedRequests: 3,
			},
			{
				name: "dispatch with error response",
				tenantMap: map[string][]pmetric.ResourceMetrics{
					"tenant-1": {createTestResourceMetrics()},
				},
				statusCode:       http.StatusBadRequest,
				wantErr:          true,
				expectedRequests: 1,
			},
			{
				name: "dispatch with multiple resources per tenant",
				tenantMap: map[string][]pmetric.ResourceMetrics{
					"tenant-1": {createTestResourceMetrics(), createTestResourceMetrics(), createTestResourceMetrics()},
					"tenant-2": {createTestResourceMetrics(), createTestResourceMetrics()},
				},
				statusCode:       http.StatusOK,
				wantErr:          false,
				expectedRequests: 2,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				mockClient := NewMockHTTPClient(ctrl)

				mockClient.EXPECT().
					Do(gomock.Any()).
					DoAndReturn(func(req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: tt.statusCode,
							Body:       io.NopCloser(bytes.NewBufferString("OK")),
						}, nil
					}).
					Times(tt.expectedRequests)

				processor := &Processor[pmetric.ResourceMetrics]{
					tenantConfig: TenantConfig{
						Label:   "tenant.id",
						Format:  "%s",
						Header:  "X-Scope-OrgID",
						Default: "default",
					},
					contentType: contentTypeProtobuf,
					clientConfig: confighttp.ClientConfig{
						Endpoint: "http://localhost:3100/mimir/api/v1/push",
					},
					httpClient: mockClient,
					marshalResources: func(rms []pmetric.ResourceMetrics, ct contentType) ([]byte, error) {
						return []byte("test-data"), nil
					},
					exporterTelemetry: func() *exporterTelemetry {
						tb, _ := metadata.NewTelemetryBuilder(component.TelemetrySettings{
							MeterProvider: noop.NewMeterProvider(),
						})
						return &exporterTelemetry{
							telemetryBuilder: tb,
							otelAttrs:        []attribute.KeyValue{},
						}
					}(),
					telemetrySettings: component.TelemetrySettings{
						Logger: zap.NewNop(),
					},
					otelcolSignal: "metrics",
				}

				ctx := context.Background()
				err := processor.dispatch(ctx, tt.tenantMap)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
			})
		}
	})

	t.Run("traces", func(t *testing.T) {
		tests := []testCase[ptrace.ResourceSpans]{
			{
				name: "dispatch to single tenant",
				tenantMap: map[string][]ptrace.ResourceSpans{
					"tenant-1": {createTestResourceSpans()},
				},
				statusCode:       http.StatusOK,
				wantErr:          false,
				expectedRequests: 1,
			},
			{
				name: "dispatch to multiple tenants",
				tenantMap: map[string][]ptrace.ResourceSpans{
					"tenant-1": {createTestResourceSpans()},
					"tenant-2": {createTestResourceSpans()},
					"tenant-3": {createTestResourceSpans()},
				},
				statusCode:       http.StatusOK,
				wantErr:          false,
				expectedRequests: 3,
			},
			{
				name: "dispatch with error response",
				tenantMap: map[string][]ptrace.ResourceSpans{
					"tenant-1": {createTestResourceSpans()},
				},
				statusCode:       http.StatusBadRequest,
				wantErr:          true,
				expectedRequests: 1,
			},
			{
				name: "dispatch with multiple resources per tenant",
				tenantMap: map[string][]ptrace.ResourceSpans{
					"tenant-1": {createTestResourceSpans(), createTestResourceSpans(), createTestResourceSpans()},
					"tenant-2": {createTestResourceSpans(), createTestResourceSpans()},
				},
				statusCode:       http.StatusOK,
				wantErr:          false,
				expectedRequests: 2,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				mockClient := NewMockHTTPClient(ctrl)

				mockClient.EXPECT().
					Do(gomock.Any()).
					DoAndReturn(func(req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: tt.statusCode,
							Body:       io.NopCloser(bytes.NewBufferString("OK")),
						}, nil
					}).
					Times(tt.expectedRequests)

				processor := &Processor[ptrace.ResourceSpans]{
					tenantConfig: TenantConfig{
						Label:   "tenant.id",
						Format:  "%s",
						Header:  "X-Scope-OrgID",
						Default: "default",
					},
					contentType: contentTypeProtobuf,
					clientConfig: confighttp.ClientConfig{
						Endpoint: "http://localhost:4318/v1/traces",
					},
					httpClient: mockClient,
					marshalResources: func(rss []ptrace.ResourceSpans, ct contentType) ([]byte, error) {
						return []byte("test-data"), nil
					},
					exporterTelemetry: func() *exporterTelemetry {
						tb, _ := metadata.NewTelemetryBuilder(component.TelemetrySettings{
							MeterProvider: noop.NewMeterProvider(),
						})
						return &exporterTelemetry{
							telemetryBuilder: tb,
							otelAttrs:        []attribute.KeyValue{},
						}
					}(),
					telemetrySettings: component.TelemetrySettings{
						Logger: zap.NewNop(),
					},
					otelcolSignal: "traces",
				}

				ctx := context.Background()
				err := processor.dispatch(ctx, tt.tenantMap)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
			})
		}
	})
}
