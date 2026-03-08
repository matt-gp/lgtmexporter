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

	type testCase[T ResourceData] struct {
		name            string
		tenantConfig    TenantConfig
		setupData       func() []T
		expectedTenants []string
	}
	t.Run("logs", func(t *testing.T) {
		tests := []testCase[plog.ResourceLogs]{
			{
				name: "partition by tenant label",
				tenantConfig: TenantConfig{
					Label:   "tenant.id",
					Format:  "%s",
					Header:  "X-Scope-OrgID",
					Default: "default",
				},
				setupData: func() []plog.ResourceLogs {
					rl1 := plog.NewResourceLogs()
					rl1.Resource().Attributes().PutStr("tenant.id", "tenant-1")
					rl2 := plog.NewResourceLogs()
					rl2.Resource().Attributes().PutStr("tenant.id", "tenant-2")
					rl3 := plog.NewResourceLogs()
					rl3.Resource().Attributes().PutStr("tenant.id", "tenant-1")
					return []plog.ResourceLogs{rl1, rl2, rl3}
				},
				expectedTenants: []string{"tenant-1", "tenant-2"},
			},
			{
				name: "partition with default tenant",
				tenantConfig: TenantConfig{
					Label:   "tenant.id",
					Format:  "%s",
					Header:  "X-Scope-OrgID",
					Default: "default-tenant",
				},
				setupData: func() []plog.ResourceLogs {
					rl1 := plog.NewResourceLogs()
					rl1.Resource().Attributes().PutStr("tenant.id", "tenant-1")
					rl2 := plog.NewResourceLogs()
					return []plog.ResourceLogs{rl1, rl2}
				},
				expectedTenants: []string{"tenant-1", "default-tenant"},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				processor := &Processor[plog.ResourceLogs]{
					tenantConfig: tt.tenantConfig,
					getResource: func(rl plog.ResourceLogs) pcommon.Resource {
						return rl.Resource()
					},
					telemetrySettings: component.TelemetrySettings{
						Logger: zap.NewNop(),
					},
				}

				result := processor.partition(tt.setupData())
				tenantsFound := make(map[string]bool)
				for tenant := range result {
					tenantsFound[tenant] = true
				}

				for _, expectedTenant := range tt.expectedTenants {
					assert.True(t, tenantsFound[expectedTenant], "Expected tenant %s not found in result", expectedTenant)
				}
				assert.Equal(t, len(tt.expectedTenants), len(result), "Number of tenants doesn't match")
			})
		}
	})

	t.Run("metrics", func(t *testing.T) {
		tests := []testCase[pmetric.ResourceMetrics]{
			{
				name: "partition by tenant label",
				tenantConfig: TenantConfig{
					Label:   "tenant.id",
					Format:  "%s",
					Header:  "X-Scope-OrgID",
					Default: "default",
				},
				setupData: func() []pmetric.ResourceMetrics {
					rm1 := pmetric.NewResourceMetrics()
					rm1.Resource().Attributes().PutStr("tenant.id", "tenant-1")
					rm2 := pmetric.NewResourceMetrics()
					rm2.Resource().Attributes().PutStr("tenant.id", "tenant-2")
					rm3 := pmetric.NewResourceMetrics()
					rm3.Resource().Attributes().PutStr("tenant.id", "tenant-1")
					return []pmetric.ResourceMetrics{rm1, rm2, rm3}
				},
				expectedTenants: []string{"tenant-1", "tenant-2"},
			},
			{
				name: "partition with default tenant",
				tenantConfig: TenantConfig{
					Label:   "tenant.id",
					Format:  "%s",
					Header:  "X-Scope-OrgID",
					Default: "default-tenant",
				},
				setupData: func() []pmetric.ResourceMetrics {
					rm1 := pmetric.NewResourceMetrics()
					rm1.Resource().Attributes().PutStr("tenant.id", "tenant-1")
					rm2 := pmetric.NewResourceMetrics()
					return []pmetric.ResourceMetrics{rm1, rm2}
				},
				expectedTenants: []string{"tenant-1", "default-tenant"},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				processor := &Processor[pmetric.ResourceMetrics]{
					tenantConfig: tt.tenantConfig,
					getResource: func(rm pmetric.ResourceMetrics) pcommon.Resource {
						return rm.Resource()
					},
					telemetrySettings: component.TelemetrySettings{
						Logger: zap.NewNop(),
					},
				}

				result := processor.partition(tt.setupData())
				tenantsFound := make(map[string]bool)
				for tenant := range result {
					tenantsFound[tenant] = true
				}

				for _, expectedTenant := range tt.expectedTenants {
					assert.True(t, tenantsFound[expectedTenant], "Expected tenant %s not found in result", expectedTenant)
				}
				assert.Equal(t, len(tt.expectedTenants), len(result), "Number of tenants doesn't match")
			})
		}
	})

	t.Run("traces", func(t *testing.T) {
		tests := []testCase[ptrace.ResourceSpans]{
			{
				name: "partition by tenant label",
				tenantConfig: TenantConfig{
					Label:   "tenant.id",
					Format:  "%s",
					Header:  "X-Scope-OrgID",
					Default: "default",
				},
				setupData: func() []ptrace.ResourceSpans {
					rs1 := ptrace.NewResourceSpans()
					rs1.Resource().Attributes().PutStr("tenant.id", "tenant-1")
					rs2 := ptrace.NewResourceSpans()
					rs2.Resource().Attributes().PutStr("tenant.id", "tenant-2")
					rs3 := ptrace.NewResourceSpans()
					rs3.Resource().Attributes().PutStr("tenant.id", "tenant-1")
					return []ptrace.ResourceSpans{rs1, rs2, rs3}
				},
				expectedTenants: []string{"tenant-1", "tenant-2"},
			},
			{
				name: "partition with default tenant",
				tenantConfig: TenantConfig{
					Label:   "tenant.id",
					Format:  "%s",
					Header:  "X-Scope-OrgID",
					Default: "default-tenant",
				},
				setupData: func() []ptrace.ResourceSpans {
					rs1 := ptrace.NewResourceSpans()
					rs1.Resource().Attributes().PutStr("tenant.id", "tenant-1")
					rs2 := ptrace.NewResourceSpans()
					return []ptrace.ResourceSpans{rs1, rs2}
				},
				expectedTenants: []string{"tenant-1", "default-tenant"},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				processor := &Processor[ptrace.ResourceSpans]{
					tenantConfig: tt.tenantConfig,
					getResource: func(rs ptrace.ResourceSpans) pcommon.Resource {
						return rs.Resource()
					},
					telemetrySettings: component.TelemetrySettings{
						Logger: zap.NewNop(),
					},
				}

				result := processor.partition(tt.setupData())
				tenantsFound := make(map[string]bool)
				for tenant := range result {
					tenantsFound[tenant] = true
				}

				for _, expectedTenant := range tt.expectedTenants {
					assert.True(t, tenantsFound[expectedTenant], "Expected tenant %s not found in result", expectedTenant)
				}
				assert.Equal(t, len(tt.expectedTenants), len(result), "Number of tenants doesn't match")
			})
		}
	})
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
