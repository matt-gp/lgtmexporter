// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package lgtmexporter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// HTTPClient is an interface for making HTTP requests.
//
//go:generate mockgen -package lgtmexporter -source processor.go -destination processor_mock.go
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// ResourceData is an interface for OTLP resource types.
type ResourceData interface {
	plog.ResourceLogs | pmetric.ResourceMetrics | ptrace.ResourceSpans
}

// Processor is a generic struct that processes incoming telemetry resource data and forwards it to the appropriate backend.
type Processor[T ResourceData] struct {
	tenantConfig      TenantConfig
	clientConfig      confighttp.ClientConfig
	httpClient        HTTPClient
	exporterTelemetry *exporterTelemetry
	telemetrySettings component.TelemetrySettings
	otelcolSignal     string
	contentType       contentType
	getResource       func(T) pcommon.Resource
	marshalResources  func([]T, contentType) ([]byte, error)
}

// NewProcessor creates a new Processor instance for the given resource type.
func NewProcessor[T ResourceData](
	ctx context.Context,
	tenantConfig TenantConfig,
	clientConfig confighttp.ClientConfig,
	host component.Host,
	exporterTelemetry *exporterTelemetry,
	telemetrySettings component.TelemetrySettings,
	otelcolSignal string,
	contentType contentType,
	getResource func(T) pcommon.Resource,
	marshalResources func([]T, contentType) ([]byte, error),
) (*Processor[T], error) {

	// Create HTTP client with proper auth and TLS configuration
	// Pass nil for host during creation - will be properly set during start
	httpClient, err := clientConfig.ToClient(ctx, host.GetExtensions(), telemetrySettings)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP client: %w", err)
	}

	return &Processor[T]{
		tenantConfig:      tenantConfig,
		clientConfig:      clientConfig,
		httpClient:        httpClient,
		exporterTelemetry: exporterTelemetry,
		telemetrySettings: telemetrySettings,
		otelcolSignal:     otelcolSignal,
		contentType:       contentType,
		getResource:       getResource,
		marshalResources:  marshalResources,
	}, nil
}

// partition partitions the resources by tenant.
func (p *Processor[T]) partition(resources []T) map[string][]T {

	tenantMap := make(map[string][]T)

	for _, resourceData := range resources {
		tenant := p.extractTenantFromResource(p.getResource(resourceData))
		if tenant == "" {
			continue
		}
		tenantMap[tenant] = append(tenantMap[tenant], resourceData)
	}

	return tenantMap
}

// dispatch sends all the requests to the target.
func (p *Processor[T]) dispatch(ctx context.Context, tenantMap map[string][]T) error {

	errg, ctx := errgroup.WithContext(ctx)
	for tenant, resources := range tenantMap {
		errg.Go(func() error {
			// Send the request
			resp, err := p.send(ctx, tenant, resources)
			if err != nil {
				p.telemetrySettings.Logger.Error("failed to send request", zap.Error(err))
				return err
			}

			// Update request count
			p.exporterTelemetry.recordLGTMSentRequests(
				ctx,
				1,
				resp.StatusCode,
				p.otelcolSignal,
				tenant,
			)

			// Update sample count
			p.exporterTelemetry.recordLGTMSentSamples(
				ctx,
				len(resources),
				resp.StatusCode,
				p.otelcolSignal,
				tenant,
			)

			// If the response status code indicates an error, log it
			if resp.StatusCode >= http.StatusBadRequest {
				p.telemetrySettings.Logger.Error(
					"error sending request",
					zap.String("otelcol.signal", p.otelcolSignal),
					zap.String("otelcol.signal.tenant", tenant),
					zap.Int("num.resources", len(resources)),
					zap.Int("http.status.code", resp.StatusCode),
				)
				return fmt.Errorf("error sending request: %d", resp.StatusCode)
			}

			// Log successful requests at debug level
			p.telemetrySettings.Logger.Debug(
				"sent request",
				zap.String("otelcol.signal", p.otelcolSignal),
				zap.String("otelcol.signal.tenant", tenant),
				zap.Int("num.resources", len(resources)),
				zap.Int("http.status.code", resp.StatusCode),
			)

			return nil
		})
	}

	return errg.Wait()
}

// send sends an individual request to the target.
func (p *Processor[T]) send(ctx context.Context, tenant string, resources []T) (http.Response, error) {

	// Marshal the request body
	body, err := p.marshalResources(resources, p.contentType)
	if err != nil {
		return http.Response{}, fmt.Errorf("failed to marshal data %w", err)
	}

	// Create the HTTP request
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		p.clientConfig.Endpoint,
		io.NopCloser(bytes.NewReader(body)),
	)
	if err != nil {
		return http.Response{}, fmt.Errorf("failed to create request %w", err)
	}

	// Set headers
	var contentTypeHeaderVal string
	switch p.contentType {
	case contentTypeJSON:
		contentTypeHeaderVal = "application/json"
	case contentTypeProtobuf:
		contentTypeHeaderVal = "application/x-protobuf"
	default:
		return http.Response{}, fmt.Errorf("unsupported content type: %s", p.contentType)
	}

	req.Header.Set("Content-Type", contentTypeHeaderVal)
	req.Header.Add(p.tenantConfig.Header, fmt.Sprintf(p.tenantConfig.Format, tenant))

	// Add custom headers
	for _, v := range p.clientConfig.Headers {
		req.Header.Add(v.Name, string(v.Value))
	}

	// Record the start time before sending the request to measure latency
	startTime := time.Now()

	// Send the request
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return http.Response{}, fmt.Errorf("failed to send request %w", err)
	}

	// Record the latency of the request with actual status code
	p.exporterTelemetry.recordLGTMWriteLatency(
		ctx,
		time.Since(startTime),
		resp.StatusCode,
		req.URL.String(),
	)
	defer func() {
		if err := resp.Body.Close(); err != nil {
			p.telemetrySettings.Logger.Error("failed to close response body", zap.Error(err))
		}
	}()

	return *resp, nil
}

// extractTenantFromResource extracts the tenant information from the resource attributes
// based on the configured tenant labels and returns it.
func (p *Processor[T]) extractTenantFromResource(resource pcommon.Resource) string {

	tenant := ""

	// First, check for the dedicated tenant label
	if p.tenantConfig.Label != "" {
		for k, v := range resource.Attributes().All() {
			if k == p.tenantConfig.Label {
				tenant = v.AsString()
				break
			}
		}
	}

	// If not found and we have additional labels, check those
	if tenant == "" && len(p.tenantConfig.Labels) > 0 {
		for k, v := range resource.Attributes().All() {
			if slices.Contains(p.tenantConfig.Labels, k) {
				tenant = v.AsString()
				break
			}
		}
	}

	// If still not found, use the default tenant if configured
	if tenant == "" {
		if p.tenantConfig.Default == "" {
			p.telemetrySettings.Logger.Warn("No tenant information found for resource and no default tenant configured. Skipping resource.")
			return ""
		}

		tenant = p.tenantConfig.Default
	}

	return tenant
}
