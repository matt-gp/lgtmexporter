// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package lgtmexporter

import (
	"context"

	"github.com/mattgp/lgtmexporter/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// NewFactory creates a new factory for the LGTM exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
}

// createDefaultConfig creates the default configuration for the LGTM exporter.
func createDefaultConfig() component.Config {
	return &Config{
		QueueBatchConfig: configoptional.Some(exporterhelper.NewDefaultQueueConfig()),
		Tenant: TenantConfig{
			Label:   "tenant.id",
			Labels:  []string{},
			Format:  "%s",
			Header:  "X-Scope-OrgID",
			Default: "default",
		},
	}
}

// createLogsExporter creates a new logs exporter based on the provided configuration and settings.
func createLogsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {

	if err := cfg.(*Config).Validate(); err != nil {
		return nil, err
	}

	lgtmExp, err := newLGTMLogsExporter(ctx, set, *cfg.(*Config))
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewLogs(
		ctx,
		set,
		cfg,
		lgtmExp.pushLogsData,
		exporterhelper.WithQueue(lgtmExp.cfg.QueueBatchConfig),
		exporterhelper.WithRetry(lgtmExp.cfg.BackOffConfig),
		exporterhelper.WithStart(lgtmExp.start),
		exporterhelper.WithShutdown(lgtmExp.shutdown),
	)
}

// createMetricsExporter creates a new metrics exporter based on the provided configuration and settings.
func createMetricsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {

	if err := cfg.(*Config).Validate(); err != nil {
		return nil, err
	}

	lgtmExp, err := newLGTMMetricsExporter(ctx, set, *cfg.(*Config))
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewMetrics(
		ctx,
		set,
		cfg,
		lgtmExp.pushMetricsData,
		exporterhelper.WithQueue(lgtmExp.cfg.QueueBatchConfig),
		exporterhelper.WithRetry(lgtmExp.cfg.BackOffConfig),
		exporterhelper.WithStart(lgtmExp.start),
		exporterhelper.WithShutdown(lgtmExp.shutdown),
	)
}

// createTracesExporter creates a new traces exporter based on the provided configuration and settings.
func createTracesExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {

	if err := cfg.(*Config).Validate(); err != nil {
		return nil, err
	}

	lgtmExp, err := newLGTMTracesExporter(ctx, set, *cfg.(*Config))
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewTraces(
		ctx,
		set,
		cfg,
		lgtmExp.pushTracesData,
		exporterhelper.WithQueue(lgtmExp.cfg.QueueBatchConfig),
		exporterhelper.WithRetry(lgtmExp.cfg.BackOffConfig),
		exporterhelper.WithStart(lgtmExp.start),
		exporterhelper.WithShutdown(lgtmExp.shutdown),
	)
}
