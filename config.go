// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package lgtmexporter

import (
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type contentType string

var _ component.Config = (*Config)(nil)

const (
	contentTypeJSON     contentType = "json"
	contentTypeProtobuf contentType = "protobuf"
)

// Config defines configuration for LGTM exporter.
type Config struct {

	// QueueBatchConfig defines the queue configuration.
	QueueBatchConfig configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`

	// RetryOnFailure defines the retry configuration.
	configretry.BackOffConfig `mapstructure:"retry_on_failure"`

	Tenant TenantConfig `mapstructure:"tenant"`

	ContentType contentType `mapstructure:"content_type"`

	Logs    confighttp.ClientConfig `mapstructure:"logs"`
	Metrics confighttp.ClientConfig `mapstructure:"metrics"`
	Traces  confighttp.ClientConfig `mapstructure:"traces"`
}

// TenantConfig defines the configuration for a tenant.
type TenantConfig struct {
	Label   string   `mapstructure:"label"`
	Labels  []string `mapstructure:"labels"`
	Format  string   `mapstructure:"format"`
	Header  string   `mapstructure:"header"`
	Default string   `mapstructure:"default"`
}

// Validate validates the Config and returns an error if it is invalid.
func (cfg *Config) Validate() error {

	if err := cfg.Logs.Validate(); err != nil {
		return err
	}

	if err := cfg.Metrics.Validate(); err != nil {
		return err
	}

	if err := cfg.Traces.Validate(); err != nil {
		return err
	}

	if cfg.Logs.Endpoint == "" && cfg.Metrics.Endpoint == "" && cfg.Traces.Endpoint == "" {
		return errors.New("at least one of logs, metrics, or traces endpoint must be configured")
	}
	return nil
}
