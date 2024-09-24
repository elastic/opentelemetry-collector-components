// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logstashexporter

import "go.opentelemetry.io/collector/config/configtelemetry"

// Config defines configuration for logging exporter.
type Config struct {
	// Verbosity defines the logging exporter verbosity. Just a sample for a param
	Verbosity configtelemetry.Level `mapstructure:"verbosity,omitempty"`
}
