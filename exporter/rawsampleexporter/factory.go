// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package rawsampleexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/elastic/opentelemetry-collector-components/exporter/rawsampleexporter/internal/metadata"
)

// NewFactory creates a factory for the raw sample exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
}

// createDefaultConfig creates the default configuration for the exporter.
func createDefaultConfig() component.Config {
	return &Config{
		Endpoint:      "http://localhost:9200",
		Index:         "logs-raw-samples",
		MaxBatchSize:  100,
		Timeout:       30 * time.Second,
		RetrySettings: configretry.NewDefaultBackOffConfig(),
	}
}

// createLogsExporter creates a logs exporter based on the config.
func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	config := cfg.(*Config)

	exp := &rawSampleExporter{
		config: config,
		logger: set.Logger,
	}

	return exporterhelper.NewLogs(
		ctx,
		set,
		cfg,
		exp.ConsumeLogs,
		exporterhelper.WithStart(exp.Start),
		exporterhelper.WithShutdown(exp.Shutdown),
		exporterhelper.WithRetry(config.RetrySettings),
	)
}
