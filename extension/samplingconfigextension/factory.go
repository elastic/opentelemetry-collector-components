// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package samplingconfigextension

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

// NewFactory creates a factory for the sampling config extension.
func NewFactory() extension.Factory {
	return extension.NewFactory(
		component.MustNewType("samplingconfigextension"),
		createDefaultConfig,
		createExtension,
		component.StabilityLevelDevelopment,
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		ConfigIndex:       ".elastic-sampling-config",
		PollInterval:      30 * time.Second,
		DefaultSampleRate: 0.0,
	}
}

func createExtension(_ context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
	config := cfg.(*Config)
	return newSamplingConfigExtension(config, set.Logger), nil
}
