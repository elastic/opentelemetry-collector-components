// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pipeline

import (
	"reflect"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.uber.org/zap"
)

// mergeComponentConfig merges default config with provided config map using confmap.
func (m *Manager) mergeComponentConfig(defaultCfg component.Config, providedCfg map[string]interface{}) (component.Config, error) {
	if len(providedCfg) == 0 {
		return defaultCfg, nil
	}

	// Convert provided config to confmap
	providedMap := confmap.NewFromStringMap(providedCfg)

	// Convert default config to confmap by marshaling and unmarshaling
	defaultMap := confmap.New()
	if err := defaultMap.Marshal(defaultCfg); err != nil {
		m.logger.Warn("Failed to marshal default config, using defaults",
			zap.Error(err))
		return defaultCfg, nil
	}

	// Create merged map starting with defaults
	mergedMap := confmap.New()
	if err := mergedMap.Merge(defaultMap); err != nil {
		m.logger.Warn("Failed to merge default config, using defaults",
			zap.Error(err))
		return defaultCfg, nil
	}

	// Merge provided config (overrides defaults)
	if err := mergedMap.Merge(providedMap); err != nil {
		m.logger.Warn("Failed to merge provided config, using defaults",
			zap.Error(err),
			zap.Any("provided_config", providedCfg))
		return defaultCfg, nil
	}

	// Create new config instance based on default config type
	// We need to create a new instance of the same type as defaultCfg
	mergedCfg := reflect.New(reflect.TypeOf(defaultCfg).Elem()).Interface().(component.Config)

	// Unmarshal merged configuration
	if err := mergedMap.Unmarshal(mergedCfg); err != nil {
		m.logger.Warn("Failed to unmarshal merged config, using defaults",
			zap.Error(err),
			zap.Any("provided_config", providedCfg))
		return defaultCfg, nil
	}

	m.logger.Info("Successfully merged component configuration",
		zap.Any("merged_config", mergedCfg))

	return mergedCfg, nil
}
