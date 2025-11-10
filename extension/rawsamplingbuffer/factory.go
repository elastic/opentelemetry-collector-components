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

package rawsamplingbuffer

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

const (
	defaultBufferSize    = 1000
	defaultMaxEntrySize  = 1048576 // 1MB
	defaultTTL           = 5 * time.Minute
	defaultEvictionPolicy = "lru"
)

// NewFactory creates a factory for the rawsamplingbuffer extension.
func NewFactory() extension.Factory {
	return extension.NewFactory(
		component.MustNewType("rawsamplingbuffer"),
		createDefaultConfig,
		createExtension,
		component.StabilityLevelDevelopment,
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		BufferSize:     defaultBufferSize,
		MaxEntrySize:   defaultMaxEntrySize,
		TTL:            defaultTTL,
		EvictionPolicy: defaultEvictionPolicy,
	}
}

func createExtension(_ context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
	config := cfg.(*Config)
	return newRawSamplingBufferExtension(config, set), nil
}
