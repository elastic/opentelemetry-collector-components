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

package ratelimitprocessor

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"

	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/metadata"
)

func TestLoadConfig(t *testing.T) {
	grpcClientConfig := configgrpc.NewDefaultClientConfig()
	grpcClientConfig.Endpoint = "localhost:1081"

	tests := []struct {
		name        string
		expected    component.Config
		expectedErr string
	}{
		{
			name: "local",
			expected: &Config{
				Type: LocalRateLimiter,
				RateLimitSettings: RateLimitSettings{
					Rate:             100,
					Burst:            200,
					Strategy:         StrategyRateLimitRequests,
					ThrottleBehavior: ThrottleBehaviorError,
					ThrottleInterval: 1 * time.Second,
				},
			},
		},
		{
			name: "strategy",
			expected: &Config{
				Type: LocalRateLimiter,
				RateLimitSettings: RateLimitSettings{
					Rate:             100,
					Burst:            200,
					Strategy:         StrategyRateLimitBytes,
					ThrottleBehavior: ThrottleBehaviorError,
					ThrottleInterval: 1 * time.Second,
				},
			},
		},
		{
			name: "gubernator",
			expected: &Config{
				Type: GubernatorRateLimiter,
				RateLimitSettings: RateLimitSettings{
					Rate:             100,
					Burst:            200,
					Strategy:         StrategyRateLimitRequests,
					ThrottleBehavior: ThrottleBehaviorError,
					ThrottleInterval: 1 * time.Second,
				},
			},
		},
		{
			name: "metadata_keys",
			expected: &Config{
				Type: LocalRateLimiter,
				RateLimitSettings: RateLimitSettings{
					Rate:             100,
					Burst:            200,
					Strategy:         StrategyRateLimitRequests,
					ThrottleBehavior: ThrottleBehaviorError,
					ThrottleInterval: 1 * time.Second,
				},
				MetadataKeys: []string{"project_id"},
			},
		},
		{
			name: "overrides_all",
			expected: &Config{
				Type: LocalRateLimiter,
				RateLimitSettings: RateLimitSettings{
					Rate:             100,
					Burst:            200,
					Strategy:         StrategyRateLimitBytes,
					ThrottleBehavior: ThrottleBehaviorError,
					ThrottleInterval: 1 * time.Second,
				},
				Overrides: map[string]RateLimitOverride{
					"project-id:e678ebd7-3a15-43dd-a95c-1cf0639a6292": {
						Rate:  ptr(300),
						Burst: ptr(400),
					},
				},
			},
		},
		{
			name: "overrides_rate",
			expected: &Config{
				Type: LocalRateLimiter,
				RateLimitSettings: RateLimitSettings{
					Rate:             100,
					Burst:            200,
					Strategy:         StrategyRateLimitBytes,
					ThrottleBehavior: ThrottleBehaviorError,
					ThrottleInterval: 1 * time.Second,
				},
				Overrides: map[string]RateLimitOverride{
					"project-id:e678ebd7-3a15-43dd-a95c-1cf0639a6292": {
						Rate: ptr(300),
					},
				},
			},
		},
		{
			name: "overrides_burst",
			expected: &Config{
				Type: LocalRateLimiter,
				RateLimitSettings: RateLimitSettings{
					Rate:             100,
					Burst:            200,
					Strategy:         StrategyRateLimitBytes,
					ThrottleBehavior: ThrottleBehaviorError,
					ThrottleInterval: 1 * time.Second,
				},
				Overrides: map[string]RateLimitOverride{
					"project-id:e678ebd7-3a15-43dd-a95c-1cf0639a6292": {
						Burst: ptr(400),
					},
				},
			},
		},
		{
			name: "overrides_throttle_interval",
			expected: &Config{
				Type: LocalRateLimiter,
				RateLimitSettings: RateLimitSettings{
					Rate:             100,
					Burst:            200,
					Strategy:         StrategyRateLimitBytes,
					ThrottleBehavior: ThrottleBehaviorError,
					ThrottleInterval: 1 * time.Second,
				},
				Overrides: map[string]RateLimitOverride{
					"project-id:e678ebd7-3a15-43dd-a95c-1cf0639a6292": {
						Rate:             ptr(400),
						ThrottleInterval: ptr(10 * time.Second),
					},
				},
			},
		},
		{
			name:        "invalid_rate",
			expectedErr: "rate must be greater than zero",
		},
		{
			name:        "invalid_burst",
			expectedErr: "burst must be greater than zero",
		},
		{
			name:        "invalid_strategy",
			expectedErr: `invalid strategy "foo", expected one of ["requests" "records" "bytes"]`,
		},
		{
			name:        "invalid_throttle_behavior",
			expectedErr: `invalid throttle behavior "foo", expected one of ["error" "delay"]`,
		},
		{
			name:        "invalid_type",
			expectedErr: `invalid rate limiter type "invalid", expected one of ["local" "gubernator"]`,
		},
	}

	factory := NewFactory()
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := factory.CreateDefaultConfig()
			id := component.NewIDWithName(metadata.Type, tt.name)
			sub, err := cm.Sub(id.String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			err = xconfmap.Validate(cfg)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, cfg)
		})
	}
}

func TestResolveRateLimitSettings(t *testing.T) {
	cfg := &Config{
		RateLimitSettings: RateLimitSettings{
			Rate:             100,
			Burst:            200,
			Strategy:         StrategyRateLimitRequests,
			ThrottleBehavior: ThrottleBehaviorError,
			ThrottleInterval: 1 * time.Second,
		},
		Overrides: map[string]RateLimitOverride{
			"project-id:e678ebd7-3a15-43dd-a95c-1cf0639a6292": {
				Rate:             ptr(300),
				Burst:            ptr(400),
				ThrottleInterval: ptr(10 * time.Second),
			},
		},
	}

	t.Run("no override", func(t *testing.T) {
		result := resolveRateLimitSettings(cfg, "default")
		require.Equal(t, cfg.RateLimitSettings, result)
	})

	t.Run("override", func(t *testing.T) {
		result := resolveRateLimitSettings(cfg, "project-id:e678ebd7-3a15-43dd-a95c-1cf0639a6292")
		require.Equal(t, RateLimitSettings{
			Rate:             300,
			Burst:            400,
			Strategy:         StrategyRateLimitRequests,
			ThrottleBehavior: ThrottleBehaviorError,
			ThrottleInterval: 10 * time.Second,
		}, result)
	})
}

func ptr[T any](v T) *T {
	return &v
}
