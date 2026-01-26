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
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"

	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/metadata"
)

var defaultDynamicRateLimiting = DynamicRateLimiting{
	DefaultWindowMultiplier: 1.3,
	WindowDuration:          2 * time.Minute,
}

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
					RetryDelay:       1 * time.Second,
				},
				Drl: defaultDynamicRateLimiting,
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
					RetryDelay:       1 * time.Second,
				},
				Drl: defaultDynamicRateLimiting,
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
					RetryDelay:       1 * time.Second,
				},
				Drl: defaultDynamicRateLimiting,
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
					RetryDelay:       1 * time.Second,
				},
				Drl:          defaultDynamicRateLimiting,
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
					RetryDelay:       1 * time.Second,
				},
				Drl: defaultDynamicRateLimiting,
				Overrides: []RateLimitOverrides{
					{
						Matches: map[string][]string{
							"project-id": {"e678ebd7-3a15-43dd-a95c-1cf0639a6292"},
						},
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
					RetryDelay:       1 * time.Second,
				},
				Drl: defaultDynamicRateLimiting,
				Overrides: []RateLimitOverrides{
					{
						Matches: map[string][]string{
							"project-id": {"e678ebd7-3a15-43dd-a95c-1cf0639a6292"},
						},
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
					RetryDelay:       1 * time.Second,
				},
				Drl: defaultDynamicRateLimiting,
				Overrides: []RateLimitOverrides{
					{
						Matches: map[string][]string{
							"project-id": {"e678ebd7-3a15-43dd-a95c-1cf0639a6292"},
						},
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
					RetryDelay:       1 * time.Second,
				},
				Drl: defaultDynamicRateLimiting,
				Overrides: []RateLimitOverrides{
					{
						Matches: map[string][]string{
							"project-id": {"e678ebd7-3a15-43dd-a95c-1cf0639a6292"},
						},
						Rate:             ptr(400),
						ThrottleInterval: ptr(10 * time.Second),
					},
				},
			},
		},
		{
			name: "overrides_disable_dynamic",
			expected: &Config{
				Type: LocalRateLimiter,
				RateLimitSettings: RateLimitSettings{
					Rate:             100,
					Burst:            200,
					Strategy:         StrategyRateLimitBytes,
					ThrottleBehavior: ThrottleBehaviorError,
					ThrottleInterval: 1 * time.Second,
					RetryDelay:       1 * time.Second,
				},
				Drl: defaultDynamicRateLimiting,
				Overrides: []RateLimitOverrides{
					{
						Matches: map[string][]string{
							"project-id": {"e678ebd7-3a15-43dd-a95c-1cf0639a6292"},
						},
						DisableDynamic: true,
					},
				},
			},
		},
		{
			name: "dynamic_rate_limit",
			expected: &Config{
				Type: GubernatorRateLimiter,
				RateLimitSettings: RateLimitSettings{
					Rate:             100,
					Burst:            200, // Unused for dynamic.
					Strategy:         StrategyRateLimitBytes,
					ThrottleBehavior: ThrottleBehaviorError,
					ThrottleInterval: 1 * time.Second,
					RetryDelay:       1 * time.Second,
				},
				Drl: DynamicRateLimiting{
					Enabled:                 true,
					DefaultWindowMultiplier: 1.5,
					WindowDuration:          time.Minute,
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
		{
			name:        "invalid_default_class",
			expectedErr: `default_class "nonexistent" does not exist in classes`,
		},
		{
			name:        "invalid_class_rate_zero",
			expectedErr: `class "trial": rate must be greater than zero`,
		},
		{
			name:        "invalid_class_rate_negative",
			expectedErr: `class "trial": rate must be greater than zero`,
		},
		{
			name:        "classes_set_but_no_class_resolver",
			expectedErr: `classes defined but class_resolver not specified`,
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

func TestResolveEffectiveRateLimit(t *testing.T) {
	cfg := &Config{
		RateLimitSettings: RateLimitSettings{
			Rate:             100,
			Burst:            200,
			Strategy:         StrategyRateLimitRequests,
			ThrottleBehavior: ThrottleBehaviorError,
			ThrottleInterval: 1 * time.Second,
		},
		Overrides: []RateLimitOverrides{
			{
				Matches: map[string][]string{
					"project-id":   {"e678ebd7-3a15-43dd-a95c-1cf0639a6292"},
					"project-type": {"test"},
				},
				Rate:             ptr(300),
				Burst:            ptr(400),
				ThrottleInterval: ptr(10 * time.Second),
			},
		},
		Classes: map[string]Class{
			"trial":   {Rate: 50, Burst: 50},
			"premium": {Rate: 1000, Burst: 2000, DisableDynamic: true},
		},
		DefaultClass: "trial",
	}

	t.Run("override successful", func(t *testing.T) {
		metadata := client.NewMetadata(map[string][]string{
			"project-id":   {"e678ebd7-3a15-43dd-a95c-1cf0639a6292"},
			"project-type": {"test"},
		})
		res, kind, class := resolveRateLimit(cfg, "trial", metadata)
		require.Equal(t, SourceKindOverride, kind)
		require.Empty(t, class)
		require.Equal(t, 300, res.Rate)
		require.Equal(t, 400, res.Burst)
		require.Equal(t, 10*time.Second, res.ThrottleInterval)
	})

	t.Run("override failed", func(t *testing.T) {
		metadata := client.NewMetadata(map[string][]string{
			"project-id": {"e678ebd7-3a15-43dd-a95c-1cf0639a6292"},
			// Project-type not matching the override
			"project-type": {"404"},
		})
		res, kind, class := resolveRateLimit(cfg, "trial", metadata)
		require.Equal(t, SourceKindClass, kind)
		require.Equal(t, "trial", class)
		require.Equal(t, 50, res.Rate)
		require.Equal(t, 50, res.Burst)
		require.Equal(t, time.Second, res.ThrottleInterval)
	})

	t.Run("override failed 2", func(t *testing.T) {
		metadata := client.NewMetadata(map[string][]string{
			"project-id": {"e678ebd7-3a15-43dd-a95c-1cf0639a6292"},
			// Project-type not present
		})
		res, kind, class := resolveRateLimit(cfg, "trial", metadata)
		require.Equal(t, SourceKindClass, kind)
		require.Equal(t, "trial", class)
		require.Equal(t, 50, res.Rate)
		require.Equal(t, 50, res.Burst)
		require.Equal(t, time.Second, res.ThrottleInterval)
	})

	t.Run("resolved class", func(t *testing.T) {
		metadata := client.NewMetadata(map[string][]string{
			"project-id": {"some-other-key"},
		})
		res, kind, class := resolveRateLimit(cfg, "premium", metadata)
		require.Equal(t, SourceKindClass, kind)
		require.Equal(t, "premium", class)
		require.Equal(t, 1000, res.Rate)
		require.Equal(t, 2000, res.Burst)
		require.True(t, res.disableDynamic, "premium is static-only")
	})

	t.Run("default class fallback", func(t *testing.T) {
		metadata := client.NewMetadata(map[string][]string{
			"project-id": {"another-key"},
		})
		res, kind, class := resolveRateLimit(cfg, "nonexistent", metadata)
		require.Equal(t, SourceKindClass, kind)
		require.Equal(t, "trial", class)
		require.Equal(t, 50, res.Rate)
		require.Equal(t, 50, res.Burst)
	})

	t.Run("top-level fallback", func(t *testing.T) {
		metadata := client.NewMetadata(map[string][]string{
			"project-id": nil,
		})
		cfgNoClasses := &Config{RateLimitSettings: cfg.RateLimitSettings}
		res, kind, class := resolveRateLimit(cfgNoClasses, "", metadata)
		require.Equal(t, SourceKindFallback, kind)
		require.Empty(t, class)
		require.Equal(t, cfg.RateLimitSettings, res)
	})
}

func ptr[T any](v T) *T {
	return &v
}
