// ELASTICSEARCH CONFIDENTIAL
// __________________
//
//  Copyright Elasticsearch B.V. All rights reserved.
//
// NOTICE:  All information contained herein is, and remains
// the property of Elasticsearch B.V. and its suppliers, if any.
// The intellectual and technical concepts contained herein
// are proprietary to Elasticsearch B.V. and its suppliers and
// may be covered by U.S. and Foreign Patents, patents in
// process, and are protected by trade secret or copyright
// law.  Dissemination of this information or reproduction of
// this material is strictly forbidden unless prior written
// permission is obtained from Elasticsearch B.V.

package ratelimitprocessor

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/confmap/confmaptest"

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
				Rate:     100,
				Burst:    200,
				Strategy: StrategyRateLimitRequests,
			},
		},
		{
			name: "strategy",
			expected: &Config{
				Rate:     100,
				Burst:    200,
				Strategy: StrategyRateLimitBytes,
			},
		},
		{
			name: "gubernator",
			expected: &Config{
				Gubernator: &GubernatorConfig{ClientConfig: *grpcClientConfig},
				Rate:       100,
				Burst:      200,
				Strategy:   StrategyRateLimitRequests,
			},
		},
		{
			name: "gubernator_behavior",
			expected: &Config{
				Gubernator: &GubernatorConfig{
					ClientConfig: *grpcClientConfig,
					Behavior:     []GubernatorBehavior{"global", "duration_is_gregorian"},
				},
				Rate:     100,
				Burst:    200,
				Strategy: StrategyRateLimitRequests,
			},
		},
		{
			name: "metadata_keys",
			expected: &Config{
				Rate:         100,
				Burst:        200,
				Strategy:     StrategyRateLimitRequests,
				MetadataKeys: []string{"project_id"},
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
			name:        "invalid_gubernator_behavior",
			expectedErr: `invalid Gubernator behavior "foo", expected one of ["batching" "drain_over_limit" "duration_is_gregorian" "global" "multi_region" "no_batching" "reset_remaining"]`,
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

			err = component.ValidateConfig(cfg)
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, cfg)
		})
	}
}
