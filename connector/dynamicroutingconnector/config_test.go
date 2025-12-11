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

package dynamicroutingconnector

import (
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/elastic/opentelemetry-collector-components/connector/dynamicroutingconnector/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"
	"go.opentelemetry.io/collector/pipeline"
)

func TestConfig(t *testing.T) {
	for _, tc := range []struct {
		name     string
		expected component.Config
		errMsg   string
	}{
		{
			name:   "empty",
			errMsg: "primary_metadata_key must be defined",
		},
		{
			name:   "no-dynamic-pipelines",
			errMsg: "atleast one pipeline needs to be defined",
		},
		{
			name:   "invalid-dynamic-pipelines/no-inf",
			errMsg: "last dynamic pipeline must have max count set to positive infinity",
		},
		{
			name:   "invalid-dynamic-pipelines/out-of-order",
			errMsg: "pipelines must be defined in ascending order of max_count",
		},
		{
			name: "invalid-dynamic-pipelines/valid",
			expected: &Config{
				PrimaryMetadataKeys: []string{"x-tenant"},
				DefaultPipelines: []pipeline.ID{
					pipeline.NewIDWithName(pipeline.SignalLogs, "default"),
				},
				EvaluationInterval: time.Minute,
				DynamicPipelines: []DynamicPipeline{
					{
						Pipelines: []pipeline.ID{
							pipeline.NewIDWithName(pipeline.SignalLogs, "test1"),
						},
						MaxCount: 10,
					},
					{
						Pipelines: []pipeline.ID{
							pipeline.NewIDWithName(pipeline.SignalLogs, "test2"),
						},
						MaxCount: 100,
					},
					{
						Pipelines: []pipeline.ID{
							pipeline.NewIDWithName(pipeline.SignalLogs, "final"),
						},
						MaxCount: math.Inf(1),
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			configPath := filepath.Join("testdata", "configs", "config.yaml")
			cm, err := confmaptest.LoadConf(configPath)
			require.NoError(t, err)

			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(metadata.Type.String() + "/" + tc.name)
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			err = xconfmap.Validate(cfg)
			if tc.errMsg == "" {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, cfg)
			} else {
				assert.ErrorContains(t, err, tc.errMsg)
			}
		})
	}
}
