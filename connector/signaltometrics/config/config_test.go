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

package config

import (
	"path/filepath"
	"testing"

	"github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestConfig(t *testing.T) {
	for _, tc := range []struct {
		path     string // relative to testdata/configs directory
		expected *Config
		errorMsg string
	}{} {
		t.Run(tc.path, func(t *testing.T) {
			dir := filepath.Join("../testdata/configs", tc.path)
			cfg := &Config{}
			cm, err := confmaptest.LoadConf(filepath.Join(dir, "config.yaml"))
			require.NoError(t, err)

			sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "").String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(&cfg))

			err = component.ValidateConfig(cfg)
			if tc.errorMsg != "" {
				assert.ErrorContains(t, err, tc.errorMsg)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tc.expected, cfg)
		})
	}
}
