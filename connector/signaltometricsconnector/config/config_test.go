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
	"fmt"
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
		path      string // relative to testdata/configs directory
		expected  *Config
		errorMsgs []string // all error message are checked
	}{
		{
			path:      "empty",
			errorMsgs: []string{"no configuration provided"},
		},
		{
			path: "without_name",
			errorMsgs: []string{
				fullErrorForSignal(t, "spans", "missing required metric name"),
				fullErrorForSignal(t, "datapoints", "missing required metric name"),
				fullErrorForSignal(t, "logs", "missing required metric name"),
			},
		},
		{
			path: "no_key_attributes",
			errorMsgs: []string{
				fullErrorForSignal(t, "spans", "attributes validation failed"),
				fullErrorForSignal(t, "datapoints", "attributes validation failed"),
				fullErrorForSignal(t, "logs", "attributes validation failed"),
			},
		},
		{
			path: "duplicate_attributes",
			errorMsgs: []string{
				fullErrorForSignal(t, "spans", "attributes validation failed"),
				fullErrorForSignal(t, "datapoints", "attributes validation failed"),
				fullErrorForSignal(t, "logs", "attributes validation failed"),
			},
		},
		{
			path: "invalid_histogram",
			errorMsgs: []string{
				fullErrorForSignal(t, "spans", "histogram validation failed"),
				fullErrorForSignal(t, "datapoints", "histogram validation failed"),
				fullErrorForSignal(t, "logs", "histogram validation failed"),
			},
		},
		{
			path: "invalid_exponential_histogram",
			errorMsgs: []string{
				fullErrorForSignal(t, "spans", "histogram validation failed"),
				fullErrorForSignal(t, "datapoints", "histogram validation failed"),
				fullErrorForSignal(t, "logs", "histogram validation failed"),
			},
		},
		{
			path: "invalid_sum",
			errorMsgs: []string{
				fullErrorForSignal(t, "spans", "sum validation failed"),
				fullErrorForSignal(t, "datapoints", "sum validation failed"),
				fullErrorForSignal(t, "logs", "sum validation failed"),
			},
		},
		{
			path: "multiple_metric",
			errorMsgs: []string{
				fullErrorForSignal(t, "spans", "exactly one of the metrics must be defined"),
				fullErrorForSignal(t, "datapoints", "exactly one of the metrics must be defined"),
				fullErrorForSignal(t, "logs", "exactly one of the metrics must be defined"),
			},
		},
		{
			path: "invalid_ottl_statements",
			errorMsgs: []string{
				fullErrorForSignal(t, "spans", "failed to parse OTTL statements"),
				fullErrorForSignal(t, "datapoints", "failed to parse OTTL statements"),
				fullErrorForSignal(t, "logs", "failed to parse OTTL statements"),
			},
		},
	} {
		t.Run(tc.path, func(t *testing.T) {
			dir := filepath.Join("..", "testdata", "configs")
			cfg := &Config{}
			cm, err := confmaptest.LoadConf(filepath.Join(dir, tc.path+".yaml"))
			require.NoError(t, err)

			sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "").String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(&cfg))

			err = component.ValidateConfig(cfg)
			if len(tc.errorMsgs) > 0 {
				for _, errMsg := range tc.errorMsgs {
					assert.ErrorContains(t, err, errMsg)
				}
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tc.expected, cfg)
		})
	}
}

const validationMsgFormat = "failed to validate %s configuration: %s"

func fullErrorForSignal(t *testing.T, signal, errMsg string) string {
	t.Helper()

	switch signal {
	case "spans", "datapoints", "logs":
		return fmt.Sprintf(validationMsgFormat, signal, errMsg)
	default:
		panic("unhandled signal type")
	}
}