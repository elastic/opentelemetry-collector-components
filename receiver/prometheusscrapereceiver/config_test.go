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

package prometheusscrapereceiver

import (
	"path/filepath"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"

	"github.com/elastic/opentelemetry-collector-components/receiver/prometheusscrapereceiver/internal/metadata"
)

func loadConfig(t *testing.T, name string) (*Config, error) {
	t.Helper()
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)
	sub, err := cm.Sub(component.NewIDWithName(metadata.Type, name).String())
	require.NoError(t, err)
	cfg := createDefaultConfig().(*Config)
	if err := sub.Unmarshal(cfg); err != nil {
		return nil, err
	}
	return cfg, confmap.Validate(cfg)
}

func TestConfigValid(t *testing.T) {
	cfg, err := loadConfig(t, "")
	require.NoError(t, err)

	require.Len(t, cfg.PrometheusConfig.ScrapeConfigs, 1)
	sc := cfg.PrometheusConfig.ScrapeConfigs[0]
	assert.Equal(t, "static", sc.JobName)
	assert.Equal(t, "test", cfg.PrometheusConfig.GlobalConfig.ExternalLabels.Get("cluster"))

	// Static target labels must survive the YAML round trip.
	require.Len(t, sc.ServiceDiscoveryConfigs, 1)
	assert.Equal(t, "static", sc.ServiceDiscoveryConfigs[0].Name())
	require.NoError(t, cfg.PrometheusConfig.Reload())
}

func TestConfigStaticLabelsPreserved(t *testing.T) {
	cfg, err := loadConfig(t, "")
	require.NoError(t, err)

	var groups discovery.StaticConfig
	for _, d := range cfg.PrometheusConfig.ScrapeConfigs[0].ServiceDiscoveryConfigs {
		if sc, ok := d.(discovery.StaticConfig); ok {
			groups = append(groups, sc...)
		}
	}
	require.Len(t, groups, 1)
	assert.Equal(t, model.LabelValue("obs"), groups[0].Labels["team"])
	assert.Equal(t, model.LabelValue("localhost:9100"), groups[0].Targets[0][model.AddressLabel])
}

func TestConfigNativeHistogramsAllowed(t *testing.T) {
	cfg, err := loadConfig(t, "native_histograms")
	require.NoError(t, err)
	assert.True(t, cfg.PrometheusConfig.ScrapeConfigs[0].ScrapeNativeHistogramsEnabled())
}

func TestConfigInvalid(t *testing.T) {
	tests := []struct {
		name   string
		errMsg string
	}{
		{"nhcb", "convert_classic_histograms_to_nhcb is not supported"},
		{"remote_write", "remote_write"},
		{"rule_files", "rule_files"},
		{"empty", "no Prometheus scrape_configs"},
		{"no_config", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadConfig(t, tt.name)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}
