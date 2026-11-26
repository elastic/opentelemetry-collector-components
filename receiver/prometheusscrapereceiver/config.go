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

package prometheusscrapereceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/prometheusscrapereceiver"

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sort"
	"strings"

	"github.com/goccy/go-yaml"
	commonconfig "github.com/prometheus/common/config"
	promconfig "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/kubernetes"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"go.opentelemetry.io/collector/confmap"
)

// Config defines configuration for the prometheusscrape receiver.
type Config struct {
	PrometheusConfig *PromConfig `mapstructure:"config"`
}

// Validate checks the receiver configuration is valid.
func (cfg *Config) Validate() error {
	if !cfg.PrometheusConfig.ContainsScrapeConfigs() {
		return errors.New("no Prometheus scrape_configs or scrape_config_files set")
	}
	return nil
}

// PromConfig is a redeclaration of promconfig.Config because we need custom unmarshaling
// as prometheus "config" uses `yaml` tags.
type PromConfig promconfig.Config

var _ confmap.Unmarshaler = (*PromConfig)(nil)

// ContainsScrapeConfigs returns true if the Prometheus config contains any scrape configs.
func (cfg *PromConfig) ContainsScrapeConfigs() bool {
	return cfg != nil && (len(cfg.ScrapeConfigs) > 0 || len(cfg.ScrapeConfigFiles) > 0)
}

// Reload re-parses the configuration through the Prometheus loader so that
// defaults are applied and scrape_config_files are resolved.
func (cfg *PromConfig) Reload() error {
	return reloadPromConfig(cfg, cfg)
}

// Unmarshal implements confmap.Unmarshaler.
func (cfg *PromConfig) Unmarshal(componentParser *confmap.Conf) error {
	cfgMap := componentParser.ToStringMap()
	if len(cfgMap) == 0 {
		return nil
	}
	return reloadPromConfig(cfg, cfgMap)
}

// Validate rejects Prometheus features this receiver does not support.
func (cfg *PromConfig) Validate() error {
	unsupportedFeatures := make([]string, 0, 4)
	if len(cfg.RemoteWriteConfigs) != 0 {
		unsupportedFeatures = append(unsupportedFeatures, "remote_write")
	}
	if len(cfg.RemoteReadConfigs) != 0 {
		unsupportedFeatures = append(unsupportedFeatures, "remote_read")
	}
	if len(cfg.RuleFiles) != 0 {
		unsupportedFeatures = append(unsupportedFeatures, "rule_files")
	}
	if len(cfg.AlertingConfig.AlertRelabelConfigs) != 0 {
		unsupportedFeatures = append(unsupportedFeatures, "alert_config.relabel_configs")
	}
	if len(cfg.AlertingConfig.AlertmanagerConfigs) != 0 {
		unsupportedFeatures = append(unsupportedFeatures, "alert_config.alertmanagers")
	}
	if len(unsupportedFeatures) != 0 {
		sort.Strings(unsupportedFeatures)
		return fmt.Errorf("unsupported features:\n\t%s", strings.Join(unsupportedFeatures, "\n\t"))
	}

	if !cfg.ContainsScrapeConfigs() {
		return nil
	}

	scrapeConfigs, err := (*promconfig.Config)(cfg).GetScrapeConfigs()
	if err != nil {
		return fmt.Errorf("failed to get scrape configs: %w", err)
	}

	for _, sc := range scrapeConfigs {
		// Since Prometheus 3.0 scrapes without a proper Content-Type fail unless a
		// fallback protocol is configured. Keep the pre-3.0 behavior by default.
		if sc.ScrapeFallbackProtocol == "" {
			sc.ScrapeFallbackProtocol = promconfig.PrometheusText0_0_4
		}

		// NHCB histograms use custom buckets, which are not exponential and can't
		// be represented as an OTel ExponentialHistogram.
		if sc.ConvertClassicHistogramsToNHCBEnabled() {
			return fmt.Errorf("scrape job %q: convert_classic_histograms_to_nhcb is not supported", sc.JobName)
		}

		if err := validateHTTPClientConfig(&sc.HTTPClientConfig); err != nil {
			return err
		}
		for _, c := range sc.ServiceDiscoveryConfigs {
			if c, ok := c.(*kubernetes.SDConfig); ok {
				if err := validateHTTPClientConfig(&c.HTTPClientConfig); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// copyStaticConfig copies static service discovery configs from src to dst to assure that labels in StaticConfig are retained.
func copyStaticConfig(dst *PromConfig, src any) {
	srcCfg, ok := src.(*PromConfig)
	if !ok {
		return
	}

	srcCfgMap := make(map[string][]*targetgroup.Group, len(srcCfg.ScrapeConfigs))
	for _, srcSC := range srcCfg.ScrapeConfigs {
		for _, srcSDC := range srcSC.ServiceDiscoveryConfigs {
			if sc, ok := srcSDC.(discovery.StaticConfig); ok {
				srcCfgMap[srcSC.JobName] = append(srcCfgMap[srcSC.JobName], sc...)
			}
		}
	}

	for _, dstSC := range dst.ScrapeConfigs {
		if len(srcCfgMap[dstSC.JobName]) == 0 {
			continue
		}
		dstSC.ServiceDiscoveryConfigs = slices.DeleteFunc(dstSC.ServiceDiscoveryConfigs, func(cfg discovery.Config) bool {
			_, ok := cfg.(discovery.StaticConfig)
			return ok
		})
		dstSC.ServiceDiscoveryConfigs = append(dstSC.ServiceDiscoveryConfigs, discovery.StaticConfig(srcCfgMap[dstSC.JobName]))
	}
}

func reloadPromConfig(dst *PromConfig, src any) error {
	yamlOut, err := yaml.MarshalWithOptions(
		src,
		yaml.CustomMarshaler(func(s commonconfig.Secret) ([]byte, error) {
			return yaml.Marshal(string(s))
		}),
	)
	if err != nil {
		return fmt.Errorf("prometheusscrape receiver: failed to marshal config to yaml: %w", err)
	}

	newCfg, err := promconfig.Load(string(yamlOut), slog.Default())
	if err != nil {
		return fmt.Errorf("prometheusscrape receiver: failed to unmarshal yaml to prometheus config object: %w", err)
	}
	copyStaticConfig((*PromConfig)(newCfg), src)
	*dst = PromConfig(*newCfg)
	return nil
}

func validateHTTPClientConfig(cfg *commonconfig.HTTPClientConfig) error {
	if cfg.Authorization != nil {
		if err := checkFile(cfg.Authorization.CredentialsFile); err != nil {
			return fmt.Errorf("error checking authorization credentials file %q: %w", cfg.Authorization.CredentialsFile, err)
		}
	}
	return checkTLSConfig(cfg.TLSConfig)
}

func checkFile(fn string) error {
	if fn == "" {
		return nil
	}
	_, err := os.Stat(fn)
	return err
}

func checkTLSConfig(tlsConfig commonconfig.TLSConfig) error {
	if err := checkFile(tlsConfig.CAFile); err != nil {
		return fmt.Errorf("error checking CA file %q: %w", tlsConfig.CAFile, err)
	}
	if err := checkFile(tlsConfig.CertFile); err != nil {
		return fmt.Errorf("error checking client cert file %q: %w", tlsConfig.CertFile, err)
	}
	if err := checkFile(tlsConfig.KeyFile); err != nil {
		return fmt.Errorf("error checking client key file %q: %w", tlsConfig.KeyFile, err)
	}
	return nil
}
