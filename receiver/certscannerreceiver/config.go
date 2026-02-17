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

package certscannerreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/certscannerreceiver"

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
)

var _ component.Config = (*Config)(nil)

// PortsConfig defines port scanning settings.
type PortsConfig struct {
	// Include specifies explicit ports to scan (used when ScanAll is false)
	Include []int `mapstructure:"include"`

	// Exclude specifies ports to skip even when discovered
	Exclude []int `mapstructure:"exclude"`

	// ScanAll when true scans all listening ports not in Exclude list.
	// When false, only scans ports in Include list.
	ScanAll bool `mapstructure:"scan_all"`
}

// Config defines configuration for the certscanner receiver.
type Config struct {
	// Interval between scans (e.g., "1h", "30m"). Minimum is 1 minute.
	Interval time.Duration `mapstructure:"interval"`

	// Timeout for each TLS connection attempt. Must be between 100ms and 1 minute.
	Timeout time.Duration `mapstructure:"timeout"`

	// Ports configuration for filtering which ports to scan
	Ports PortsConfig `mapstructure:"ports"`

	// EmitMetrics enables optional metrics emission in addition to logs
	EmitMetrics bool `mapstructure:"emit_metrics"`
}

// Validate checks if the receiver configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.Interval < time.Minute {
		return errors.New("interval must be at least 1 minute")
	}
	if cfg.Timeout < 100*time.Millisecond {
		return errors.New("timeout must be at least 100ms")
	}
	if cfg.Timeout > time.Minute {
		return errors.New("timeout must not exceed 1 minute")
	}
	if !cfg.Ports.ScanAll && len(cfg.Ports.Include) == 0 {
		return errors.New("ports.include must be specified when scan_all is false")
	}

	// Validate port ranges in include list
	for _, p := range cfg.Ports.Include {
		if p < 1 || p > 65535 {
			return fmt.Errorf("invalid port %d in include list: must be between 1 and 65535", p)
		}
	}

	// Validate port ranges in exclude list
	for _, p := range cfg.Ports.Exclude {
		if p < 1 || p > 65535 {
			return fmt.Errorf("invalid port %d in exclude list: must be between 1 and 65535", p)
		}
	}

	return nil
}
