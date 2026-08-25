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

package httpexporter // import "github.com/elastic/opentelemetry-collector-components/exporter/httpexporter"

import (
	"errors"
	"fmt"
	"net/url"

	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config configures the HTTP body exporter.
type Config struct {
	// ClientConfig holds standard HTTP client settings (endpoint, headers, TLS, timeout).
	confighttp.ClientConfig `mapstructure:",squash"`

	// RetryConfig defines retry configuration for failed exports.
	RetryConfig configretry.BackOffConfig `mapstructure:"retry_on_failure"`

	// QueueConfig defines optional sending queue settings.
	QueueConfig configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
}

func (cfg *Config) Validate() error {
	if cfg.Endpoint == "" {
		return errors.New("endpoint is required")
	}
	u, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("endpoint must be a valid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("endpoint scheme must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("endpoint must include a host")
	}
	return nil
}
