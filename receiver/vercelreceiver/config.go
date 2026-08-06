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

package vercelreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/vercelreceiver"

import (
	"errors"
	"strings"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
)

const (
	defaultEndpoint = ":4322"
	defaultRoute    = "/"

	// defaultFlushItems and defaultFlushBytes cap how many records or bytes a
	// single decoded batch holds before it is handed to the pipeline, bounding
	// per-request memory.
	defaultFlushItems int64 = 1000
	defaultFlushBytes int64 = 1 << 20 // 1MiB
)

var (
	errMissingRoute    = errors.New("route must be set")
	errInvalidRoute    = errors.New("route must start with /")
	errMissingEncoding = errors.New("encoding extension must be set")
	errNegativeFlush   = errors.New("encoding flush items and bytes must not be negative")
)

type Config struct {
	*confighttp.ServerConfig `mapstructure:",squash"`
	Route                    string         `mapstructure:"route"`
	Encoding                 EncodingConfig `mapstructure:"encoding"`
}

// EncodingConfig selects the encoding extension and the flush policy the
// receiver uses to drive it. The extension owns the decoding mechanism; the
// receiver owns the batch-size policy.
type EncodingConfig struct {
	Extension component.ID `mapstructure:"extension"`
	Flush     FlushConfig  `mapstructure:"flush"`
}

// FlushConfig bounds a decoded batch by record count and byte size. Whichever
// limit is hit first ends the batch. A zero value disables that trigger.
type FlushConfig struct {
	Items int64 `mapstructure:"items"`
	Bytes int64 `mapstructure:"bytes"`
}

func createDefaultConfig() component.Config {
	serverConfig := confighttp.NewDefaultServerConfig()
	serverConfig.NetAddr.Endpoint = defaultEndpoint
	return &Config{
		ServerConfig: &serverConfig,
		Route:        defaultRoute,
		Encoding: EncodingConfig{
			Flush: FlushConfig{
				Items: defaultFlushItems,
				Bytes: defaultFlushBytes,
			},
		},
	}
}

func (cfg *Config) Validate() error {
	var errs []error
	if cfg.Route == "" {
		errs = append(errs, errMissingRoute)
	} else if !strings.HasPrefix(cfg.Route, "/") {
		errs = append(errs, errInvalidRoute)
	}
	if cfg.Encoding.Extension == (component.ID{}) {
		errs = append(errs, errMissingEncoding)
	}
	if cfg.Encoding.Flush.Items < 0 || cfg.Encoding.Flush.Bytes < 0 {
		errs = append(errs, errNegativeFlush)
	}
	return errors.Join(errs...)
}
