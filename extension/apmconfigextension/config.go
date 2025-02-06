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

package apmconfigextension // import "github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension"

import (
	"time"

	"github.com/elastic/opentelemetry-lib/config/configelasticsearch"
	"go.opentelemetry.io/collector/component"
)

type Config struct {
	RemoteConfig RemoteConfig `mapstructure:"remote_config"`
	OpAMP        OpAMPConfig  `mapstructure:"opamp"`
}

type RemoteConfig struct {
	Elasticsearch configelasticsearch.ClientConfig `mapstructure:"elasticsearch"`
	CacheDuration time.Duration                    `mapstructure:"cache_duration"`
}

type OpAMPConfig struct {
	Server OpAMPServerConfig `mapstructure:"server"`
}

type OpAMPServerConfig struct {
	Endpoint string `mapstructure:"endpoint"`
}

var _ component.Config = (*Config)(nil)
