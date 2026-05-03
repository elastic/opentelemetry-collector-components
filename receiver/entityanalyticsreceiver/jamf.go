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

package entityanalyticsreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/entityanalyticsreceiver"

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/confmap"

	"github.com/elastic/entcollect"
	ecjamf "github.com/elastic/entcollect/provider/jamf"
)

func init() {
	Register("jamf", jamfProvider)
}

func jamfProvider(cfg *confmap.Conf) (entcollect.Provider, error) {
	ec := ecjamf.DefaultConfig()
	if cfg != nil {
		// jamfLocalConf mirrors ecjamf.Config with mapstructure tags for confmap.Conf
		// Unmarshal. If ecjamf.Config gains a new field, add it here too.
		type jamfLocalConf struct {
			TenantID    configopaque.String `mapstructure:"jamf_tenant"`
			Username    configopaque.String `mapstructure:"jamf_username"`
			Password    configopaque.String `mapstructure:"jamf_password"`
			PageSize    int                 `mapstructure:"page_size"`
			IDSetShards int                 `mapstructure:"idset_shards"`
			TokenGrace  time.Duration       `mapstructure:"token_grace_period"`
		}

		lc := jamfLocalConf{
			PageSize:    ec.PageSize,
			IDSetShards: ec.IDSetShards,
			TokenGrace:  ec.TokenGrace,
		}
		err := cfg.Unmarshal(&lc)
		if err != nil {
			return nil, fmt.Errorf("jamf: unmarshal config: %w", err)
		}
		ec.TenantID = string(lc.TenantID)
		ec.Username = string(lc.Username)
		ec.Password = string(lc.Password)
		ec.PageSize = lc.PageSize
		ec.IDSetShards = lc.IDSetShards
		ec.TokenGrace = lc.TokenGrace
	} else {
		// cfg is nil when the receiver has no provider-specific sub-config
		// (the current default). Credentials and optional settings are read
		// from environment variables.
		ec.TenantID = os.Getenv("JAMF_TENANT")
		ec.Username = os.Getenv("JAMF_USERNAME")
		ec.Password = os.Getenv("JAMF_PASSWORD")
		if v := os.Getenv("JAMF_PAGE_SIZE"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("jamf: parse JAMF_PAGE_SIZE: %w", err)
			}
			ec.PageSize = n
		}
		if v := os.Getenv("JAMF_IDSET_SHARDS"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("jamf: parse JAMF_IDSET_SHARDS: %w", err)
			}
			ec.IDSetShards = n
		}
		if v := os.Getenv("JAMF_TOKEN_GRACE_PERIOD"); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("jamf: parse JAMF_TOKEN_GRACE_PERIOD: %w", err)
			}
			ec.TokenGrace = d
		}
	}
	err := ec.Validate()
	if err != nil {
		return nil, err
	}
	return ecjamf.New(ec), nil
}
