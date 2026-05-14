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
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"

	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap"

	"github.com/elastic/entcollect"
	ecad "github.com/elastic/entcollect/provider/ad"
)

func init() {
	Register("activedirectory", adProvider)
}

func adProvider(cfg *confmap.Conf) (entcollect.Provider, error) {
	ec := ecad.DefaultConfig()
	if cfg != nil {
		// adLocalConf mirrors ecad.Config with mapstructure tags for confmap.Conf
		// Unmarshal. If ecad.Config gains a new field, add it here too.
		type adLocalConf struct {
			URL                configopaque.String     `mapstructure:"ad_url"`
			BaseDN             configopaque.String     `mapstructure:"ad_base_dn"`
			User               configopaque.String     `mapstructure:"ad_user"`
			Password           configopaque.String     `mapstructure:"ad_password"`
			Dataset            string                  `mapstructure:"dataset"`
			UserQuery          string                  `mapstructure:"user_query"`
			DeviceQuery        string                  `mapstructure:"device_query"`
			IncludeEmptyGroups bool                    `mapstructure:"include_empty_groups"`
			UserAttrs          []string                `mapstructure:"user_attributes"`
			GrpAttrs           []string                `mapstructure:"group_attributes"`
			PagingSize         uint32                  `mapstructure:"ad_paging_size"`
			IDSetShards        int                     `mapstructure:"idset_shards"`
			TLS                *configtls.ClientConfig `mapstructure:"tls"`
		}

		lc := adLocalConf{
			PagingSize:         ec.PagingSize,
			IDSetShards:        ec.IDSetShards,
			IncludeEmptyGroups: ec.IncludeEmptyGroups,
		}
		err := cfg.Unmarshal(&lc)
		if err != nil {
			return nil, fmt.Errorf("activedirectory: unmarshal config: %w", err)
		}
		ec.URL = string(lc.URL)
		ec.BaseDN = string(lc.BaseDN)
		ec.User = string(lc.User)
		ec.Password = string(lc.Password)
		ec.Dataset = lc.Dataset
		ec.UserQuery = lc.UserQuery
		ec.DeviceQuery = lc.DeviceQuery
		ec.IncludeEmptyGroups = lc.IncludeEmptyGroups
		ec.UserAttrs = lc.UserAttrs
		ec.GrpAttrs = lc.GrpAttrs
		ec.PagingSize = lc.PagingSize
		ec.IDSetShards = lc.IDSetShards

		if lc.TLS != nil {
			tc, err := buildADTLS(lc.TLS)
			if err != nil {
				return nil, fmt.Errorf("activedirectory: build TLS: %w", err)
			}
			ec.TLS = tc
		}
	} else {
		ec.URL = os.Getenv("AD_URL")
		ec.BaseDN = os.Getenv("AD_BASE")
		ec.User = os.Getenv("AD_USER")
		ec.Password = os.Getenv("AD_PASS")
		if v := os.Getenv("AD_DATASET"); v != "" {
			ec.Dataset = v
		}
		if v := os.Getenv("AD_PAGING_SIZE"); v != "" {
			n, err := strconv.ParseUint(v, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("activedirectory: parse AD_PAGING_SIZE: %w", err)
			}
			ec.PagingSize = uint32(n)
		}
		if v := os.Getenv("AD_IDSET_SHARDS"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("activedirectory: parse AD_IDSET_SHARDS: %w", err)
			}
			ec.IDSetShards = n
		}
		if v := os.Getenv("AD_INCLUDE_EMPTY_GROUPS"); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("activedirectory: parse AD_INCLUDE_EMPTY_GROUPS: %w", err)
			}
			ec.IncludeEmptyGroups = b
		}
		if v := os.Getenv("AD_TLS_SKIP_VERIFY"); v != "" {
			skip, err := strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("activedirectory: parse AD_TLS_SKIP_VERIFY: %w", err)
			}
			if skip {
				ec.TLS = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // user-configured
			}
		}
	}
	err := ec.Validate()
	if err != nil {
		return nil, err
	}
	return ecad.New(ec)
}

func buildADTLS(tc *configtls.ClientConfig) (*tls.Config, error) {
	return tc.LoadTLSConfig(context.Background())
}
