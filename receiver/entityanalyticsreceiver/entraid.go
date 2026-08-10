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
	"strings"

	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/confmap"

	"github.com/elastic/entcollect"
	ecentraid "github.com/elastic/entcollect/provider/entraid"
)

func init() {
	Register("azure-ad", entraidProvider)
}

func entraidProvider(cfg *confmap.Conf) (entcollect.Provider, error) {
	ec := ecentraid.DefaultConfig()
	if cfg != nil {
		// entraidLocalConf mirrors ecentraid.Config with mapstructure
		// tags for confmap.Conf Unmarshal. SyncInterval and
		// UpdateInterval are owned by the receiver's top-level Config
		// and are intentionally absent here. If ecentraid.Config gains
		// a new field, add it here too; TestEntraidProviderConfigRoundTrip
		// will catch any drift.
		type entraidLocalConf struct {
			TenantID     configopaque.String `mapstructure:"tenant_id"`
			ClientID     configopaque.String `mapstructure:"client_id"`
			ClientSecret configopaque.String `mapstructure:"client_secret"`

			LoginEndpoint string   `mapstructure:"login_endpoint"`
			LoginScopes   []string `mapstructure:"login_scopes"`
			APIEndpoint   string   `mapstructure:"api_endpoint"`

			Dataset    string   `mapstructure:"dataset"`
			EnrichWith []string `mapstructure:"enrich_with"`

			SelectUsers   []string `mapstructure:"select_users"`
			SelectGroups  []string `mapstructure:"select_groups"`
			SelectDevices []string `mapstructure:"select_devices"`

			ScratchDir string `mapstructure:"scratch_dir"`
		}

		lc := entraidLocalConf{
			LoginEndpoint: ec.LoginEndpoint,
			LoginScopes:   ec.LoginScopes,
			APIEndpoint:   ec.APIEndpoint,
			Dataset:       ec.Dataset,
			EnrichWith:    ec.EnrichWith,
			SelectUsers:   ec.SelectUsers,
			SelectGroups:  ec.SelectGroups,
			SelectDevices: ec.SelectDevices,
		}
		err := cfg.Unmarshal(&lc)
		if err != nil {
			return nil, fmt.Errorf("azure-ad: unmarshal config: %w", err)
		}
		ec.TenantID = string(lc.TenantID)
		ec.ClientID = string(lc.ClientID)
		ec.ClientSecret = string(lc.ClientSecret)
		ec.LoginEndpoint = lc.LoginEndpoint
		ec.LoginScopes = lc.LoginScopes
		ec.APIEndpoint = lc.APIEndpoint
		ec.Dataset = lc.Dataset
		ec.EnrichWith = lc.EnrichWith
		ec.SelectUsers = lc.SelectUsers
		ec.SelectGroups = lc.SelectGroups
		ec.SelectDevices = lc.SelectDevices
		ec.ScratchDir = lc.ScratchDir
	} else {
		ec.TenantID = os.Getenv("ENTRAID_TENANT_ID")
		ec.ClientID = os.Getenv("ENTRAID_CLIENT_ID")
		ec.ClientSecret = os.Getenv("ENTRAID_CLIENT_SECRET")
		if v := os.Getenv("ENTRAID_LOGIN_ENDPOINT"); v != "" {
			ec.LoginEndpoint = v
		}
		if v := os.Getenv("ENTRAID_LOGIN_SCOPES"); v != "" {
			ec.LoginScopes = strings.Split(v, ",")
		}
		if v := os.Getenv("ENTRAID_API_ENDPOINT"); v != "" {
			ec.APIEndpoint = v
		}
		if v := os.Getenv("ENTRAID_DATASET"); v != "" {
			ec.Dataset = v
		}
		if v := os.Getenv("ENTRAID_ENRICH_WITH"); v != "" {
			ec.EnrichWith = strings.Split(v, ",")
		}
		if v := os.Getenv("ENTRAID_SELECT_USERS"); v != "" {
			ec.SelectUsers = strings.Split(v, ",")
		}
		if v := os.Getenv("ENTRAID_SELECT_GROUPS"); v != "" {
			ec.SelectGroups = strings.Split(v, ",")
		}
		if v := os.Getenv("ENTRAID_SELECT_DEVICES"); v != "" {
			ec.SelectDevices = strings.Split(v, ",")
		}
	}
	err := ec.Validate()
	if err != nil {
		return nil, err
	}
	return ecentraid.New(ec), nil
}
