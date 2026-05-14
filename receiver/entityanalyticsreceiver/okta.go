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
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/confmap"

	"github.com/elastic/entcollect"
	ecokta "github.com/elastic/entcollect/provider/okta"
)

func init() {
	Register("okta", oktaProvider)
}

func oktaProvider(cfg *confmap.Conf) (entcollect.Provider, error) {
	ec := ecokta.DefaultConfig()
	if cfg != nil {
		type oktaOAuth2Conf struct {
			ClientID     configopaque.String `mapstructure:"client_id"`
			ClientSecret configopaque.String `mapstructure:"client_secret"`
			Scopes       []string            `mapstructure:"scopes"`
			TokenURL     string              `mapstructure:"token_url"`
			JWK          json.RawMessage     `mapstructure:"jwk"`
		}

		// oktaLocalConf mirrors ecokta.Config with mapstructure tags for
		// confmap.Conf Unmarshal. SyncInterval and UpdateInterval are
		// owned by the receiver's top-level Config and are intentionally
		// absent here. If ecokta.Config gains a new field, add it here
		// too; TestOktaProviderConfigRoundTrip will catch any drift.
		type oktaLocalConf struct {
			Domain      configopaque.String `mapstructure:"okta_domain"`
			Token       configopaque.String `mapstructure:"okta_token"`
			OAuth2      *oktaOAuth2Conf     `mapstructure:"oauth2"`
			Dataset     string              `mapstructure:"dataset"`
			EnrichWith  []string            `mapstructure:"enrich_with"`
			BatchSize   int                 `mapstructure:"batch_size"`
			IDSetShards int                 `mapstructure:"idset_shards"`
			LimitWindow time.Duration       `mapstructure:"limit_window"`
			LimitFixed  *int                `mapstructure:"limit_fixed"`
		}

		lc := oktaLocalConf{
			EnrichWith:  ec.EnrichWith,
			IDSetShards: ec.IDSetShards,
			LimitWindow: ec.LimitWindow,
		}
		err := cfg.Unmarshal(&lc)
		if err != nil {
			return nil, fmt.Errorf("okta: unmarshal config: %w", err)
		}
		ec.Domain = string(lc.Domain)
		ec.Token = string(lc.Token)
		ec.Dataset = lc.Dataset
		ec.EnrichWith = lc.EnrichWith
		ec.BatchSize = lc.BatchSize
		ec.IDSetShards = lc.IDSetShards
		ec.LimitWindow = lc.LimitWindow
		ec.LimitFixed = lc.LimitFixed

		if lc.OAuth2 != nil {
			ec.OAuth2 = &ecokta.OAuth2Config{
				ClientID:     string(lc.OAuth2.ClientID),
				ClientSecret: string(lc.OAuth2.ClientSecret),
				TokenURL:     lc.OAuth2.TokenURL,
				Scopes:       lc.OAuth2.Scopes,
				JWK:          lc.OAuth2.JWK,
			}
		}
	} else {
		ec.Domain = os.Getenv("OKTA_DOMAIN")
		ec.Token = os.Getenv("OKTA_TOKEN")
		if v := os.Getenv("OKTA_DATASET"); v != "" {
			ec.Dataset = v
		}
		if v := os.Getenv("OKTA_ENRICH_WITH"); v != "" {
			ec.EnrichWith = strings.Split(v, ",")
		}
		if v := os.Getenv("OKTA_BATCH_SIZE"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("okta: parse OKTA_BATCH_SIZE: %w", err)
			}
			ec.BatchSize = n
		}
		if v := os.Getenv("OKTA_IDSET_SHARDS"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("okta: parse OKTA_IDSET_SHARDS: %w", err)
			}
			ec.IDSetShards = n
		}
		if v := os.Getenv("OKTA_LIMIT_WINDOW"); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("okta: parse OKTA_LIMIT_WINDOW: %w", err)
			}
			ec.LimitWindow = d
		}
		if v := os.Getenv("OKTA_LIMIT_FIXED"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("okta: parse OKTA_LIMIT_FIXED: %w", err)
			}
			ec.LimitFixed = &n
		}

		clientID := os.Getenv("OKTA_OAUTH2_CLIENT_ID")
		if clientID != "" {
			ec.OAuth2 = &ecokta.OAuth2Config{
				ClientID:     clientID,
				ClientSecret: os.Getenv("OKTA_OAUTH2_CLIENT_SECRET"),
				TokenURL:     os.Getenv("OKTA_OAUTH2_TOKEN_URL"),
			}
			if v := os.Getenv("OKTA_OAUTH2_SCOPES"); v != "" {
				ec.OAuth2.Scopes = strings.Split(v, ",")
			}
			if v := os.Getenv("OKTA_OAUTH2_JWK"); v != "" {
				ec.OAuth2.JWK = json.RawMessage(v)
			}
		}
	}
	err := ec.Validate()
	if err != nil {
		return nil, err
	}
	return ecokta.New(ec), nil
}
