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

package elasticapmreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmreceiver"

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"
	"go.opentelemetry.io/collector/config/confighttp"
)

const defaultElasticsearchEnvName = "ELASTICSEARCH_URL"

var (
	errConfigEndpointRequired = errors.New("exactly one of [endpoint, endpoints, cloudid] must be specified")
	errConfigEmptyEndpoint    = errors.New("endpoint must not be empty")
)

// Config defines configuration for the Elastic APM receiver.
type Config struct {
	Elasticsearch ElasticSearchClient `mapstructure:"elasticsearch"`

	confighttp.ServerConfig `mapstructure:",squash"`
}

type ElasticSearchClient struct {
	// This setting is required if CloudID is not set and if the
	// ELASTICSEARCH_URL environment variable is not set.
	Endpoints []string `mapstructure:"endpoints"`

	// CloudID holds the cloud ID to identify the Elastic Cloud cluster to send events to.
	// https://www.elastic.co/guide/en/cloud/current/ec-cloud-id.html
	//
	// This setting is required if no URL is configured.
	CloudID string `mapstructure:"cloudid"`

	confighttp.ClientConfig `mapstructure:",squash"`

	// TODO: move this repository to upstream contrib and unify these
	// structures in internal/

	// TelemetrySettings contains settings useful for testing/debugging purposes
	// This is experimental and may change at any time.
	elasticsearchexporter.TelemetrySettings `mapstructure:"telemetry"`

	Discovery elasticsearchexporter.DiscoverySettings `mapstructure:"discover"`
	Retry     elasticsearchexporter.RetrySettings     `mapstructure:"retry"`

	cacheDuration time.Duration `mapstructure:"cache_duration"`
}

// Validate checks the receiver configuration is valid.
func (cfg *Config) Validate() error {
	endpoints, err := cfg.endpoints()
	if err != nil {
		return err
	}
	for _, endpoint := range endpoints {
		if err := validateEndpoint(endpoint); err != nil {
			return fmt.Errorf("invalid endpoint %q: %w", endpoint, err)
		}
	}

	if cfg.Elasticsearch.Retry.MaxRequests != 0 && cfg.Elasticsearch.Retry.MaxRetries != 0 {
		return errors.New("must not specify both retry::max_requests and retry::max_retries")
	}
	if cfg.Elasticsearch.Retry.MaxRequests < 0 {
		return errors.New("retry::max_requests should be non-negative")
	}
	if cfg.Elasticsearch.Retry.MaxRetries < 0 {
		return errors.New("retry::max_retries should be non-negative")
	}
	return nil
}

func validateEndpoint(endpoint string) error {
	if endpoint == "" {
		return errConfigEmptyEndpoint
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return err
	}
	switch u.Scheme {
	case "http", "https":
	default:
		return fmt.Errorf(`invalid scheme %q, expected "http" or "https"`, u.Scheme)
	}
	return nil
}

func (cfg *Config) endpoints() ([]string, error) {
	// Exactly one of endpoint, endpoints, or cloudid must be configured.
	// If none are set, then $ELASTICSEARCH_URL may be specified instead.
	var endpoints []string
	var numEndpointConfigs int

	if cfg.Elasticsearch.Endpoint != "" {
		numEndpointConfigs++
		endpoints = []string{cfg.Elasticsearch.Endpoint}
	}
	if len(cfg.Elasticsearch.Endpoints) > 0 {
		numEndpointConfigs++
		endpoints = cfg.Elasticsearch.Endpoints
	}
	if cfg.Elasticsearch.CloudID != "" {
		numEndpointConfigs++
		u, err := parseCloudID(cfg.Elasticsearch.CloudID)
		if err != nil {
			return nil, err
		}
		endpoints = []string{u.String()}
	}
	if numEndpointConfigs == 0 {
		if v := os.Getenv(defaultElasticsearchEnvName); v != "" {
			numEndpointConfigs++
			endpoints = strings.Split(v, ",")
			for i, endpoint := range endpoints {
				endpoints[i] = strings.TrimSpace(endpoint)
			}
		}
	}
	if numEndpointConfigs != 1 {
		return nil, errConfigEndpointRequired
	}
	return endpoints, nil
}

// Based on "addrFromCloudID" in go-elasticsearch.
func parseCloudID(input string) (*url.URL, error) {
	_, after, ok := strings.Cut(input, ":")
	if !ok {
		return nil, fmt.Errorf("invalid CloudID %q", input)
	}

	decoded, err := base64.StdEncoding.DecodeString(after)
	if err != nil {
		return nil, err
	}

	before, after, ok := strings.Cut(string(decoded), "$")
	if !ok {
		return nil, fmt.Errorf("invalid decoded CloudID %q", string(decoded))
	}
	return url.Parse(fmt.Sprintf("https://%s.%s", after, before))
}
