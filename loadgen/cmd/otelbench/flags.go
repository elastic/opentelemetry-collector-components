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

package main

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

var Config struct {
	ServerURLOTLP     *url.URL
	ServerURLOTLPHTTP *url.URL

	SecretToken         string
	APIKey              string
	Insecure            bool
	InsecureSkipVerify  bool
	Headers             map[string]string
	CollectorConfigPath string

	Logs    bool
	Metrics bool
	Traces  bool

	ExporterOTLP     bool
	ExporterOTLPHTTP bool

	ConcurrencyList []int

	Telemetry TelemetryConfig
}

type TelemetryConfig struct {
	ElasticsearchURL      []string
	ElasticsearchUserName string
	ElasticsearchPassword string
	ElasticsearchAPIKey   string
	ElasticsearchTimeout  time.Duration
	ElasticsearchIndex    string
	FilterClusterName     string
	FilterProjectID       string
	Metrics               []string
}

var defaultTelemetryMetrics = []string{
	"otelcol_process_cpu_seconds",
	"otelcol_process_memory_rss",
	"otelcol_process_runtime_total_alloc_bytes",
	"otelcol_process_runtime_total_sys_memory_bytes",
	"otelcol_process_uptime",
}

func Init() error {
	// Server config
	flag.Func(
		"endpoint",
		"target server endpoint for both otlp and otlphttp exporters (default to value in config yaml), equivalent to setting both -endpoint-otlp and -endpoint-otlphttp",
		func(server string) (err error) {
			if server != "" {
				Config.ServerURLOTLP, err = url.Parse(server)
				Config.ServerURLOTLPHTTP = Config.ServerURLOTLP
			}
			return
		})
	flag.Func(
		"endpoint-otlp",
		"target server endpoint for otlp exporter (default to value in config yaml)",
		func(server string) (err error) {
			if server != "" {
				Config.ServerURLOTLP, err = url.Parse(server)
			}
			return
		})
	flag.Func(
		"endpoint-otlphttp",
		"target server endpoint for otlphttp exporter (default to value in config yaml)",
		func(server string) (err error) {
			if server != "" {
				Config.ServerURLOTLPHTTP, err = url.Parse(server)
			}
			return
		})
	flag.StringVar(&Config.SecretToken, "secret-token", "", "secret token for target server")
	flag.StringVar(&Config.APIKey, "api-key", "", "API key for target server")

	flag.BoolVar(&Config.Insecure, "insecure", false, "disable TLS, ignored by otlphttp exporter (default to value in config yaml)")
	flag.BoolVar(&Config.InsecureSkipVerify, "insecure-skip-verify", false, "skip validating the remote server TLS certificates (default to value in config yaml)")

	flag.Func("header",
		"extra headers in key=value format when sending data to the server. Can be repeated. e.g. -header X-FIRST-HEADER=foo -header X-SECOND-HEADER=bar",
		func(s string) error {
			k, v, ok := strings.Cut(s, "=")
			if !ok {
				return fmt.Errorf("invalid header '%s': format must be key=value", s)
			}
			if len(Config.Headers) == 0 {
				Config.Headers = make(map[string]string)
			}
			Config.Headers[k] = v
			return nil
		},
	)

	flag.StringVar(&Config.CollectorConfigPath, "config", "", "path to collector config yaml. If empty, the config.yaml embedded in the binary will be used.")

	flag.BoolVar(&Config.ExporterOTLP, "exporter-otlp", true, "benchmark exporter otlp")
	flag.BoolVar(&Config.ExporterOTLPHTTP, "exporter-otlphttp", true, "benchmark exporter otlphttp")

	flag.BoolVar(&Config.Logs, "logs", true, "benchmark logs")
	flag.BoolVar(&Config.Metrics, "metrics", true, "benchmark metrics")
	flag.BoolVar(&Config.Traces, "traces", true, "benchmark traces")

	// `concurrency` is similar to `agents` config in apmbench
	// Each value passed into `concurrency` list will be used as loadgenreceiver `concurrency` config
	flag.Func("concurrency", "comma-separated `list` of concurrency (number of simulated agents) to run each benchmark with. Use 'auto' to set concurrency based on GOMAXPROCS",
		func(input string) error {
			// Handle 'auto' value specially
			if input == "auto" {
				// Use the current GOMAXPROCS value
				n := runtime.GOMAXPROCS(0)
				fmt.Printf("using GOMAXPROCS value %d for concurrency\n", n)
				Config.ConcurrencyList = append(Config.ConcurrencyList, n)
				return nil
			}

			var concurrencyList []int
			for _, val := range strings.Split(input, ",") {
				val = strings.TrimSpace(val)
				if val == "" {
					continue
				}
				n, err := strconv.Atoi(val)
				if err != nil || n <= 0 {
					return fmt.Errorf("invalid value %q for -concurrency", val)
				}
				concurrencyList = append(concurrencyList, n)
			}
			sort.Ints(concurrencyList)
			Config.ConcurrencyList = concurrencyList
			return nil
		},
	)

	flag.Func("telemetry-elasticsearch-url", "optional comma-separated `list` of remote Elasticsearch telemetry hosts",
		func(input string) error {
			var urls []string
			for _, val := range strings.Split(input, ",") {
				val = strings.TrimSpace(val)
				if val == "" {
					continue
				}
				urls = append(urls, val)
			}
			Config.Telemetry.ElasticsearchURL = urls
			return nil
		},
	)
	flag.StringVar(&Config.Telemetry.ElasticsearchUserName, "telemetry-elasticsearch-username", "", "optional remote Elasticsearch telemetry username")
	flag.StringVar(&Config.Telemetry.ElasticsearchPassword, "telemetry-elasticsearch-password", "", "optional remote Elasticsearch telemetry password")
	flag.StringVar(&Config.Telemetry.ElasticsearchAPIKey, "telemetry-elasticsearch-api-key", "", "optional remote Elasticsearch telemetry API key")
	flag.DurationVar(&Config.Telemetry.ElasticsearchTimeout, "telemetry-elasticsearch-timeout", time.Minute, "optional remote Elasticsearch telemetry request timeout")
	flag.StringVar(&Config.Telemetry.ElasticsearchIndex, "telemetry-elasticsearch-index", "metrics-*", "optional remote Elasticsearch telemetry metrics index pattern")
	flag.StringVar(&Config.Telemetry.FilterClusterName, "telemetry-filter-cluster-name", "", "optional remote Elasticsearch telemetry cluster name metrics filter")
	flag.StringVar(&Config.Telemetry.FilterProjectID, "telemetry-filter-project-id", "", "optional remote Elasticsearch telemetry project id metrics filter")
	flag.Func("telemetry-metrics", "optional comma-separated `list` of remote Elasticsearch telemetry metrics to be reported",
		func(input string) error {
			var m []string
			for _, val := range strings.Split(input, ",") {
				val = strings.TrimSpace(val)
				if val == "" {
					continue
				}
				m = append(m, val)
			}
			Config.Telemetry.Metrics = m
			return nil
		},
	)
	flag.Lookup("telemetry-metrics").DefValue = strings.Join(defaultTelemetryMetrics, ",")
	// Set needs to be done separately since `DefValue` won't set default value for flag.Func.
	if err := flag.Set("telemetry-metrics", strings.Join(defaultTelemetryMetrics, ",")); err != nil {
		return fmt.Errorf(`error setting default flag "telemetry-metrics" value: %w`, err)
	}

	// For configs that can be set via environment variables, set the required
	// flags from env if they are not explicitly provided via command line
	return setFlagsFromEnv()
}

func getAuthorizationHeaderValue(apiKey, secretToken string) string {
	if apiKey != "" {
		return fmt.Sprintf("ApiKey %s", apiKey)
	} else if secretToken != "" {
		return fmt.Sprintf("Bearer %s", secretToken)
	}
	return ""
}

// setFlagsFromEnv sets flags from some Elastic APM env vars
func setFlagsFromEnv() error {
	// value[0] is environment key
	// value[1] is default value
	flagEnvMap := map[string][]string{
		"endpoint":                         {"ELASTIC_APM_SERVER_URL", ""},
		"secret-token":                     {"ELASTIC_APM_SECRET_TOKEN", ""},
		"api-key":                          {"ELASTIC_APM_API_KEY", ""},
		"telemetry-elasticsearch-url":      {"TELEMETRY_ELASTICSEARCH_URL", ""},
		"telemetry-elasticsearch-username": {"TELEMETRY_ELASTICSEARCH_USERNAME", ""},
		"telemetry-elasticsearch-password": {"TELEMETRY_ELASTICSEARCH_PASSWORD", ""},
		"telemetry-elasticsearch-api-key":  {"TELEMETRY_ELASTICSEARCH_API_KEY", ""},
		"telemetry-elasticsearch-index":    {"TELEMETRY_ELASTICSEARCH_INDEX", ""},
	}

	for k, v := range flagEnvMap {
		envVarValue := getEnvOrDefault(v[0], v[1])
		if err := flag.Set(k, envVarValue); err != nil {
			return fmt.Errorf("error setting flag \"-%s\" from env var %q with value %q: %w", k, v[0], envVarValue, err)
		}
	}
	return nil
}

func getEnvOrDefault(name, defaultValue string) string {
	value := os.Getenv(name)
	if value != "" {
		return value
	}
	return defaultValue
}

// setsToConfigs converts --set to --config
func setsToConfigs(sets []string) (configFiles []string) {
	for _, s := range sets {
		idx := strings.Index(s, "=")
		if idx == -1 {
			panic("missing = in --set") // Should never happen as all the strings are hardcoded in this file
		}
		v := "yaml:" + strings.TrimSpace(strings.ReplaceAll(s[:idx], ".", "::")) + ": " + strings.TrimSpace(s[idx+1:])
		configFiles = append(configFiles, v)
	}
	return
}

func ExporterConfigs(exporter string) (configFiles []string) {
	var configSets []string
	configSets = append(configSets, fmt.Sprintf("service.pipelines.logs.exporters=[%s]", exporter))
	configSets = append(configSets, fmt.Sprintf("service.pipelines.metrics.exporters=[%s]", exporter))
	configSets = append(configSets, fmt.Sprintf("service.pipelines.traces.exporters=[%s]", exporter))

	if Config.ServerURLOTLP != nil {
		configSets = append(configSets, fmt.Sprintf("exporters.otlp.endpoint=%s", Config.ServerURLOTLP))
	}

	if Config.ServerURLOTLPHTTP != nil {
		configSets = append(configSets, fmt.Sprintf("exporters.otlphttp.endpoint=%s", Config.ServerURLOTLPHTTP))
	}

	if v := getAuthorizationHeaderValue(Config.APIKey, Config.SecretToken); v != "" {
		configSets = append(configSets, fmt.Sprintf("exporters.%s.headers.Authorization=%s", exporter, v))
	}

	for k, v := range Config.Headers {
		configSets = append(configSets, fmt.Sprintf("exporters.%s.headers.%s=%s", exporter, k, v))
	}

	// Only set insecure and insecure_skip_verify on true, so that corresponding config value in yaml is used on default.
	if Config.Insecure {
		configSets = append(configSets, fmt.Sprintf("exporters.%s.tls.insecure=%v", exporter, Config.Insecure))
	}
	if Config.InsecureSkipVerify {
		configSets = append(configSets, fmt.Sprintf("exporters.%s.tls.insecure_skip_verify=%v", exporter, Config.InsecureSkipVerify))
	}

	return setsToConfigs(configSets)
}

func DisableSignal(signal string) (configFiles []string) {
	return setsToConfigs([]string{
		fmt.Sprintf("service.pipelines.%s.receivers=[nop]", signal),
		fmt.Sprintf("service.pipelines.%s.exporters=[nop]", signal),
	})
}

func SetIterations(iterations int) (configFiles []string) {
	return setsToConfigs([]string{
		fmt.Sprintf("receivers.loadgen.logs.max_replay=%d", iterations),
		fmt.Sprintf("receivers.loadgen.metrics.max_replay=%d", iterations),
		fmt.Sprintf("receivers.loadgen.traces.max_replay=%d", iterations),
	})
}

func SetConcurrency(concurrency int) (configFiles []string) {
	return setsToConfigs([]string{
		fmt.Sprintf("receivers.loadgen.concurrency=%d", concurrency),
	})
}
