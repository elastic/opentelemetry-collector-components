package main

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
)

var Config struct {
	ServerURL           *url.URL
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
}

func Init() {
	// Server config
	flag.Func(
		"endpoint",
		"target server URL (defaults to value in config yaml)",
		func(server string) (err error) {
			if server != "" {
				Config.ServerURL, err = url.Parse(server)
			}
			return
		})
	flag.StringVar(&Config.SecretToken, "secret-token", "", "secret token for target server")
	flag.StringVar(&Config.APIKey, "api-key", "", "API key for target server")

	flag.BoolVar(&Config.Insecure, "insecure", false, "disable TLS, ignored by otlphttp exporter (defaults to value in config yaml)")
	flag.BoolVar(&Config.InsecureSkipVerify, "insecure-skip-verify", false, "skip validating the remote server TLS certificates (defaults to value in config yaml)")

	flag.Func("header",
		"extra headers to use when sending data to the server",
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

	flag.StringVar(&Config.CollectorConfigPath, "config", "config.yaml", "path collector config yaml")

	flag.BoolVar(&Config.ExporterOTLP, "exporter-otlp", true, "benchmark exporter otlp")
	flag.BoolVar(&Config.ExporterOTLPHTTP, "exporter-otlphttp", true, "benchmark exporter otlphttp")

	flag.BoolVar(&Config.Logs, "logs", true, "benchmark logs")
	flag.BoolVar(&Config.Metrics, "metrics", true, "benchmark metrics")
	flag.BoolVar(&Config.Traces, "traces", true, "benchmark traces")

	// For configs that can be set via environment variables, set the required
	// flags from env if they are not explicitly provided via command line
	setFlagsFromEnv()
}

func getAuthorizationHeaderValue(apiKey, secretToken string) string {
	if apiKey != "" {
		return fmt.Sprintf("ApiKey %s", apiKey)
	} else if secretToken != "" {
		return fmt.Sprintf("Bearer %s", secretToken)
	}
	return ""
}

func setFlagsFromEnv() {
	// value[0] is environment key
	// value[1] is default value
	flagEnvMap := map[string][]string{
		"server":       {"ELASTIC_APM_SERVER_URL", ""},
		"secret-token": {"ELASTIC_APM_SECRET_TOKEN", ""},
		"api-key":      {"ELASTIC_APM_API_KEY", ""},
		"secure":       {"ELASTIC_APM_VERIFY_SERVER_CERT", "false"},
	}

	for k, v := range flagEnvMap {
		flag.Set(k, getEnvOrDefault(v[0], v[1]))
	}
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

	if Config.ServerURL != nil {
		configSets = append(configSets, fmt.Sprintf("exporters.%s.endpoint=%s", exporter, Config.ServerURL))
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
