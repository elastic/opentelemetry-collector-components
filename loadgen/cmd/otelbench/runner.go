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
	Secure              bool
	Headers             map[string]string
	CollectorConfigPath string
	Exporter            string // should be one of [otlp, otlphttp]

	Logs    bool
	Metrics bool
	Traces  bool
}

func Init() {
	// Server config
	flag.Func(
		"server",
		"server URL (default http://127.0.0.1:8200)",
		func(server string) (err error) {
			if server != "" {
				Config.ServerURL, err = url.Parse(server)
			}
			return
		})
	flag.StringVar(&Config.SecretToken, "secret-token", "", "secret token for APM Server")
	flag.StringVar(&Config.APIKey, "api-key", "", "API key for APM Server")
	flag.BoolVar(&Config.Secure, "secure", false, "validate the remote server TLS certificates")
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

	flag.StringVar(&Config.CollectorConfigPath, "config", "", "Collector config path")

	flag.StringVar(&Config.Exporter, "exporter", "otlp", "exporter to use, one of [otlp, otlphttp]")

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
		"server":       {"ELASTIC_APM_SERVER_URL", "http://127.0.0.1:8200"},
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

// CollectorConfigFilesFromConfig returns a slice of strings, each can be passed to the collector using --config
func CollectorConfigFilesFromConfig(exporter, signal string, iterations int) (configFiles []string) {
	sets := CollectorSetFromConfig(exporter, signal, iterations)
	for _, s := range sets {
		idx := strings.Index(s, "=")
		if idx == -1 {
			panic("missing = in --set") // Should never happen as all the strings are hardcoded below.
		}
		v := "yaml:" + strings.TrimSpace(strings.ReplaceAll(s[:idx], ".", "::")) + ": " + strings.TrimSpace(s[idx+1:])
		configFiles = append(configFiles, v)
	}
	return
}

// CollectorSetFromConfig returns a slice of strings, each can be passed to the collector using --set
func CollectorSetFromConfig(exporter, signal string, iterations int) (configSets []string) {
	configSets = append(configSets, fmt.Sprintf("service.pipelines.%s.receivers=[loadgen]", signal))
	configSets = append(configSets, fmt.Sprintf("service.pipelines.%s.processors=[transform/rewrite]", signal))
	configSets = append(configSets, fmt.Sprintf("service.pipelines.%s.exporters=[%s]", signal, exporter))

	configSets = append(configSets, fmt.Sprintf("receivers.loadgen.%s.max_replay=%d", signal, iterations))

	configSets = append(configSets, fmt.Sprintf("exporters.%s.endpoint=%s", exporter, Config.ServerURL))

	if v := getAuthorizationHeaderValue(Config.APIKey, Config.SecretToken); v != "" {
		configSets = append(configSets, fmt.Sprintf("exporters.%s.headers.Authorization=%s", exporter, v))
	}

	for k, v := range Config.Headers {
		configSets = append(configSets, fmt.Sprintf("exporters.%s.headers.%s=%s", exporter, k, v))
	}

	configSets = append(configSets, fmt.Sprintf("exporters.%s.tls.insecure=%v", exporter, !Config.Secure))

	return
}
