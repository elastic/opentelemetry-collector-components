package elasticinframetricsprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/elasticinframetricsprocessor"

type Config struct {
	AddSystemMetrics bool `mapstructure:"add_system_metrics"`
}

func (c *Config) Validate() error {
	return nil
}
