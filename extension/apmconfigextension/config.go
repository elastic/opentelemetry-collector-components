package apmconfigextension

import (
	"go.opentelemetry.io/collector/component"
)

type Config struct {
	CentralConfig CentralConfig `mapstructure:"centralconfig"`
	OpAMP         OpAMPConfig   `mapstructure:"opamp"`
}

type CentralConfig struct {
	Elastic ElasticConfig `mapstructure:"elastic"`
}

type ElasticConfig struct {
	Apm struct {
		Server struct {
			URLs []string `mapstructure:"urls"`
			// TODO add timeout
		} `mapstructure:"server"`
		SecretToken string `mapstructure:"secret_token"`
	} `mapstructure:"apm"`
}

type OpAMPConfig struct {
	Server OpAMPServerConfig `mapstructure:"server"`
}

type OpAMPServerConfig struct {
	Endpoint string `mapstructure:"endpoint"`
}

var _ component.Config = (*Config)(nil)
