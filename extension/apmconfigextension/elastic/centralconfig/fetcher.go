package centralconfig

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
	otelapmconfig "github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
	"github.com/elastic/opentelemetry-lib/agentcfg"
	"github.com/open-telemetry/opamp-go/protobufs"
	semconv "go.opentelemetry.io/collector/semconv/v1.5.0"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

var _ apmconfig.RemoteConfigClient = (*fetcherAPMWatcher)(nil)

type fetcherAPMWatcher struct {
	configFetcher agentcfg.Fetcher

	cacheDuration time.Duration

	logger *zap.Logger
}

func NewFetcherAPMWatcher(fetcher agentcfg.Fetcher, cacheDuration time.Duration, logger *zap.Logger) *fetcherAPMWatcher {
	return &fetcherAPMWatcher{
		fetcher,
		cacheDuration,
		logger,
	}
}

// TODO: move config to type instead of map[string]string
func configHash(encodedConfig []byte) ([]byte, error) {
	hasher := sha256.New()
	_, err := hasher.Write(encodedConfig)
	if err != nil {
		return nil, err
	}
	return hasher.Sum(nil), nil
}

// TODO: what we do with the eTag?
func changeToConfig(change agentcfg.Result) (apmconfig.RemoteConfig, error) {
	encodedConfig, err := yaml.Marshal(change.Source.Settings)
	if err != nil {
		return otelapmconfig.RemoteConfig{}, err
	}

	configHash, err := configHash(encodedConfig)
	if err != nil {
		return otelapmconfig.RemoteConfig{}, err
	}

	return otelapmconfig.RemoteConfig{Hash: configHash, Attrs: change.Source.Settings}, nil
}

func (fw *fetcherAPMWatcher) RemoteConfig(ctx context.Context, agentMsg *protobufs.AgentToServer) (apmconfig.RemoteConfig, error) {
	var params agentcfg.Query
	if agentMsg.AgentDescription != nil {
		for _, attr := range agentMsg.GetAgentDescription().GetIdentifyingAttributes() {
			switch attr.GetKey() {
			case semconv.AttributeServiceName:
				params.Service.Name = attr.GetValue().GetStringValue()
			case semconv.AttributeDeploymentEnvironment:
				params.Service.Environment = attr.GetValue().GetStringValue()
			}
		}
	}
	if params.Service.Name == "" {
		return apmconfig.RemoteConfig{}, errors.New("unidentified agent: service.name attribute must be provided")
	}
	result, err := fw.configFetcher.Fetch(ctx, agentcfg.Query{
		Service: agentcfg.Service{
			Name:        params.Service.Name,
			Environment: params.Service.Environment,
		},
	})
	if err != nil {
		return apmconfig.RemoteConfig{}, err
	}

	return changeToConfig(result)
}
