package centralconfig

import (
	"context"
	"crypto/sha256"
	"fmt"
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

	// OpAMP instanceID to service mapping
	uidToService map[string]agentcfg.Service

	logger *zap.Logger
}

func NewFetcherAPMWatcher(fetcher agentcfg.Fetcher, cacheDuration time.Duration, logger *zap.Logger) *fetcherAPMWatcher {
	return &fetcherAPMWatcher{
		fetcher,
		cacheDuration,
		make(map[string]agentcfg.Service),
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
	var serviceParams agentcfg.Service
	if agentMsg.AgentDescription != nil {
		for _, attr := range agentMsg.GetAgentDescription().GetIdentifyingAttributes() {
			switch attr.GetKey() {
			case semconv.AttributeServiceName:
				serviceParams.Name = attr.GetValue().GetStringValue()
			case semconv.AttributeDeploymentEnvironment:
				serviceParams.Environment = attr.GetValue().GetStringValue()
			}
		}
		fw.uidToService[string(agentMsg.GetInstanceUid())] = serviceParams
	} else {
		serviceParams = fw.uidToService[string(agentMsg.GetInstanceUid())]
	}
	if serviceParams.Name == "" {
		return apmconfig.RemoteConfig{}, fmt.Errorf("%w: service.name attribute must be provided", apmconfig.UnidentifiedAgent)
	}
	result, err := fw.configFetcher.Fetch(ctx, agentcfg.Query{
		Service: serviceParams,
	})
	if err != nil {
		return apmconfig.RemoteConfig{}, err
	}

	return changeToConfig(result)
}
