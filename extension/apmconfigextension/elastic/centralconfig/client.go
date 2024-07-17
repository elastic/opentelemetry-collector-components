package centralconfig

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"

	"go.elastic.co/apm/v2/apmconfig"
	"go.elastic.co/apm/v2/transport"
	"gopkg.in/yaml.v3"

	otelapmconfig "github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
)

type APMClient struct {
	apmconfig.Watcher
}

func initialTransport(opts transport.HTTPTransportOptions) (transport.Transport, error) {
	// User-Agent should be "apm-agent-go/<agent-version> (service-name service-version)".
	httpTransport, err := transport.NewHTTPTransport(opts)
	if err != nil {
		return nil, err
	}
	return httpTransport, nil
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

func (c *APMClient) RemoteConfig(ctx context.Context, agentParams otelapmconfig.Params) (otelapmconfig.RemoteConfig, error) {
	params := apmconfig.WatchParams{}
	params.Service.Name = fmt.Sprintf(agentParams.Service.Name)
	params.Service.Environment = agentParams.Service.Environment

	// get first remote config
	config := <-c.WatchConfig(ctx, params)
	if config.Err != nil {
		return otelapmconfig.RemoteConfig{}, config.Err
	}

	encodedConfig, err := yaml.Marshal(config.Attrs)
	if err != nil {
		return otelapmconfig.RemoteConfig{}, err
	}

	configHash, err := configHash(encodedConfig)
	if err != nil {
		return otelapmconfig.RemoteConfig{}, err
	}

	return otelapmconfig.RemoteConfig{Hash: configHash, Attrs: config.Attrs}, nil
}

func (c *APMClient) EffectiveConfig(context.Context, otelapmconfig.Params) error {
	// TODO: update transport to notify latest applied remote config
	return nil
}

var _ apmconfig.Watcher = (*APMClient)(nil)

func NewCentralConfigClient(urls []*url.URL, token string) (*APMClient, error) {
	userAgent := fmt.Sprintf("%s (%s)", transport.DefaultUserAgent(), "apmconfigextension/0.0.1")
	transportOpts := transport.HTTPTransportOptions{
		UserAgent:   userAgent,
		SecretToken: token,
		ServerURLs:  urls,
	}
	initialTransport, err := initialTransport(transportOpts)
	if err != nil {
		return nil, err
	}

	if cw, ok := initialTransport.(apmconfig.Watcher); ok {
		return &APMClient{
			cw,
		}, nil
	}

	return nil, errors.New("transport does not implement the Watcher")
}
