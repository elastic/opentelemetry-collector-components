package centralconfig

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"

	"go.elastic.co/apm/v2/apmconfig"
	"go.elastic.co/apm/v2/transport"
	"gopkg.in/yaml.v3"

	otelapmconfig "github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
)

type APMClient struct {
	apmconfig.Watcher
}

// Regular expression matching comment characters to escape in the User-Agent header value.
//
// See https://httpwg.org/specs/rfc7230.html#field.components
var httpComment = regexp.MustCompile("[^\\t \\x21-\\x27\\x2a-\\x5b\\x5d-\\x7e\\x80-\\xff]")

func initialTransport(serviceName, serviceVersion string) (transport.Transport, error) {
	// User-Agent should be "apm-agent-go/<agent-version> (service-name service-version)".
	service := serviceName
	if serviceVersion != "" {
		service += " " + httpComment.ReplaceAllString(serviceVersion, "_")
	}
	userAgent := fmt.Sprintf("%s (%s)", transport.DefaultUserAgent(), service)
	httpTransport, err := transport.NewHTTPTransport(transport.HTTPTransportOptions{
		UserAgent: userAgent,
	})
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

func NewCentralConfigClient() (*APMClient, error) {
	initialTransport, err := initialTransport("opamp", "0.8.0")
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
