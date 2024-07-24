package centralconfig

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"

	"go.elastic.co/apm/v2/apmconfig"
	"go.elastic.co/apm/v2/transport"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	otelapmconfig "github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
)

type APMClient struct {
	client apmconfig.Watcher

	myCtx context.Context

	logger *zap.Logger

	agents            map[string]<-chan apmconfig.Change
	agentscancelFuncs map[string]context.CancelFunc
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

func (c *APMClient) agentChange(ctx context.Context, agentUid string) apmconfig.Change {
	var change apmconfig.Change
	if changeChan, ok := c.agents[agentUid]; ok {
		select {
		case change, ok = <-changeChan:
			return change
		case <-ctx.Done():
		default:
		}
	}
	return change
}

func (c *APMClient) RemoteConfig(ctx context.Context, agentParams otelapmconfig.Params) (otelapmconfig.RemoteConfig, error) {
	params := apmconfig.WatchParams{}
	params.Service.Name = fmt.Sprintf(agentParams.Service.Name)
	params.Service.Environment = agentParams.Service.Environment

	var config apmconfig.Change
	if _, ok := c.agents[agentParams.AgentUiD]; !ok {
		ctx, cancelFn := context.WithCancel(c.myCtx)
		c.agentscancelFuncs[agentParams.AgentUiD] = cancelFn
		c.agents[agentParams.AgentUiD] = c.client.WatchConfig(ctx, params)

		// non blocking call if already received first config
		config = <-c.agents[agentParams.AgentUiD]

	} else {
		config = c.agentChange(ctx, agentParams.AgentUiD)
	}

	if config.Err != nil {
		return otelapmconfig.RemoteConfig{}, config.Err
	} else if len(config.Attrs) == 0 {
		return otelapmconfig.RemoteConfig{}, nil
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

func (c *APMClient) LastConfig(context.Context, otelapmconfig.Params, []byte) error {
	// TODO: update transport to notify latest applied remote config
	return nil
}

func NewCentralConfigClient(urls []*url.URL, token string, logger *zap.Logger) (*APMClient, error) {
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
			context.Background(),
			logger,
			make(map[string]<-chan apmconfig.Change),
			make(map[string]context.CancelFunc),
		}, nil
	}

	return nil, errors.New("transport does not implement the Watcher")
}
