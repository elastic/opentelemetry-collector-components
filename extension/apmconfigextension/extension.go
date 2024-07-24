package apmconfigextension

import (
	"context"
	"net/url"

	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/elastic/centralconfig"
	"github.com/open-telemetry/opamp-go/server"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.uber.org/zap"
)

type apmConfigExtension struct {
	logger *zap.Logger

	extensionConfig *Config
	opampServer     server.OpAMPServer
}

var _ component.Component = (*apmConfigExtension)(nil)

func newApmConfigExtension(cfg *Config, set extension.Settings) *apmConfigExtension {
	return &apmConfigExtension{logger: set.Logger, opampServer: server.New(nil), extensionConfig: cfg}
}

func (op *apmConfigExtension) Start(ctx context.Context, _ component.Host) error {
	serverUrls := make([]*url.URL, len(op.extensionConfig.CentralConfig.Elastic.Apm.Server.URLs))

	for i := range op.extensionConfig.CentralConfig.Elastic.Apm.Server.URLs {
		parsedUrl, err := url.Parse(op.extensionConfig.CentralConfig.Elastic.Apm.Server.URLs[i])
		if err != nil {
			return err
		}
		serverUrls[i] = parsedUrl
	}

	apmConfigClient, err := centralconfig.NewCentralConfigClient(serverUrls, op.extensionConfig.CentralConfig.Elastic.Apm.SecretToken, op.logger)
	if err != nil {
		return err
	}

	err = op.opampServer.Start(server.StartSettings{ListenEndpoint: op.extensionConfig.OpAMP.Server.Endpoint, Settings: server.Settings{Callbacks: newConfigOpAMPCallbacks(apmConfigClient, op.logger)}})
	if err != nil {
		return err
	}

	op.logger.Warn("APM config extension started")
	return nil
}

func (op *apmConfigExtension) Shutdown(ctx context.Context) error {
	return op.opampServer.Stop(ctx)
}
