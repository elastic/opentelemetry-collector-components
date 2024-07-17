package apmconfigextension

import (
	"context"

	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/elastic/centralconfig"
	"github.com/open-telemetry/opamp-go/server"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.uber.org/zap"
)

type apmConfigExtension struct {
	logger *zap.Logger

	apmConfigClient apmconfig.RemoteClient
	opampServer     server.OpAMPServer
}

var _ component.Component = (*apmConfigExtension)(nil)

func newApmConfigExtension(_ *Config, set extension.Settings) *apmConfigExtension {
	return &apmConfigExtension{logger: set.Logger, opampServer: server.New(nil)}
}

func (op *apmConfigExtension) Start(ctx context.Context, _ component.Host) error {
	var err error
	op.apmConfigClient, err = centralconfig.NewCentralConfigClient()
	if err != nil {
		return err
	}

	err = op.opampServer.Start(server.StartSettings{ListenEndpoint: ":4320", Settings: server.Settings{Callbacks: newConfigOpAMPCallbacks(op.apmConfigClient, op.logger)}})
	if err != nil {
		return err
	}

	op.logger.Warn("APM config extension started")
	return nil
}

func (op *apmConfigExtension) Shutdown(ctx context.Context) error {
	return op.opampServer.Stop(ctx)
}
