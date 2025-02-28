// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package apmconfigextension // import "github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension"

import (
	"context"

	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
	"github.com/open-telemetry/opamp-go/server"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

type configClientFactory = func(context.Context, component.Host, component.TelemetrySettings) (apmconfig.RemoteConfigClient, error)

type apmConfigExtension struct {
	telemetrySettings component.TelemetrySettings

	extensionConfig *Config
	opampServer     server.OpAMPServer
	clientFactory   configClientFactory

	cancelFn context.CancelFunc
}

var _ component.Component = (*apmConfigExtension)(nil)

func newApmConfigExtension(cfg *Config, set extension.Settings, clientFactory configClientFactory) *apmConfigExtension {
	return &apmConfigExtension{telemetrySettings: set.TelemetrySettings, opampServer: server.New(nil), extensionConfig: cfg, clientFactory: clientFactory}
}

func (op *apmConfigExtension) Start(ctx context.Context, host component.Host) error {
	ctx, op.cancelFn = context.WithCancel(ctx)

	remoteConfigClient, err := op.clientFactory(ctx, host, op.telemetrySettings)
	if err != nil {
		return err
	}

	return op.opampServer.Start(server.StartSettings{ListenEndpoint: op.extensionConfig.OpAMP.Server.Endpoint, Settings: server.Settings{Callbacks: *newRemoteConfigCallbacks(remoteConfigClient, op.telemetrySettings.Logger)}})
}

func (op *apmConfigExtension) Shutdown(ctx context.Context) error {
	if op.cancelFn != nil {
		op.cancelFn()
	}
	return op.opampServer.Stop(ctx)
}
