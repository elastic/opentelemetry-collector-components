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
	"fmt"
	"net/url"

	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
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
	configClient    apmconfig.Client
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

	var err error
	op.configClient, err = centralconfig.NewCentralConfigClient(serverUrls, op.extensionConfig.CentralConfig.Elastic.Apm.SecretToken, op.logger)
	if err != nil {
		return err
	}

	err = op.opampServer.Start(server.StartSettings{ListenEndpoint: op.extensionConfig.OpAMP.Server.Endpoint, Settings: server.Settings{Callbacks: newConfigOpAMPCallbacks(op.configClient, op.logger)}})
	if err != nil {
		return err
	}

	op.logger.Info("APM config extension started")
	return nil
}

func (op *apmConfigExtension) Shutdown(ctx context.Context) error {
	if op.configClient != nil {
		err := op.configClient.Close()
		if err != nil {
			return err
		}
	}
	fmt.Println("shuting down the opamp server")
	return op.opampServer.Stop(ctx)
}
