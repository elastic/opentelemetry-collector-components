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
	"errors"
	"net"
	"net/http"
	"sync"

	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
	"github.com/open-telemetry/opamp-go/server"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/extension"
	"go.uber.org/zap"
)

const (
	defaultOpAMPPath = "/v1/opamp"
)

// configClientFactory creates the remote config client. The returned stop
// function, if not nil, is called on shutdown after the context passed to the
// factory has been canceled; it must release all resources held by the client.
type configClientFactory = func(context.Context, component.Host, component.TelemetrySettings) (apmconfig.RemoteConfigClient, func(context.Context) error, error)

type apmConfigExtension struct {
	telemetrySettings component.TelemetrySettings

	extensionConfig *Config
	serverHTTP      *http.Server
	shutdownWG      sync.WaitGroup
	clientFactory   configClientFactory
	stopClient      func(context.Context) error

	cancelFn context.CancelFunc
}

var _ component.Component = (*apmConfigExtension)(nil)

func newApmConfigExtension(cfg *Config, set extension.Settings, clientFactory configClientFactory) *apmConfigExtension {
	return &apmConfigExtension{telemetrySettings: set.TelemetrySettings, extensionConfig: cfg, clientFactory: clientFactory}
}

func (op *apmConfigExtension) Start(ctx context.Context, host component.Host) error {
	ctx, op.cancelFn = context.WithCancel(ctx)

	remoteConfigClient, stopClient, err := op.clientFactory(ctx, host, op.telemetrySettings)
	op.stopClient = stopClient
	if err != nil {
		return err
	}

	opampCallbacks, err := newRemoteConfigCallbacks(ctx, remoteConfigClient, op.extensionConfig.OpAMP.Cache, op.telemetrySettings.Logger)
	if err != nil {
		return err
	}

	opampHandler, conContext, err := server.New(newLoggerFromZap(op.telemetrySettings.Logger)).Attach(server.Settings{Callbacks: *opampCallbacks.Callbacks})
	if err != nil {
		return err
	}

	httpMux := http.NewServeMux()
	httpMux.HandleFunc(defaultOpAMPPath, opampHandler)

	op.serverHTTP, err = op.extensionConfig.OpAMP.ServerConfig.ToServer(ctx, host.GetExtensions(), op.telemetrySettings, httpMux)
	if err != nil {
		return err
	}
	op.serverHTTP.ConnContext = conContext

	op.telemetrySettings.Logger.Info("Starting HTTP server", zap.String("endpoint", op.extensionConfig.OpAMP.ServerConfig.NetAddr.Endpoint))
	var hln net.Listener
	if hln, err = op.extensionConfig.OpAMP.ServerConfig.ToListener(ctx); err != nil {
		return err
	}

	op.shutdownWG.Add(1)
	go func() {
		defer op.shutdownWG.Done()

		if errHTTP := op.serverHTTP.Serve(hln); errHTTP != nil && !errors.Is(errHTTP, http.ErrServerClosed) {
			componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(errHTTP))
		}
	}()
	return nil
}

func (op *apmConfigExtension) Shutdown(ctx context.Context) error {
	var err error

	if op.cancelFn != nil {
		op.cancelFn()
	}

	if op.serverHTTP != nil {
		err = op.serverHTTP.Shutdown(ctx)
	}
	op.shutdownWG.Wait()

	if op.stopClient != nil {
		err = errors.Join(err, op.stopClient(ctx))
	}
	return err
}
