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

package apmconfigextension

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/extension/extensiontest"

	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/internal/metadata"
)

func newTestExtension(t *testing.T, endpoint string, stop func(factoryCtx context.Context, shutdownCtx context.Context) error) *apmConfigExtension {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	cfg.OpAMP.ServerConfig.NetAddr.Endpoint = endpoint
	factory := func(ctx context.Context, _ component.Host, _ component.TelemetrySettings) (apmconfig.RemoteConfigClient, func(context.Context) error, error) {
		return &remoteConfigMock{}, func(shutdownCtx context.Context) error {
			return stop(ctx, shutdownCtx)
		}, nil
	}
	return newApmConfigExtension(cfg, extensiontest.NewNopSettings(metadata.Type), factory)
}

func TestShutdownCancelsThenWaitsForClientStop(t *testing.T) {
	stopStarted := make(chan struct{})
	releaseStop := make(chan struct{})
	ext := newTestExtension(t, "localhost:0", func(factoryCtx, _ context.Context) error {
		// The client context must be canceled before stop is called.
		assert.ErrorIs(t, factoryCtx.Err(), context.Canceled)
		close(stopStarted)
		<-releaseStop
		return nil
	})
	require.NoError(t, ext.Start(context.Background(), componenttest.NewNopHost()))

	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- ext.Shutdown(context.Background()) }()

	select {
	case <-stopStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not call the client stop function")
	}
	select {
	case <-shutdownErr:
		t.Fatal("Shutdown returned before the client stop function finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseStop)
	require.NoError(t, <-shutdownErr)
}

func TestShutdownReturnsClientStopError(t *testing.T) {
	stopErr := errors.New("stop failed")
	ext := newTestExtension(t, "localhost:0", func(context.Context, context.Context) error {
		return stopErr
	})
	require.NoError(t, ext.Start(context.Background(), componenttest.NewNopHost()))
	require.ErrorIs(t, ext.Shutdown(context.Background()), stopErr)
}

func TestShutdownStopsClientAfterFailedStart(t *testing.T) {
	stopped := false
	// An invalid endpoint makes Start fail after the client has been created.
	ext := newTestExtension(t, "invalid-endpoint", func(context.Context, context.Context) error {
		stopped = true
		return nil
	})
	require.Error(t, ext.Start(context.Background(), componenttest.NewNopHost()))
	require.NoError(t, ext.Shutdown(context.Background()))
	assert.True(t, stopped)
}
