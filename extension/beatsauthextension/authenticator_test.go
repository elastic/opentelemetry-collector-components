package beatsauthextension

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configauth"
	"go.opentelemetry.io/collector/config/confighttp"

	"github.com/elastic/elastic-agent-libs/transport/tlscommon"
	"github.com/elastic/opentelemetry-collector-components/extension/beatsauthextension/internal/metadata"
)

func TestAuthenticator(t *testing.T) {
	httpClientConfig := confighttp.NewDefaultClientConfig()
	httpClientConfig.Auth = &configauth.Authentication{
		AuthenticatorID: component.NewID(metadata.Type),
	}

	cfg := &Config{
		TLS: &TLSConfig{
			VerificationMode: tlscommon.VerifyNone,
		},
	}
	settings := componenttest.NewNopTelemetrySettings()
	auth, err := newAuthenticator(cfg, settings)
	require.NoError(t, err)

	host := extensionsMap{component.NewID(metadata.Type): auth}
	err = auth.Start(context.Background(), host)
	require.NoError(t, err)

	client, err := httpClientConfig.ToClient(context.Background(), host, settings)
	require.NoError(t, err)
	require.NotNil(t, client)

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	}))
	t.Cleanup(srv.Close)

	// TODO verify each of the verification_mode behaviours
	resp, err := client.Get(srv.URL)
	require.NoError(t, err)
	resp.Body.Close()
}

type extensionsMap map[component.ID]component.Component

func (m extensionsMap) GetExtensions() map[component.ID]component.Component {
	return m
}
