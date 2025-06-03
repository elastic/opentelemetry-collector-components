package apmconfigextension

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"
)

func TestUnmarshalDefaultConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "default.yaml"))
	require.NoError(t, err)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NoError(t, cm.Unmarshal(&cfg))
	assert.Equal(t, factory.CreateDefaultConfig(), cfg)
}

func TestUnmarshalConfigInvalidProtocol(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "unsupported_protocol.yaml"))
	require.NoError(t, err)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.ErrorContains(t, cm.Unmarshal(&cfg), "'opamp.protocols' has invalid keys: ws")
}

func TestUnmarshalConfigEmpty(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NoError(t, confmap.New().Unmarshal(&cfg))
	assert.EqualError(t, xconfmap.Validate(cfg), "agent_config::elasticsearch: exactly one of [endpoint, endpoints, cloudid] must be specified")
}

func TestUnmarshalConfigEmptyProtocols(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "bad_no_proto_config.yaml"))
	require.NoError(t, err)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NoError(t, cm.Unmarshal(&cfg))
	assert.EqualError(t, xconfmap.Validate(cfg), "must specify at least one protocol when using the apmconfig extension")
}
