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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	assert.EqualError(t, xconfmap.Validate(cfg), "agent_config::elasticsearch: exactly one of [endpoint, endpoints, cloudid] must be specified")
}

func TestUnmarshalConfigInvalidProtocol(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "unsupported_protocol.yaml"))
	require.NoError(t, err)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.ErrorContains(t, cm.Unmarshal(&cfg), "'opamp.protocols' has invalid keys: ws")
}

func TestUnmarshalConfigEmptyProtocols(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "bad_no_proto_config.yaml"))
	require.NoError(t, err)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NoError(t, cm.Unmarshal(&cfg))
	assert.EqualError(t, xconfmap.Validate(cfg), "must specify at least one protocol when using the apmconfig extension")
}
