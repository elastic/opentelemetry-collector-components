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

package ratelimitprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap/xconfmap"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg)
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
	assert.Error(t, xconfmap.Validate(cfg)) // rate & burst must be specified

	cfg.(*Config).Rate = 1
	cfg.(*Config).Burst = 1
	assert.NoError(t, xconfmap.Validate(cfg))
}

func TestCreateProcessor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Rate = 1
	cfg.Burst = 1
	require.NoError(t, cfg.Validate())
	params := processortest.NewNopSettings()

	lp, err := factory.CreateLogs(ctx, params, cfg, consumertest.NewNop())
	assert.NoError(t, err)
	assert.NotNil(t, lp)

	tp, err := factory.CreateTraces(ctx, params, cfg, consumertest.NewNop())
	assert.NoError(t, err)
	assert.NotNil(t, tp)

	mp, err := factory.CreateMetrics(ctx, params, cfg, consumertest.NewNop())
	assert.NoError(t, err)
	assert.NotNil(t, mp)

	pp, err := factory.CreateProfiles(ctx, params, cfg, consumertest.NewNop())
	assert.NoError(t, err)
	assert.NotNil(t, pp)
}
