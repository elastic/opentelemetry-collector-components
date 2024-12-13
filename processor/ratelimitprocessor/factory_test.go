// ELASTICSEARCH CONFIDENTIAL
// __________________
//
//  Copyright Elasticsearch B.V. All rights reserved.
//
// NOTICE:  All information contained herein is, and remains
// the property of Elasticsearch B.V. and its suppliers, if any.
// The intellectual and technical concepts contained herein
// are proprietary to Elasticsearch B.V. and its suppliers and
// may be covered by U.S. and Foreign Patents, patents in
// process, and are protected by trade secret or copyright
// law.  Dissemination of this information or reproduction of
// this material is strictly forbidden unless prior written
// permission is obtained from Elasticsearch B.V.

package ratelimitprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg)
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
	assert.Error(t, component.ValidateConfig(cfg)) // rate & burst must be specified

	cfg.(*Config).Rate = 1
	cfg.(*Config).Burst = 1
	assert.NoError(t, component.ValidateConfig(cfg))
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
