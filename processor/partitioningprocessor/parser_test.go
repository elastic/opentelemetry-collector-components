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

package partitioningprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
)

func TestNewLogsPartitioner_ResourceContext(t *testing.T) {
	keys := []PartitionKeyConfig{
		{Name: "tenant_id", Value: `resource.attributes["tenant.id"]`},
		{Name: "service", Value: `resource.attributes["service.name"]`},
	}
	p, err := newLogsPartitioner(keys, componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	_, ok := p.(*resourceLogsPartitioner)
	assert.True(t, ok, "expected resourceLogsPartitioner when all expressions use resource context")
}

func TestNewLogsPartitioner_LogContext(t *testing.T) {
	keys := []PartitionKeyConfig{
		{Name: "severity", Value: `log.severity_text`},
	}
	p, err := newLogsPartitioner(keys, componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	_, ok := p.(*logRecordPartitioner)
	assert.True(t, ok, "expected logRecordPartitioner when expression uses log context")
}

func TestNewLogsPartitioner_ScopeContext(t *testing.T) {
	keys := []PartitionKeyConfig{
		{Name: "scope_name", Value: `scope.name`},
	}
	p, err := newLogsPartitioner(keys, componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	_, ok := p.(*scopeLogsPartitioner)
	assert.True(t, ok, "expected scopeLogsPartitioner when expression uses scope context")
}

func TestNewLogsPartitioner_ScopeAndResourceContext(t *testing.T) {
	keys := []PartitionKeyConfig{
		{Name: "tenant_id", Value: `resource.attributes["tenant.id"]`},
		{Name: "scope_name", Value: `scope.name`},
	}
	p, err := newLogsPartitioner(keys, componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	_, ok := p.(*scopeLogsPartitioner)
	assert.True(t, ok, "expected scopeLogsPartitioner when mixed resource+scope contexts used")
}

func TestNewLogsPartitioner_MixedContext(t *testing.T) {
	keys := []PartitionKeyConfig{
		{Name: "tenant_id", Value: `resource.attributes["tenant.id"]`},
		{Name: "severity", Value: `log.severity_text`},
	}
	p, err := newLogsPartitioner(keys, componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	_, ok := p.(*logRecordPartitioner)
	assert.True(t, ok, "expected logRecordPartitioner when mixed contexts used")
}

func TestNewLogsPartitioner_InvalidExpression(t *testing.T) {
	keys := []PartitionKeyConfig{
		{Name: "bad", Value: `not_a_valid_expression(`},
	}
	_, err := newLogsPartitioner(keys, componenttest.NewNopTelemetrySettings())
	assert.Error(t, err)
}
