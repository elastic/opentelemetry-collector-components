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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestPartitionKey(t *testing.T) {
	assert.Equal(t, "1:11:2", partitionKey([]string{"1", "2"}))
	assert.Equal(t, "1:y", partitionKey([]string{"y"}))
	assert.Equal(t, "", partitionKey(nil))

	// Length prefixing avoids collisions between distinct value sequences.
	assert.NotEqual(t,
		partitionKey([]string{"b", ""}),
		partitionKey([]string{"bc"}),
	)
	assert.NotEqual(t,
		partitionKey([]string{"a", "bc"}),
		partitionKey([]string{"ab", "c"}),
	)
}

func TestPartitionLogs_SinglePartition(t *testing.T) {
	logs := plog.NewLogs()
	rl1 := logs.ResourceLogs().AppendEmpty()
	rl1.Resource().Attributes().PutStr("tenant.id", "t1")
	rl1.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("log1")

	rl2 := logs.ResourceLogs().AppendEmpty()
	rl2.Resource().Attributes().PutStr("tenant.id", "t1")
	rl2.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("log2")

	result := partitionLogs(t, logs,
		PartitionKeyConfig{Name: "tenant_id", Value: `resource.attributes["tenant.id"]`},
	)
	assert.Len(t, result, 1)
	assert.Equal(t, 2, result[0].logs.ResourceLogs().Len())
}

func TestPartitionLogs_MultiplePartitions(t *testing.T) {
	logs := plog.NewLogs()
	rl1 := logs.ResourceLogs().AppendEmpty()
	rl1.Resource().Attributes().PutStr("tenant.id", "t1")
	rl1.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("log1")

	rl2 := logs.ResourceLogs().AppendEmpty()
	rl2.Resource().Attributes().PutStr("tenant.id", "t2")
	rl2.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("log2")

	result := partitionLogs(t, logs,
		PartitionKeyConfig{Name: "tenant_id", Value: `resource.attributes["tenant.id"]`},
	)
	require.Len(t, result, 2)

	tenants := make(map[string]bool)
	for _, pl := range result {
		require.Len(t, pl.values, 1)
		tenants[pl.values[0]] = true
	}
	assert.True(t, tenants["t1"])
	assert.True(t, tenants["t2"])
}

func TestPartitionLogs_NilAttributeValue(t *testing.T) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("other", "value")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("log1")

	result := partitionLogs(t, logs,
		PartitionKeyConfig{Name: "tenant_id", Value: `resource.attributes["missing"]`},
	)
	require.Len(t, result, 1)
	require.Len(t, result[0].values, 1)
	assert.Equal(t, "", result[0].values[0])
}

func TestPartitionLogs_MultipleKeys(t *testing.T) {
	logs := plog.NewLogs()
	rl1 := logs.ResourceLogs().AppendEmpty()
	rl1.Resource().Attributes().PutStr("tenant.id", "t1")
	rl1.Resource().Attributes().PutStr("service.name", "svc-a")
	rl1.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("log1")

	rl2 := logs.ResourceLogs().AppendEmpty()
	rl2.Resource().Attributes().PutStr("tenant.id", "t1")
	rl2.Resource().Attributes().PutStr("service.name", "svc-b")
	rl2.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("log2")

	result := partitionLogs(t, logs,
		PartitionKeyConfig{Name: "tenant_id", Value: `resource.attributes["tenant.id"]`},
		PartitionKeyConfig{Name: "service", Value: `resource.attributes["service.name"]`},
	)
	assert.Len(t, result, 2)
}

func TestPartitionLogs_LogLevelPartitioning(t *testing.T) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test")
	sl := rl.ScopeLogs().AppendEmpty()

	lr1 := sl.LogRecords().AppendEmpty()
	lr1.Body().SetStr("error log")
	lr1.SetSeverityText("ERROR")

	lr2 := sl.LogRecords().AppendEmpty()
	lr2.Body().SetStr("info log")
	lr2.SetSeverityText("INFO")

	lr3 := sl.LogRecords().AppendEmpty()
	lr3.Body().SetStr("another error")
	lr3.SetSeverityText("ERROR")

	result := partitionLogs(t, logs,
		PartitionKeyConfig{Name: "severity", Value: `log.severity_text`},
	)
	require.Len(t, result, 2)

	severities := make(map[string]int)
	for _, pl := range result {
		require.Len(t, pl.values, 1)
		severities[pl.values[0]] = pl.logs.LogRecordCount()
	}
	assert.Equal(t, 2, severities["ERROR"])
	assert.Equal(t, 1, severities["INFO"])
}

func TestPartitionLogs_ScopePartitioning(t *testing.T) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test")

	sl1 := rl.ScopeLogs().AppendEmpty()
	sl1.Scope().SetName("scope-a")
	sl1.LogRecords().AppendEmpty().Body().SetStr("log1")
	sl1.LogRecords().AppendEmpty().Body().SetStr("log2")

	sl2 := rl.ScopeLogs().AppendEmpty()
	sl2.Scope().SetName("scope-b")
	sl2.LogRecords().AppendEmpty().Body().SetStr("log3")

	result := partitionLogs(t, logs,
		PartitionKeyConfig{Name: "scope_name", Value: `scope.name`},
	)
	require.Len(t, result, 2)

	scopes := make(map[string]int)
	for _, pl := range result {
		require.Len(t, pl.values, 1)
		scopes[pl.values[0]] = pl.logs.LogRecordCount()
	}
	assert.Equal(t, 2, scopes["scope-a"])
	assert.Equal(t, 1, scopes["scope-b"])
}

func TestPartitionLogs_NonStringValueErrors(t *testing.T) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutInt("count", 42)
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("log1")

	p, err := newLogsPartitioner([]PartitionKeyConfig{
		{Name: "count", Value: `resource.attributes["count"]`},
	}, componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)

	_, err = p.partitionLogs(context.Background(), logs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected value expression to evaluate to a string")
}

func TestPartitionLogs_KeyOrderInvariance(t *testing.T) {
	build := func() plog.Logs {
		logs := plog.NewLogs()
		for _, tenant := range []string{"t1", "t2", "t1"} {
			for _, svc := range []string{"svc-a", "svc-b"} {
				rl := logs.ResourceLogs().AppendEmpty()
				rl.Resource().Attributes().PutStr("tenant.id", tenant)
				rl.Resource().Attributes().PutStr("service.name", svc)
				rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("x")
			}
		}
		return logs
	}

	tenantFirst := partitionLogs(t, build(),
		PartitionKeyConfig{Name: "tenant_id", Value: `resource.attributes["tenant.id"]`},
		PartitionKeyConfig{Name: "service", Value: `resource.attributes["service.name"]`},
	)
	serviceFirst := partitionLogs(t, build(),
		PartitionKeyConfig{Name: "service", Value: `resource.attributes["service.name"]`},
		PartitionKeyConfig{Name: "tenant_id", Value: `resource.attributes["tenant.id"]`},
	)

	// Sort order is [service, tenant_id] regardless of input order.
	summarize := func(parts []partitionedLogs) map[string]int {
		out := make(map[string]int)
		for _, pl := range parts {
			out[pl.values[1]+"|"+pl.values[0]] = pl.logs.LogRecordCount()
		}
		return out
	}
	assert.Equal(t, summarize(tenantFirst), summarize(serviceFirst))
	assert.Len(t, tenantFirst, 4) // 2 tenants x 2 services
}

func TestPartitionLogs_LogRecordPartitionerDedup(t *testing.T) {
	// One source RL + SL, records split across two partitions — each
	// partition's output should have exactly one RL and one SL.
	t.Run("split across partitions", func(t *testing.T) {
		logs := plog.NewLogs()
		rl := logs.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "test")
		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName("s")
		for _, sev := range []string{"ERROR", "INFO", "ERROR"} {
			lr := sl.LogRecords().AppendEmpty()
			lr.SetSeverityText(sev)
		}

		result := partitionLogs(t, logs,
			PartitionKeyConfig{Name: "severity", Value: `log.severity_text`},
		)
		require.Len(t, result, 2)
		for _, pl := range result {
			assert.Equal(t, 1, pl.logs.ResourceLogs().Len())
			assert.Equal(t, 1, pl.logs.ResourceLogs().At(0).ScopeLogs().Len())
		}
	})

	// One source RL + SL, all records → same partition, output has exactly
	// one RL and one SL (no duplication).
	t.Run("all same partition", func(t *testing.T) {
		logs := plog.NewLogs()
		rl := logs.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "test")
		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName("s")
		for i := 0; i < 3; i++ {
			sl.LogRecords().AppendEmpty().SetSeverityText("ERROR")
		}

		result := partitionLogs(t, logs,
			PartitionKeyConfig{Name: "severity", Value: `log.severity_text`},
		)
		require.Len(t, result, 1)
		require.Equal(t, 1, result[0].logs.ResourceLogs().Len())
		destRL := result[0].logs.ResourceLogs().At(0)
		require.Equal(t, 1, destRL.ScopeLogs().Len())
		assert.Equal(t, 3, destRL.ScopeLogs().At(0).LogRecords().Len())
	})

	// Two source RLs, records landing in same partition — preserve source
	// identity, so the partition has two dest RLs (not merged).
	t.Run("two source RLs same partition", func(t *testing.T) {
		logs := plog.NewLogs()
		for _, svc := range []string{"svc-a", "svc-b"} {
			rl := logs.ResourceLogs().AppendEmpty()
			rl.Resource().Attributes().PutStr("service.name", svc)
			rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().SetSeverityText("ERROR")
		}

		result := partitionLogs(t, logs,
			PartitionKeyConfig{Name: "severity", Value: `log.severity_text`},
		)
		require.Len(t, result, 1)
		assert.Equal(t, 2, result[0].logs.ResourceLogs().Len())
	})
}

func TestPartitionLogs_ScopePartitionerDedup(t *testing.T) {
	// Two scopes under same RL, both landing in same partition — one dest
	// RL with two dest SLs.
	t.Run("two scopes same RL same partition", func(t *testing.T) {
		logs := plog.NewLogs()
		rl := logs.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("tenant.id", "t1")
		rl.ScopeLogs().AppendEmpty().Scope().SetName("scope-a")
		rl.ScopeLogs().AppendEmpty().Scope().SetName("scope-b")

		result := partitionLogs(t, logs,
			PartitionKeyConfig{Name: "tenant_id", Value: `resource.attributes["tenant.id"]`},
		)
		require.Len(t, result, 1)
		require.Equal(t, 1, result[0].logs.ResourceLogs().Len())
		assert.Equal(t, 2, result[0].logs.ResourceLogs().At(0).ScopeLogs().Len())
	})

	// Two scopes under different RLs, both landing in same partition — two
	// dest RLs each with one dest SL.
	t.Run("two scopes different RLs same partition", func(t *testing.T) {
		logs := plog.NewLogs()
		for _, svc := range []string{"svc-a", "svc-b"} {
			rl := logs.ResourceLogs().AppendEmpty()
			rl.Resource().Attributes().PutStr("service.name", svc)
			rl.ScopeLogs().AppendEmpty().Scope().SetName("shared-scope")
		}

		result := partitionLogs(t, logs,
			PartitionKeyConfig{Name: "scope_name", Value: `scope.name`},
		)
		require.Len(t, result, 1)
		require.Equal(t, 2, result[0].logs.ResourceLogs().Len())
		for i := 0; i < result[0].logs.ResourceLogs().Len(); i++ {
			assert.Equal(t, 1, result[0].logs.ResourceLogs().At(i).ScopeLogs().Len())
		}
	})
}

func TestPartitionLogs_PreservesSchemaURL(t *testing.T) {
	t.Run("scope partitioner", func(t *testing.T) {
		logs := plog.NewLogs()
		rl := logs.ResourceLogs().AppendEmpty()
		rl.SetSchemaUrl("https://example.com/resource-schema")
		rl.Resource().Attributes().PutStr("service.name", "test")
		sl := rl.ScopeLogs().AppendEmpty()
		sl.SetSchemaUrl("https://example.com/scope-schema")
		sl.Scope().SetName("s")
		sl.LogRecords().AppendEmpty()

		result := partitionLogs(t, logs,
			PartitionKeyConfig{Name: "scope_name", Value: `scope.name`},
		)
		require.Len(t, result, 1)
		destRL := result[0].logs.ResourceLogs().At(0)
		assert.Equal(t, "https://example.com/resource-schema", destRL.SchemaUrl())
		assert.Equal(t, "https://example.com/scope-schema", destRL.ScopeLogs().At(0).SchemaUrl())
	})

	t.Run("log record partitioner", func(t *testing.T) {
		logs := plog.NewLogs()
		rl := logs.ResourceLogs().AppendEmpty()
		rl.SetSchemaUrl("https://example.com/resource-schema")
		sl := rl.ScopeLogs().AppendEmpty()
		sl.SetSchemaUrl("https://example.com/scope-schema")
		sl.Scope().SetName("s")
		sl.LogRecords().AppendEmpty().SetSeverityText("ERROR")

		result := partitionLogs(t, logs,
			PartitionKeyConfig{Name: "severity", Value: `log.severity_text`},
		)
		require.Len(t, result, 1)
		destRL := result[0].logs.ResourceLogs().At(0)
		assert.Equal(t, "https://example.com/resource-schema", destRL.SchemaUrl())
		assert.Equal(t, "https://example.com/scope-schema", destRL.ScopeLogs().At(0).SchemaUrl())
	})
}

func TestPartitionLogs_EmptyInput(t *testing.T) {
	result := partitionLogs(t, plog.NewLogs(),
		PartitionKeyConfig{Name: "tenant_id", Value: `resource.attributes["tenant.id"]`},
	)
	assert.Empty(t, result)
}

func partitionLogs(t *testing.T, logs plog.Logs, keys ...PartitionKeyConfig) []partitionedLogs {
	t.Helper()
	p, err := newLogsPartitioner(keys, componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	result, err := p.partitionLogs(context.Background(), logs)
	require.NoError(t, err)
	return result
}
