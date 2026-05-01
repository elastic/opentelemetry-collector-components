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

package partitioningprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/partitioningprocessor"

import (
	"context"
	"fmt"
	"strings"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"go.opentelemetry.io/collector/pdata/plog"
)

type logsPartitioner interface {
	partitionLogs(ctx context.Context, ld plog.Logs) ([]partitionedLogs, error)
}

// partitionedLogs holds the logs for a single partition along with the
// evaluated values for each key (in the same order as the processor's
// logsKeyNames).
type partitionedLogs struct {
	values []string
	logs   plog.Logs
}

// resourceLogsPartitioner evaluates expressions at the ResourceLogs level.
type resourceLogsPartitioner struct {
	expressions []*ottl.ValueExpression[*ottlresource.TransformContext]
}

func (p *resourceLogsPartitioner) partitionLogs(ctx context.Context, ld plog.Logs) ([]partitionedLogs, error) {
	groups := make(map[string]*partitionedLogs)

	for _, rl := range ld.ResourceLogs().All() {
		values, err := p.evaluateResource(ctx, rl)
		if err != nil {
			return nil, err
		}
		pk := partitionKey(values)
		group, ok := groups[pk]
		if !ok {
			group = &partitionedLogs{values: values, logs: plog.NewLogs()}
			groups[pk] = group
		}
		rl.CopyTo(group.logs.ResourceLogs().AppendEmpty())
	}

	result := make([]partitionedLogs, 0, len(groups))
	for _, group := range groups {
		result = append(result, *group)
	}
	return result, nil
}

func (p *resourceLogsPartitioner) evaluateResource(ctx context.Context, rl plog.ResourceLogs) ([]string, error) {
	tCtx := ottlresource.NewTransformContextPtr(rl.Resource(), rl)
	defer tCtx.Close()
	return evaluateStringExpressions(ctx, p.expressions, tCtx)
}

// scopeLogsPartitioner evaluates expressions at the ScopeLogs level.
type scopeLogsPartitioner struct {
	expressions []*ottl.ValueExpression[*ottlscope.TransformContext]
}

func (p *scopeLogsPartitioner) partitionLogs(ctx context.Context, ld plog.Logs) ([]partitionedLogs, error) {
	type scopeGroup struct {
		partitionedLogs
		destResources map[plog.ResourceLogs]plog.ResourceLogs
	}
	groups := make(map[string]*scopeGroup)

	for _, rl := range ld.ResourceLogs().All() {
		for _, sl := range rl.ScopeLogs().All() {
			values, err := p.evaluateScope(ctx, rl, sl)
			if err != nil {
				return nil, err
			}
			pk := partitionKey(values)

			group, ok := groups[pk]
			if !ok {
				group = &scopeGroup{
					partitionedLogs: partitionedLogs{values: values, logs: plog.NewLogs()},
					destResources:   make(map[plog.ResourceLogs]plog.ResourceLogs),
				}
				groups[pk] = group
			}

			destRL := findOrCreateResourceLogs(group.logs, rl, group.destResources)
			sl.CopyTo(destRL.ScopeLogs().AppendEmpty())
		}
	}

	result := make([]partitionedLogs, 0, len(groups))
	for _, group := range groups {
		result = append(result, group.partitionedLogs)
	}
	return result, nil
}

func (p *scopeLogsPartitioner) evaluateScope(ctx context.Context, rl plog.ResourceLogs, sl plog.ScopeLogs) ([]string, error) {
	tCtx := ottlscope.NewTransformContextPtr(sl.Scope(), rl.Resource(), sl)
	defer tCtx.Close()
	return evaluateStringExpressions(ctx, p.expressions, tCtx)
}

// logRecordPartitioner evaluates expressions at the LogRecord level.
type logRecordPartitioner struct {
	expressions []*ottl.ValueExpression[*ottllog.TransformContext]
}

func (p *logRecordPartitioner) partitionLogs(ctx context.Context, ld plog.Logs) ([]partitionedLogs, error) {
	type recordGroup struct {
		partitionedLogs
		destResources map[plog.ResourceLogs]plog.ResourceLogs
		destScopes    map[plog.ScopeLogs]plog.ScopeLogs
	}

	groups := make(map[string]*recordGroup)

	for _, rl := range ld.ResourceLogs().All() {
		for _, sl := range rl.ScopeLogs().All() {
			for _, lr := range sl.LogRecords().All() {
				values, err := p.evaluateLogRecord(ctx, rl, sl, lr)
				if err != nil {
					return nil, err
				}
				pk := partitionKey(values)

				group, ok := groups[pk]
				if !ok {
					group = &recordGroup{
						partitionedLogs: partitionedLogs{values: values, logs: plog.NewLogs()},
						destResources:   make(map[plog.ResourceLogs]plog.ResourceLogs),
						destScopes:      make(map[plog.ScopeLogs]plog.ScopeLogs),
					}
					groups[pk] = group
				}

				destRL := findOrCreateResourceLogs(group.logs, rl, group.destResources)
				destSL, ok := group.destScopes[sl]
				if !ok {
					destSL = destRL.ScopeLogs().AppendEmpty()
					sl.Scope().CopyTo(destSL.Scope())
					destSL.SetSchemaUrl(sl.SchemaUrl())
					group.destScopes[sl] = destSL
				}
				lr.CopyTo(destSL.LogRecords().AppendEmpty())
			}
		}
	}

	result := make([]partitionedLogs, 0, len(groups))
	for _, group := range groups {
		result = append(result, group.partitionedLogs)
	}
	return result, nil
}

func (p *logRecordPartitioner) evaluateLogRecord(ctx context.Context, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) ([]string, error) {
	tCtx := ottllog.NewTransformContextPtr(rl, sl, lr)
	defer tCtx.Close()
	return evaluateStringExpressions(ctx, p.expressions, tCtx)
}

// findOrCreateResourceLogs finds a ResourceLogs in dest matching src, or creates one.
func findOrCreateResourceLogs(dest plog.Logs, src plog.ResourceLogs, seen map[plog.ResourceLogs]plog.ResourceLogs) plog.ResourceLogs {
	if destRL, ok := seen[src]; ok {
		return destRL
	}
	destRL := dest.ResourceLogs().AppendEmpty()
	src.Resource().CopyTo(destRL.Resource())
	destRL.SetSchemaUrl(src.SchemaUrl())
	seen[src] = destRL
	return destRL
}

// evaluateStringExpressions evaluates each expression against tCtx and
// returns the resulting string values in order. A nil result is treated as
// an empty string (e.g. a missing attribute); any non-nil, non-string result
// is a configuration error and produces a diagnostic.
func evaluateStringExpressions[K any](ctx context.Context, exprs []*ottl.ValueExpression[K], tCtx K) ([]string, error) {
	values := make([]string, len(exprs))
	for i, expr := range exprs {
		val, err := expr.Eval(ctx, tCtx)
		if err != nil {
			return nil, fmt.Errorf("evaluating key at index %d: %w", i, err)
		}
		switch v := val.(type) {
		case nil:
		case string:
			values[i] = v
		default:
			return nil, fmt.Errorf("key at index %d: expected value expression to evaluate to a string, got %T", i, val)
		}
	}
	return values, nil
}

// partitionKey returns a collision-free map key derived from values.
// Values are in a fixed order established at partitioner construction, so
// only lengths + contents need to be encoded (no separators, no key names).
func partitionKey(values []string) string {
	var b strings.Builder
	for _, v := range values {
		fmt.Fprintf(&b, "%d:%s", len(v), v)
	}
	return b.String()
}
