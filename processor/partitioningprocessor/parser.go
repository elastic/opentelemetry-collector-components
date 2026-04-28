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
	"sort"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
	"go.opentelemetry.io/collector/component"
)

func newLogsPartitioner(partitionKeys []PartitionKeyConfig, settings component.TelemetrySettings) (logsPartitioner, error) {
	sortedKeys := make([]PartitionKeyConfig, len(partitionKeys))
	copy(sortedKeys, partitionKeys)
	sort.Slice(sortedKeys, func(i, j int) bool { return sortedKeys[i].Name < sortedKeys[j].Name })

	expressions := make([]string, len(sortedKeys))
	for i, k := range sortedKeys {
		expressions[i] = k.Value
	}

	resourceParser, err := ottlresource.NewParser(
		ottlfuncs.StandardConverters[*ottlresource.TransformContext](),
		settings,
		ottlresource.EnablePathContextNames(),
	)
	if err != nil {
		return nil, err
	}

	scopeParser, err := ottlscope.NewParser(
		ottlfuncs.StandardConverters[*ottlscope.TransformContext](),
		settings,
		ottlscope.EnablePathContextNames(),
	)
	if err != nil {
		return nil, err
	}

	logParser, err := ottllog.NewParser(
		ottlfuncs.StandardConverters[*ottllog.TransformContext](),
		settings,
		ottllog.EnablePathContextNames(),
	)
	if err != nil {
		return nil, err
	}

	pc, err := ottl.NewParserCollection[logsPartitioner](
		settings,
		ottl.WithParserCollectionContext[*ottlresource.TransformContext, logsPartitioner](
			ottlresource.ContextName,
			&resourceParser,
			ottl.WithValueExpressionConverter(resourceValueExpressionConverter()),
		),
		ottl.WithParserCollectionContext[*ottlscope.TransformContext, logsPartitioner](
			ottlscope.ContextName,
			&scopeParser,
			ottl.WithValueExpressionConverter(scopeValueExpressionConverter()),
		),
		ottl.WithParserCollectionContext[*ottllog.TransformContext, logsPartitioner](
			ottllog.ContextName,
			&logParser,
			ottl.WithValueExpressionConverter(logValueExpressionConverter()),
		),
	)
	if err != nil {
		return nil, err
	}

	return pc.ParseValueExpressions(ottl.NewValueExpressionsGetter(expressions))
}

func resourceValueExpressionConverter() ottl.ParsedValueExpressionsConverter[*ottlresource.TransformContext, logsPartitioner] {
	return func(
		_ *ottl.ParserCollection[logsPartitioner],
		_ ottl.ValueExpressionsGetter,
		parsed []*ottl.ValueExpression[*ottlresource.TransformContext],
	) (logsPartitioner, error) {
		return &resourceLogsPartitioner{expressions: parsed}, nil
	}
}

func scopeValueExpressionConverter() ottl.ParsedValueExpressionsConverter[*ottlscope.TransformContext, logsPartitioner] {
	return func(
		_ *ottl.ParserCollection[logsPartitioner],
		_ ottl.ValueExpressionsGetter,
		parsed []*ottl.ValueExpression[*ottlscope.TransformContext],
	) (logsPartitioner, error) {
		return &scopeLogsPartitioner{expressions: parsed}, nil
	}
}

func logValueExpressionConverter() ottl.ParsedValueExpressionsConverter[*ottllog.TransformContext, logsPartitioner] {
	return func(
		_ *ottl.ParserCollection[logsPartitioner],
		_ ottl.ValueExpressionsGetter,
		parsed []*ottl.ValueExpression[*ottllog.TransformContext],
	) (logsPartitioner, error) {
		return &logRecordPartitioner{expressions: parsed}, nil
	}
}
