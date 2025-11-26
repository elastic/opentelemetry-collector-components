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

package elasticapmprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor"

import "github.com/elastic/opentelemetry-lib/enrichments/config"

type Config struct {
	config.Config `mapstructure:",squash"`

	// SkipEnrichment controls whether enrichment should be skipped for logs and metrics
	// when the mapping mode is not "ecs". When true, logs and metrics are only enriched when
	// the x-elastic-mapping-mode metadata is set to "ecs". Traces are always enriched regardless
	// of this setting. Defaults to false for backwards compatibility (always enrich).
	SkipEnrichment bool `mapstructure:"skip_enrichment"`
}
