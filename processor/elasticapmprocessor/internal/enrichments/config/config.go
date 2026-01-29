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

package config

// Config configures the enrichment attributes produced.
type Config struct {
	Resource    ResourceConfig           `mapstructure:"resource"`
	Scope       ScopeConfig              `mapstructure:"scope"`
	Transaction ElasticTransactionConfig `mapstructure:"elastic_transaction"`
	Span        ElasticSpanConfig        `mapstructure:"elastic_span"`
	SpanEvent   SpanEventConfig          `mapstructure:"span_event"`
	Log         ElasticLogConfig         `mapstructure:"elastic_log"`
	Metric      ElasticMetricConfig      `mapstructure:"elastic_metric"`
}

// ResourceConfig configures the enrichment of resource attributes.
type ResourceConfig struct {
	AgentName             AttributeConfig `mapstructure:"agent_name"`
	AgentVersion          AttributeConfig `mapstructure:"agent_version"`
	OverrideHostName      AttributeConfig `mapstructure:"override_host_name"`
	DeploymentEnvironment AttributeConfig `mapstructure:"deployment_environment"`
	ServiceInstanceID     AttributeConfig `mapstructure:"service_instance_id"`
}

// ScopeConfig configures the enrichment of scope attributes.
type ScopeConfig struct {
	ServiceFrameworkName    AttributeConfig `mapstructure:"service_framework_name"`
	ServiceFrameworkVersion AttributeConfig `mapstructure:"service_framework_version"`
}

// ElasticTransactionConfig configures the enrichment attributes for the
// spans which are identified as elastic transaction.
type ElasticTransactionConfig struct {
	// TimestampUs is a temporary attribute to enable higher
	// resolution timestamps in Elasticsearch. For more details see:
	// https://github.com/elastic/opentelemetry-dev/issues/374.
	TimestampUs AttributeConfig `mapstructure:"timestamp_us"`
	Sampled     AttributeConfig `mapstructure:"sampled"`
	ID          AttributeConfig `mapstructure:"id"`
	// ClearSpanID sets the span ID to an empty value so that the
	// ID is only represented by the `transaction.id` attribute.
	// Applicable only when ID is enabled.
	// Disabled by default.
	ClearSpanID AttributeConfig `mapstructure:"clear_span_id"`
	Root        AttributeConfig `mapstructure:"root"`
	Name        AttributeConfig `mapstructure:"name"`
	// ClearSpanName sets the span name to an empty value so that the
	// name is only represented by the `transaction.name` attribute.
	// Applicable only when Name is enabled.
	// Disabled by default.
	ClearSpanName       AttributeConfig `mapstructure:"clear_span_name"`
	ProcessorEvent      AttributeConfig `mapstructure:"processor_event"`
	RepresentativeCount AttributeConfig `mapstructure:"representative_count"`
	DurationUs          AttributeConfig `mapstructure:"duration_us"`
	Type                AttributeConfig `mapstructure:"type"`
	Result              AttributeConfig `mapstructure:"result"`
	EventOutcome        AttributeConfig `mapstructure:"event_outcome"`
	InferredSpans       AttributeConfig `mapstructure:"inferred_spans"`
	UserAgent           AttributeConfig `mapstructure:"user_agent"`
	RemoveMessaging     AttributeConfig `mapstructure:"remove_messaging"`
	MessageQueueName    AttributeConfig `mapstructure:"message_queue_name"`
}

// ElasticSpanConfig configures the enrichment attributes for the spans
// which are NOT identified as elastic transaction.
type ElasticSpanConfig struct {
	// TimestampUs is a temporary attribute to enable higher
	// resolution timestamps in Elasticsearch. For more details see:
	// https://github.com/elastic/opentelemetry-dev/issues/374.
	TimestampUs         AttributeConfig `mapstructure:"timestamp_us"`
	ProcessorEvent      AttributeConfig `mapstructure:"processor_event"`
	RepresentativeCount AttributeConfig `mapstructure:"representative_count"`
	Action              AttributeConfig `mapstructure:"action"`
	TypeSubtype         AttributeConfig `mapstructure:"type_subtype"`
	DurationUs          AttributeConfig `mapstructure:"duration_us"`
	EventOutcome        AttributeConfig `mapstructure:"event_outcome"`
	ServiceTarget       AttributeConfig `mapstructure:"service_target"`
	DestinationService  AttributeConfig `mapstructure:"destination_service"`
	InferredSpans       AttributeConfig `mapstructure:"inferred_spans"`
	UserAgent           AttributeConfig `mapstructure:"user_agent"`
	RemoveMessaging     AttributeConfig `mapstructure:"remove_messaging"`
	MessageQueueName    AttributeConfig `mapstructure:"message_queue_name"`
}

// SpanEventConfig configures enrichment attributes for the span events.
type SpanEventConfig struct {
	// TimestampUs is a temporary attribute to enable higher
	// resolution timestamps in Elasticsearch. For more details see:
	// https://github.com/elastic/opentelemetry-dev/issues/374.
	TimestampUs        AttributeConfig `mapstructure:"timestamp_us"`
	TransactionSampled AttributeConfig `mapstructure:"transaction_sampled"`
	TransactionType    AttributeConfig `mapstructure:"transaction_type"`
	ProcessorEvent     AttributeConfig `mapstructure:"processor_event"`

	// For exceptions/errors
	ErrorID               AttributeConfig `mapstructure:"error_id"`
	ErrorExceptionHandled AttributeConfig `mapstructure:"error_exception_handled"`
	ErrorGroupingKey      AttributeConfig `mapstructure:"error_grouping_key"`
	ErrorGroupingName     AttributeConfig `mapstructure:"error_grouping_name"`
}

// ElasticLogConfig configures the enrichment attributes for logs
type ElasticLogConfig struct {
	ProcessorEvent AttributeConfig `mapstructure:"processor_event"`
}

// ElasticMetricConfig configures the enrichment attributes for metrics
type ElasticMetricConfig struct {
	ProcessorEvent AttributeConfig `mapstructure:"processor_event"`
}

// AttributeConfig is the configuration options for each attribute.
type AttributeConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

// Enabled returns a config with all default enrichments enabled.
func Enabled() Config {
	return Config{
		Resource: ResourceConfig{
			AgentName:             AttributeConfig{Enabled: true},
			AgentVersion:          AttributeConfig{Enabled: true},
			OverrideHostName:      AttributeConfig{Enabled: true},
			DeploymentEnvironment: AttributeConfig{Enabled: true},
			ServiceInstanceID:     AttributeConfig{Enabled: true},
		},
		Scope: ScopeConfig{
			ServiceFrameworkName:    AttributeConfig{Enabled: true},
			ServiceFrameworkVersion: AttributeConfig{Enabled: true},
		},
		Transaction: ElasticTransactionConfig{
			TimestampUs:         AttributeConfig{Enabled: true},
			Sampled:             AttributeConfig{Enabled: true},
			ID:                  AttributeConfig{Enabled: true},
			Root:                AttributeConfig{Enabled: true},
			Name:                AttributeConfig{Enabled: true},
			ProcessorEvent:      AttributeConfig{Enabled: true},
			DurationUs:          AttributeConfig{Enabled: true},
			Type:                AttributeConfig{Enabled: true},
			Result:              AttributeConfig{Enabled: true},
			EventOutcome:        AttributeConfig{Enabled: true},
			RepresentativeCount: AttributeConfig{Enabled: true},
			InferredSpans:       AttributeConfig{Enabled: true},
			UserAgent:           AttributeConfig{Enabled: true},
			RemoveMessaging:     AttributeConfig{Enabled: true},
			MessageQueueName:    AttributeConfig{Enabled: true},
		},
		Span: ElasticSpanConfig{
			TimestampUs:         AttributeConfig{Enabled: true},
			ProcessorEvent:      AttributeConfig{Enabled: true},
			Action:              AttributeConfig{Enabled: true},
			TypeSubtype:         AttributeConfig{Enabled: true},
			DurationUs:          AttributeConfig{Enabled: true},
			EventOutcome:        AttributeConfig{Enabled: true},
			ServiceTarget:       AttributeConfig{Enabled: true},
			DestinationService:  AttributeConfig{Enabled: true},
			RepresentativeCount: AttributeConfig{Enabled: true},
			InferredSpans:       AttributeConfig{Enabled: true},
			UserAgent:           AttributeConfig{Enabled: true},
			RemoveMessaging:     AttributeConfig{Enabled: true},
			MessageQueueName:    AttributeConfig{Enabled: true},
		},
		SpanEvent: SpanEventConfig{
			TimestampUs:           AttributeConfig{Enabled: true},
			TransactionSampled:    AttributeConfig{Enabled: true},
			TransactionType:       AttributeConfig{Enabled: true},
			ProcessorEvent:        AttributeConfig{Enabled: true},
			ErrorID:               AttributeConfig{Enabled: true},
			ErrorExceptionHandled: AttributeConfig{Enabled: true},
			ErrorGroupingKey:      AttributeConfig{Enabled: true},
			ErrorGroupingName:     AttributeConfig{Enabled: true},
		},
	}
}
