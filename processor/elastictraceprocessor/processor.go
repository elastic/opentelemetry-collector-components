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

package elastictraceprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/elastictraceprocessor"

import (
	"context"
	"github.com/elastic/opentelemetry-lib/enrichments/trace"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	semconv26 "go.opentelemetry.io/collector/semconv/v1.26.0"
	semconv "go.opentelemetry.io/collector/semconv/v1.27.0"
	"go.uber.org/zap"
	"strings"
	"time"
)

var _ processor.Traces = (*Processor)(nil)

type Processor struct {
	component.StartFunc
	component.ShutdownFunc

	next     consumer.Traces
	enricher *trace.Enricher
	logger   *zap.Logger
}

func newProcessor(cfg *Config, next consumer.Traces, logger *zap.Logger) *Processor {
	return &Processor{
		next:     next,
		logger:   logger,
		enricher: trace.NewEnricher(cfg.Config),
	}
}

func (p *Processor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *Processor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {

	resourceSpans := td.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		resourceSpan := resourceSpans.At(i)
		resource := resourceSpan.Resource()
		translateResourceMetadata(resource)
		attributes := resource.Attributes()

		serviceName, _ := attributes.Get("service.name")

		dataStreamType := "logs" // Default to traces; adjust based on logic

		attributes.PutStr("data_stream.type", dataStreamType)
		attributes.PutStr("data_stream.dataset", "apm."+serviceName.AsString())
		attributes.PutStr("data_stream.namespace", "default") // Use a default or configurable namespace

		scopeSpans := resourceSpan.ScopeSpans()

		for j := 0; j < scopeSpans.Len(); j++ {
			scopeSpan := scopeSpans.At(j)
			spans := scopeSpan.Spans()

			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				attributes := span.Attributes()

				// Add "@timestamp" attribute
				timestamp := span.StartTimestamp().AsTime().Format(time.RFC3339Nano)
				attributes.PutStr("timestamp", timestamp)
			}
		}
	}
	p.enricher.Enrich(td)
	ecsCtx := client.NewContext(ctx, withMappingMode(client.FromContext(ctx), "ecs"))
	return p.next.ConsumeTraces(ecsCtx, td)
}

func withMappingMode(info client.Info, mode string) client.Info {
	return client.Info{
		Addr:     info.Addr,
		Auth:     info.Auth,
		Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {mode}}),
	}
}

func translateResourceMetadata(resource pcommon.Resource) {
	attributes := resource.Attributes()

	attributes.Range(func(k string, v pcommon.Value) bool {
		if !isSupportedAttribute(k) {
			attributes.PutStr("labels."+replaceDots(k), v.AsString())
			attributes.Remove(k)
		}
		return true
	})
}

// Helper function to replace dots in attribute keys
func replaceDots(key string) string {
	return strings.ReplaceAll(key, ".", "_")
}

func isSupportedAttribute(attr string) bool {
	switch attr {
	// service.*
	case semconv.AttributeServiceName,
		semconv.AttributeServiceVersion,
		semconv.AttributeServiceInstanceID,
		semconv.AttributeServiceNamespace:
		return true

	// deployment.*
	case semconv26.AttributeDeploymentEnvironment, semconv.AttributeDeploymentEnvironmentName:
		return true

	// telemetry.sdk.*
	case semconv.AttributeTelemetrySDKName,
		semconv.AttributeTelemetrySDKVersion,
		semconv.AttributeTelemetrySDKLanguage:
		return true

	// cloud.*
	case semconv.AttributeCloudProvider,
		semconv.AttributeCloudAccountID,
		semconv.AttributeCloudRegion,
		semconv.AttributeCloudAvailabilityZone,
		semconv.AttributeCloudPlatform:
		return true

	// container.*
	case semconv.AttributeContainerName,
		semconv.AttributeContainerID,
		semconv.AttributeContainerImageName,
		"container.image.tag",
		"container.runtime":
		return true

	// k8s.*
	case semconv.AttributeK8SNamespaceName,
		semconv.AttributeK8SNodeName,
		semconv.AttributeK8SPodName,
		semconv.AttributeK8SPodUID:
		return true

	// host.*
	case semconv.AttributeHostName,
		semconv.AttributeHostID,
		semconv.AttributeHostType,
		"host.arch",
		semconv.AttributeHostIP:
		return true

	// process.*
	case semconv.AttributeProcessPID,
		semconv.AttributeProcessCommandLine,
		semconv.AttributeProcessExecutablePath,
		"process.runtime.name",
		"process.runtime.version",
		semconv.AttributeProcessOwner:
		return true

	// os.*
	case semconv.AttributeOSType,
		semconv.AttributeOSDescription,
		semconv.AttributeOSName,
		semconv.AttributeOSVersion:
		return true

	// device.*
	case semconv.AttributeDeviceID,
		semconv.AttributeDeviceModelIdentifier,
		semconv.AttributeDeviceModelName,
		"device.manufacturer":
		return true

	// data_stream.*
	case "data_stream.dataset",
		"data_stream.namespace":
		return true

	// Legacy OpenCensus attributes
	case "opencensus.exporterversion":
		return true
	}

	return false
}
