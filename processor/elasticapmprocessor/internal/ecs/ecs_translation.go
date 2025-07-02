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

package ecs // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/ecs"

import (
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv26 "go.opentelemetry.io/collector/semconv/v1.26.0"
	semconv "go.opentelemetry.io/collector/semconv/v1.27.0"
)

func TranslateResourceMetadata(resource pcommon.Resource) {
	attributes := resource.Attributes()

	attributes.Range(func(k string, v pcommon.Value) bool {
		if !isSupportedAttribute(k) {
			attributes.PutStr("labels."+replaceDots(k), v.AsString())
			attributes.Remove(k)
		}
		return true
	})
}

func replaceDots(key string) string {
	return strings.ReplaceAll(key, ".", "_")
}

// This is based to what found in the apm-data repo
// (e.g https://github.com/elastic/apm-data/blob/main/input/otlp/metadata.go)
// plus other extra fields
func isSupportedAttribute(attr string) bool {
	switch attr {
	// service.*
	case semconv.AttributeServiceName,
		semconv.AttributeServiceVersion,
		semconv.AttributeServiceInstanceID,
		semconv.AttributeServiceNamespace,
		"service.language.name",
		"service.language.version":
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

	// APM Agent enrichment
	case "agent.name",
		"agent.version":
		return true
	}

	return false
}
