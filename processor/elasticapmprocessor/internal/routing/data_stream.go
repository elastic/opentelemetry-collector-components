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

package routing // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/routing"

import "go.opentelemetry.io/collector/pdata/pcommon"

// DataStreamType tracks the text associated with a data stream type.
const (
	DataStreamTypeLogs    = "logs"
	DataStreamTypeMetrics = "metrics"
	DataStreamTypeTraces  = "traces"

	ServiceNameAttributeKey = "service.name"

	NamespaceDefault = "default"
)

func EncodeDataStream(resource pcommon.Resource, dataStreamType string, serviceNameInDataset bool) {
	if serviceNameInDataset {
		encodeDataStreamWithServiceName(resource, dataStreamType)
	} else {
		encodeDataStreamDefault(resource, dataStreamType)
	}
}

func encodeDataStreamDefault(resource pcommon.Resource, dataStreamType string) {
	attributes := resource.Attributes()

	attributes.PutStr("data_stream.type", dataStreamType)
	attributes.PutStr("data_stream.dataset", "apm")
	attributes.PutStr("data_stream.namespace", NamespaceDefault) //TODO: make this configurable
}

func encodeDataStreamWithServiceName(resource pcommon.Resource, dataStreamType string) {
	attributes := resource.Attributes()

	serviceName, ok := attributes.Get(ServiceNameAttributeKey)
	if !ok || serviceName.Str() == "" {
		serviceName = pcommon.NewValueStr("unknown_service")
	}

	attributes.PutStr("data_stream.type", dataStreamType)
	attributes.PutStr("data_stream.dataset", "apm.app."+serviceName.Str())
	attributes.PutStr("data_stream.namespace", NamespaceDefault) //TODO: make this configurable
}
