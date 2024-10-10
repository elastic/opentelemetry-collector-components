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

package model // import "github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/internal/model"

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/collector/semconv/v1.26.0"
)

// CollectorInstanceInfo holds the attributes that could uniquely identify
// the current collector instance. These attributes are initialized from the
// telemetry settings. The CollectorInstanceInfo can copy these attributes,
// with a given prefix, to a provided map.
type CollectorInstanceInfo struct {
	prefix string

	size              int
	serviceInstanceID string
	serviceName       string
	serviceNamespace  string
}

func NewCollectorInstanceInfo(
	prefix string,
	set component.TelemetrySettings,
) *CollectorInstanceInfo {
	info := CollectorInstanceInfo{prefix: prefix}
	set.Resource.Attributes().Range(func(k string, v pcommon.Value) bool {
		switch k {
		case semconv.AttributeServiceInstanceID:
			if str := v.Str(); str != "" {
				info.serviceInstanceID = str
				info.size++
			}
		case semconv.AttributeServiceName:
			if str := v.Str(); str != "" {
				info.serviceName = str
				info.size++
			}
		case semconv.AttributeServiceNamespace:
			if str := v.Str(); str != "" {
				info.serviceNamespace = str
				info.size++
			}
		}
		return true
	})
	return &info
}

// Size returns the max number of attributes that defines a collector's
// instance information. Can be used to presize the attributes.
func (info CollectorInstanceInfo) Size() int {
	return info.size
}

func (info CollectorInstanceInfo) Copy(to pcommon.Map) {
	to.EnsureCapacity(info.Size())
	if info.serviceInstanceID != "" {
		to.PutStr(keyWithPrefix(info.prefix, semconv.AttributeServiceInstanceID), info.serviceInstanceID)
	}
	if info.serviceName != "" {
		to.PutStr(keyWithPrefix(info.prefix, semconv.AttributeServiceName), info.serviceName)
	}
	if info.serviceNamespace != "" {
		to.PutStr(keyWithPrefix(info.prefix, semconv.AttributeServiceNamespace), info.serviceNamespace)
	}
}

func keyWithPrefix(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}
