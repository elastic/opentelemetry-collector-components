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

package routing_test

import (
	"testing"

	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/routing"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestDataStremaEncoderDefault(t *testing.T) {
	resource := pcommon.NewResource()
	routing.EncodeDataStream(resource, "logs", false)

	attributes := resource.Attributes()

	dataStreamType, ok := attributes.Get("data_stream.type")
	assert.True(t, ok)
	assert.Equal(t, "logs", dataStreamType.Str())

	dataStreamDataset, ok := attributes.Get("data_stream.dataset")
	assert.True(t, ok)
	assert.Equal(t, "apm", dataStreamDataset.Str())

	dataStreamNamespace, ok := attributes.Get("data_stream.namespace")
	assert.True(t, ok)
	assert.Equal(t, "default", dataStreamNamespace.Str())
}

func TestDataStreamEncoderWithServiceName(t *testing.T) {
	resource := pcommon.NewResource()
	attributes := resource.Attributes()
	attributes.PutStr("service.name", "my-service")

	routing.EncodeDataStream(resource, "metrics", true)

	dataStreamType, ok := attributes.Get("data_stream.type")
	assert.True(t, ok)
	assert.Equal(t, "metrics", dataStreamType.Str())

	dataStreamDataset, ok := attributes.Get("data_stream.dataset")
	assert.True(t, ok)
	assert.Equal(t, "apm.app.my_service", dataStreamDataset.Str())

	dataStreamNamespace, ok := attributes.Get("data_stream.namespace")
	assert.True(t, ok)
	assert.Equal(t, "default", dataStreamNamespace.Str())
}
