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

package ecs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestTranslateOTLPLogAttributesForMIS(t *testing.T) {
	attrs := pcommon.NewMap()
	attrs.PutStr("http.method", "GET")
	attrs.PutInt("http.status_code", 200)
	attrs.PutInt("http.response_content_length", 1024)
	attrs.PutBool("request.succeeded", true)
	attrs.PutStr("event.name", "device.crash")
	attrs.PutStr("exception.message", "something failed")
	attrs.PutStr("labels.existing_label", "existing")
	attrs.PutDouble("numeric_labels.existing_numeric", 42)
	attrs.PutStr("data_stream.dataset", "apm.app.my_service_logs")

	mapValue := attrs.PutEmptyMap("http.request")
	mapValue.PutStr("id", "req-1")

	intSlice := attrs.PutEmptySlice("http.codes")
	intSlice.AppendEmpty().SetInt(200)
	intSlice.AppendEmpty().SetInt(201)

	TranslateOTLPLogAttributesForMIS(attrs)

	httpMethod, _ := attrs.Get("labels.http_method")
	assert.Equal(t, "GET", httpMethod.Str())
	httpStatusCode, _ := attrs.Get("numeric_labels.http_status_code")
	assert.Equal(t, float64(200), httpStatusCode.Double())
	httpResponseLength, _ := attrs.Get("numeric_labels.http_response_content_length")
	assert.Equal(t, float64(1024), httpResponseLength.Double())
	requestSucceeded, _ := attrs.Get("labels.request_succeeded")
	assert.Equal(t, "true", requestSucceeded.Str())

	codes, _ := attrs.Get("numeric_labels.http_codes")
	assert.Equal(t, []any{float64(200), float64(201)}, codes.Slice().AsRaw())

	_, hasHTTPMethod := attrs.Get("http.method")
	assert.False(t, hasHTTPMethod)
	_, hasHTTPStatusCode := attrs.Get("http.status_code")
	assert.False(t, hasHTTPStatusCode)
	_, hasHTTPResponseLength := attrs.Get("http.response_content_length")
	assert.False(t, hasHTTPResponseLength)

	_, hasMapAttribute := attrs.Get("http.request")
	assert.False(t, hasMapAttribute)

	eventName, _ := attrs.Get("event.name")
	assert.Equal(t, "device.crash", eventName.Str())
	exceptionMessage, _ := attrs.Get("exception.message")
	assert.Equal(t, "something failed", exceptionMessage.Str())

	existingLabel, _ := attrs.Get("labels.existing_label")
	assert.Equal(t, "existing", existingLabel.Str())
	existingNumeric, _ := attrs.Get("numeric_labels.existing_numeric")
	assert.Equal(t, float64(42), existingNumeric.Double())

	dataStreamDataset, _ := attrs.Get("data_stream.dataset")
	assert.Equal(t, "apm.app.my_service_logs", dataStreamDataset.Str())
}
