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

package attr // import "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmreceiver/internal"

// These constants hold attribute names that are defined by the Elastic APM data model and do not match
// any SemConv attribute. These fields are not used by the UI, and store information related to a specific span type
const (
	SpanDBLink                  = "span.db.link"
	SpanDBRowsAffected          = "span.db.rows_affected"
	SpanDBUserName              = "span.db.user_name"
	HTTPRequestBody             = "http.request.body"
	HTTPRequestID               = "http.request.id"
	HTTPRequestReferrer         = "http.request.referrer"
	HTTPResponseDecodedBodySize = "http.response.decoded_body_size"
	HTTPResponseEncodedBodySize = "http.response.encoded_body_size"
	HTTPResponseTransferSize    = "http.response.transfer_size"
	SpanMessageBody             = "span.message.body"
)
