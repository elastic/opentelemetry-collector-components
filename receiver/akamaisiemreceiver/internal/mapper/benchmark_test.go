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

package mapper

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
)

// BenchmarkMapToOTelLog_FullEvent benchmarks mapping a realistic full event
// with all field types: HTTP, attack, geo, bot, client, user risk, identity.
func BenchmarkMapToOTelLog_FullEvent(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lr := plog.NewLogRecord()
		if err := MapToOTelLog(testEventFull, lr); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMapToOTelLog_MinimalEvent benchmarks mapping a minimal event
// with only HTTP fields (no attack data, no geo, no bot).
func BenchmarkMapToOTelLog_MinimalEvent(b *testing.B) {
	event := `{"httpMessage":{"start":"1762365006","host":"test.com","method":"GET","status":"200","path":"/","protocol":"HTTP/1.1","tls":"tls1.3"}}`
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lr := plog.NewLogRecord()
		if err := MapToOTelLog(event, lr); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeRuleField benchmarks the URL-decode → split → base64-decode chain.
func BenchmarkDecodeRuleField(b *testing.B) {
	// Realistic encoded rule field with 6 rules.
	input := "MzkwNDAwNg%3d%3d%3bMzkwNDAwNw%3d%3d%3bMzkwNDAyMA%3d%3d%3bMzkwNDA1Mg%3d%3d%3bMzkwNDA1Mw%3d%3d%3bQk9ULUJST1dTRVItSU1QRVJTT05BVE9S"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = decodeRuleField(input)
	}
}
