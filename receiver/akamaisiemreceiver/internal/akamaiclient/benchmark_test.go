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

package akamaiclient

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

const benchEvent = `{"attackData":{"appliedAction":"tarpit","clientIP":"198.51.100.1","configId":"67217","policyId":"PNWD_110088"},"httpMessage":{"bytes":"0","host":"example.com","method":"GET","path":"/api/test","port":"443","protocol":"HTTP/1.1","query":"q=test","requestId":"f3fe4c34","start":"1762365006","status":"200","tls":"tls1.3"},"type":"akamai_siem","version":"1.0"}`

func BenchmarkStreamEvents_10(b *testing.B)     { benchStreamEvents(b, 10) }
func BenchmarkStreamEvents_100(b *testing.B)    { benchStreamEvents(b, 100) }
func BenchmarkStreamEvents_1000(b *testing.B)   { benchStreamEvents(b, 1000) }
func BenchmarkStreamEvents_10000(b *testing.B)  { benchStreamEvents(b, 10000) }
func BenchmarkStreamEvents_100000(b *testing.B) { benchStreamEvents(b, 100000) }

func benchStreamEvents(b *testing.B, n int) {
	b.Helper()
	body := buildBody(n)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		reader := strings.NewReader(body)
		ch := make(chan string, 100)
		var count int
		go func() {
			defer close(ch)
			_, count, _ = StreamEvents(ctx, reader, ch)
		}()
		// Drain channel.
		received := 0
		for range ch {
			received++
		}
		if received != n || count != n {
			b.Fatalf("expected %d events, got received=%d count=%d", n, received, count)
		}
	}
	b.ReportMetric(float64(n), "events/op")
}

func buildBody(n int) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		sb.WriteString(benchEvent)
		sb.WriteString("\n")
	}
	sb.WriteString(fmt.Sprintf(`{"offset":"bench-cursor","total":%d,"limit":%d}`, n, n+1))
	sb.WriteString("\n")
	return sb.String()
}
