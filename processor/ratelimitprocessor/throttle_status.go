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

package ratelimitprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor"

import (
	"sync"

	"go.opentelemetry.io/collector/component"
)

// Consecutive requests with tokensBefore < 0 required to flip to RecoverableError.
const throttledThreshold = 5

// Consecutive healthy requests (tokensBefore >= 0) required to flip back to OK.
const recoveredThreshold = 3

// throttleStatusReporter debounces the tokensBefore < 0 signal to avoid
// noise from transient batch-induced spikes, and reports component status
// transitions via the OTel Collector componentstatus API.
//
// A nil *throttleStatusReporter is safe to use — observe is a no-op.
type throttleStatusReporter struct {
	mu            sync.Mutex
	host          component.Host
	throttleCount int
	recoverCount  int
	isThrottled   bool
}

func newThrottleStatusReporter() *throttleStatusReporter {
	return &throttleStatusReporter{}
}

func (r *throttleStatusReporter) setHost(host component.Host) {
	r.mu.Lock()
	r.host = host
	r.mu.Unlock()
}

// observe records one request's pre-consumption token state, updates the
// debounced throttle state, reports component status on transitions, and
// returns the current debounced state (true = throttled).
// tokensBeforeNegative should be true when result.TokensBefore < 0.
func (r *throttleStatusReporter) observe(tokensBeforeNegative bool) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if tokensBeforeNegative {
		r.throttleCount++
		r.recoverCount = 0
		if !r.isThrottled && r.throttleCount >= throttledThreshold {
			r.isThrottled = true
		}
	} else {
		r.recoverCount++
		r.throttleCount = 0
		if r.isThrottled && r.recoverCount >= recoveredThreshold {
			r.isThrottled = false
		}
	}

	return r.isThrottled
}
