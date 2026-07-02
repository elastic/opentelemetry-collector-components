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

package cachebench // import "github.com/elastic/opentelemetry-collector-components/internal/cachebench"

import (
	"fmt"
	"math/rand/v2"
)

// Workload models a stream of auth requests over a fixed set of source keys.
// Under the one-key-per-source constraint, KeyspaceSize equals the number of
// active sources, which is the quantity that dwarfs any per-pod cache.
type Workload struct {
	// KeyspaceSize is the number of distinct source keys (active sources).
	KeyspaceSize int
	// Zipf skews access toward a hot subset of sources. s>1 concentrates load;
	// s==0 selects a uniform distribution instead.
	ZipfS float64
	keys  []string
	zipf  *rand.Zipf
	rng   *rand.Rand
}

// NewWorkload precomputes the key set and access distribution. seed makes runs
// reproducible so backends are compared on identical request streams.
func NewWorkload(keyspaceSize int, zipfS float64, seed uint64) *Workload {
	rng := rand.New(rand.NewPCG(seed, seed*2+1))
	keys := make([]string, keyspaceSize)
	for i := range keys {
		// Realistic source-key shape: "<source>-<id>-<deployment>".
		keys[i] = fmt.Sprintf("vercel-%08d-dep%06d", i, i%5000)
	}
	w := &Workload{KeyspaceSize: keyspaceSize, ZipfS: zipfS, keys: keys, rng: rng}
	if zipfS > 0 {
		// imax is keyspaceSize-1; v=1 is the standard Zipf offset.
		w.zipf = rand.NewZipf(rng, zipfS, 1, uint64(keyspaceSize-1))
	}
	return w
}

// Next returns the next requested key according to the distribution.
func (w *Workload) Next() string {
	if w.zipf != nil {
		return w.keys[w.zipf.Uint64()]
	}
	return w.keys[w.rng.IntN(w.KeyspaceSize)]
}

// KeyAt returns the i-th key, for deterministic preload loops.
func (w *Workload) KeyAt(i int) string { return w.keys[i%w.KeyspaceSize] }

// makeEntry builds a representative authorized entry for a key.
func makeEntry(key string) *Entry {
	e := &Entry{Authorized: true, Username: "svc-" + key[:min(12, len(key))]}
	for i := range e.DerivedKey {
		e.DerivedKey[i] = key[i%len(key)]
	}
	return e
}
