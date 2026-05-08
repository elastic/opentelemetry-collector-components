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

package elasticapmintakereceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver"

import (
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/elastic/apm-data/model/modelpb"
	"github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal/mappers"
)

// signalGroups caches the ScopeSpans / ScopeLogs already created during a
// single processBatch call, keyed by a hash of the resource-affecting fields
// of the source event. Trace and log events with matching fingerprints are
// appended under the same ResourceSpans / ResourceLogs, which removes the
// per-event cost of repopulating the resource attribute map for the common
// case where every event in a batch comes from the same agent metadata.
//
// Metric events are intentionally excluded: each metricset writes a
// metricset-name-dependent resource attribute, and individual samples become
// distinct Metrics under a ScopeMetrics, so collapsing per-event would risk
// producing duplicate metric names within a single scope.
type signalGroups struct {
	traces []traceGroup
	logs   []logGroup
}

type traceGroup struct {
	fp uint64
	ss ptrace.ScopeSpans
}

type logGroup struct {
	fp uint64
	sl plog.ScopeLogs
}

// traceScope returns a cached ScopeSpans for the given resource fingerprint
// or false if none exists. Linear search is intentional: typical batches
// have a single resource, so the first slot is the only hot path.
func (g *signalGroups) traceScope(fp uint64) (ptrace.ScopeSpans, bool) {
	for i := range g.traces {
		if g.traces[i].fp == fp {
			return g.traces[i].ss, true
		}
	}
	return ptrace.ScopeSpans{}, false
}

func (g *signalGroups) recordTraceScope(fp uint64, ss ptrace.ScopeSpans) {
	g.traces = append(g.traces, traceGroup{fp: fp, ss: ss})
}

func (g *signalGroups) logScope(fp uint64) (plog.ScopeLogs, bool) {
	for i := range g.logs {
		if g.logs[i].fp == fp {
			return g.logs[i].sl, true
		}
	}
	return plog.ScopeLogs{}, false
}

func (g *signalGroups) recordLogScope(fp uint64, sl plog.ScopeLogs) {
	g.logs = append(g.logs, logGroup{fp: fp, sl: sl})
}

// fpKVSep separates a key from a value within a single attribute write.
// fpEntrySep separates one attribute write from the next.
//
// Including the key in the hash makes write order irrelevant for fields
// the visitor sees as Put*(key, value) — re-ordering the walker visits
// would produce the same fingerprint for the same set of attributes.
var (
	fpKVSep    = []byte{0x00}
	fpEntrySep = []byte{0x01}
)

// resourceFingerprint computes a stable hash of the resource attributes
// that setResourceAttributes would write for event. It is implemented as
// a visitor over mappers.WalkResourceAttributes — the *same* walker the
// pcommon path uses — so the fingerprint and the actual resource map are
// guaranteed to remain in lockstep when fields are added or removed.
//
// h is reused across calls; the caller must not assume any prior state.
func resourceFingerprint(event *modelpb.APMEvent, h *xxhash.Digest) uint64 {
	h.Reset()
	mappers.WalkResourceAttributes(event, hashResourceVisitor{h: h})
	return h.Sum64()
}

// hashResourceVisitor adapts mappers.ResourceAttrVisitor onto an
// xxhash.Digest. Each Put* call writes (key + sep + canonicalised value
// + entry-sep) into the digest.
type hashResourceVisitor struct {
	h *xxhash.Digest
}

func (v hashResourceVisitor) PutStr(key, value string) {
	_, _ = v.h.WriteString(key)
	_, _ = v.h.Write(fpKVSep)
	_, _ = v.h.WriteString(value)
	_, _ = v.h.Write(fpEntrySep)
}

func (v hashResourceVisitor) PutInt(key string, value int64) {
	_, _ = v.h.WriteString(key)
	_, _ = v.h.Write(fpKVSep)
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(value))
	_, _ = v.h.Write(buf[:])
	_, _ = v.h.Write(fpEntrySep)
}

func (v hashResourceVisitor) PutBool(key string, value bool) {
	_, _ = v.h.WriteString(key)
	_, _ = v.h.Write(fpKVSep)
	if value {
		_, _ = v.h.Write([]byte{1})
	} else {
		_, _ = v.h.Write([]byte{0})
	}
	_, _ = v.h.Write(fpEntrySep)
}

func (v hashResourceVisitor) PutDouble(key string, value float64) {
	_, _ = v.h.WriteString(key)
	_, _ = v.h.Write(fpKVSep)
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(int64(value*1e9))) // canonical-ish; precision loss is acceptable for grouping
	_, _ = v.h.Write(buf[:])
	_, _ = v.h.Write(fpEntrySep)
}

func (v hashResourceVisitor) PutStrSlice(key string, values []string) {
	_, _ = v.h.WriteString(key)
	_, _ = v.h.Write(fpKVSep)
	for _, s := range values {
		_, _ = v.h.WriteString(s)
		_, _ = v.h.Write(fpKVSep)
	}
	_, _ = v.h.Write(fpEntrySep)
}
