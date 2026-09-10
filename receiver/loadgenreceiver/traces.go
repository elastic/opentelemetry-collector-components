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

package loadgenreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver"

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/binary"
	"errors"
	"io"
	"math/rand/v2"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver/internal/list"
)

//go:embed testdata/traces.jsonl
var demoTraces []byte

type tracesGenerator struct {
	cfg    *Config
	logger *zap.Logger

	samples *list.LoopingList[ptrace.Traces]

	// idSeed salts the per-pass trace/span ID rewrite so that separate
	// generator runs replaying the same file also produce distinct IDs.
	idSeed uint64

	stats   Stats
	statsMu sync.Mutex

	consumer consumer.Traces

	cancelFn            context.CancelFunc
	inflightConcurrency sync.WaitGroup
}

func createTracesReceiver(
	ctx context.Context,
	set receiver.Settings,
	config component.Config,
	consumer consumer.Traces,
) (_ receiver.Traces, err error) {
	genConfig := config.(*Config)

	parser := ptrace.JSONUnmarshaler{}
	var sampleTraces io.Reader = bytes.NewReader(demoTraces)

	if genConfig.Traces.Path != "" {
		var rc io.ReadCloser
		rc, err = openJSONLFile(genConfig.Traces.JsonlFile)
		if err != nil {
			return nil, err
		}
		defer func() {
			if closeErr := rc.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
		}()
		sampleTraces = rc
	}

	maxBufferSize := genConfig.Traces.MaxBufferSize
	if maxBufferSize == 0 {
		maxBufferSize = maxScannerBufSize
	}

	var items []ptrace.Traces
	scanner := bufio.NewScanner(sampleTraces)
	scanner.Buffer(make([]byte, 0, maxBufferSize), maxBufferSize)
	for scanner.Scan() {
		traceBytes := scanner.Bytes()
		lineTraces, err := parser.UnmarshalTraces(traceBytes)
		if err != nil {
			return nil, err
		}
		items = append(items, lineTraces)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return &tracesGenerator{
		cfg:      genConfig,
		logger:   set.Logger,
		consumer: consumer,
		samples:  list.NewLoopingList(items, genConfig.Traces.MaxReplay),
		idSeed:   rand.Uint64(),
	}, nil
}

func (ar *tracesGenerator) Start(ctx context.Context, _ component.Host) error {
	startCtx, cancelFn := context.WithCancel(ctx)
	ar.cancelFn = cancelFn

	for i := 0; i < ar.cfg.Concurrency; i++ {
		ar.inflightConcurrency.Add(1)
		go func() {
			defer ar.inflightConcurrency.Done()
			next := ptrace.NewTraces() // per-worker temporary container to avoid allocs
			for {
				select {
				case <-startCtx.Done():
					return
				default:
				}
				if ar.cfg.DisablePdataReuse || next.IsReadOnly() {
					// As the optimization to reuse pdata is not compatible with fanoutconsumer,
					// i.e. in pipelines where there are more than 1 consumer,
					// as fanoutconsumer will mark the pdata struct as read only and cannot be reused.
					// See https://github.com/open-telemetry/opentelemetry-collector/blob/461a3558086a03ab13ea121d12e28e185a1c79b0/internal/fanoutconsumer/logs.go#L70
					next = ptrace.NewTraces()
				}
				err := ar.nextTraces(next)
				if errors.Is(err, list.ErrLoopLimitReached) {
					return
				}
				late, hasLate := ar.splitLateSpans(next)
				// For graceful shutdown, use ctx instead of startCtx to shield Consume* from context canceled
				// In other words, Consume* will finish at its own pace, which may take indefinitely long.
				ar.consume(ctx, next)
				if hasLate {
					ar.inflightConcurrency.Add(1)
					go func() {
						defer ar.inflightConcurrency.Done()
						ls := ar.cfg.Traces.LateSpans
						// Emit only if the delay elapses before shutdown.
						if waitJitter(startCtx, &JitterRange{Min: ls.DelayMin, Max: ls.DelayMax}) {
							ar.consume(ctx, late)
						}
					}()
				}
				if !waitJitter(startCtx, ar.cfg.Traces.Jitter) {
					return
				}
			}
		}()
	}
	go func() {
		ar.inflightConcurrency.Wait()
		if ar.cfg.Traces.doneCh != nil {
			ar.cfg.Traces.doneCh <- ar.stats
		}
	}()
	return nil
}

func (ar *tracesGenerator) Shutdown(context.Context) error {
	if ar.cancelFn != nil {
		ar.cancelFn()
	}
	ar.inflightConcurrency.Wait()
	return nil
}

func (ar *tracesGenerator) nextTraces(next ptrace.Traces) error {
	sample, loop, err := ar.samples.NextLoop()
	if err != nil {
		return err
	}
	sample.CopyTo(next)

	var idSalt [16]byte
	if ar.cfg.Traces.RewriteIDs {
		// The salt is deterministic per (seed, loop pass): spans of one trace
		// remain grouped even across payloads within a pass, while every pass
		// yields globally new IDs. IDs are remapped by hashing salt+original
		// ID rather than XOR: hashing yields uniformly distributed IDs even
		// from low-entropy originals (e.g. counter-based corpus IDs), which
		// hash-based tail samplers such as the probabilistic tail_sampling
		// policy depend on.
		binary.LittleEndian.PutUint64(idSalt[:8], ar.idSeed)
		binary.LittleEndian.PutUint64(idSalt[8:], uint64(loop))
	}

	rm := next.ResourceSpans()
	for i := 0; i < rm.Len(); i++ {
		for j := 0; j < rm.At(i).ScopeSpans().Len(); j++ {
			for k := 0; k < rm.At(i).ScopeSpans().At(j).Spans().Len(); k++ {
				sspan := rm.At(i).ScopeSpans().At(j).Spans().At(k)
				now := time.Now()
				// Set end timestamp to now and maintain the same duration.
				duration := time.Duration(sspan.EndTimestamp() - sspan.StartTimestamp())
				sspan.SetEndTimestamp(pcommon.NewTimestampFromTime(now))
				sspan.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-duration)))

				if ar.cfg.Traces.RewriteIDs {
					sspan.SetTraceID(remapTraceID(sspan.TraceID(), idSalt))
					sspan.SetSpanID(remapSpanID(sspan.SpanID(), idSalt))
					sspan.SetParentSpanID(remapSpanID(sspan.ParentSpanID(), idSalt))
					for l := 0; l < sspan.Links().Len(); l++ {
						link := sspan.Links().At(l)
						link.SetTraceID(remapTraceID(link.TraceID(), idSalt))
						link.SetSpanID(remapSpanID(link.SpanID(), idSalt))
					}
				}
			}
		}
	}

	return nil
}

// remapTraceID remaps a trace ID to SHA-256(salt, id). Empty (all-zero) IDs
// stay empty as they mark the absence of an ID.
func remapTraceID(id pcommon.TraceID, salt [16]byte) pcommon.TraceID {
	if id.IsEmpty() {
		return id
	}
	sum := sha256.Sum256(append(salt[:], id[:]...))
	copy(id[:], sum[:])
	return id
}

// remapSpanID remaps a span ID to SHA-256(salt, id). Empty (all-zero) IDs stay
// empty: an empty parent span ID marks a root span.
func remapSpanID(id pcommon.SpanID, salt [16]byte) pcommon.SpanID {
	if id.IsEmpty() {
		return id
	}
	sum := sha256.Sum256(append(salt[:], id[:]...))
	copy(id[:], sum[:])
	return id
}

// splitLateSpans moves up to cfg.Traces.LateSpans.Spans spans from the tail of
// next into a separately owned payload for delayed emission. It must run after
// nextTraces (so held spans carry the rewritten IDs) and before next is handed
// to the consumer.
func (ar *tracesGenerator) splitLateSpans(next ptrace.Traces) (ptrace.Traces, bool) {
	ls := ar.cfg.Traces.LateSpans
	if ls == nil || rand.Float64() >= ls.Fraction {
		return ptrace.Traces{}, false
	}
	maxHeld := ls.Spans
	if maxHeld <= 0 {
		maxHeld = 1
	}
	if next.SpanCount() <= maxHeld {
		// Holding back the whole payload would just delay it, not make spans
		// late relative to their trace.
		return ptrace.Traces{}, false
	}

	late := ptrace.NewTraces()
	held := 0
	rm := next.ResourceSpans()
	for i := rm.Len() - 1; i >= 0 && held < maxHeld; i-- {
		rs := rm.At(i)
		var lateRS ptrace.ResourceSpans
		hasLateRS := false
		for j := rs.ScopeSpans().Len() - 1; j >= 0 && held < maxHeld; j-- {
			ss := rs.ScopeSpans().At(j)
			spans := ss.Spans()
			n := min(maxHeld-held, spans.Len())
			if n == 0 {
				continue
			}
			if !hasLateRS {
				lateRS = late.ResourceSpans().AppendEmpty()
				rs.Resource().CopyTo(lateRS.Resource())
				lateRS.SetSchemaUrl(rs.SchemaUrl())
				hasLateRS = true
			}
			lateSS := lateRS.ScopeSpans().AppendEmpty()
			ss.Scope().CopyTo(lateSS.Scope())
			lateSS.SetSchemaUrl(ss.SchemaUrl())
			cut := spans.Len() - n
			idx := 0
			spans.RemoveIf(func(s ptrace.Span) bool {
				idx++
				if idx <= cut {
					return false
				}
				s.MoveTo(lateSS.Spans().AppendEmpty())
				return true
			})
			held += n
		}
	}
	return late, held > 0
}

func (ar *tracesGenerator) consume(ctx context.Context, td ptrace.Traces) {
	recordCount := td.SpanCount()
	if err := ar.consumer.ConsumeTraces(ctx, td); err != nil {
		ar.logger.Error(err.Error())
		ar.statsMu.Lock()
		ar.stats.FailedRequests++
		ar.stats.FailedSpans += recordCount
		ar.statsMu.Unlock()
	} else {
		ar.statsMu.Lock()
		ar.stats.Requests++
		ar.stats.Spans += recordCount
		ar.statsMu.Unlock()
	}
}
