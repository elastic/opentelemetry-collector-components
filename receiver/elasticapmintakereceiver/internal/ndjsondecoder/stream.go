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

package ndjsondecoder // import "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal/ndjsondecoder"

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/netip"
	"sort"
	"strings"

	xxhash "github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
)

// BatchConsumer is called once per logical batch with the decoded events.
// Any of the three pdata arguments may be nil if no events of that type were
// decoded in the batch.
type BatchConsumer func(ctx context.Context, ld *plog.Logs, md *pmetric.Metrics, td *ptrace.Traces) error

// HandleStream processes the NDJSON event stream from body, routing events to
// consumer in batches of batchSize. It returns the number of accepted events
// and a slice of non-fatal per-event errors.
func HandleStream(
	ctx context.Context,
	body io.Reader,
	batchSize, maxLineLength int,
	logger *zap.Logger,
	consumer BatchConsumer,
) (int, []error) {
	dec := NewNDJSONStreamDecoder(body, maxLineLength)
	meta, err := DecodeMetadata(dec)
	if err != nil {
		return 0, []error{err}
	}

	keyIndex, allGlobalKeys := buildKeyIndex(meta)
	useBig := len(keyIndex) > 64
	h := xxhash.New()

	var (
		accepted   int
		errs       []error
		batchCount int
		main       batchBuf
		shadows    shadowBufs
	)

	flush := func() {
		errs = append(errs, flushBatch(ctx, &main, &shadows, keyIndex, allGlobalKeys, consumer)...)
		main = batchBuf{}
		shadows = shadowBufs{}
		batchCount = 0
	}

	for {
		if err := ctx.Err(); err != nil {
			return accepted, append(errs, err)
		}

		rawLine, readErr := dec.ReadAhead()
		isEOF := readErr == io.EOF

		if len(rawLine) > 0 {
			eventType := detectEventType(rawLine)
			var appendErr error
			switch eventType {
			case "transaction":
				var tx *transaction
				tx, appendErr = DecodeTransaction(dec)
				if appendErr == nil {
					svc := TransactionContextService(tx)
					tags := tx.Context.Tags
					target := routeTarget(&main, &shadows, tags, keyIndex, useBig)
					extras := &eventResourceExtras{
						request:     &tx.Context.Request,
						faas:        &tx.FAAS,
						cloudOrigin: &tx.Context.Cloud.Origin,
					}
					fp := fingerprintEvent(meta, svc, tags, &tx.Context.User, extras, h)
					ss := getOrCreateTraceScope(fp, target, meta, svc, tags, &tx.Context.User, extras)
					txSpan := AppendTransaction(ss, tx, logger)
					AppendDroppedSpanStats(ss, txSpan, tx.DroppedSpanStats)
				}
			case "span":
				var sp *span
				sp, appendErr = DecodeSpan(dec)
				if appendErr == nil {
					svc := SpanContextService(sp)
					tags := mergeOTelElasticLabels(sp.Context.Tags, sp.OTel.Attributes)
					target := routeTarget(&main, &shadows, tags, keyIndex, useBig)
					var extras *eventResourceExtras
					if ip := validateIP(sp.Context.Destination.Address.Val); ip != "" {
						extras = &eventResourceExtras{destIP: ip}
					}
					fp := fingerprintEvent(meta, svc, tags, nil, extras, h)
					ss := getOrCreateTraceScope(fp, target, meta, svc, tags, nil, extras)
					AppendSpan(ss, sp, 0, logger)
				}
			case "error":
				var e *errorEvent
				e, appendErr = DecodeError(dec)
				if appendErr == nil {
					svc := ErrorContextService(e)
					tags := e.Context.Tags
					target := routeTarget(&main, &shadows, tags, keyIndex, useBig)
					extras := &eventResourceExtras{request: &e.Context.Request}
					fp := fingerprintEvent(meta, svc, tags, &e.Context.User, extras, h)
					sl, _ := getOrCreateLogScope(fp, target, meta, svc, tags, &e.Context.User, extras)
					AppendError(sl, e, logger)
				}
			case "log":
				var l *log
				l, appendErr = DecodeLog(dec)
				if appendErr == nil {
					svc := LogContextService(l)
					target := routeTarget(&main, &shadows, l.Labels, keyIndex, useBig)
					fp := fingerprintLog(meta, svc, &l.FAAS, l.Labels, h)
					sl, newAttrs := getOrCreateLogScope(fp, target, meta, svc, nil, nil, nil)
					if newAttrs != nil {
						writeFAASAttrs(&l.FAAS, *newAttrs)
						writeTagLabels(l.Labels, *newAttrs)
					}
					AppendLog(sl, l, logger)
				}
			case "metricset":
				var ms *metricset
				ms, appendErr = DecodeMetricset(dec)
				if appendErr == nil {
					svc := MetricsetContextService(ms)
					tags := MetricsetTags(ms)
					target := routeTarget(&main, &shadows, tags, keyIndex, useBig)
					appendMetricEvent(target, meta, svc, ms)
				}
			default:
				var raw json.RawMessage
				if decErr := dec.Decode(&raw); decErr != nil && eventType == "" {
					appendErr = decErr
				} else {
					appendErr = ValidationError{err: fmt.Errorf("did not recognize object type: %q", eventType)}
				}
			}

			if appendErr != nil {
				errs = append(errs, appendErr)
			} else if eventType != "" {
				accepted++
				batchCount++
				if batchCount == batchSize {
					flush()
				}
			}
		} else if readErr != nil && !isEOF {
			errs = append(errs, readErr)
			if readErr != ErrLineTooLong {
				break
			}
		}

		if isEOF {
			break
		}
	}

	if batchCount > 0 {
		flush()
	}
	return accepted, errs
}

type batchBuf struct {
	ld     *plog.Logs
	md     *pmetric.Metrics
	td     *ptrace.Traces
	groups streamGroups
}

type shadowedBatch struct {
	useSmallMask    bool
	globalKeyMask64 uint64
	globalKeyMask   big.Int
	batchBuf
}

type shadowBufs struct {
	smallIdx map[uint64]int
	largeIdx map[string]int
	batches  []shadowedBatch
}

type streamGroups struct {
	traces []traceEntry
	logs   []logEntry
}

type traceEntry struct {
	fp uint64
	ss ptrace.ScopeSpans
}

type logEntry struct {
	fp uint64
	sl plog.ScopeLogs
}

func (g *streamGroups) traceScope(fp uint64) (ptrace.ScopeSpans, bool) {
	for i := range g.traces {
		if g.traces[i].fp == fp {
			return g.traces[i].ss, true
		}
	}
	return ptrace.ScopeSpans{}, false
}

func (g *streamGroups) logScope(fp uint64) (plog.ScopeLogs, bool) {
	for i := range g.logs {
		if g.logs[i].fp == fp {
			return g.logs[i].sl, true
		}
	}
	return plog.ScopeLogs{}, false
}

func routeTarget(main *batchBuf, shadows *shadowBufs, tags map[string]any, keyIndex map[string]globalKeyInfo, useBig bool) *batchBuf {
	if len(keyIndex) == 0 {
		return main
	}
	if !useBig {
		mask, shadowed := shadowMask64(tags, keyIndex)
		if !shadowed {
			return main
		}
		if shadows.smallIdx == nil {
			shadows.smallIdx = make(map[uint64]int)
		}
		idx, ok := shadows.smallIdx[mask]
		if !ok {
			shadows.batches = append(shadows.batches, shadowedBatch{useSmallMask: true, globalKeyMask64: mask})
			idx = len(shadows.batches) - 1
			shadows.smallIdx[mask] = idx
		}
		return &shadows.batches[idx].batchBuf
	}
	mask, shadowed := shadowMaskBig(tags, keyIndex)
	if !shadowed {
		return main
	}
	maskKey := mask.String()
	if shadows.largeIdx == nil {
		shadows.largeIdx = make(map[string]int)
	}
	idx, ok := shadows.largeIdx[maskKey]
	if !ok {
		shadows.batches = append(shadows.batches, shadowedBatch{globalKeyMask: mask})
		idx = len(shadows.batches) - 1
		shadows.largeIdx[maskKey] = idx
	}
	return &shadows.batches[idx].batchBuf
}

func getOrCreateTraceScope(fp uint64, buf *batchBuf, meta *metadata, svc *contextService, tags map[string]any, u *user, extras *eventResourceExtras) ptrace.ScopeSpans {
	if ss, ok := buf.groups.traceScope(fp); ok {
		return ss
	}
	if buf.td == nil {
		t := ptrace.NewTraces()
		buf.td = &t
	}
	rs := buf.td.ResourceSpans().AppendEmpty()
	writeFullResourceAttrs(rs.Resource().Attributes(), meta, svc, tags, u, extras)
	ss := rs.ScopeSpans().AppendEmpty()
	buf.groups.traces = append(buf.groups.traces, traceEntry{fp: fp, ss: ss})
	return ss
}

// getOrCreateLogScope returns the ScopeLogs for fp; if it was just created,
// newAttrs is non-nil and points to the resource attributes map so callers
// can write additional per-event resource attrs (e.g. log-level FaaS/labels).
func getOrCreateLogScope(fp uint64, buf *batchBuf, meta *metadata, svc *contextService, tags map[string]any, u *user, extras *eventResourceExtras) (plog.ScopeLogs, *pcommon.Map) {
	if sl, ok := buf.groups.logScope(fp); ok {
		return sl, nil
	}
	if buf.ld == nil {
		l := plog.NewLogs()
		buf.ld = &l
	}
	rl := buf.ld.ResourceLogs().AppendEmpty()
	attrs := rl.Resource().Attributes()
	writeFullResourceAttrs(attrs, meta, svc, tags, u, extras)
	sl := rl.ScopeLogs().AppendEmpty()
	buf.groups.logs = append(buf.groups.logs, logEntry{fp: fp, sl: sl})
	return sl, &attrs
}

func appendMetricEvent(buf *batchBuf, meta *metadata, svc *contextService, ms *metricset) {
	if buf.md == nil {
		m := pmetric.NewMetrics()
		buf.md = &m
	}
	rm := buf.md.ResourceMetrics().AppendEmpty()
	attrs := rm.Resource().Attributes()
	WriteResourceAttributes(meta, svc, attrs)
	tags := MetricsetTags(ms)
	if len(tags) > 0 {
		writeTagLabels(tags, attrs)
	}
	WriteFAASResourceAttrs(attrs, ms)
	attrs.PutStr(elasticattr.MetricsetName, MetricsetName(ms))
	AppendMetricset(rm.ScopeMetrics().AppendEmpty(), ms)
}

// eventResourceExtras carries per-event resource attributes that vary by event
// type and are not present in the metadata (request IPs, user-agent, FaaS,
// cloud.origin, destination.ip).
type eventResourceExtras struct {
	request     *contextRequest     // client IP, source IP, source.nat.ip, user-agent
	faas        *faas               // faas.* resource attributes
	cloudOrigin *contextCloudOrigin // cloud.origin.* resource attributes
	destIP      string              // pre-validated destination.ip
}

func writeFullResourceAttrs(attrs pcommon.Map, meta *metadata, svc *contextService, tags map[string]any, u *user, extras *eventResourceExtras) {
	WriteResourceAttributes(meta, svc, attrs)
	if len(tags) > 0 {
		writeTagLabels(tags, attrs)
	}
	if u != nil && userHasFields(u) {
		// Per-event user replaces metadata user entirely.
		attrs.Remove(string(semconv.UserIDKey))
		attrs.Remove(string(semconv.UserEmailKey))
		attrs.Remove(string(semconv.UserNameKey))
		attrs.Remove(elasticattr.UserDomain)
		writeUserAttrs(u, attrs)
	}
	if extras != nil {
		writeEventResourceExtras(attrs, extras)
	}
}

func writeEventResourceExtras(attrs pcommon.Map, extras *eventResourceExtras) {
	clientIP, sourceIP, natIP := extractRequestIPs(extras.request)
	if clientIP != "" {
		attrs.PutStr(string(semconv.ClientAddressKey), clientIP)
	}
	if sourceIP != "" {
		attrs.PutStr(string(semconv.SourceAddressKey), sourceIP)
	}
	if natIP != "" {
		attrs.PutStr(elasticattr.SourceNATIP, natIP)
	}
	if ua := userAgentOriginal(extras.request); ua != "" {
		attrs.PutStr(string(semconv.UserAgentOriginalKey), ua)
	}
	writeFAASAttrs(extras.faas, attrs)
	writeCloudOriginAttrs(extras.cloudOrigin, attrs)
	if extras.destIP != "" {
		attrs.PutStr(elasticattr.DestinationIP, extras.destIP)
	}
}

func writeTagLabels(tags map[string]any, attrs pcommon.Map) {
	strKeys, numKeys := partitionTagKeys(tags)
	for _, k := range strKeys {
		switch sv := tags[k].(type) {
		case string:
			if sv != "" {
				attrs.PutStr("labels."+k, sv)
			}
		case bool:
			if sv {
				attrs.PutStr("labels."+k, "true")
			} else {
				attrs.PutStr("labels."+k, "false")
			}
		case []interface{}:
			sl := attrs.PutEmptySlice("labels." + k)
			for _, item := range sv {
				if str, ok := item.(string); ok {
					sl.AppendEmpty().SetStr(str)
				}
			}
		}
	}
	for _, k := range numKeys {
		n := tags[k].(json.Number)
		if f, err := n.Float64(); err == nil {
			attrs.PutDouble("numeric_labels."+k, f)
		}
	}
}

func writeUserAttrs(u *user, attrs pcommon.Map) {
	if u.ID.IsSet() {
		switch id := u.ID.Val.(type) {
		case string:
			if id != "" {
				attrs.PutStr(string(semconv.UserIDKey), id)
			}
		case json.Number:
			attrs.PutStr(string(semconv.UserIDKey), id.String())
		}
	}
	if u.Email.IsSet() {
		if u.Email.Val != "" {
			attrs.PutStr(string(semconv.UserEmailKey), u.Email.Val)
		}
	}
	if u.Name.IsSet() {
		if u.Name.Val != "" {
			attrs.PutStr(string(semconv.UserNameKey), u.Name.Val)
		}
	}
	if u.Domain.IsSet() {
		if u.Domain.Val != "" {
			attrs.PutStr(elasticattr.UserDomain, u.Domain.Val)
		}
	}
}

func userHasFields(u *user) bool {
	return u.ID.IsSet() || u.Email.IsSet() || u.Name.IsSet() || u.Domain.IsSet()
}

// fingerprintEvent returns a stable hash for the resource attrs writeFullResourceAttrs
// would produce: metadata base + per-event tags + per-event user + event extras.
// h is reset before use.
func fingerprintEvent(meta *metadata, svc *contextService, tags map[string]any, u *user, extras *eventResourceExtras, h *xxhash.Digest) uint64 {
	h.Reset()
	walkMetadataResourceAttributes(meta, svc, hashWriter{h: h})
	if len(tags) > 0 {
		hashTagLabels(tags, h)
	}
	if u != nil && userHasFields(u) {
		hashUserFields(u, h)
	}
	if extras != nil {
		hashEventResourceExtras(extras, h)
	}
	return h.Sum64()
}

// fingerprintLog hashes the log resource: metadata base + FaaS + log-level labels.
func fingerprintLog(meta *metadata, svc *contextService, f *faas, labels map[string]any, h *xxhash.Digest) uint64 {
	h.Reset()
	walkMetadataResourceAttributes(meta, svc, hashWriter{h: h})
	hashFAASFields(f, h)
	if len(labels) > 0 {
		hashTagLabels(labels, h)
	}
	return h.Sum64()
}

func hashEventResourceExtras(extras *eventResourceExtras, h *xxhash.Digest) {
	hw := hashWriter{h: h}
	clientIP, sourceIP, natIP := extractRequestIPs(extras.request)
	if clientIP != "" {
		hw.putStr(string(semconv.ClientAddressKey), clientIP)
	}
	if sourceIP != "" {
		hw.putStr(string(semconv.SourceAddressKey), sourceIP)
	}
	if natIP != "" {
		hw.putStr(elasticattr.SourceNATIP, natIP)
	}
	if ua := userAgentOriginal(extras.request); ua != "" {
		hw.putStr(string(semconv.UserAgentOriginalKey), ua)
	}
	hashFAASFields(extras.faas, h)
	hashCloudOriginFields(extras.cloudOrigin, h)
	if extras.destIP != "" {
		hw.putStr(elasticattr.DestinationIP, extras.destIP)
	}
}

func hashTagLabels(tags map[string]any, h *xxhash.Digest) {
	strKeys, numKeys := partitionTagKeys(tags)
	hw := hashWriter{h: h}
	for _, k := range strKeys {
		switch sv := tags[k].(type) {
		case string:
			if sv != "" {
				hw.putStr("labels."+k, sv)
			}
		case bool:
			if sv {
				hw.putStr("labels."+k, "true")
			} else {
				hw.putStr("labels."+k, "false")
			}
		case []interface{}:
			strs := make([]string, 0, len(sv))
			for _, item := range sv {
				if str, ok := item.(string); ok {
					strs = append(strs, str)
				}
			}
			hw.putStrSlice("labels."+k, strs)
		}
	}
	for _, k := range numKeys {
		n := tags[k].(json.Number)
		if f, err := n.Float64(); err == nil {
			hw.putDouble("numeric_labels."+k, f)
		}
	}
}

func hashUserFields(u *user, h *xxhash.Digest) {
	hw := hashWriter{h: h}
	if u.ID.IsSet() {
		switch id := u.ID.Val.(type) {
		case string:
			hw.putStr(string(semconv.UserIDKey), id)
		case json.Number:
			hw.putStr(string(semconv.UserIDKey), id.String())
		}
	}
	if u.Email.IsSet() {
		hw.putStr(string(semconv.UserEmailKey), u.Email.Val)
	}
	if u.Name.IsSet() {
		hw.putStr(string(semconv.UserNameKey), u.Name.Val)
	}
	if u.Domain.IsSet() {
		hw.putStr(elasticattr.UserDomain, u.Domain.Val)
	}
}

type globalKeyInfo struct {
	bitPos      int
	prefixedKey string
}

func buildKeyIndex(meta *metadata) (map[string]globalKeyInfo, []string) {
	if len(meta.Labels) == 0 {
		return nil, nil
	}
	idx := make(map[string]globalKeyInfo, len(meta.Labels))
	var bitPos int
	var allKeys []string
	for k, v := range meta.Labels {
		if k == "" || v == nil {
			continue
		}
		if _, isNum := v.(json.Number); isNum {
			idx[k] = globalKeyInfo{bitPos: bitPos, prefixedKey: "numeric_labels." + k}
		} else {
			idx[k] = globalKeyInfo{bitPos: bitPos, prefixedKey: "labels." + k}
		}
		allKeys = append(allKeys, idx[k].prefixedKey)
		bitPos++
	}
	return idx, allKeys
}

func shadowMask64(tags map[string]any, keyIndex map[string]globalKeyInfo) (uint64, bool) {
	var mask uint64
	var shadowed bool
	for key, info := range keyIndex {
		if v, ok := tags[key]; ok && v != nil {
			shadowed = true // key is shadowed by per-event tag
		} else {
			mask |= 1 << uint(info.bitPos) // key is retained
		}
	}
	return mask, shadowed
}

func shadowMaskBig(tags map[string]any, keyIndex map[string]globalKeyInfo) (big.Int, bool) {
	var mask big.Int
	var shadowed bool
	for key, info := range keyIndex {
		if v, ok := tags[key]; ok && v != nil {
			shadowed = true
		} else {
			mask.SetBit(&mask, info.bitPos, 1)
		}
	}
	return mask, shadowed
}

func resolveGlobalKeysUint64(mask uint64, keyIndex map[string]globalKeyInfo) []string {
	keys := make([]string, 0, len(keyIndex))
	for _, info := range keyIndex {
		if mask&(1<<uint(info.bitPos)) != 0 {
			keys = append(keys, info.prefixedKey)
		}
	}
	return keys
}

func resolveGlobalKeysBigInt(mask *big.Int, keyIndex map[string]globalKeyInfo) []string {
	keys := make([]string, 0, len(keyIndex))
	for _, info := range keyIndex {
		if mask.Bit(info.bitPos) != 0 {
			keys = append(keys, info.prefixedKey)
		}
	}
	return keys
}

func flushBatch(ctx context.Context, main *batchBuf, shadows *shadowBufs, keyIndex map[string]globalKeyInfo, allGlobalKeys []string, consumer BatchConsumer) []error {
	var errs []error
	mainCtx := ctx
	if len(allGlobalKeys) > 0 {
		mainCtx = withDynamicResourceAttributes(ctx, allGlobalKeys)
	}
	if err := consumer(mainCtx, main.ld, main.md, main.td); err != nil {
		errs = append(errs, err)
	}
	for i := range shadows.batches {
		sb := &shadows.batches[i]
		var dynamicAttrs []string
		if sb.useSmallMask {
			dynamicAttrs = resolveGlobalKeysUint64(sb.globalKeyMask64, keyIndex)
		} else {
			dynamicAttrs = resolveGlobalKeysBigInt(&sb.globalKeyMask, keyIndex)
		}
		sbCtx := withDynamicResourceAttributes(ctx, dynamicAttrs)
		if err := consumer(sbCtx, sb.ld, sb.md, sb.td); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func withDynamicResourceAttributes(ctx context.Context, globalLabelKeys []string) context.Context {
	info := client.FromContext(ctx)
	newMeta := make(map[string][]string)
	for k := range info.Metadata.Keys() {
		newMeta[k] = info.Metadata.Get(k)
	}
	newMeta[elasticattr.MetadataDynamicResourceAttributes] = globalLabelKeys
	return client.NewContext(ctx, client.Info{
		Addr:     info.Addr,
		Auth:     info.Auth,
		Metadata: client.NewMetadata(newMeta),
	})
}

// extractRequestIPs derives (clientIP, sourceIP, natIP) from a request context.
// When X-Forwarded-For is present, the XFF IP becomes client+source and the
// socket address becomes the NAT IP. Otherwise the socket is client+source.
// Invalid or absent IPs return empty strings.
func extractRequestIPs(req *contextRequest) (clientIP, sourceIP, natIP string) {
	if req == nil {
		return
	}
	xff := ""
	if req.Headers.IsSet() {
		if vals := req.Headers.Val[http.CanonicalHeaderKey("X-Forwarded-For")]; len(vals) > 0 {
			// XFF may be "ip1, ip2, ..." — take the first entry.
			first, _, _ := strings.Cut(vals[0], ",")
			xff = validateIP(strings.TrimSpace(first))
		}
	}
	socket := validateIP(req.Socket.RemoteAddress.Val)
	if xff != "" {
		clientIP = xff
		sourceIP = xff
		natIP = socket
	} else if socket != "" {
		clientIP = socket
		sourceIP = socket
	}
	return
}

func validateIP(ip string) string {
	if ip == "" {
		return ""
	}
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return ""
	}
	return addr.String()
}

func userAgentOriginal(req *contextRequest) string {
	if req == nil || !req.Headers.IsSet() {
		return ""
	}
	vals := req.Headers.Val[http.CanonicalHeaderKey("User-Agent")]
	if len(vals) == 0 {
		return ""
	}
	return strings.Join(vals, ", ")
}

func writeFAASAttrs(f *faas, attrs pcommon.Map) {
	if f == nil || !f.IsSet() {
		return
	}
	if f.ID.IsSet() {
		attrs.PutStr(string(semconv.FaaSInstanceKey), f.ID.Val)
	}
	if f.Name.IsSet() {
		attrs.PutStr(string(semconv.FaaSNameKey), f.Name.Val)
	}
	if f.Version.IsSet() {
		attrs.PutStr(string(semconv.FaaSVersionKey), f.Version.Val)
	}
	if f.Trigger.Type.IsSet() {
		attrs.PutStr(string(semconv.FaaSTriggerKey), f.Trigger.Type.Val)
	}
	if f.Coldstart.IsSet() {
		attrs.PutBool(string(semconv.FaaSColdstartKey), f.Coldstart.Val)
	}
	if f.Trigger.RequestID.IsSet() {
		attrs.PutStr(elasticattr.FaaSTriggerRequestID, f.Trigger.RequestID.Val)
	}
	if f.Execution.IsSet() {
		attrs.PutStr(elasticattr.FaaSExecution, f.Execution.Val)
	}
}

func writeCloudOriginAttrs(o *contextCloudOrigin, attrs pcommon.Map) {
	if o == nil || !o.IsSet() {
		return
	}
	if o.Account.ID.Val != "" {
		attrs.PutStr(elasticattr.CloudOriginAccountID, o.Account.ID.Val)
	}
	if o.Provider.Val != "" {
		attrs.PutStr(elasticattr.CloudOriginProvider, o.Provider.Val)
	}
	if o.Region.Val != "" {
		attrs.PutStr(elasticattr.CloudOriginRegion, o.Region.Val)
	}
	if o.Service.Name.Val != "" {
		attrs.PutStr(elasticattr.CloudOriginServiceName, o.Service.Name.Val)
	}
}

func hashFAASFields(f *faas, h *xxhash.Digest) {
	if f == nil || !f.IsSet() {
		return
	}
	hw := hashWriter{h: h}
	if f.ID.IsSet() {
		hw.putStr(string(semconv.FaaSInstanceKey), f.ID.Val)
	}
	if f.Name.IsSet() {
		hw.putStr(string(semconv.FaaSNameKey), f.Name.Val)
	}
	if f.Version.IsSet() {
		hw.putStr(string(semconv.FaaSVersionKey), f.Version.Val)
	}
	if f.Trigger.Type.IsSet() {
		hw.putStr(string(semconv.FaaSTriggerKey), f.Trigger.Type.Val)
	}
	if f.Coldstart.IsSet() {
		if f.Coldstart.Val {
			hw.putStr(string(semconv.FaaSColdstartKey), "true")
		} else {
			hw.putStr(string(semconv.FaaSColdstartKey), "false")
		}
	}
	if f.Trigger.RequestID.IsSet() {
		hw.putStr(elasticattr.FaaSTriggerRequestID, f.Trigger.RequestID.Val)
	}
	if f.Execution.IsSet() {
		hw.putStr(elasticattr.FaaSExecution, f.Execution.Val)
	}
}

func hashCloudOriginFields(o *contextCloudOrigin, h *xxhash.Digest) {
	if o == nil || !o.IsSet() {
		return
	}
	hw := hashWriter{h: h}
	if o.Account.ID.Val != "" {
		hw.putStr(elasticattr.CloudOriginAccountID, o.Account.ID.Val)
	}
	if o.Provider.Val != "" {
		hw.putStr(elasticattr.CloudOriginProvider, o.Provider.Val)
	}
	if o.Region.Val != "" {
		hw.putStr(elasticattr.CloudOriginRegion, o.Region.Val)
	}
	if o.Service.Name.Val != "" {
		hw.putStr(elasticattr.CloudOriginServiceName, o.Service.Name.Val)
	}
}

// mergeOTelElasticLabels returns a merged tags map that includes OTel attributes
// whose keys start with "elastic.", transforming dots to underscores
// (e.g. "elastic.foo.bar" → "elastic_foo_bar"). Returns tags unchanged when no
// elastic.* OTel attributes exist; otherwise returns a new merged map.
func mergeOTelElasticLabels(tags map[string]any, otelAttrs map[string]any) map[string]any {
	var merged map[string]any
	for k, v := range otelAttrs {
		if !strings.HasPrefix(k, "elastic.") {
			continue
		}
		if merged == nil {
			merged = make(map[string]any, len(tags)+1)
			for tk, tv := range tags {
				merged[tk] = tv
			}
		}
		merged[strings.ReplaceAll(k, ".", "_")] = v
	}
	if merged == nil {
		return tags
	}
	return merged
}

// partitionTagKeys splits tag keys into (string/bool keys, numeric keys), both sorted.
func partitionTagKeys(tags map[string]any) (strKeys, numKeys []string) {
	for k, v := range tags {
		if k == "" || v == nil {
			continue
		}
		if _, isNum := v.(json.Number); isNum {
			numKeys = append(numKeys, k)
		} else {
			strKeys = append(strKeys, k)
		}
	}
	sort.Strings(strKeys)
	sort.Strings(numKeys)
	return
}

func detectEventType(line []byte) string {
	start := bytes.IndexByte(line, '{')
	if start < 0 {
		return ""
	}
	for i := start + 1; i < len(line); i++ {
		if line[i] == '"' {
			end := bytes.IndexByte(line[i+1:], '"')
			if end < 0 {
				return ""
			}
			return string(line[i+1 : i+1+end])
		}
	}
	return ""
}
