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

package vercelencodingextension

import (
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"

	"github.com/elastic/opentelemetry-collector-components/extension/vercelencodingextension/internal/metadata"
)

// resourceKeyer identifies a Vercel resource.
type resourceKeyer interface {
	comparable
	putAttributes(pcommon.Map)
}

// scopeKeyer identifies an instrumentation scope.
type scopeKeyer interface {
	comparable
	putScope(pcommon.InstrumentationScope)
}

// logsBatch accumulates decoded log records into grouped resource and scope
// structures.
type logsBatch[RK resourceKeyer, SK scopeKeyer] struct {
	logs      plog.Logs
	resources map[RK]*resourceLogsGroup[SK]
}

type resourceLogsGroup[SK scopeKeyer] struct {
	resourceLogs plog.ResourceLogs
	scopes       map[SK]plog.ScopeLogs
}

func newLogsBatch[RK resourceKeyer, SK scopeKeyer]() *logsBatch[RK, SK] {
	return &logsBatch[RK, SK]{
		logs:      plog.NewLogs(),
		resources: make(map[RK]*resourceLogsGroup[SK]),
	}
}

// scope returns the scope logs for the given resource and scope identity,
// creating and populating the resource and scope attributes on first use.
func (b *logsBatch[RK, SK]) scope(rk RK, sk SK) plog.ScopeLogs {
	rg, ok := b.resources[rk]
	if !ok {
		resourceLogs := b.logs.ResourceLogs().AppendEmpty()
		rk.putAttributes(resourceLogs.Resource().Attributes())
		rg = &resourceLogsGroup[SK]{
			resourceLogs: resourceLogs,
			scopes:       make(map[SK]plog.ScopeLogs),
		}
		b.resources[rk] = rg
	}

	sl, ok := rg.scopes[sk]
	if !ok {
		sl = rg.resourceLogs.ScopeLogs().AppendEmpty()
		sk.putScope(sl.Scope())
		rg.scopes[sk] = sl
	}
	return sl
}

// newLogsDecoder builds the log payload and a per-record decode function for the
// request schema.
func newLogsDecoder(recordSchema schema) (plog.Logs, func(recordSource) error) {
	switch recordSchema {
	case schemaWebAnalytics:
		batch := newLogsBatch[webAnalyticsResourceKey, webAnalyticsScopeKey]()
		return batch.logs, func(src recordSource) error {
			return appendWebAnalytics(src, batch)
		}
	case schemaAuditLogs:
		batch := newLogsBatch[auditLogResourceKey, auditLogScopeKey]()
		return batch.logs, func(src recordSource) error {
			return appendAuditLog(src, batch)
		}
	default:
		batch := newLogsBatch[logResourceKey, logScopeKey]()
		return batch.logs, func(src recordSource) error {
			return appendLog(src, batch)
		}
	}
}

// logResourceKey identifies the Vercel resource a plain log record belongs to.
type logResourceKey struct {
	projectID    string
	deploymentID string
	environment  string
	projectName  string
}

// putAttributes writes the resource-scoped attributes for a plain log record.
func (k logResourceKey) putAttributes(attrs pcommon.Map) {
	putStr(attrs, attrVercelProjectID, k.projectID)
	putStr(attrs, string(semconv.DeploymentIDKey), k.deploymentID)
	putStr(attrs, string(semconv.DeploymentEnvironmentNameKey), k.environment)
	putStr(attrs, attrVercelProjectName, k.projectName)
}

// logScopeKey identifies the instrumentation scope a plain log record belongs
// to.
type logScopeKey struct {
}

// putScope sets the scope name and format tag for plain logs.
func (logScopeKey) putScope(scope pcommon.InstrumentationScope) {
	scope.SetName(metadata.ScopeName)
	scope.Attributes().PutStr(formatIdentificationTag, string(formatLogs))
}

// appendLog consumes one plain log record and ensures its scope exists.
func appendLog(src recordSource, batch *logsBatch[logResourceKey, logScopeKey]) error {
	var record logSchema
	if err := src.decode(&record); err != nil {
		return err
	}

	scope := batch.scope(
		logResourceKey{
			projectID:    record.ProjectID,
			deploymentID: record.DeploymentID,
			environment:  record.Environment,
			projectName:  record.ProjectName,
		},
		logScopeKey{},
	)

	logRecord := scope.LogRecords().AppendEmpty()
	if record.Timestamp != 0 {
		logRecord.SetTimestamp(pcommon.NewTimestampFromTime(time.UnixMilli(record.Timestamp)))
	}
	putSeverity(logRecord, record.Level)
	putTraceContext(logRecord, record.traceID(), record.spanID())
	if record.Message != "" {
		logRecord.Body().SetStr(record.Message)
	}

	attrs := logRecord.Attributes()
	putStr(attrs, string(semconv.LogRecordUIDKey), record.ID)
	putStr(attrs, attrVercelLogSource, record.Source)
	putStr(attrs, string(semconv.ServerAddressKey), record.Host)
	putStr(attrs, attrVercelLogBuildID, record.BuildID)
	putStr(attrs, attrVercelLogEntrypoint, record.Entrypoint)
	putStr(attrs, attrVercelLogDestination, record.Destination)
	putStr(attrs, string(semconv.HTTPRouteKey), record.Path)
	putStr(attrs, attrVercelLogType, record.Type)
	if record.StatusCode != 0 {
		attrs.PutInt(string(semconv.HTTPResponseStatusCodeKey), record.StatusCode)
	}
	putStr(attrs, attrVercelLogRequestID, record.RequestID)
	putStr(attrs, attrVercelLogBranch, record.Branch)
	putStr(attrs, string(semconv.TLSClientJa3Key), record.JA3Digest)
	putStr(attrs, attrTLSClientJA4, record.JA4Digest)
	putStr(attrs, attrVercelLogEdgeType, record.EdgeType)
	putStr(attrs, attrVercelExecutionRegion, record.ExecutionRegion)
	putProxyAttrs(attrs, record.Proxy)
	return nil
}

type logSchema struct {
	ID              string    `json:"id"`
	DeploymentID    string    `json:"deploymentId"`
	Source          string    `json:"source"`
	Host            string    `json:"host"`
	Timestamp       int64     `json:"timestamp"`
	ProjectID       string    `json:"projectId"`
	Level           string    `json:"level"`
	Message         string    `json:"message"`
	BuildID         string    `json:"buildId"`
	Entrypoint      string    `json:"entrypoint"`
	Destination     string    `json:"destination"`
	Path            string    `json:"path"`
	Type            string    `json:"type"`
	StatusCode      int64     `json:"statusCode"`
	RequestID       string    `json:"requestId"`
	Environment     string    `json:"environment"`
	Branch          string    `json:"branch"`
	JA3Digest       string    `json:"ja3Digest"`
	JA4Digest       string    `json:"ja4Digest"`
	EdgeType        string    `json:"edgeType"`
	ProjectName     string    `json:"projectName"`
	ExecutionRegion string    `json:"executionRegion"`
	TraceID         string    `json:"traceId"`
	SpanID          string    `json:"spanId"`
	TraceIDAttr     string    `json:"trace.id"`
	SpanIDAttr      string    `json:"span.id"`
	Proxy           *proxyLog `json:"proxy"`
}

func (s logSchema) traceID() string {
	if s.TraceIDAttr != "" {
		return s.TraceIDAttr
	}
	return s.TraceID
}

func (s logSchema) spanID() string {
	if s.SpanIDAttr != "" {
		return s.SpanIDAttr
	}
	return s.SpanID
}

type proxyLog struct {
	Timestamp        int64    `json:"timestamp"`
	Method           string   `json:"method"`
	Host             string   `json:"host"`
	Path             string   `json:"path"`
	UserAgent        []string `json:"userAgent"`
	Referer          string   `json:"referer"`
	Region           string   `json:"region"`
	StatusCode       int64    `json:"statusCode"`
	ClientIP         string   `json:"clientIp"`
	Scheme           string   `json:"scheme"`
	ResponseByteSize int64    `json:"responseByteSize"`
	CacheID          string   `json:"cacheId"`
	PathType         string   `json:"pathType"`
	PathTypeVariant  string   `json:"pathTypeVariant"`
	VercelID         string   `json:"vercelId"`
	VercelCache      string   `json:"vercelCache"`
	LambdaRegion     string   `json:"lambdaRegion"`
	WAFAction        string   `json:"wafAction"`
	WAFRuleID        string   `json:"wafRuleId"`
}

func putSeverity(record plog.LogRecord, level string) {
	if level == "" {
		return
	}
	record.SetSeverityText(strings.ToUpper(level))
	switch strings.ToLower(level) {
	case "info":
		record.SetSeverityNumber(plog.SeverityNumberInfo)
	case "warning", "warn":
		record.SetSeverityNumber(plog.SeverityNumberWarn)
	case "error":
		record.SetSeverityNumber(plog.SeverityNumberError)
	case "fatal":
		record.SetSeverityNumber(plog.SeverityNumberFatal)
	}
}

func putTraceContext(record plog.LogRecord, traceID, spanID string) {
	if parsed, ok := parseTraceID(traceID); ok {
		record.SetTraceID(parsed)
	}
	if parsed, ok := parseSpanID(spanID); ok {
		record.SetSpanID(parsed)
	}
}

func parseTraceID(value string) (pcommon.TraceID, bool) {
	var traceID pcommon.TraceID
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(traceID) {
		return traceID, false
	}
	copy(traceID[:], decoded)
	return traceID, true
}

func parseSpanID(value string) (pcommon.SpanID, bool) {
	var spanID pcommon.SpanID
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(spanID) {
		return spanID, false
	}
	copy(spanID[:], decoded)
	return spanID, true
}

func putProxyAttrs(attrs pcommon.Map, proxy *proxyLog) {
	if proxy == nil {
		return
	}
	putStr(attrs, string(semconv.HTTPRequestMethodKey), proxy.Method)
	putStr(attrs, string(semconv.URLSchemeKey), proxy.Scheme)
	putStr(attrs, string(semconv.ClientAddressKey), proxy.ClientIP)
	putURLPathAttrs(attrs, proxy.Path)
	if proxy.ResponseByteSize != 0 {
		attrs.PutInt(string(semconv.HTTPResponseBodySizeKey), proxy.ResponseByteSize)
	}
	if len(proxy.UserAgent) > 0 {
		putStr(attrs, string(semconv.UserAgentOriginalKey), strings.Join(proxy.UserAgent, ", "))
	}

	proxyAttrs := attrs.PutEmptyMap(attrVercelProxy)
	if proxy.Timestamp != 0 {
		proxyAttrs.PutInt("timestamp", proxy.Timestamp)
	}
	putStr(proxyAttrs, "host", proxy.Host)
	putStr(proxyAttrs, "path", proxy.Path)
	putStr(proxyAttrs, "referer", proxy.Referer)
	putStr(proxyAttrs, "region", proxy.Region)
	if proxy.StatusCode != 0 {
		proxyAttrs.PutInt("status_code", proxy.StatusCode)
	}
	putStr(proxyAttrs, "cache_id", proxy.CacheID)
	putStr(proxyAttrs, "path_type", proxy.PathType)
	putStr(proxyAttrs, "path_type_variant", proxy.PathTypeVariant)
	putStr(proxyAttrs, "vercel_id", proxy.VercelID)
	putStr(proxyAttrs, "vercel_cache", proxy.VercelCache)
	putStr(proxyAttrs, "lambda_region", proxy.LambdaRegion)
	putStr(proxyAttrs, "waf_action", proxy.WAFAction)
	putStr(proxyAttrs, "waf_rule_id", proxy.WAFRuleID)
}

// auditLogResourceKey identifies the Vercel resource an audit log record belongs
// to.
type auditLogResourceKey struct {
	teamID    string
	projectID string
}

// putAttributes writes the resource-scoped attributes for an audit log record.
func (k auditLogResourceKey) putAttributes(attrs pcommon.Map) {
	putStr(attrs, attrVercelTeamID, k.teamID)
	putStr(attrs, attrVercelProjectID, k.projectID)
}

// auditLogScopeKey identifies the instrumentation scope an audit log record
// belongs to.
type auditLogScopeKey struct{}

// putScope sets the scope name and format tag for audit logs.
func (auditLogScopeKey) putScope(scope pcommon.InstrumentationScope) {
	scope.SetName(metadata.ScopeName)
	scope.Attributes().PutStr(formatIdentificationTag, string(formatAuditLogs))
}

// auditLogActor is a principal in the audit log record, used for both the
// acting principal and each entry of the delegation chain.
type auditLogActor struct {
	Type  string `json:"type"`
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// auditLogSchema mirrors the Vercel audit log record. The payload is decoded
// opaquely because its fields depend entirely on the action value.
//
// See: https://vercel.com/docs/drains/reference/audit-logs#audit-log-schema.
type auditLogSchema struct {
	ID        string          `json:"id"`
	TeamID    string          `json:"teamId"`
	ProjectID string          `json:"projectId"`
	Action    string          `json:"action"`
	Timestamp int64           `json:"timestamp"`
	Actor     auditLogActor   `json:"actor"`
	Via       []auditLogActor `json:"via"`
	RequestID string          `json:"requestId"`
	UserAgent string          `json:"userAgent"`
	IPAddress string          `json:"ipAddress"`
	TokenID   string          `json:"tokenId"`
	Payload   map[string]any  `json:"payload"`
}

// appendAuditLog consumes one audit log record, mapping its fields onto the
// resource, scope, and log record as documented in README.md.
func appendAuditLog(src recordSource, batch *logsBatch[auditLogResourceKey, auditLogScopeKey]) error {
	var record auditLogSchema
	if err := src.decode(&record); err != nil {
		return err
	}

	scope := batch.scope(
		auditLogResourceKey{teamID: record.TeamID, projectID: record.ProjectID},
		auditLogScopeKey{},
	)

	logRecord := scope.LogRecords().AppendEmpty()
	if record.Timestamp != 0 {
		logRecord.SetTimestamp(pcommon.NewTimestampFromTime(time.UnixMilli(record.Timestamp)))
	}
	logRecord.SetEventName(record.Action)

	attrs := logRecord.Attributes()
	putStr(attrs, string(semconv.LogRecordUIDKey), record.ID)
	putStr(attrs, attrVercelActorType, record.Actor.Type)
	putStr(attrs, string(semconv.UserIDKey), record.Actor.ID)
	putStr(attrs, string(semconv.UserNameKey), record.Actor.Name)
	putStr(attrs, string(semconv.UserEmailKey), record.Actor.Email)
	putStr(attrs, attrVercelRequestID, record.RequestID)
	putStr(attrs, string(semconv.UserAgentOriginalKey), record.UserAgent)
	putStr(attrs, string(semconv.ClientAddressKey), record.IPAddress)
	putStr(attrs, attrVercelTokenID, record.TokenID)

	putAuditLogVia(attrs, record.Via)

	// payload is action-specific and unbounded, so it is stored opaquely as a
	// nested map rather than mapped field by field. Keys are normalized to
	// snake_case for consistency with the rest of the pipeline.
	if len(record.Payload) > 0 {
		putSnakeCaseMap(attrs.PutEmptyMap(attrVercelAuditLogPayload), record.Payload)
	}

	return nil
}

// putAuditLogVia stores the delegation chain as a slice of maps under a single
// attribute, preserving every entry rather than flattening to a single actor.
func putAuditLogVia(attrs pcommon.Map, via []auditLogActor) {
	if len(via) == 0 {
		return
	}
	slice := attrs.PutEmptySlice(attrVercelVia)
	slice.EnsureCapacity(len(via))
	for _, actor := range via {
		entry := slice.AppendEmpty().SetEmptyMap()
		putStr(entry, "type", actor.Type)
		putStr(entry, "id", actor.ID)
		putStr(entry, "name", actor.Name)
		putStr(entry, "email", actor.Email)
	}
}

// webAnalyticsResourceKey identifies the Vercel resource a web analytics record
// belongs to.
type webAnalyticsResourceKey struct {
	projectID    string
	ownerID      string
	environment  string
	url          string
	deploymentID string
}

// putAttributes writes the resource-scoped attributes documented in README.md.
func (k webAnalyticsResourceKey) putAttributes(attrs pcommon.Map) {
	putStr(attrs, attrVercelProjectID, k.projectID)
	putStr(attrs, attrVercelOwnerID, k.ownerID)
	putStr(attrs, string(semconv.DeploymentEnvironmentNameKey), k.environment)
	putStr(attrs, attrVercelURL, k.url)
	putStr(attrs, string(semconv.DeploymentIDKey), k.deploymentID)
}

// webAnalyticsScopeKey identifies the instrumentation scope a web analytics
// record belongs to.
type webAnalyticsScopeKey struct {
	sdkVersion     string
	sdkName        string
	sdkVersionFull string
}

// putScope sets the scope name and its scope-scoped attributes.
func (k webAnalyticsScopeKey) putScope(scope pcommon.InstrumentationScope) {
	scope.SetName(metadata.ScopeName)
	attrs := scope.Attributes()
	attrs.PutStr(formatIdentificationTag, string(formatWebAnalytics))
	putStr(attrs, attrVercelWebAnalyticsSDKVersion, k.sdkVersion)
	putStr(attrs, attrVercelWebAnalyticsSDKName, k.sdkName)
	putStr(attrs, attrVercelWebAnalyticsSDKVersionFull, k.sdkVersionFull)
}

type webAnalyticsSchema struct {
	Schema               string `json:"schema"`
	EventType            string `json:"eventType"`
	EventName            string `json:"eventName"`
	EventData            string `json:"eventData"`
	Timestamp            int64  `json:"timestamp"`
	ProjectID            string `json:"projectId"`
	OwnerID              string `json:"ownerId"`
	DeviceID             int64  `json:"deviceId"`
	Origin               string `json:"origin"`
	Path                 string `json:"path"`
	Referrer             string `json:"referrer"`
	QueryParams          string `json:"queryParams"`
	Route                string `json:"route"`
	Country              string `json:"country"`
	Region               string `json:"region"`
	City                 string `json:"city"`
	OSName               string `json:"osName"`
	OSVersion            string `json:"osVersion"`
	ClientName           string `json:"clientName"`
	ClientType           string `json:"clientType"`
	ClientVersion        string `json:"clientVersion"`
	DeviceType           string `json:"deviceType"`
	DeviceBrand          string `json:"deviceBrand"`
	DeviceModel          string `json:"deviceModel"`
	BrowserEngine        string `json:"browserEngine"`
	BrowserEngineVersion string `json:"browserEngineVersion"`
	SDKVersion           string `json:"sdkVersion"`
	SDKName              string `json:"sdkName"`
	SDKVersionFull       string `json:"sdkVersionFull"`
	VercelEnvironment    string `json:"vercelEnvironment"`
	VercelURL            string `json:"vercelUrl"`
	Flags                string `json:"flags"`
	Deployment           string `json:"deployment"`
}

// appendWebAnalytics decodes one web analytics record and appends its log record
// to the batch, mapping fields to resource, scope, and record attributes as
// documented in README.md. Decoding is done by the caller so records can be read
// straight from the stream without a second parse.
func appendWebAnalytics(src recordSource, batch *logsBatch[webAnalyticsResourceKey, webAnalyticsScopeKey]) error {
	var record webAnalyticsSchema
	if err := src.decode(&record); err != nil {
		return err
	}

	scope := batch.scope(
		webAnalyticsResourceKey{
			projectID:    record.ProjectID,
			ownerID:      record.OwnerID,
			environment:  record.VercelEnvironment,
			url:          record.VercelURL,
			deploymentID: record.Deployment,
		},
		webAnalyticsScopeKey{
			sdkVersion:     record.SDKVersion,
			sdkName:        record.SDKName,
			sdkVersionFull: record.SDKVersionFull,
		},
	)

	logRecord := scope.LogRecords().AppendEmpty()
	if record.Timestamp != 0 {
		logRecord.SetTimestamp(pcommon.NewTimestampFromTime(time.UnixMilli(record.Timestamp)))
	}

	attrs := logRecord.Attributes()
	putStr(attrs, attrVercelAnalyticsEventType, record.EventType)
	putStr(attrs, attrVercelAnalyticsEventName, record.EventName)
	putStr(attrs, attrVercelAnalyticsEventData, record.EventData)
	if record.DeviceID != 0 {
		attrs.PutStr(string(semconv.DeviceIDKey), strconv.FormatInt(record.DeviceID, 10))
	}
	putURLAttrs(attrs, record.Origin, record.Path, record.QueryParams)
	putStr(attrs, attrVercelReferrer, record.Referrer)
	putStr(attrs, string(semconv.HTTPRouteKey), record.Route)
	putStr(attrs, string(semconv.GeoCountryISOCodeKey), record.Country)
	putStr(attrs, string(semconv.GeoRegionISOCodeKey), record.Region)
	putStr(attrs, string(semconv.GeoLocalityNameKey), record.City)
	putStr(attrs, string(semconv.UserAgentOSNameKey), record.OSName)
	putStr(attrs, string(semconv.UserAgentOSVersionKey), record.OSVersion)
	putStr(attrs, string(semconv.UserAgentNameKey), record.ClientName)
	putStr(attrs, attrVercelClientType, record.ClientType)
	putStr(attrs, string(semconv.UserAgentVersionKey), record.ClientVersion)
	putStr(attrs, attrVercelDeviceType, record.DeviceType)
	putStr(attrs, string(semconv.DeviceManufacturerKey), record.DeviceBrand)
	putStr(attrs, string(semconv.DeviceModelNameKey), record.DeviceModel)
	putStr(attrs, attrVercelBrowserEngineName, record.BrowserEngine)
	putStr(attrs, attrVercelBrowserEngineVersion, record.BrowserEngineVersion)
	putStr(attrs, attrVercelAnalyticsFlags, record.Flags)

	return nil
}
