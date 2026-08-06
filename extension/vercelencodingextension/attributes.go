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
	"fmt"
	"net/url"
	"strings"

	"github.com/goccy/go-json"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

// Vercel-specific attribute keys that are not part of the OpenTelemetry
// semantic conventions. Semantic-convention keys use the semconv package. These
// live in one place so the vocabulary stays a single source of truth across the
// metrics and logs decoders.
const (
	// Resource-scoped keys shared by every Vercel schema.
	attrVercelProjectID = "vercel.project.id"
	attrVercelOwnerID   = "vercel.owner.id"
	attrVercelURL       = "vercel.url"

	// Client and device keys shared by speed insights and web analytics.
	attrVercelClientType           = "vercel.client.type"
	attrVercelDeviceType           = "vercel.device.type"
	attrVercelConnectionSpeed      = "vercel.connection.speed"
	attrVercelBrowserEngineName    = "vercel.browser.engine.name"
	attrVercelBrowserEngineVersion = "vercel.browser.engine.version"

	// Speed insights keys.
	attrVercelSpeedInsightsScriptVersion = "vercel.speed_insights.script.version"
	attrVercelSpeedInsightsSDKVersion    = "vercel.speed_insights.sdk.version"
	attrVercelSpeedInsightsSDKName       = "vercel.speed_insights.sdk.name"
	attrVercelSpeedInsightsAttribution   = "vercel.speed_insights.attribution"

	// Web analytics keys.
	attrVercelAnalyticsEventType         = "vercel.analytics.event.type"
	attrVercelAnalyticsEventName         = "vercel.analytics.event.name"
	attrVercelAnalyticsEventData         = "vercel.analytics.event.data"
	attrVercelReferrer                   = "vercel.referrer"
	attrVercelAnalyticsFlags             = "vercel.analytics.flags"
	attrVercelWebAnalyticsSDKVersion     = "vercel.web_analytics.sdk.version"
	attrVercelWebAnalyticsSDKName        = "vercel.web_analytics.sdk.name"
	attrVercelWebAnalyticsSDKVersionFull = "vercel.web_analytics.sdk.version_full"

	// Plain log keys.
	attrVercelProjectName     = "vercel.project.name"
	attrVercelLogSource       = "vercel.log.source"
	attrVercelLogBuildID      = "vercel.log.build.id"
	attrVercelLogEntrypoint   = "vercel.log.entrypoint"
	attrVercelLogDestination  = "vercel.log.destination"
	attrVercelLogType         = "vercel.log.type"
	attrVercelLogRequestID    = "vercel.log.request.id"
	attrVercelLogBranch       = "vercel.log.branch"
	attrVercelLogEdgeType     = "vercel.log.edge.type"
	attrVercelExecutionRegion = "vercel.execution.region"
	attrVercelProxy           = "vercel.proxy"
	attrTLSClientJA4          = "tls.client.ja4"
	// Audit log keys.
	attrVercelTeamID          = "vercel.team.id"
	attrVercelActorType       = "vercel.actor.type"
	attrVercelVia             = "vercel.via"
	attrVercelRequestID       = "vercel.request.id"
	attrVercelTokenID         = "vercel.token.id"
	attrVercelAuditLogPayload = "vercel.audit_log.payload"
)

// putStr sets a string attribute only when the value is non-empty, keeping
// records free of empty attributes.
func putStr(attrs pcommon.Map, key, value string) {
	if value == "" {
		return
	}
	attrs.PutStr(key, value)
}

// putURLAttrs derives the semconv url.* attributes from a Vercel origin
// (scheme://host) plus its separately supplied path and query. It reconstructs
// url.full so the complete page URL is queryable, and also emits the scheme and
// domain components. If origin is not a parseable absolute URL, the raw value
// is kept as url.full so nothing is lost.
func putURLAttrs(attrs pcommon.Map, origin, path, query string) {
	urlPath, urlQuery := splitURLPathQuery(path, query)
	putStr(attrs, string(semconv.URLPathKey), urlPath)
	putStr(attrs, string(semconv.URLQueryKey), urlQuery)

	parsed, err := url.Parse(origin)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		putStr(attrs, string(semconv.URLFullKey), origin)
		return
	}

	putStr(attrs, string(semconv.URLSchemeKey), parsed.Scheme)
	putStr(attrs, string(semconv.URLDomainKey), parsed.Hostname())

	// path and query arrive already percent-encoded from Vercel (the SDK
	// records location.pathname/search). Assigning path to url.URL.Path and
	// calling String() would re-escape the '%', double-encoding sequences like
	// %2F into %252F. Build url.full from the base origin so the original
	// encoded form is preserved verbatim.
	//
	// For example, given:
	//   origin = "https://example.com"
	//   path   = "/products/foo%2Fbar"
	//
	//   url.URL.Path + String() -> "https://example.com/products/foo%252Fbar" (wrong)
	//   base origin + path       -> "https://example.com/products/foo%2Fbar"  (correct)
	base := url.URL{Scheme: parsed.Scheme, Host: parsed.Host, User: parsed.User}
	full := base.String() + urlPath
	if urlQuery != "" {
		full += "?" + urlQuery
	}
	putStr(attrs, string(semconv.URLFullKey), full)
}

func putURLPathAttrs(attrs pcommon.Map, path string) {
	urlPath, query := splitURLPathQuery(path, "")
	putStr(attrs, string(semconv.URLPathKey), urlPath)
	putStr(attrs, string(semconv.URLQueryKey), query)
}

func splitURLPathQuery(path, query string) (string, string) {
	if query != "" {
		return path, query
	}
	urlPath, urlQuery, hasQuery := strings.Cut(path, "?")
	if !hasQuery {
		return path, ""
	}
	return urlPath, urlQuery
}

// putSnakeCaseMap copies src into dst, converting every key to snake_case and
// recursing into nested objects and arrays.
func putSnakeCaseMap(dst pcommon.Map, src map[string]any) {
	dst.EnsureCapacity(len(src))
	for key, value := range src {
		putSnakeCaseValue(dst.PutEmpty(toSnakeCase(key)), value)
	}
}

// putSnakeCaseValue writes value into dst, recursing so nested object keys are
// also converted to snake_case.
func putSnakeCaseValue(dst pcommon.Value, value any) {
	switch v := value.(type) {
	case map[string]any:
		putSnakeCaseMap(dst.SetEmptyMap(), v)
	case []any:
		slice := dst.SetEmptySlice()
		slice.EnsureCapacity(len(v))
		for _, item := range v {
			putSnakeCaseValue(slice.AppendEmpty(), item)
		}
	case string:
		dst.SetStr(v)
	case bool:
		dst.SetBool(v)
	case float64:
		dst.SetDouble(v)
	case json.Number:
		if i, err := v.Int64(); err == nil {
			dst.SetInt(i)
			return
		}
		if f, err := v.Float64(); err == nil {
			dst.SetDouble(f)
			return
		}
		dst.SetStr(v.String())
	case nil:
		// Leave dst as an empty value.
	default:
		dst.SetStr(fmt.Sprintf("%v", v))
	}
}

// toSnakeCase converts a camelCase or PascalCase key to snake_case, handling
// acronym runs so drainUrl and drainURL both become drain_url. Keys with no
// uppercase letters are already snake_case and returned unchanged.
func toSnakeCase(s string) string {
	if !hasUpper(s) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 5)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !isUpper(c) {
			b.WriteByte(c)
			continue
		}
		if i > 0 && startsNewWord(s, i) {
			b.WriteByte('_')
		}
		b.WriteByte(toLower(c))
	}
	return b.String()
}

// startsNewWord reports whether the uppercase byte at index i begins a new word
// and so needs a preceding underscore: either it follows a lowercase letter or
// digit (drainUrl), or it ends an acronym run that a lowercase word follows
// (HTTPServer becomes http_server).
func startsNewWord(s string, i int) bool {
	prev := s[i-1]
	if isLower(prev) || isDigit(prev) {
		return true
	}
	nextIsLower := i+1 < len(s) && isLower(s[i+1])
	return isUpper(prev) && nextIsLower
}

func hasUpper(s string) bool {
	for i := 0; i < len(s); i++ {
		if isUpper(s[i]) {
			return true
		}
	}
	return false
}

func isUpper(c byte) bool { return c >= 'A' && c <= 'Z' }
func isLower(c byte) bool { return c >= 'a' && c <= 'z' }
func isDigit(c byte) bool { return c >= '0' && c <= '9' }
func toLower(c byte) byte { return c - 'A' + 'a' }
