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

// Package mapper converts raw Akamai SIEM JSON events into OTel semantic
// convention attributes on plog.LogRecord. The mapping is derived from the
// Akamai integration's ingest pipeline
// (integrations/packages/akamai/data_stream/siem/elasticsearch/ingest_pipeline/default.yml).
package mapper // import "github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/mapper"

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// siemEvent is the typed representation of an Akamai SIEM API JSON event.
// All leaf fields are strings in the raw API — numeric conversions happen
// during attribute mapping, not during JSON unmarshaling.
type siemEvent struct {
	HTTPMessage  httpMessage   `json:"httpMessage"`
	AttackData   *attackData   `json:"attackData,omitempty"`
	Geo          *geoData      `json:"geo,omitempty"`
	BotData      *botData      `json:"botData,omitempty"`
	ClientData   *clientData   `json:"clientData,omitempty"`
	UserRiskData *userRiskData `json:"userRiskData,omitempty"`
	Identity     *identity     `json:"identity,omitempty"`
}

type httpMessage struct {
	Start     string `json:"start"`
	Method    string `json:"method"`
	Host      string `json:"host"`
	Path      string `json:"path"`
	Port      string `json:"port"`
	Query     string `json:"query"`
	Protocol  string `json:"protocol"`
	TLS       string `json:"tls"`
	RequestID string `json:"requestId"`
	Status    string `json:"status"`
	Bytes     string `json:"bytes"`
}

type attackData struct {
	ClientIP         string `json:"clientIP"`
	ConfigID         string `json:"configId"`
	PolicyID         string `json:"policyId"`
	AppliedAction    string `json:"appliedAction"`
	SlowPostAction   string `json:"slowPostAction"`
	SlowPostRate     string `json:"slowPostRate"`
	ClientReputation string `json:"clientReputation"`
	Rules            string `json:"rules"`
	RuleActions      string `json:"ruleActions"`
	RuleData         string `json:"ruleData"`
	RuleMessages     string `json:"ruleMessages"`
	RuleSelectors    string `json:"ruleSelectors"`
	RuleTags         string `json:"ruleTags"`
	RuleVersions     string `json:"ruleVersions"`
}

type geoData struct {
	Country    string `json:"country"`
	RegionCode string `json:"regionCode"`
	City       string `json:"city"`
	Continent  string `json:"continent"`
	ASN        string `json:"asn"`
}

type botData struct {
	BotScore        string `json:"botScore"`
	ResponseSegment string `json:"responseSegment"`
}

type clientData struct {
	AppBundleID   string `json:"appBundleId"`
	AppVersion    string `json:"appVersion"`
	SDKVersion    string `json:"sdkVersion"`
	TelemetryType string `json:"telemetryType"`
}

type userRiskData struct {
	UUID    string `json:"uuid"`
	Status  string `json:"status"`
	Score   string `json:"score"`
	Allow   string `json:"allow"`
	Risk    string `json:"risk"`
	Trust   string `json:"trust"`
	General string `json:"general"`
}

type identity struct {
	JA4              string `json:"ja4"`
	TLSFingerprintV2 string `json:"tlsFingerprintV2"`
	TLSFingerprintV3 string `json:"tlsFingerprintV3"`
}

// MapToOTelLog parses rawJSON and populates lr with OTel semantic convention
// attributes. The original raw JSON is preserved in the log body.
//
// Mapping strategy:
//   - Fields with OTel semantic conventions use the standard attribute name
//   - Akamai-specific fields use the "akamai.siem.*" namespace
//   - The raw JSON is always preserved in the body for debugging/reprocessing
func MapToOTelLog(rawJSON string, lr plog.LogRecord) error {
	var event siemEvent
	if err := json.Unmarshal([]byte(rawJSON), &event); err != nil {
		return fmt.Errorf("failed to parse event JSON: %w", err)
	}

	attrs := lr.Attributes()

	// Preserve raw event in body.
	lr.Body().SetStr(rawJSON)

	// Set severity — security events are at minimum informational.
	lr.SetSeverityNumber(plog.SeverityNumberInfo)
	lr.SetSeverityText("INFO")

	// === httpMessage (always present) ===
	mapHTTPMessage(&event.HTTPMessage, lr, attrs)

	// === attackData ===
	if event.AttackData != nil {
		mapAttackData(event.AttackData, attrs, lr)
	}

	// === geo ===
	if event.Geo != nil {
		mapGeo(event.Geo, attrs)
	}

	// === botData ===
	if event.BotData != nil {
		mapBotData(event.BotData, attrs)
	}

	// === clientData ===
	if event.ClientData != nil {
		mapClientData(event.ClientData, attrs)
	}

	// === userRiskData ===
	if event.UserRiskData != nil {
		mapUserRiskData(event.UserRiskData, attrs)
	}

	// === identity ===
	if event.Identity != nil {
		putStr(attrs, "akamai.siem.identity.ja4", event.Identity.JA4)
		putStr(attrs, "akamai.siem.identity.tls_fingerprint_v2", event.Identity.TLSFingerprintV2)
		putStr(attrs, "akamai.siem.identity.tls_fingerprint_v3", event.Identity.TLSFingerprintV3)
	}

	// Static fields matching the ingest pipeline.
	attrs.PutStr("observer.vendor", "akamai")
	attrs.PutStr("observer.type", "proxy")
	attrs.PutStr("event.kind", "event")
	attrs.PutStr("event.category", "network")

	return nil
}

func mapHTTPMessage(msg *httpMessage, lr plog.LogRecord, attrs pcommon.Map) {
	// Timestamp from Unix seconds (supports both integer and float strings).
	if msg.Start != "" {
		if f, err := strconv.ParseFloat(msg.Start, 64); err == nil {
			sec := int64(f)
			nsec := int64((f - float64(sec)) * 1e9)
			lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(sec, nsec)))
		}
	}

	// HTTP semantic conventions.
	putStr(attrs, "http.request.method", msg.Method)

	if msg.Status != "" {
		if code, err := strconv.ParseInt(msg.Status, 10, 64); err == nil {
			attrs.PutInt("http.response.status_code", code)
		}
	}

	if msg.Bytes != "" {
		if b, err := strconv.ParseInt(msg.Bytes, 10, 64); err == nil {
			attrs.PutInt("http.response.body.size", b)
		}
	}

	// URL fields.
	putStr(attrs, "url.domain", msg.Host)

	if msg.Path != "" {
		attrs.PutStr("url.path", urlDecode(msg.Path))
	}
	if msg.Query != "" {
		attrs.PutStr("url.query", urlDecode(msg.Query))
	}

	if msg.Port != "" {
		if p, err := strconv.ParseInt(msg.Port, 10, 64); err == nil {
			attrs.PutInt("server.port", p)
		}
	}

	// Protocol: "HTTP/1.1" → network.protocol.name = "http", network.protocol.version = "1.1"
	if msg.Protocol != "" {
		if parts := strings.SplitN(msg.Protocol, "/", 2); len(parts) == 2 {
			attrs.PutStr("network.protocol.name", strings.ToLower(parts[0]))
			attrs.PutStr("network.protocol.version", parts[1])
		}
		if strings.HasPrefix(strings.ToLower(msg.Protocol), "http") {
			attrs.PutStr("network.transport", "tcp")
		}
	}

	// TLS: "tls1.3" → tls.protocol.name = "tls", tls.protocol.version = "1.3"
	if msg.TLS != "" {
		tls := strings.ToLower(msg.TLS)
		if strings.HasPrefix(tls, "tls") {
			version := strings.TrimPrefix(tls, "tls")
			// Handle both "tls1.3" and "tlsv1.3"
			version = strings.TrimPrefix(version, "v")
			if version != "" {
				attrs.PutStr("tls.protocol.name", "tls")
				attrs.PutStr("tls.protocol.version", version)
			}
		}
	}

	putStr(attrs, "event.id", msg.RequestID)
	putStr(attrs, "http.request.id", msg.RequestID)

	// Build url.full.
	buildURLFull(msg, attrs)
}

func mapAttackData(data *attackData, attrs pcommon.Map, lr plog.LogRecord) {
	// Source IP — OTel semantic convention.
	if data.ClientIP != "" {
		attrs.PutStr("source.address", data.ClientIP)
		attrs.PutStr("source.ip", data.ClientIP)
		// related.ip for correlation.
		relatedIP := attrs.PutEmptySlice("related.ip")
		relatedIP.AppendEmpty().SetStr(data.ClientIP)
	}

	// Akamai-specific attack fields.
	putStr(attrs, "akamai.siem.config_id", data.ConfigID)
	putStr(attrs, "akamai.siem.policy_id", data.PolicyID)
	putStr(attrs, "akamai.siem.client_reputation", data.ClientReputation)
	putStr(attrs, "akamai.siem.slow_post_action", data.SlowPostAction)

	if data.SlowPostRate != "" {
		if rate, err := strconv.ParseInt(data.SlowPostRate, 10, 64); err == nil {
			attrs.PutInt("akamai.siem.slow_post_rate", rate)
		}
	}

	// Applied action affects severity.
	if data.AppliedAction != "" {
		attrs.PutStr("akamai.siem.applied_action", data.AppliedAction)
		mapSeverityFromAction(data.AppliedAction, lr)
	}

	// Rule fields: URL-decode → split on ";" → base64-decode each element.
	ruleFields := []struct {
		val string
		dst string
	}{
		{data.Rules, "akamai.siem.rules"},
		{data.RuleActions, "akamai.siem.rule_actions"},
		{data.RuleData, "akamai.siem.rule_data"},
		{data.RuleMessages, "akamai.siem.rule_messages"},
		{data.RuleTags, "akamai.siem.rule_tags"},
		{data.RuleSelectors, "akamai.siem.rule_selectors"},
		{data.RuleVersions, "akamai.siem.rule_versions"},
	}
	for _, rf := range ruleFields {
		if rf.val != "" {
			decoded := decodeRuleField(rf.val)
			if len(decoded) > 0 {
				sl := attrs.PutEmptySlice(rf.dst)
				for _, v := range decoded {
					sl.AppendEmpty().SetStr(v)
				}
			}
		}
	}
}

func mapGeo(geo *geoData, attrs pcommon.Map) {
	putStr(attrs, "source.geo.country_iso_code", geo.Country)
	putStr(attrs, "source.geo.city_name", geo.City)
	putStr(attrs, "source.geo.continent_code", geo.Continent)

	// Build region_iso_code from country + regionCode (e.g., "BR-SP").
	if geo.Country != "" && geo.RegionCode != "" {
		attrs.PutStr("source.geo.region_iso_code", geo.Country+"-"+geo.RegionCode)
	}

	if geo.ASN != "" {
		if asn, err := strconv.ParseInt(geo.ASN, 10, 64); err == nil {
			attrs.PutInt("source.as.number", asn)
		}
	}
}

func mapBotData(data *botData, attrs pcommon.Map) {
	if data.BotScore != "" {
		if score, err := strconv.ParseInt(data.BotScore, 10, 64); err == nil {
			attrs.PutInt("akamai.siem.bot.score", score)
		}
	}
	if data.ResponseSegment != "" {
		if seg, err := strconv.ParseInt(data.ResponseSegment, 10, 64); err == nil {
			attrs.PutInt("akamai.siem.bot.response_segment", seg)
		}
	}
}

func mapClientData(data *clientData, attrs pcommon.Map) {
	putStr(attrs, "akamai.siem.client_data.app_bundle_id", data.AppBundleID)
	putStr(attrs, "akamai.siem.client_data.app_version", data.AppVersion)
	putStr(attrs, "akamai.siem.client_data.sdk_version", data.SDKVersion)
	if data.TelemetryType != "" {
		if tt, err := strconv.ParseInt(data.TelemetryType, 10, 64); err == nil {
			attrs.PutInt("akamai.siem.client_data.telemetry_type", tt)
		}
	}
}

func mapUserRiskData(data *userRiskData, attrs pcommon.Map) {
	putStr(attrs, "akamai.siem.user_risk.uuid", data.UUID)
	if data.Status != "" {
		if s, err := strconv.ParseInt(data.Status, 10, 64); err == nil {
			attrs.PutInt("akamai.siem.user_risk.status", s)
		}
	}
	if data.Score != "" {
		if s, err := strconv.ParseInt(data.Score, 10, 64); err == nil {
			attrs.PutInt("akamai.siem.user_risk.score", s)
		}
	}
	if data.Allow != "" {
		if a, err := strconv.ParseInt(data.Allow, 10, 64); err == nil {
			attrs.PutInt("akamai.siem.user_risk.allow", a)
		}
	}
	putStr(attrs, "akamai.siem.user_risk.risk", data.Risk)
	putStr(attrs, "akamai.siem.user_risk.trust", data.Trust)
	putStr(attrs, "akamai.siem.user_risk.general", data.General)
}

func mapSeverityFromAction(action string, lr plog.LogRecord) {
	switch strings.ToLower(action) {
	case "deny", "abort":
		lr.SetSeverityNumber(plog.SeverityNumberError)
		lr.SetSeverityText("ERROR")
	case "tarpit", "slow":
		lr.SetSeverityNumber(plog.SeverityNumberWarn)
		lr.SetSeverityText("WARN")
	case "monitor", "alert":
		lr.SetSeverityNumber(plog.SeverityNumberInfo)
		lr.SetSeverityText("INFO")
	}
}

// decodeRuleField performs the same transform as the ingest pipeline:
// 1. URL-decode the whole string
// 2. Split on ";"
// 3. Base64-decode each element
func decodeRuleField(raw string) []string {
	unescaped := urlDecode(raw)
	parts := strings.Split(unescaped, ";")
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		decoded, err := base64.StdEncoding.DecodeString(p)
		if err != nil {
			// Try URL-safe base64.
			decoded, err = base64.RawStdEncoding.DecodeString(p)
			if err != nil {
				// Can't decode — keep raw value like the ingest pipeline does.
				result = append(result, p)
				continue
			}
		}
		result = append(result, string(decoded))
	}
	return result
}

func buildURLFull(msg *httpMessage, attrs pcommon.Map) {
	if msg.Host == "" {
		return
	}
	var sb strings.Builder
	sb.WriteString(msg.Host)
	if msg.Path != "" {
		sb.WriteString(msg.Path)
	}
	if msg.Query != "" {
		sb.WriteString("?")
		sb.WriteString(msg.Query)
	}
	attrs.PutStr("url.full", sb.String())
}

// putStr sets an attribute only if the value is non-empty.
func putStr(attrs pcommon.Map, key, val string) {
	if val != "" {
		attrs.PutStr(key, val)
	}
}

func urlDecode(s string) string {
	decoded, err := url.QueryUnescape(s)
	if err != nil {
		return s // Return raw on failure, like the ingest pipeline.
	}
	return decoded
}
