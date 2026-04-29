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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

const testEventFull = `{
	"attackData": {
		"appliedAction": "deny",
		"clientIP": "198.51.100.1",
		"configId": "67217",
		"policyId": "PNWD_110088",
		"ruleActions": "bW9uaXRvcg%3d%3d%3bbW9uaXRvcg%3d%3d",
		"ruleMessages": "TWlzc2luZyBDb29raWUgSGVhZGVy%3bTm9uLVBlcnNpc3RlbnQgSFRUUCBDb25uZWN0aW9u",
		"rules": "MzkwNDAwNg%3d%3d%3bMzkwNDAwNw%3d%3d",
		"ruleTags": "QUtBTUFJL0JPVC9CSUQ%3d%3bQUtBTUFJL0JPVC9CSUQ%3d",
		"ruleData": "",
		"ruleSelectors": "",
		"clientReputation": "high"
	},
	"geo": {
		"asn": "28573",
		"city": "SOROCABA",
		"continent": "SA",
		"country": "BR",
		"regionCode": "SP"
	},
	"httpMessage": {
		"bytes": "1234",
		"host": "example.com",
		"method": "GET",
		"path": "/api/test%20path",
		"port": "443",
		"protocol": "HTTP/1.1",
		"query": "q%3Dtest%26page%3D1",
		"requestId": "f3fe4c34",
		"start": "1762365006",
		"status": "403",
		"tls": "tls1.3"
	},
	"botData": {
		"botScore": "85",
		"responseSegment": "3"
	},
	"clientData": {
		"appBundleId": "com.example.app",
		"appVersion": "2.1.0",
		"sdkVersion": "3.0.1",
		"telemetryType": "2"
	},
	"userRiskData": {
		"uuid": "abc-123",
		"status": "0",
		"score": "75",
		"allow": "0",
		"risk": "udfp:1234",
		"trust": "ugp:US",
		"general": "duc_1h:5"
	},
	"identity": {
		"ja4": "t13d131000_f57a46bbacb6_e7c285222651",
		"tlsFingerprintV2": "c6a4ce37a8dc4153"
	},
	"type": "akamai_siem",
	"version": "1.0"
}`

func TestMapToOTelLog_FullEvent(t *testing.T) {
	lr := plog.NewLogRecord()
	err := MapToOTelLog(testEventFull, lr)
	require.NoError(t, err)

	attrs := lr.Attributes()

	// Timestamp from httpMessage.start.
	assert.Equal(t, int64(1762365006000000000), int64(lr.Timestamp()))

	// HTTP semantic conventions.
	assertStr(t, attrs, "http.request.method", "GET")
	assertInt(t, attrs, "http.response.status_code", 403)
	assertInt(t, attrs, "http.response.body.size", 1234)
	assertStr(t, attrs, "event.id", "f3fe4c34")
	assertStr(t, attrs, "http.request.id", "f3fe4c34")

	// URL fields.
	assertStr(t, attrs, "url.domain", "example.com")
	assertStr(t, attrs, "url.path", "/api/test path") // URL-decoded
	assertStr(t, attrs, "url.query", "q=test&page=1") // URL-decoded
	assertInt(t, attrs, "server.port", 443)

	// Protocol.
	assertStr(t, attrs, "network.protocol.name", "http")
	assertStr(t, attrs, "network.protocol.version", "1.1")
	assertStr(t, attrs, "network.transport", "tcp")

	// TLS.
	assertStr(t, attrs, "tls.protocol.name", "tls")
	assertStr(t, attrs, "tls.protocol.version", "1.3")

	// Source IP.
	assertStr(t, attrs, "source.address", "198.51.100.1")
	assertStr(t, attrs, "source.ip", "198.51.100.1")

	// Geo.
	assertStr(t, attrs, "source.geo.country_iso_code", "BR")
	assertStr(t, attrs, "source.geo.city_name", "SOROCABA")
	assertStr(t, attrs, "source.geo.continent_code", "SA")
	assertStr(t, attrs, "source.geo.region_iso_code", "BR-SP")
	assertInt(t, attrs, "source.as.number", 28573)

	// Attack data.
	assertStr(t, attrs, "akamai.siem.config_id", "67217")
	assertStr(t, attrs, "akamai.siem.policy_id", "PNWD_110088")
	assertStr(t, attrs, "akamai.siem.client_reputation", "high")
	assertStr(t, attrs, "akamai.siem.applied_action", "deny")

	// Severity from "deny" action.
	assert.Equal(t, plog.SeverityNumberError, lr.SeverityNumber())
	assert.Equal(t, "ERROR", lr.SeverityText())

	// Rules: URL-decode → split ";" → base64-decode.
	rulesSlice, ok := attrs.Get("akamai.siem.rules")
	require.True(t, ok, "akamai.siem.rules missing")
	assert.Equal(t, 2, rulesSlice.Slice().Len())
	assert.Equal(t, "3904006", rulesSlice.Slice().At(0).Str())
	assert.Equal(t, "3904007", rulesSlice.Slice().At(1).Str())

	ruleTagsSlice, ok := attrs.Get("akamai.siem.rule_tags")
	require.True(t, ok, "akamai.siem.rule_tags missing")
	assert.Equal(t, 2, ruleTagsSlice.Slice().Len())
	assert.Equal(t, "AKAMAI/BOT/BID", ruleTagsSlice.Slice().At(0).Str())

	ruleMessagesSlice, ok := attrs.Get("akamai.siem.rule_messages")
	require.True(t, ok, "akamai.siem.rule_messages missing")
	assert.Equal(t, 2, ruleMessagesSlice.Slice().Len())
	assert.Equal(t, "Missing Cookie Header", ruleMessagesSlice.Slice().At(0).Str())
	assert.Equal(t, "Non-Persistent HTTP Connection", ruleMessagesSlice.Slice().At(1).Str())

	// Bot data.
	assertInt(t, attrs, "akamai.siem.bot.score", 85)
	assertInt(t, attrs, "akamai.siem.bot.response_segment", 3)

	// Client data.
	assertStr(t, attrs, "akamai.siem.client_data.app_bundle_id", "com.example.app")
	assertStr(t, attrs, "akamai.siem.client_data.app_version", "2.1.0")
	assertStr(t, attrs, "akamai.siem.client_data.sdk_version", "3.0.1")
	assertInt(t, attrs, "akamai.siem.client_data.telemetry_type", 2)

	// User risk data.
	assertStr(t, attrs, "akamai.siem.user_risk.uuid", "abc-123")
	assertInt(t, attrs, "akamai.siem.user_risk.status", 0)
	assertInt(t, attrs, "akamai.siem.user_risk.score", 75)
	assertInt(t, attrs, "akamai.siem.user_risk.allow", 0)
	assertStr(t, attrs, "akamai.siem.user_risk.risk", "udfp:1234")
	assertStr(t, attrs, "akamai.siem.user_risk.trust", "ugp:US")

	// Identity.
	assertStr(t, attrs, "akamai.siem.identity.ja4", "t13d131000_f57a46bbacb6_e7c285222651")

	// Static fields.
	assertStr(t, attrs, "observer.vendor", "akamai")
	assertStr(t, attrs, "observer.type", "proxy")
	assertStr(t, attrs, "event.kind", "event")
	assertStr(t, attrs, "event.category", "network")

	// Body preserves raw JSON.
	assert.Contains(t, lr.Body().Str(), `"clientIP"`)

	// related.ip populated.
	relatedIP, ok := attrs.Get("related.ip")
	require.True(t, ok)
	assert.Equal(t, "198.51.100.1", relatedIP.Slice().At(0).Str())
}

func TestMapToOTelLog_SeverityMapping(t *testing.T) {
	tests := []struct {
		action   string
		severity plog.SeverityNumber
		text     string
	}{
		{"deny", plog.SeverityNumberError, "ERROR"},
		{"abort", plog.SeverityNumberError, "ERROR"},
		{"tarpit", plog.SeverityNumberWarn, "WARN"},
		{"slow", plog.SeverityNumberWarn, "WARN"},
		{"monitor", plog.SeverityNumberInfo, "INFO"},
		{"alert", plog.SeverityNumberInfo, "INFO"},
	}
	for _, tt := range tests {
		t.Run(tt.action, func(t *testing.T) {
			event := `{"attackData":{"appliedAction":"` + tt.action + `","clientIP":"1.2.3.4"},"httpMessage":{"start":"1000"}}`
			lr := plog.NewLogRecord()
			require.NoError(t, MapToOTelLog(event, lr))
			assert.Equal(t, tt.severity, lr.SeverityNumber())
			assert.Equal(t, tt.text, lr.SeverityText())
		})
	}
}

func TestMapToOTelLog_MinimalEvent(t *testing.T) {
	event := `{"httpMessage":{"start":"1762365006","host":"test.com","method":"POST","status":"200"}}`
	lr := plog.NewLogRecord()
	require.NoError(t, MapToOTelLog(event, lr))

	attrs := lr.Attributes()
	assertStr(t, attrs, "http.request.method", "POST")
	assertInt(t, attrs, "http.response.status_code", 200)
	assertStr(t, attrs, "url.domain", "test.com")

	// No attack data fields should exist.
	_, ok := attrs.Get("source.ip")
	assert.False(t, ok)
	_, ok = attrs.Get("akamai.siem.config_id")
	assert.False(t, ok)
}

func TestMapToOTelLog_MissingTimestamp(t *testing.T) {
	// 500 errors from Akamai may not include a timestamp.
	event := `{"httpMessage":{"host":"test.com","status":"500"}}`
	lr := plog.NewLogRecord()
	require.NoError(t, MapToOTelLog(event, lr))
	// Timestamp should be zero (not set).
	assert.Equal(t, int64(0), int64(lr.Timestamp()))
}

func TestMapToOTelLog_FloatTimestamp(t *testing.T) {
	// Akamai API can return sub-second timestamps like "1470923133.026".
	event := `{"httpMessage":{"start":"1470923133.026","host":"test.com","method":"GET","status":"200"}}`
	lr := plog.NewLogRecord()
	require.NoError(t, MapToOTelLog(event, lr))
	ts := lr.Timestamp().AsTime()
	assert.Equal(t, int64(1470923133), ts.Unix())
	// Float64 precision means .026 may not be exactly 26000000 ns.
	assert.InDelta(t, 26000000, ts.Nanosecond(), 100) // within 100ns of .026s
}

func TestMapToOTelLog_InvalidJSON(t *testing.T) {
	lr := plog.NewLogRecord()
	err := MapToOTelLog(`{invalid`, lr)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse event JSON")
}

func TestMapToOTelLog_EmptyRuleFields(t *testing.T) {
	event := `{"attackData":{"rules":"","ruleActions":"","clientIP":"1.2.3.4"},"httpMessage":{"start":"1000"}}`
	lr := plog.NewLogRecord()
	require.NoError(t, MapToOTelLog(event, lr))

	attrs := lr.Attributes()
	_, ok := attrs.Get("akamai.siem.rules")
	assert.False(t, ok, "empty rules should not create attribute")
}

func TestMapToOTelLog_ProtocolParsing(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
		wantName string
		wantVer  string
	}{
		{"HTTP/1.1", "HTTP/1.1", "http", "1.1"},
		{"HTTP/2", "HTTP/2", "http", "2"},
		{"no slash", "HTTP", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := `{"httpMessage":{"protocol":"` + tt.protocol + `","start":"1000"}}`
			lr := plog.NewLogRecord()
			require.NoError(t, MapToOTelLog(event, lr))
			attrs := lr.Attributes()
			if tt.wantName != "" {
				assertStr(t, attrs, "network.protocol.name", tt.wantName)
				assertStr(t, attrs, "network.protocol.version", tt.wantVer)
			} else {
				_, ok := attrs.Get("network.protocol.name")
				assert.False(t, ok)
			}
		})
	}
}

func TestMapToOTelLog_TLSParsing(t *testing.T) {
	tests := []struct {
		name    string
		tls     string
		wantVer string
	}{
		{"tls1.3", "tls1.3", "1.3"},
		{"tlsv1.2", "tlsv1.2", "1.2"},
		{"TLS1.3 uppercase", "TLS1.3", "1.3"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := `{"httpMessage":{"tls":"` + tt.tls + `","start":"1000"}}`
			lr := plog.NewLogRecord()
			require.NoError(t, MapToOTelLog(event, lr))
			attrs := lr.Attributes()
			if tt.wantVer != "" {
				assertStr(t, attrs, "tls.protocol.version", tt.wantVer)
			} else {
				_, ok := attrs.Get("tls.protocol.version")
				assert.False(t, ok)
			}
		})
	}
}

func TestDecodeRuleField(t *testing.T) {
	// "MzkwNDAwNg==" is base64 for "3904006"
	// "MzkwNDAwNw==" is base64 for "3904007"
	// URL-encoded: "MzkwNDAwNg%3d%3d%3bMzkwNDAwNw%3d%3d"
	input := "MzkwNDAwNg%3d%3d%3bMzkwNDAwNw%3d%3d"
	result := decodeRuleField(input)
	require.Len(t, result, 2)
	assert.Equal(t, "3904006", result[0])
	assert.Equal(t, "3904007", result[1])
}

func TestDecodeRuleField_Empty(t *testing.T) {
	assert.Nil(t, decodeRuleField(""))
}

func TestDecodeRuleField_InvalidBase64(t *testing.T) {
	// If base64 decode fails, raw value is kept.
	result := decodeRuleField("not-base64")
	require.Len(t, result, 1)
	assert.Equal(t, "not-base64", result[0])
}

func TestURLDecode(t *testing.T) {
	assert.Equal(t, "/api/test path", urlDecode("/api/test%20path"))
	assert.Equal(t, "q=test&page=1", urlDecode("q%3Dtest%26page%3D1"))
	// Invalid sequence returns raw.
	assert.Equal(t, "%zz", urlDecode("%zz"))
}

// --- test helpers ---

func assertStr(t *testing.T, attrs pcommon.Map, key, want string) {
	t.Helper()
	v, ok := attrs.Get(key)
	require.True(t, ok, "attribute %q missing", key)
	assert.Equal(t, want, v.Str(), "attribute %q", key)
}

func assertInt(t *testing.T, attrs pcommon.Map, key string, want int64) {
	t.Helper()
	v, ok := attrs.Get(key)
	require.True(t, ok, "attribute %q missing", key)
	assert.Equal(t, want, v.Int(), "attribute %q", key)
}
