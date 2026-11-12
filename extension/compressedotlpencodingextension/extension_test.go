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

package compressedotlpencodingextension

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

	"github.com/elastic/opentelemetry-collector-components/extension/compressedotlpencodingextension/internal/metadatatest"
)

func TestExtension_Start(t *testing.T) {
	test := func(t *testing.T, getExtension func() (extension.Extension, error), expectedErr string) {
		ext, err := getExtension()
		if expectedErr != "" && err != nil {
			require.ErrorContains(t, err, expectedErr)
		} else {
			require.NoError(t, err)
		}
		err = ext.Start(context.Background(), componenttest.NewNopHost())
		if expectedErr != "" && err != nil {
			require.ErrorContains(t, err, expectedErr)
		} else {
			require.NoError(t, err)
		}
	}

	for p := protocolMin; p < protocolMax; p++ {
		for c := compressionMin; c < compressionMax; c++ {
			t.Run(fmt.Sprintf("%s-%s", p.String(), c.String()), func(t *testing.T) {
				test(t, func() (extension.Extension, error) {
					factory := NewFactory()
					cfg := factory.CreateDefaultConfig()
					cfg.(*Config).Protocol = p.String()
					cfg.(*Config).Compression = c.String()
					return factory.Create(context.Background(), extensiontest.NewNopSettings(factory.Type()), cfg)
				}, "")
			})
		}
	}
}

func testOTLPMarshal(t *testing.T, ex *compressedOTLPExtension) {
	traces := generateTraces()
	_, err := ex.MarshalTraces(traces)
	require.NoError(t, err)

	logs := generateLogs()
	_, err = ex.MarshalLogs(logs)
	require.NoError(t, err)

	metrics := generateMetrics()
	_, err = ex.MarshalMetrics(metrics)
	require.NoError(t, err)

	profiles := generateProfiles()
	_, err = ex.MarshalProfiles(profiles)
	require.NoError(t, err)
}

func testOTLPUnmarshal(t *testing.T, ex *compressedOTLPExtension) {
	traces := generateTraces()
	logs := generateLogs()
	metrics := generateMetrics()
	profiles := generateProfiles()

	traceBuf, err := ex.MarshalTraces(traces)
	require.NoError(t, err)
	logBuf, err := ex.MarshalLogs(logs)
	require.NoError(t, err)
	metricBuf, err := ex.MarshalMetrics(metrics)
	require.NoError(t, err)
	profileBuf, err := ex.MarshalProfiles(profiles)
	require.NoError(t, err)

	traces0, err := ex.UnmarshalTraces(traceBuf)
	require.NoError(t, err)
	logs0, err := ex.UnmarshalLogs(logBuf)
	require.NoError(t, err)
	metrics0, err := ex.UnmarshalMetrics(metricBuf)
	require.NoError(t, err)
	profiles0, err := ex.UnmarshalProfiles(profileBuf)
	require.NoError(t, err)

	require.Equal(t, traces0.ResourceSpans().Len(), traces.ResourceSpans().Len())
	require.Equal(t, logs0.ResourceLogs().Len(), logs.ResourceLogs().Len())
	require.Equal(t, metrics0.ResourceMetrics().Len(), metrics.ResourceMetrics().Len())
	require.Equal(t, profiles0.ResourceProfiles().Len(), profiles.ResourceProfiles().Len())
}

func testOTLPUnmarshalFallback(t *testing.T, ex *compressedOTLPExtension) {
	traces := generateTraces()
	logs := generateLogs()
	metrics := generateMetrics()
	profiles := generateProfiles()

	traceMarshaler := &ptrace.ProtoMarshaler{}
	traceBuf, err := traceMarshaler.MarshalTraces(traces)
	require.NoError(t, err)

	logMarshaler := &plog.ProtoMarshaler{}
	logBuf, err := logMarshaler.MarshalLogs(logs)
	require.NoError(t, err)

	metricMarshaler := &pmetric.ProtoMarshaler{}
	metricBuf, err := metricMarshaler.MarshalMetrics(metrics)
	require.NoError(t, err)

	profileMarshaler := &pprofile.ProtoMarshaler{}
	profileBuf, err := profileMarshaler.MarshalProfiles(profiles)
	require.NoError(t, err)

	traces0, err := ex.UnmarshalTraces(traceBuf)
	require.NoError(t, err)
	logs0, err := ex.UnmarshalLogs(logBuf)
	require.NoError(t, err)
	metrics0, err := ex.UnmarshalMetrics(metricBuf)
	require.NoError(t, err)
	profiles0, err := ex.UnmarshalProfiles(profileBuf)
	require.NoError(t, err)

	require.Equal(t, traces0.ResourceSpans().Len(), traces.ResourceSpans().Len())
	require.Equal(t, logs0.ResourceLogs().Len(), logs.ResourceLogs().Len())
	require.Equal(t, metrics0.ResourceMetrics().Len(), metrics.ResourceMetrics().Len())
	require.Equal(t, profiles0.ResourceProfiles().Len(), profiles.ResourceProfiles().Len())
}

func assertEncodingMetrics(t *testing.T, tt *componenttest.Telemetry, encodingEnabled bool) {
	var wantCountDps []metricdata.DataPoint[int64]
	for _, sig := range []string{"traces", "logs", "metrics", "profiles"} {
		wantCountDps = append(wantCountDps, metricdata.DataPoint[int64]{
			Value: 1,
			Attributes: attribute.NewSet(
				attribute.String("signal", sig),
				attribute.String("outcome", "success"),
				attribute.Bool("encoding_enabled", encodingEnabled),
			),
		})
	}
	metadatatest.AssertEqualCompressedotlpEncodingCount(t, tt, wantCountDps, metricdatatest.IgnoreTimestamp())

	if encodingEnabled {
		var wantSizeDps []metricdata.DataPoint[int64]
		for _, sig := range []string{"traces", "logs", "metrics", "profiles"} {
			wantSizeDps = append(wantSizeDps, metricdata.DataPoint[int64]{
				Value: 0,
				Attributes: attribute.NewSet(
					attribute.String("signal", sig),
				),
			})
		}
		metadatatest.AssertEqualCompressedotlpEncodingCompressedSize(t, tt, wantSizeDps, metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreValue())
		metadatatest.AssertEqualCompressedotlpEncodingUncompressedSize(t, tt, wantSizeDps, metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreValue())
	}
}

func TestMarshal(t *testing.T) {
	for p := protocolMin; p < protocolMax; p++ {
		for c := compressionMin; c < compressionMax; c++ {
			t.Run(fmt.Sprintf("%s-%s", p.String(), c.String()), func(t *testing.T) {
				conf := createDefaultConfig().(*Config)
				conf.Protocol = p.String()
				conf.Compression = c.String()
				ex, tt := createAndExtension0(t, conf)

				testOTLPMarshal(t, ex)

				assertEncodingMetrics(t, tt, true)
			})
		}
	}
}

func TestMarshalEncodingEnabled(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("%v", enabled), func(t *testing.T) {
			conf := &Config{EncodingEnabled: enabled, Protocol: "otlp_proto", Compression: "none"}
			ex, tt := createAndExtension0(t, conf)

			magic := []byte{0x11, 0x22, 0x00, 0x00}

			traces := generateTraces()
			bytes, err := ex.MarshalTraces(traces)
			require.NoError(t, err)
			if enabled {
				assert.Equal(t, magic, bytes[:4])
			} else {
				assert.NotEqual(t, magic, bytes[:4])
			}

			logs := generateLogs()
			bytes, err = ex.MarshalLogs(logs)
			require.NoError(t, err)
			if enabled {
				assert.Equal(t, magic, bytes[:4])
			} else {
				assert.NotEqual(t, magic, bytes[:4])
			}

			metrics := generateMetrics()
			bytes, err = ex.MarshalMetrics(metrics)
			require.NoError(t, err)
			if enabled {
				assert.Equal(t, magic, bytes[:4])
			} else {
				assert.NotEqual(t, magic, bytes[:4])
			}

			profiles := generateProfiles()
			bytes, err = ex.MarshalProfiles(profiles)
			require.NoError(t, err)
			if enabled {
				assert.Equal(t, magic, bytes[:4])
			} else {
				assert.NotEqual(t, magic, bytes[:4])
			}

			assertEncodingMetrics(t, tt, enabled)
		})
	}
}

func assertDecodingMetrics(t *testing.T, tt *componenttest.Telemetry, fallback bool) {
	magicParsingOutcome := "success"
	if fallback {
		magicParsingOutcome = "failure"
	}
	var wantCountDps []metricdata.DataPoint[int64]
	for _, sig := range []string{"traces", "logs", "metrics", "profiles"} {

		wantCountDps = append(wantCountDps, metricdata.DataPoint[int64]{
			Value: 1,
			Attributes: attribute.NewSet(
				attribute.String("signal", sig),
				attribute.String("outcome", "success"),
				attribute.String("magic_parsing_outcome", magicParsingOutcome),
			),
		})
	}
	metadatatest.AssertEqualCompressedotlpDecodingCount(t, tt, wantCountDps, metricdatatest.IgnoreTimestamp())

	if !fallback {
		var wantSizeDps []metricdata.DataPoint[int64]
		for _, sig := range []string{"traces", "logs", "metrics", "profiles"} {
			wantSizeDps = append(wantSizeDps, metricdata.DataPoint[int64]{
				Value: 0,
				Attributes: attribute.NewSet(
					attribute.String("signal", sig),
				),
			})
		}
		metadatatest.AssertEqualCompressedotlpDecodingCompressedSize(t, tt, wantSizeDps, metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreValue())
		metadatatest.AssertEqualCompressedotlpDecodingUncompressedSize(t, tt, wantSizeDps, metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreValue())
	}
}

func TestUnmarshal(t *testing.T) {
	for p := protocolMin; p < protocolMax; p++ {
		for c := compressionMin; c < compressionMax; c++ {
			t.Run(fmt.Sprintf("%s-%s", p.String(), c.String()), func(t *testing.T) {
				conf := createDefaultConfig().(*Config)
				conf.Protocol = p.String()
				conf.Compression = c.String()
				ex, tt := createAndExtension0(t, conf)

				testOTLPUnmarshal(t, ex)

				assertDecodingMetrics(t, tt, false)
			})
		}
	}
}

func TestUnmarshalFallback(t *testing.T) {
	for p := protocolMin; p < protocolMax; p++ {
		for c := compressionMin; c < compressionMax; c++ {
			t.Run(fmt.Sprintf("%s-%s", p.String(), c.String()), func(t *testing.T) {
				conf := createDefaultConfig().(*Config)
				conf.Protocol = p.String()
				conf.Compression = c.String()
				ex, tt := createAndExtension0(t, conf)

				testOTLPUnmarshalFallback(t, ex)

				assertDecodingMetrics(t, tt, true)
			})
		}
	}
}

// createAndExtension0 Create extension
func createAndExtension0(t *testing.T, c *Config) (*compressedOTLPExtension, *componenttest.Telemetry) {
	set := extensiontest.NewNopSettings(extensiontest.NopType)
	tel := componenttest.NewTelemetry()
	set.TelemetrySettings = tel.NewTelemetrySettings()

	ex, err := newExtension(c, set)
	require.NoError(t, err)
	err = ex.Start(context.TODO(), nil)
	require.NoError(t, err)
	return ex, tel
}

func generateTraces() ptrace.Traces {
	return generateTracesCount(10)
}

func generateTracesCount(count int) ptrace.Traces {
	md := ptrace.NewTraces()
	ilm := md.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	ilm.Spans().EnsureCapacity(count)
	for i := 0; i < count; i++ {
		im := ilm.Spans().AppendEmpty()
		im.SetName(fmt.Sprintf("test_name_%d", i))
		im.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(-10 * time.Second)))
		im.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	}
	return md
}

func generateLogs() plog.Logs {
	return generateLogsCount(10)
}

func generateLogsCount(count int) plog.Logs {
	md := plog.NewLogs()
	ilm := md.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	ilm.LogRecords().EnsureCapacity(count)
	for i := 0; i < count; i++ {
		im := ilm.LogRecords().AppendEmpty()
		im.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		im.Body().SetStr(fmt.Sprintf("test_name_%d", i))
	}
	return md
}

func generateMetrics() pmetric.Metrics {
	return generateMetricsCount(10)
}

func generateMetricsCount(count int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	ilm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	ilm.Metrics().EnsureCapacity(count)
	for i := 0; i < count; i++ {
		im := ilm.Metrics().AppendEmpty()
		im.SetName(fmt.Sprintf("test_name_%d", i))
		idp := im.SetEmptySum().DataPoints().AppendEmpty()
		idp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(-10 * time.Second)))
		idp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		idp.SetIntValue(123)
	}
	return md
}

func generateProfiles() pprofile.Profiles {
	return generateProfilesCount(10)
}

func generateProfilesCount(count int) pprofile.Profiles {
	pd := pprofile.NewProfiles()
	ilm := pd.ResourceProfiles().AppendEmpty().ScopeProfiles().AppendEmpty()
	ilm.Profiles().EnsureCapacity(count)
	for i := 0; i < count; i++ {
		im := ilm.Profiles().AppendEmpty()
		im.SetProfileID([16]byte{0x01, 0x02, 0x03, 0x04})
		im.SetTime(pcommon.NewTimestampFromTime(time.Now()))
		im.SetDurationNano(uint64(time.Second.Nanoseconds()))
	}
	return pd
}
