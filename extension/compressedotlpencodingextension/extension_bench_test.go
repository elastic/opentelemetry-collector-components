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

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func BenchmarkMarshal(b *testing.B) {
	for p := protocolMin; p < protocolMax; p++ {
		for c := compressionMin; c < compressionMax; c++ {
			for _, signal := range []string{"traces", "logs", "metrics", "profiles"} {
				b.Run(fmt.Sprintf("%s-%s-%s", p.String(), c.String(), signal), func(b *testing.B) {
					set := extensiontest.NewNopSettings(extensiontest.NopType)
					set.ID = component.NewID(component.MustNewType("compressedotlpencoding"))
					conf := &Config{EncodingEnabled: true, Protocol: p.String(), Compression: c.String()}
					ex, err := newExtension(conf, set)
					require.NoError(b, err)
					err = ex.Start(context.TODO(), nil)
					require.NoError(b, err)

					b.ReportAllocs()
					switch signal {
					case "traces":
						traces := generateTraces()
						_, err := ex.MarshalTraces(traces)
						require.NoError(b, err)
						b.ResetTimer()
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								if _, err := ex.MarshalTraces(traces); err != nil {
									b.Error(err)
									return
								}
							}
						})
					case "logs":
						logs := generateLogs()
						_, err := ex.MarshalLogs(logs)
						require.NoError(b, err)
						b.ResetTimer()
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								if _, err := ex.MarshalLogs(logs); err != nil {
									b.Error(err)
									return
								}
							}
						})
					case "metrics":
						metrics := generateMetrics()
						_, err := ex.MarshalMetrics(metrics)
						require.NoError(b, err)
						b.ResetTimer()
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								if _, err := ex.MarshalMetrics(metrics); err != nil {
									b.Error(err)
									return
								}
							}
						})
					case "profiles":
						profiles := generateProfiles()
						_, err := ex.MarshalProfiles(profiles)
						require.NoError(b, err)
						b.ResetTimer()
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								if _, err := ex.MarshalProfiles(profiles); err != nil {
									b.Error(err)
									return
								}
							}
						})
					}
				})
			}

		}
	}
	for _, signal := range []string{"traces", "logs", "metrics", "profiles"} {
		b.Run(fmt.Sprintf("baseline-%s", signal), func(b *testing.B) {
			b.ReportAllocs()
			switch signal {
			case "traces":
				traces := generateTraces()
				m := &ptrace.ProtoMarshaler{}
				_, err := m.MarshalTraces(traces)
				require.NoError(b, err)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if _, err := m.MarshalTraces(traces); err != nil {
							b.Error(err)
							return
						}
					}
				})
			case "logs":
				logs := generateLogs()
				m := &plog.ProtoMarshaler{}
				_, err := m.MarshalLogs(logs)
				require.NoError(b, err)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if _, err := m.MarshalLogs(logs); err != nil {
							b.Error(err)
							return
						}
					}
				})
			case "metrics":
				metrics := generateMetrics()
				m := &pmetric.ProtoMarshaler{}
				_, err := m.MarshalMetrics(metrics)
				require.NoError(b, err)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if _, err := m.MarshalMetrics(metrics); err != nil {
							b.Error(err)
							return
						}
					}
				})
			case "profiles":
				profiles := generateProfiles()
				m := &pprofile.ProtoMarshaler{}
				_, err := m.MarshalProfiles(profiles)
				require.NoError(b, err)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if _, err := m.MarshalProfiles(profiles); err != nil {
							b.Error(err)
							return
						}
					}
				})
			}
		})
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	for p := protocolMin; p < protocolMax; p++ {
		for c := compressionMin; c < compressionMax; c++ {
			for _, signal := range []string{"traces", "logs", "metrics", "profiles"} {
				b.Run(fmt.Sprintf("%s-%s-%s", p.String(), c.String(), signal), func(b *testing.B) {
					set := extensiontest.NewNopSettings(extensiontest.NopType)
					set.ID = component.NewID(component.MustNewType("compressedotlpencoding"))
					conf := &Config{EncodingEnabled: true, Protocol: p.String(), Compression: c.String()}
					ex, err := newExtension(conf, set)
					require.NoError(b, err)
					err = ex.Start(context.TODO(), nil)
					require.NoError(b, err)

					b.ReportAllocs()
					switch signal {
					case "traces":
						traces := generateTracesCount(100)
						bytes, err := ex.MarshalTraces(traces)
						require.NoError(b, err)
						_, err = ex.UnmarshalTraces(bytes)
						require.NoError(b, err)
						b.ResetTimer()
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								if _, err := ex.UnmarshalTraces(bytes); err != nil {
									b.Error(err)
									return
								}
							}
						})
					case "logs":
						logs := generateLogsCount(100)
						bytes, err := ex.MarshalLogs(logs)
						require.NoError(b, err)
						_, err = ex.UnmarshalLogs(bytes)
						require.NoError(b, err)
						b.ResetTimer()
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								if _, err := ex.UnmarshalLogs(bytes); err != nil {
									b.Error(err)
									return
								}
							}
						})
					case "metrics":
						metrics := generateMetricsCount(100)
						bytes, err := ex.MarshalMetrics(metrics)
						require.NoError(b, err)
						_, err = ex.UnmarshalMetrics(bytes)
						require.NoError(b, err)
						b.ResetTimer()
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								if _, err := ex.UnmarshalMetrics(bytes); err != nil {
									b.Error(err)
									return
								}
							}
						})
					case "profiles":
						profiles := generateProfilesCount(100)
						bytes, err := ex.MarshalProfiles(profiles)
						require.NoError(b, err)
						_, err = ex.UnmarshalProfiles(bytes)
						require.NoError(b, err)
						b.ResetTimer()
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								if _, err := ex.UnmarshalProfiles(bytes); err != nil {
									b.Error(err)
									return
								}
							}
						})
					}
				})
			}

		}
	}

	for _, signal := range []string{"traces", "logs", "metrics", "profiles"} {
		b.Run(fmt.Sprintf("baseline-%s", signal), func(b *testing.B) {
			b.ReportAllocs()
			switch signal {
			case "traces":
				traces := generateTraces()
				m := &ptrace.ProtoMarshaler{}
				bytes, err := m.MarshalTraces(traces)
				require.NoError(b, err)
				um := &ptrace.ProtoUnmarshaler{}
				_, err = um.UnmarshalTraces(bytes)
				require.NoError(b, err)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if _, err := um.UnmarshalTraces(bytes); err != nil {
							b.Error(err)
							return
						}
					}
				})
			case "logs":
				logs := generateLogs()
				m := &plog.ProtoMarshaler{}
				bytes, err := m.MarshalLogs(logs)
				require.NoError(b, err)
				um := &plog.ProtoUnmarshaler{}
				_, err = um.UnmarshalLogs(bytes)
				require.NoError(b, err)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if _, err := um.UnmarshalLogs(bytes); err != nil {
							b.Error(err)
							return
						}
					}
				})
			case "metrics":
				metrics := generateMetrics()
				m := &pmetric.ProtoMarshaler{}
				bytes, err := m.MarshalMetrics(metrics)
				require.NoError(b, err)
				um := &pmetric.ProtoUnmarshaler{}
				_, err = um.UnmarshalMetrics(bytes)
				require.NoError(b, err)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if _, err := um.UnmarshalMetrics(bytes); err != nil {
							b.Error(err)
							return
						}
					}
				})
			case "profiles":
				profiles := generateProfiles()
				m := &pprofile.ProtoMarshaler{}
				bytes, err := m.MarshalProfiles(profiles)
				require.NoError(b, err)
				um := &pprofile.ProtoUnmarshaler{}
				_, err = um.UnmarshalProfiles(bytes)
				require.NoError(b, err)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if _, err := um.UnmarshalProfiles(bytes); err != nil {
							b.Error(err)
							return
						}
					}
				})
			}
		})
	}
}

func BenchmarkMarshalRealisticPayloads(b *testing.B) {
	ex := newBenchmarkExtension(b, otlpProto.String(), compressionZSTD.String())

	logs := generateHighCardinalityLogs(2000, 12)
	logsRaw, err := (&plog.ProtoMarshaler{}).MarshalLogs(logs)
	require.NoError(b, err)

	metrics := generateHighCardinalityMetrics(200, 5, 12)
	metricsRaw, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(metrics)
	require.NoError(b, err)

	b.Run("logs_2000x12attrs", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(logsRaw)))
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := ex.MarshalLogs(logs); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})

	b.Run("metrics_200x5x12attrs", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(metricsRaw)))
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := ex.MarshalMetrics(metrics); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}

func BenchmarkUnmarshalRealisticPayloads(b *testing.B) {
	ex := newBenchmarkExtension(b, otlpProto.String(), compressionZSTD.String())

	logs := generateHighCardinalityLogs(2000, 12)
	logsEncoded, err := ex.MarshalLogs(logs)
	require.NoError(b, err)
	logsRaw, err := (&plog.ProtoMarshaler{}).MarshalLogs(logs)
	require.NoError(b, err)

	metrics := generateHighCardinalityMetrics(200, 5, 12)
	metricsEncoded, err := ex.MarshalMetrics(metrics)
	require.NoError(b, err)
	metricsRaw, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(metrics)
	require.NoError(b, err)

	b.Run("logs_2000x12attrs", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(logsRaw)))
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := ex.UnmarshalLogs(logsEncoded); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})

	b.Run("metrics_200x5x12attrs", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(metricsRaw)))
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := ex.UnmarshalMetrics(metricsEncoded); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}

func newBenchmarkExtension(b *testing.B, protocol, compression string) *compressedOTLPExtension {
	b.Helper()

	set := extensiontest.NewNopSettings(extensiontest.NopType)
	set.ID = component.NewID(component.MustNewType("compressedotlpencoding"))
	conf := &Config{EncodingEnabled: true, Protocol: protocol, Compression: compression}
	ex, err := newExtension(conf, set)
	require.NoError(b, err)
	require.NoError(b, ex.Start(context.Background(), nil))
	b.Cleanup(func() {
		require.NoError(b, ex.Shutdown(context.Background()))
	})
	return ex
}

func generateHighCardinalityLogs(recordCount, attrsPerRecord int) plog.Logs {
	now := pcommon.NewTimestampFromTime(time.Now())

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	resourceAttrs := rl.Resource().Attributes()
	resourceAttrs.PutStr("service.name", "bench-ingest")
	resourceAttrs.PutStr("deployment.environment", "prod")
	resourceAttrs.PutStr("cloud.region", "gcp-us-central1")

	sl := rl.ScopeLogs().AppendEmpty()
	records := sl.LogRecords()
	records.EnsureCapacity(recordCount)
	for i := 0; i < recordCount; i++ {
		record := records.AppendEmpty()
		record.SetTimestamp(now)
		record.Body().SetStr(fmt.Sprintf("bench-log-%d", i))

		attrs := record.Attributes()
		attrs.EnsureCapacity(attrsPerRecord)
		for j := 0; j < attrsPerRecord; j++ {
			key := fmt.Sprintf("attr_%d", j)
			if j%2 == 0 {
				attrs.PutStr(key, fmt.Sprintf("value-%d-%d", i, j))
				continue
			}
			attrs.PutInt(key, int64(i+j))
		}
	}
	return logs
}

func generateHighCardinalityMetrics(metricCount, datapointsPerMetric, attrsPerPoint int) pmetric.Metrics {
	now := pcommon.NewTimestampFromTime(time.Now())

	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	resourceAttrs := rm.Resource().Attributes()
	resourceAttrs.PutStr("service.name", "bench-ingest")
	resourceAttrs.PutStr("deployment.environment", "prod")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Metrics().EnsureCapacity(metricCount)
	for i := 0; i < metricCount; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName(fmt.Sprintf("bench_metric_%d", i))
		sum := m.SetEmptySum()
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		sum.SetIsMonotonic(true)

		dps := sum.DataPoints()
		dps.EnsureCapacity(datapointsPerMetric)
		for j := 0; j < datapointsPerMetric; j++ {
			dp := dps.AppendEmpty()
			dp.SetStartTimestamp(now)
			dp.SetTimestamp(now)
			dp.SetIntValue(int64(j))

			attrs := dp.Attributes()
			attrs.EnsureCapacity(attrsPerPoint)
			for k := 0; k < attrsPerPoint; k++ {
				key := fmt.Sprintf("label_%d", k)
				if k%2 == 0 {
					attrs.PutInt(key, int64(i+j+k))
					continue
				}
				attrs.PutStr(key, fmt.Sprintf("v-%d-%d-%d", i, j, k))
			}
		}
	}
	return metrics
}
