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
	"bytes"
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"

	"github.com/elastic/opentelemetry-collector-components/extension/compressedotlpencodingextension/internal/metadata"
)

// signal and outcome index constants for pre-computed attribute.Set tables
const (
	signalIdxTraces   = 0
	signalIdxLogs     = 1
	signalIdxMetrics  = 2
	signalIdxProfiles = 3

	outcomeIdxSuccess = 0
	outcomeIdxFailure = 1

	magicIdxSuccess = 0 // magic_parsing_outcome=success
	magicIdxFailure = 1 // magic_parsing_outcome=failure
)

type protocol byte

const (
	otlpProto protocol = iota
	otlpJSON
	protocolMax

	protocolMin = otlpProto
)

func (f protocol) String() string {
	switch f {
	case otlpProto:
		return "otlp_proto"
	case otlpJSON:
		return "otlp_json"
	default:
		return "unknown"
	}
}

func (f protocol) Valid() bool {
	return f >= protocolMin && f < protocolMax
}

type compression byte

const (
	compressionNone compression = iota
	compressionLZ4
	compressionZSTD
	compressionMax

	compressionMin = compressionNone
)

func (c compression) String() string {
	switch c {
	case compressionNone:
		return "none"
	case compressionLZ4:
		return "lz4"
	case compressionZSTD:
		return "zstd"
	default:
		return "unknown"
	}
}

func (c compression) Valid() bool {
	return c >= compressionMin && c < compressionMax
}

var (
	_ encoding.TracesMarshalerExtension     = (*compressedOTLPExtension)(nil)
	_ encoding.TracesUnmarshalerExtension   = (*compressedOTLPExtension)(nil)
	_ encoding.LogsMarshalerExtension       = (*compressedOTLPExtension)(nil)
	_ encoding.LogsUnmarshalerExtension     = (*compressedOTLPExtension)(nil)
	_ encoding.MetricsMarshalerExtension    = (*compressedOTLPExtension)(nil)
	_ encoding.MetricsUnmarshalerExtension  = (*compressedOTLPExtension)(nil)
	_ encoding.ProfilesMarshalerExtension   = (*compressedOTLPExtension)(nil)
	_ encoding.ProfilesUnmarshalerExtension = (*compressedOTLPExtension)(nil)
)

type compressedOTLPExtension struct {
	config           *Config
	magicBytes       []byte
	compressor       compressor
	zstdCompressor   *ZSTDCompressor
	traceMarshaler   ptrace.Marshaler
	logMarshaler     plog.Marshaler
	metricMarshaler  pmetric.Marshaler
	profileMarshaler pprofile.Marshaler
	tb               *metadata.TelemetryBuilder

	// pre-computed per-signal attribute.Set for size metrics; avoids attribute.NewSet on every call
	tracesSignalAttrSet   attribute.Set
	logsSignalAttrSet     attribute.Set
	metricsSignalAttrSet  attribute.Set
	profilesSignalAttrSet attribute.Set

	// pre-computed attribute.Set for encoding count metric: [signalIdx][outcomeIdx]
	marshalCountAttrSets [4][2]attribute.Set
	// pre-computed attribute.Set for decoding count metric: [signalIdx][outcomeIdx][magicIdx]
	unmarshalCountAttrSets [4][2][2]attribute.Set

	// pre-computed metric.MeasurementOption for count metrics; avoids per-call interface boxing of withAttributeSet
	marshalCountMeasureOpts   [4][2]metric.MeasurementOption
	unmarshalCountMeasureOpts [4][2][2]metric.MeasurementOption
	// pre-computed metric.MeasurementOption for size metrics
	signalSizeMeasureOpts [4]metric.MeasurementOption
}

func newExtension(config *Config, set extension.Settings) (*compressedOTLPExtension, error) {
	var ex *compressedOTLPExtension
	var magic Magic

	zstdCompressor, err := NewZSTDCompressor()
	if err != nil {
		return nil, err
	}

	switch config.Protocol {
	case otlpProto.String():
		ex = &compressedOTLPExtension{
			config:           config,
			zstdCompressor:   zstdCompressor,
			traceMarshaler:   &ptrace.ProtoMarshaler{},
			logMarshaler:     &plog.ProtoMarshaler{},
			metricMarshaler:  &pmetric.ProtoMarshaler{},
			profileMarshaler: &pprofile.ProtoMarshaler{},
		}
		magic.protocol = otlpProto
	case otlpJSON.String():
		ex = &compressedOTLPExtension{
			config:           config,
			zstdCompressor:   zstdCompressor,
			traceMarshaler:   &ptrace.JSONMarshaler{},
			logMarshaler:     &plog.JSONMarshaler{},
			metricMarshaler:  &pmetric.JSONMarshaler{},
			profileMarshaler: &pprofile.JSONMarshaler{},
		}
		magic.protocol = otlpJSON
	default:
		return nil, fmt.Errorf("unsupported protocol: %q", config.Protocol)
	}

	switch config.Compression {
	case compressionZSTD.String():
		c, err := ex.compressorForCompression(compressionZSTD)
		if err != nil {
			return nil, err
		}
		ex.compressor = c
		magic.compression = compressionZSTD
	case compressionLZ4.String():
		ex.compressor = LZ4Compressor{}
		magic.compression = compressionLZ4
	case compressionNone.String():
		ex.compressor = NoneCompressor{}
		magic.compression = compressionNone
	default:
		return nil, fmt.Errorf("unsupported compression: %q", config.Compression)
	}

	ex.magicBytes = magic.ToBytes()

	tb, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	ex.tb = tb

	ex.tracesSignalAttrSet = attribute.NewSet(attribute.String("signal", "traces"))
	ex.logsSignalAttrSet = attribute.NewSet(attribute.String("signal", "logs"))
	ex.metricsSignalAttrSet = attribute.NewSet(attribute.String("signal", "metrics"))
	ex.profilesSignalAttrSet = attribute.NewSet(attribute.String("signal", "profiles"))

	sigNames := [4]string{"traces", "logs", "metrics", "profiles"}
	sigAttrSets := [4]attribute.Set{ex.tracesSignalAttrSet, ex.logsSignalAttrSet, ex.metricsSignalAttrSet, ex.profilesSignalAttrSet}
	outcomes := [2]string{"success", "failure"}
	magics := [2]string{"success", "failure"}
	for i, sig := range sigNames {
		ex.signalSizeMeasureOpts[i] = metric.WithAttributeSet(sigAttrSets[i])
		for j, out := range outcomes {
			ex.marshalCountAttrSets[i][j] = attribute.NewSet(
				attribute.String("signal", sig),
				attribute.String("outcome", out),
				attribute.Bool("encoding_enabled", config.EncodingEnabled),
			)
			ex.marshalCountMeasureOpts[i][j] = metric.WithAttributeSet(ex.marshalCountAttrSets[i][j])
			for k, magic := range magics {
				ex.unmarshalCountAttrSets[i][j][k] = attribute.NewSet(
					attribute.String("signal", sig),
					attribute.String("outcome", out),
					attribute.String("magic_parsing_outcome", magic),
				)
				ex.unmarshalCountMeasureOpts[i][j][k] = metric.WithAttributeSet(ex.unmarshalCountAttrSets[i][j][k])
			}
		}
	}

	return ex, nil
}

func (ex *compressedOTLPExtension) compressorForCompression(c compression) (compressor, error) {
	switch c {
	case compressionZSTD:
		if ex.zstdCompressor == nil {
			return nil, errors.New("zstd compressor not initialized")
		}
		return ex.zstdCompressor, nil
	case compressionLZ4:
		return LZ4Compressor{}, nil
	case compressionNone:
		return NoneCompressor{}, nil
	default:
		return nil, fmt.Errorf("unsupported compression: %q", c)
	}
}

func (ex *compressedOTLPExtension) UnmarshalTraces(encoded []byte) (_ ptrace.Traces, reterr error) {
	magicIdx := magicIdxSuccess
	defer func() {
		outcomeIdx := outcomeIdxSuccess
		if reterr != nil {
			outcomeIdx = outcomeIdxFailure
		}
		ex.tb.CompressedotlpDecodingCount.Add(
			context.Background(),
			1,
			ex.unmarshalCountMeasureOpts[signalIdxTraces][outcomeIdx][magicIdx],
		)
	}()
	m, payload, err := parseMagic(encoded)
	if err != nil {
		magicIdx = magicIdxFailure
		// fallback to otlp_proto if magic is not recognized for smooth migration
		unmarshaler := &ptrace.ProtoUnmarshaler{}
		trace, unmarshalErr := unmarshaler.UnmarshalTraces(encoded)
		if unmarshalErr != nil {
			return ptrace.Traces{}, errors.Join(err, unmarshalErr)
		}
		return trace, nil
	}
	c, err := ex.compressorForCompression(m.compression)
	if err != nil {
		return ptrace.Traces{}, err
	}
	uncompressed, err := c.Decompress(payload)
	if err != nil {
		return ptrace.Traces{}, err
	}
	ex.tb.CompressedotlpDecodingCompressedSize.Add(context.Background(), int64(len(encoded)), ex.signalSizeMeasureOpts[signalIdxTraces])
	ex.tb.CompressedotlpDecodingUncompressedSize.Add(context.Background(), int64(len(uncompressed)), ex.signalSizeMeasureOpts[signalIdxTraces])

	var unmarshaler ptrace.Unmarshaler
	switch m.protocol {
	case otlpProto:
		unmarshaler = &ptrace.ProtoUnmarshaler{}
	case otlpJSON:
		unmarshaler = &ptrace.JSONUnmarshaler{}
	default:
		return ptrace.Traces{}, fmt.Errorf("unsupported protocol: %q", m.protocol)
	}
	return unmarshaler.UnmarshalTraces(uncompressed)
}

func (ex *compressedOTLPExtension) MarshalTraces(traces ptrace.Traces) (_ []byte, reterr error) {
	defer func() {
		outcomeIdx := outcomeIdxSuccess
		if reterr != nil {
			outcomeIdx = outcomeIdxFailure
		}
		ex.tb.CompressedotlpEncodingCount.Add(
			context.Background(),
			1,
			ex.marshalCountMeasureOpts[signalIdxTraces][outcomeIdx],
		)
	}()
	if !ex.config.EncodingEnabled {
		m := &ptrace.ProtoMarshaler{}
		return m.MarshalTraces(traces)
	}
	marshaled, err := ex.traceMarshaler.MarshalTraces(traces)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.Grow(len(ex.magicBytes) + len(marshaled))
	buf.Write(ex.magicBytes)
	if err := ex.compressor.Compress(&buf, marshaled); err != nil {
		return nil, err
	}
	encoded := buf.Bytes()
	ex.tb.CompressedotlpEncodingUncompressedSize.Add(context.Background(), int64(len(marshaled)), ex.signalSizeMeasureOpts[signalIdxTraces])
	ex.tb.CompressedotlpEncodingCompressedSize.Add(context.Background(), int64(len(encoded)), ex.signalSizeMeasureOpts[signalIdxTraces])
	return encoded, nil
}

func (ex *compressedOTLPExtension) UnmarshalMetrics(encoded []byte) (_ pmetric.Metrics, reterr error) {
	magicIdx := magicIdxSuccess
	defer func() {
		outcomeIdx := outcomeIdxSuccess
		if reterr != nil {
			outcomeIdx = outcomeIdxFailure
		}
		ex.tb.CompressedotlpDecodingCount.Add(
			context.Background(),
			1,
			ex.unmarshalCountMeasureOpts[signalIdxMetrics][outcomeIdx][magicIdx],
		)
	}()
	m, payload, err := parseMagic(encoded)
	if err != nil {
		magicIdx = magicIdxFailure
		// fallback to otlp_proto if magic is not recognized for smooth migration
		unmarshaler := &pmetric.ProtoUnmarshaler{}
		metric, unmarshalErr := unmarshaler.UnmarshalMetrics(encoded)
		if unmarshalErr != nil {
			return pmetric.Metrics{}, errors.Join(err, unmarshalErr)
		}
		return metric, nil
	}
	c, err := ex.compressorForCompression(m.compression)
	if err != nil {
		return pmetric.Metrics{}, err
	}
	uncompressed, err := c.Decompress(payload)
	if err != nil {
		return pmetric.Metrics{}, err
	}
	ex.tb.CompressedotlpDecodingCompressedSize.Add(context.Background(), int64(len(encoded)), ex.signalSizeMeasureOpts[signalIdxMetrics])
	ex.tb.CompressedotlpDecodingUncompressedSize.Add(context.Background(), int64(len(uncompressed)), ex.signalSizeMeasureOpts[signalIdxMetrics])

	var unmarshaler pmetric.Unmarshaler
	switch m.protocol {
	case otlpProto:
		unmarshaler = &pmetric.ProtoUnmarshaler{}
	case otlpJSON:
		unmarshaler = &pmetric.JSONUnmarshaler{}
	default:
		return pmetric.Metrics{}, fmt.Errorf("unsupported protocol: %q", m.protocol)
	}
	return unmarshaler.UnmarshalMetrics(uncompressed)
}

func (ex *compressedOTLPExtension) MarshalMetrics(metrics pmetric.Metrics) (_ []byte, reterr error) {
	defer func() {
		outcomeIdx := outcomeIdxSuccess
		if reterr != nil {
			outcomeIdx = outcomeIdxFailure
		}
		ex.tb.CompressedotlpEncodingCount.Add(
			context.Background(),
			1,
			ex.marshalCountMeasureOpts[signalIdxMetrics][outcomeIdx],
		)
	}()
	if !ex.config.EncodingEnabled {
		m := &pmetric.ProtoMarshaler{}
		return m.MarshalMetrics(metrics)
	}
	marshaled, err := ex.metricMarshaler.MarshalMetrics(metrics)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.Grow(len(ex.magicBytes) + len(marshaled))
	buf.Write(ex.magicBytes)
	if err := ex.compressor.Compress(&buf, marshaled); err != nil {
		return nil, err
	}
	encoded := buf.Bytes()
	ex.tb.CompressedotlpEncodingUncompressedSize.Add(context.Background(), int64(len(marshaled)), ex.signalSizeMeasureOpts[signalIdxMetrics])
	ex.tb.CompressedotlpEncodingCompressedSize.Add(context.Background(), int64(len(encoded)), ex.signalSizeMeasureOpts[signalIdxMetrics])
	return encoded, nil
}

func (ex *compressedOTLPExtension) UnmarshalLogs(encoded []byte) (_ plog.Logs, reterr error) {
	magicIdx := magicIdxSuccess
	defer func() {
		outcomeIdx := outcomeIdxSuccess
		if reterr != nil {
			outcomeIdx = outcomeIdxFailure
		}
		ex.tb.CompressedotlpDecodingCount.Add(
			context.Background(),
			1,
			ex.unmarshalCountMeasureOpts[signalIdxLogs][outcomeIdx][magicIdx],
		)
	}()
	m, payload, err := parseMagic(encoded)
	if err != nil {
		magicIdx = magicIdxFailure
		// fallback to otlp_proto if magic is not recognized for smooth migration
		unmarshaler := &plog.ProtoUnmarshaler{}
		log, unmarshalErr := unmarshaler.UnmarshalLogs(encoded)
		if unmarshalErr != nil {
			return plog.Logs{}, errors.Join(err, unmarshalErr)
		}
		return log, nil
	}
	c, err := ex.compressorForCompression(m.compression)
	if err != nil {
		return plog.Logs{}, err
	}
	uncompressed, err := c.Decompress(payload)
	if err != nil {
		return plog.Logs{}, err
	}
	ex.tb.CompressedotlpDecodingCompressedSize.Add(context.Background(), int64(len(encoded)), ex.signalSizeMeasureOpts[signalIdxLogs])
	ex.tb.CompressedotlpDecodingUncompressedSize.Add(context.Background(), int64(len(uncompressed)), ex.signalSizeMeasureOpts[signalIdxLogs])

	var unmarshaler plog.Unmarshaler
	switch m.protocol {
	case otlpProto:
		unmarshaler = &plog.ProtoUnmarshaler{}
	case otlpJSON:
		unmarshaler = &plog.JSONUnmarshaler{}
	default:
		return plog.Logs{}, fmt.Errorf("unsupported protocol: %q", m.protocol)
	}
	return unmarshaler.UnmarshalLogs(uncompressed)
}

func (ex *compressedOTLPExtension) MarshalLogs(logs plog.Logs) (_ []byte, reterr error) {
	defer func() {
		outcomeIdx := outcomeIdxSuccess
		if reterr != nil {
			outcomeIdx = outcomeIdxFailure
		}
		ex.tb.CompressedotlpEncodingCount.Add(
			context.Background(),
			1,
			ex.marshalCountMeasureOpts[signalIdxLogs][outcomeIdx],
		)
	}()
	if !ex.config.EncodingEnabled {
		m := &plog.ProtoMarshaler{}
		return m.MarshalLogs(logs)
	}
	marshaled, err := ex.logMarshaler.MarshalLogs(logs)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.Grow(len(ex.magicBytes) + len(marshaled))
	buf.Write(ex.magicBytes)
	if err := ex.compressor.Compress(&buf, marshaled); err != nil {
		return nil, err
	}
	encoded := buf.Bytes()
	ex.tb.CompressedotlpEncodingUncompressedSize.Add(context.Background(), int64(len(marshaled)), ex.signalSizeMeasureOpts[signalIdxLogs])
	ex.tb.CompressedotlpEncodingCompressedSize.Add(context.Background(), int64(len(encoded)), ex.signalSizeMeasureOpts[signalIdxLogs])
	return encoded, nil
}

// UnmarshalProfiles implements encoding.ProfilesUnmarshalerExtension.
func (ex *compressedOTLPExtension) UnmarshalProfiles(encoded []byte) (_ pprofile.Profiles, reterr error) {
	magicIdx := magicIdxSuccess
	defer func() {
		outcomeIdx := outcomeIdxSuccess
		if reterr != nil {
			outcomeIdx = outcomeIdxFailure
		}
		ex.tb.CompressedotlpDecodingCount.Add(
			context.Background(),
			1,
			ex.unmarshalCountMeasureOpts[signalIdxProfiles][outcomeIdx][magicIdx],
		)
	}()
	m, payload, err := parseMagic(encoded)
	if err != nil {
		magicIdx = magicIdxFailure
		// fallback to otlp_proto if magic is not recognized for smooth migration
		unmarshaler := &pprofile.ProtoUnmarshaler{}
		profile, unmarshalErr := unmarshaler.UnmarshalProfiles(encoded)
		if unmarshalErr != nil {
			return pprofile.Profiles{}, errors.Join(err, unmarshalErr)
		}
		return profile, nil
	}
	c, err := ex.compressorForCompression(m.compression)
	if err != nil {
		return pprofile.Profiles{}, err
	}
	uncompressed, err := c.Decompress(payload)
	if err != nil {
		return pprofile.Profiles{}, err
	}
	ex.tb.CompressedotlpDecodingCompressedSize.Add(context.Background(), int64(len(encoded)), ex.signalSizeMeasureOpts[signalIdxProfiles])
	ex.tb.CompressedotlpDecodingUncompressedSize.Add(context.Background(), int64(len(uncompressed)), ex.signalSizeMeasureOpts[signalIdxProfiles])

	var unmarshaler pprofile.Unmarshaler
	switch m.protocol {
	case otlpProto:
		unmarshaler = &pprofile.ProtoUnmarshaler{}
	case otlpJSON:
		unmarshaler = &pprofile.JSONUnmarshaler{}
	default:
		return pprofile.Profiles{}, fmt.Errorf("unsupported protocol: %q", m.protocol)
	}
	return unmarshaler.UnmarshalProfiles(uncompressed)
}

// MarshalProfiles implements encoding.ProfilesMarshalerExtension.
func (ex *compressedOTLPExtension) MarshalProfiles(profiles pprofile.Profiles) (_ []byte, reterr error) {
	defer func() {
		outcomeIdx := outcomeIdxSuccess
		if reterr != nil {
			outcomeIdx = outcomeIdxFailure
		}
		ex.tb.CompressedotlpEncodingCount.Add(
			context.Background(),
			1,
			ex.marshalCountMeasureOpts[signalIdxProfiles][outcomeIdx],
		)
	}()
	if !ex.config.EncodingEnabled {
		m := &pprofile.ProtoMarshaler{}
		return m.MarshalProfiles(profiles)
	}
	marshaled, err := ex.profileMarshaler.MarshalProfiles(profiles)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.Grow(len(ex.magicBytes) + len(marshaled))
	buf.Write(ex.magicBytes)
	if err := ex.compressor.Compress(&buf, marshaled); err != nil {
		return nil, err
	}
	encoded := buf.Bytes()
	ex.tb.CompressedotlpEncodingUncompressedSize.Add(context.Background(), int64(len(marshaled)), ex.signalSizeMeasureOpts[signalIdxProfiles])
	ex.tb.CompressedotlpEncodingCompressedSize.Add(context.Background(), int64(len(encoded)), ex.signalSizeMeasureOpts[signalIdxProfiles])
	return encoded, nil
}

func (*compressedOTLPExtension) Start(context.Context, component.Host) error {
	return nil
}

func (ex *compressedOTLPExtension) Shutdown(context.Context) error {
	ex.tb.Shutdown()
	if ex.zstdCompressor != nil {
		return ex.zstdCompressor.close()
	}
	return nil
}
