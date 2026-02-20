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

package profilingmetricsconnector // import "github.com/elastic/opentelemetry-collector-components/connector/profilingmetricsconnector"

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/elastic/opentelemetry-collector-components/connector/profilingmetricsconnector/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

var testDataDir = "testdata"

// Wraps envelope creation
type testProfiles struct {
	profiles pprofile.Profiles
	dict     pprofile.ProfilesDictionary

	attrGoIdx     int32
	attrPyIdx     int32
	attrKernelIdx int32
	attrNativeIdx int32
}

type testFrame struct {
	frameType string
	fileName  string
	funcName  string
}

func (tf testFrame) withFilename(name string) testFrame {
	tf.fileName = name
	return tf
}

func (tf testFrame) withFunction(name string) testFrame {
	tf.funcName = name
	return tf
}

func goFrame() testFrame {
	return testFrame{frameType: frameTypeGo}
}

func pyFrame() testFrame {
	return testFrame{frameType: frameTypePython}
}

func nativeFrame() testFrame {
	return testFrame{frameType: frameTypeNative}
}

func kernelFrame() testFrame {
	return testFrame{frameType: frameTypeKernel}
}

func newTestProfiles() *testProfiles {
	profiles := pprofile.NewProfiles()
	dict := profiles.Dictionary()

	strTable := dict.StringTable()
	attrTable := dict.AttributeTable()
	locTable := dict.LocationTable()
	mappingTable := dict.MappingTable()
	stackTable := dict.StackTable()

	strTable.Append("")
	strTable.Append("samples")
	strTable.Append("count")

	locTable.AppendEmpty()
	mappingTable.AppendEmpty()
	attrTable.AppendEmpty()
	stackTable.AppendEmpty()

	ftIdx := int32(strTable.Len())
	strTable.Append(string(semconv.ProfileFrameTypeKey))

	// Add all used frame types as attributes
	attrGoIdx := attrTable.Len()
	attrGo := attrTable.AppendEmpty()
	attrGo.SetKeyStrindex(ftIdx)
	attrGo.Value().SetStr(frameTypeGo)

	attrPyIdx := attrTable.Len()
	attrPy := attrTable.AppendEmpty()
	attrPy.SetKeyStrindex(ftIdx)
	attrPy.Value().SetStr(frameTypePython)

	attrKernelIdx := attrTable.Len()
	attrKernel := attrTable.AppendEmpty()
	attrKernel.SetKeyStrindex(ftIdx)
	attrKernel.Value().SetStr(frameTypeKernel)

	attrNativeIdx := attrTable.Len()
	attrNative := attrTable.AppendEmpty()
	attrNative.SetKeyStrindex(ftIdx)
	attrNative.Value().SetStr(frameTypeNative)

	return &testProfiles{
		profiles: profiles,
		dict:     dict,

		attrGoIdx:     int32(attrGoIdx),
		attrPyIdx:     int32(attrPyIdx),
		attrKernelIdx: int32(attrKernelIdx),
		attrNativeIdx: int32(attrNativeIdx),
	}
}

// newProfile initializes and appends a Profile to the wrapped Profiles instance.
// All intermediate envelopes are automatically created and initialized.
func (tp *testProfiles) newProfile() pprofile.Profile {
	resProf := tp.profiles.ResourceProfiles().AppendEmpty()
	scopeProf := resProf.ScopeProfiles().AppendEmpty()
	prof := scopeProf.Profiles().AppendEmpty()
	prof.SetTime(pcommon.NewTimestampFromTime(time.Now()))
	st := prof.SampleType()

	st.SetTypeStrindex(1)
	st.SetUnitStrindex(2)

	return prof
}

func (tp *testProfiles) addSample(t *testing.T, prof pprofile.Profile,
	multiplier int64, frames ...testFrame,
) {
	strTable := tp.dict.StringTable()
	locTable := tp.dict.LocationTable()
	mappingTable := tp.dict.MappingTable()
	funcTable := tp.dict.FunctionTable()
	stackTable := tp.dict.StackTable()

	sample := prof.Samples().AppendEmpty()
	for range multiplier {
		sample.TimestampsUnixNano().Append(uint64(time.Now().UnixNano()))
	}

	// Set sample to reference the stack
	sample.SetStackIndex(int32(stackTable.Len()))
	stack := stackTable.AppendEmpty()

	for _, frame := range frames {
		// Add a location referencing the attribute
		locIdx := locTable.Len()
		loc := locTable.AppendEmpty()

		ftIdx := int32(0)
		switch frame.frameType {
		case frameTypeGo:
			ftIdx = tp.attrGoIdx
		case frameTypePython:
			ftIdx = tp.attrPyIdx
		case frameTypeKernel:
			ftIdx = tp.attrKernelIdx
			if frame.funcName != "" {
				ln := loc.Lines().AppendEmpty()
				ln.SetFunctionIndex(int32(funcTable.Len()))
				fn := funcTable.AppendEmpty()
				fn.SetNameStrindex(int32(strTable.Len()))
				strTable.Append(frame.funcName)
			}
		case frameTypeNative:
			ftIdx = tp.attrNativeIdx
			if frame.fileName != "" {
				loc.SetMappingIndex(int32(mappingTable.Len()))
				mapping := mappingTable.AppendEmpty()
				mapping.SetFilenameStrindex(int32(strTable.Len()))
				strTable.Append(frame.fileName)
			}
		default:
			t.Errorf("Unimplemented frame type: %v", frame.frameType)
		}

		loc.AttributeIndices().Append(ftIdx)
		// Add location index to the stack's location indices
		stack.LocationIndices().Append(int32(locIdx))
	}
}

func TestConsumeProfiles_FrameMetrics(t *testing.T) {
	m := new(consumertest.MetricsSink)
	cfg := &Config{
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
	}
	conn := &profilesToMetricsConnector{
		nextConsumer: m,
		config:       cfg,
		mb:           metadata.NewMetricsBuilder(cfg.MetricsBuilderConfig, connectortest.NewNopSettings(metadata.Type)),
	}

	// Create a Profile and higher-level envelopes
	tp := newTestProfiles()
	prof := tp.newProfile()

	tp.addSample(t, prof, 0, goFrame())
	tp.addSample(t, prof, 42, pyFrame())

	err := conn.ConsumeProfiles(context.Background(), tp.profiles)
	assert.NoError(t, err)
	actualMetrics := m.AllMetrics()
	assert.Len(t, actualMetrics, 1)
	// err = golden.WriteMetrics(t, filepath.Join(testDataDir, "frame_metrics", "output-metrics.yaml"), actualMetrics[0])
	// assert.NoError(t, err)
	expectedMetrics, err := golden.ReadMetrics(filepath.Join(testDataDir, "frame_metrics", "output-metrics.yaml"))
	assert.NoError(t, err)
	require.NoError(t, pmetrictest.CompareMetrics(expectedMetrics, actualMetrics[0], pmetrictest.IgnoreStartTimestamp(), pmetrictest.IgnoreDatapointAttributesOrder(), pmetrictest.IgnoreTimestamp(), pmetrictest.IgnoreMetricDataPointsOrder()))
}

func TestConsumeProfiles_FrameMetricsMultiple(t *testing.T) {
	m := new(consumertest.MetricsSink)
	cfg := &Config{
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
	}
	conn := &profilesToMetricsConnector{
		nextConsumer: m,
		config:       cfg,
		mb:           metadata.NewMetricsBuilder(cfg.MetricsBuilderConfig, connectortest.NewNopSettings(metadata.Type)),
	}

	// Create a Profile and higher-level envelopes
	tp := newTestProfiles()
	prof := tp.newProfile()

	tp.addSample(t, prof, 7, kernelFrame())
	tp.addSample(t, prof, 0, kernelFrame())
	tp.addSample(t, prof, 0, nativeFrame(), pyFrame())
	tp.addSample(t, prof, 12, nativeFrame().withFilename("libc.so.6"), goFrame())
	tp.addSample(t, prof, 0, goFrame())
	tp.addSample(t, prof, 0, pyFrame())
	tp.addSample(t, prof, 0, pyFrame())
	tp.addSample(t, prof, 3, pyFrame())
	tp.addSample(t, prof, 0, goFrame())

	err := conn.ConsumeProfiles(context.Background(), tp.profiles)
	assert.NoError(t, err)
	actualMetrics := m.AllMetrics()
	assert.Len(t, actualMetrics, 1)
	// err = golden.WriteMetrics(t, filepath.Join(testDataDir, "frame_metrics_multiple", "output-metrics.yaml"), actualMetrics[0])
	// assert.NoError(t, err)
	expectedMetrics, err := golden.ReadMetrics(filepath.Join(testDataDir, "frame_metrics_multiple", "output-metrics.yaml"))
	assert.NoError(t, err)
	require.NoError(t, pmetrictest.CompareMetrics(expectedMetrics, actualMetrics[0], pmetrictest.IgnoreStartTimestamp(), pmetrictest.IgnoreDatapointAttributesOrder(), pmetrictest.IgnoreTimestamp(), pmetrictest.IgnoreMetricDataPointsOrder()))
}

func kstackToFrames(functions ...string) []testFrame {
	frames := []testFrame{}
	for _, fn := range functions {
		frames = append(frames, kernelFrame().withFunction(fn))
	}
	return frames
}

func TestConsumeProfiles_FrameMetricsKernel(t *testing.T) {
	m := new(consumertest.MetricsSink)
	cfg := &Config{
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
	}
	conn := &profilesToMetricsConnector{
		nextConsumer: m,
		config:       cfg,
		mb:           metadata.NewMetricsBuilder(cfg.MetricsBuilderConfig, connectortest.NewNopSettings(metadata.Type)),
	}

	// Create a Profile and higher-level envelopes
	tp := newTestProfiles()
	prof := tp.newProfile()

	tp.addSample(t, prof, 0, kstackToFrames(
		"handle_mm_fault", "alloc_pages", "tcp_recvmsg", "__x64_sys_read")...)
	tp.addSample(t, prof, 0, kstackToFrames(
		"sock_recvmsg", "tcp_recvmsg", "__schedule")...)
	tp.addSample(t, prof, 0, kstackToFrames(
		"tcp_sendmsg", "sock_sendmsg", "wake_up")...)
	tp.addSample(t, prof, 42, kstackToFrames(
		"pipe_read", "udp_sendpage", "pipe_read", "ksys_write")...)
	tp.addSample(t, prof, 0, kstackToFrames(
		"sock_write_iter", "unix_stream_sendmsg")...)
	tp.addSample(t, prof, 0, kstackToFrames(
		"sock_read_iter", "pipe_read")...)
	tp.addSample(t, prof, 0, kstackToFrames(
		"foo__schedule", "bar", "baz")...)
	tp.addSample(t, prof, 20, kstackToFrames(
		"futex", "bar", "baz")...)
	tp.addSample(t, prof, 10, kstackToFrames(
		"futex", "bar", "baz", "__x64_sys_futex")...)
	tp.addSample(t, prof, 5, kstackToFrames(
		"tcp_rcv_established", "tcp_v4_do_rcv", "tcp_v4_rcv",
		"ip_output", "__ip_queue_xmit", "__tcp_transmit_skb",
		"tcp_sendmsg_locked", "tcp_sendmsg", "ksys_write")...)
	tp.addSample(t, prof, 6, kstackToFrames(
		"generic_file_read_iter", "wake_up", "futex_", "sock_recvmsg")...)
	tp.addSample(t, prof, 0, kstackToFrames(
		"free_pages", "sock_read_iter", "do_munmap")...)
	tp.addSample(t, prof, 0, kstackToFrames(
		"wake_up", "futex_", "sock_sendmsg", "generic_file_write_iter")...)
	tp.addSample(t, prof, 0, kstackToFrames(
		"do_munmap", "sock_write_iter", "free_pages")...)
	tp.addSample(t, prof, 0, kstackToFrames(
		"do_mmap", "alloc_pages", "filemap_read")...)
	tp.addSample(t, prof, 0, kstackToFrames(
		"alloc_pages", "futex_", "ext4_file_write_iter")...)
	tp.addSample(t, prof, 0, kstackToFrames(
		"alloc_pages", "futex_", "wake_up_", "__arm64_sys_mmap")...)

	err := conn.ConsumeProfiles(context.Background(), tp.profiles)
	assert.NoError(t, err)
	actualMetrics := m.AllMetrics()
	assert.Len(t, actualMetrics, 1)
	// err = golden.WriteMetrics(t, filepath.Join(testDataDir, "frame_metrics_kernel", "output-metrics.yaml"), actualMetrics[0])
	// assert.NoError(t, err)
	expectedMetrics, err := golden.ReadMetrics(filepath.Join(testDataDir,
		"frame_metrics_kernel", "output-metrics.yaml"))
	assert.NoError(t, err)
	require.NoError(t, pmetrictest.CompareMetrics(expectedMetrics, actualMetrics[0], pmetrictest.IgnoreStartTimestamp(), pmetrictest.IgnoreDatapointAttributesOrder(), pmetrictest.IgnoreTimestamp(), pmetrictest.IgnoreMetricDataPointsOrder()))
}
