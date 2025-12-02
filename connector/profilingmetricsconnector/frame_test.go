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
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

var metricRx = regexp.MustCompile(`samples\..+\.count`)

type metricsConsumerStub struct {
	t *testing.T

	execCount atomic.Int64
	counts    map[string]int64
}

func (m *metricsConsumerStub) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func newMetricsConsumer(t *testing.T) *metricsConsumerStub {
	return &metricsConsumerStub{
		t:      t,
		counts: map[string]int64{},
	}
}

func (m *metricsConsumerStub) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	// Generic test method that contains assertions applicable to all tests
	m.execCount.Add(1)
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sm := rms.At(i).ScopeMetrics()
		for j := 0; j < sm.Len(); j++ {
			metrics := sm.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				name := metric.Name()
				if metricRx.MatchString(name) {
					// Verify it's a Sum metric and then its properties
					assert.Equal(m.t, pmetric.MetricTypeSum, metric.Type())
					assert.Equal(m.t, "1", metric.Unit())
					sum := metric.Sum()
					assert.Equal(m.t, true, sum.IsMonotonic())
					assert.Equal(m.t, pmetric.AggregationTemporalityDelta,
						sum.AggregationTemporality())
					assert.Equal(m.t, 1, sum.DataPoints().Len())
					dp := sum.DataPoints().At(0)

					// For native metrics, this is convenient way to test library name extraction
					if strings.HasSuffix(name, metricNative.name) {
						if shlibName, exists := dp.Attributes().Get(nativeLibraryAttrName); exists {
							name = fmt.Sprintf("%v/%v", name, shlibName.AsString())
						}
					} else if strings.HasSuffix(name, metricKernel.name) {
						// For kernel metrics, we need two levels of annotation:
						//   1. "class0/class1/class2" for classes
						//   2. "[syscall]" as string postfix only for syscall name
						classValues := map[string]string{}
						syscall := ""
						for attrName, attrValue := range dp.Attributes().All() {
							if strings.HasPrefix(attrName, kernelClassAttrPrefix) {
								assert.Contains(m.t, kernelClassAttrNames, attrName)
								classValues[attrName] = attrValue.AsString()
							} else {
								assert.Equal(m.t, syscallAttrName, attrName)
								syscall = attrValue.AsString()
							}
						}
						switch len(classValues) {
						case 3:
							name = fmt.Sprintf("%v/%v/%v/%v", name,
								classValues[kernelClassAttrNames[0]],
								classValues[kernelClassAttrNames[1]],
								classValues[kernelClassAttrNames[2]])
						case 2:
							name = fmt.Sprintf("%v/%v/%v", name,
								classValues[kernelClassAttrNames[0]],
								classValues[kernelClassAttrNames[1]])
						case 1:
							name = fmt.Sprintf("%v/%v", name,
								classValues[kernelClassAttrNames[0]])
						}
						if syscall != "" {
							name = fmt.Sprintf("%v[%v]", name, syscall)
						}
					} else {
						// Non-native, non-kernel metrics should not have attributes attached
						assert.Equal(m.t, 0, dp.Attributes().Len())
					}
					m.counts[name] += dp.IntValue()
				}
			}
		}
	}
	return nil
}

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
	m := newMetricsConsumer(t)
	conn := &profilesToMetricsConnector{
		nextConsumer: m,
		config: &Config{
			ByFrame: true,
		},
	}

	// Create a Profile and higher-level envelopes
	tp := newTestProfiles()
	prof := tp.newProfile()

	tp.addSample(t, prof, 0, goFrame())
	tp.addSample(t, prof, 42, pyFrame())

	err := conn.ConsumeProfiles(context.Background(), tp.profiles)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), m.execCount.Load())
	assert.Equal(t, map[string]int64{
		"samples.go.count":      1,
		"samples.user.count":    43,
		"samples.cpython.count": 42,
	},
		m.counts)
}

func TestConsumeProfiles_FrameMetricsMultiple(t *testing.T) {
	m := newMetricsConsumer(t)
	conn := &profilesToMetricsConnector{
		nextConsumer: m,
		config: &Config{
			ByFrame: true,
		},
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
	assert.Equal(t, int64(1), m.execCount.Load())
	assert.Equal(t, map[string]int64{
		"samples.go.count":          2,
		"samples.cpython.count":     5,
		"samples.user.count":        20,
		"samples.kernel.count":      8,
		"samples.native.count":      1,
		"samples.native.count/libc": 12,
	},
		m.counts)
}

func kstackToFrames(functions ...string) []testFrame {
	frames := []testFrame{}
	for _, fn := range functions {
		frames = append(frames, kernelFrame().withFunction(fn))
	}
	return frames
}

func TestConsumeProfiles_FrameMetricsKernel(t *testing.T) {
	m := newMetricsConsumer(t)
	conn := &profilesToMetricsConnector{
		nextConsumer: m,
		config: &Config{
			ByFrame: true,
		},
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
	assert.Equal(t, int64(1), m.execCount.Load())
	assert.Equal(t, map[string]int64{
		"samples.kernel.count/network/tcp/read":         1,
		"samples.kernel.count/network/tcp/read[read]":   1,
		"samples.kernel.count/network/tcp/write":        1,
		"samples.kernel.count/network/udp/write[write]": 42,
		"samples.kernel.count/ipc/write":                1,
		"samples.kernel.count/ipc/read":                 1,
		"samples.kernel.count/disk/read":                1,
		"samples.kernel.count/disk/write":               1,
		"samples.kernel.count/network/other/read":       7,
		"samples.kernel.count/network/other/write":      2,
		"samples.kernel.count/synchronization":          1,
		"samples.kernel.count/memory[mmap]":             1,
	},
		m.counts)
}
