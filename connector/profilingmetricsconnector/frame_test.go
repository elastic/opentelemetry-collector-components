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

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

var metricRx = regexp.MustCompile(`frametest\.samples\..+\.count`)

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
					} else {
						// Non-native metrics should not have attributes attached
						assert.Equal(m.t, 0, dp.Attributes().Len())
					}
					m.counts[name] += dp.IntValue()
				}
			}
		}
	}
	return nil
}

// newProfiles creates a new Profiles instance and initializes its Dictionary.
func newProfiles() (pprofile.Profiles,
	pprofile.ProfilesDictionary,
	pcommon.StringSlice,
	pprofile.KeyValueAndUnitSlice,
	pprofile.LocationSlice,
	pprofile.StackSlice,
) {
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

	return profiles, dict, strTable, attrTable, locTable, stackTable
}

// newProfile initializes and appends a Profile to a Profiles instance.
// All intermediate envelopes are automatically created and initialized.
func newProfile(profiles pprofile.Profiles) pprofile.Profile {
	resProf := profiles.ResourceProfiles().AppendEmpty()
	scopeProf := resProf.ScopeProfiles().AppendEmpty()
	prof := scopeProf.Profiles().AppendEmpty()
	st := prof.SampleType()

	st.SetTypeStrindex(1)
	st.SetUnitStrindex(2)

	return prof
}

func TestConsumeProfiles_FrameMetrics(t *testing.T) {
	m := newMetricsConsumer(t)
	cfg := &Config{
		MetricsPrefix: "frametest.",
		ByFrame:       true,
	}
	conn := &profilesToMetricsConnector{
		nextConsumer: m,
		config:       cfg,
	}

	// Create a Profile and higher-level envelopes
	profiles, _, strTable, attrTable, locTable, stackTable := newProfiles()
	prof := newProfile(profiles)

	// Create a profiles object with a sample that has a location with a frame type attribute.
	sample := prof.Sample().AppendEmpty()

	// Add an attribute for frame type
	attr := attrTable.AppendEmpty()
	attr.SetKeyStrindex(int32(strTable.Len()))
	strTable.Append(string(semconv.ProfileFrameTypeKey))
	attr.Value().SetStr(frameTypeGo)

	// Add a location referencing the attribute
	loc := locTable.AppendEmpty()
	loc.AttributeIndices().Append(1)

	// Set sample to reference the stack
	sample.SetStackIndex(int32(stackTable.Len()))
	stack := stackTable.AppendEmpty()

	// Add location index to the stack's location indices
	stack.LocationIndices().Append(1)

	err := conn.ConsumeProfiles(context.Background(), profiles)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), m.execCount.Load())
	assert.Equal(t, map[string]int64{
		"frametest.samples.go.count":   1,
		"frametest.samples.user.count": 1,
	},
		m.counts)
}

func TestConsumeProfiles_FrameMetricsMultiple(t *testing.T) {
	m := newMetricsConsumer(t)
	cfg := &Config{
		MetricsPrefix: "frametest.",
		ByFrame:       true,
	}
	conn := &profilesToMetricsConnector{
		nextConsumer: m,
		config:       cfg,
	}

	// Create a Profile and higher-level envelopes
	profiles, dict, strTable, attrTable, locTable, stackTable := newProfiles()
	prof := newProfile(profiles)

	mappingTable := dict.MappingTable()

	// Add four attributes for frame types (Go,Python,Kernel,Native)
	attrGo := attrTable.AppendEmpty()
	attrGo.SetKeyStrindex(int32(strTable.Len()))
	strTable.Append(string(semconv.ProfileFrameTypeKey))
	attrGo.Value().SetStr(frameTypeGo)
	attrPy := attrTable.AppendEmpty()
	attrPy.SetKeyStrindex(int32(strTable.Len() - 1))
	attrPy.Value().SetStr(frameTypePython)
	attrKernel := attrTable.AppendEmpty()
	attrKernel.SetKeyStrindex(int32(strTable.Len() - 1))
	attrKernel.Value().SetStr(frameTypeKernel)
	attrNative := attrTable.AppendEmpty()
	attrNative.SetKeyStrindex(int32(strTable.Len() - 1))
	attrNative.Value().SetStr(frameTypeNative)

	// Add five locations, referencing all added frame type attributes
	locGo := locTable.AppendEmpty()
	locGo.AttributeIndices().Append(1)
	locPy := locTable.AppendEmpty()
	locPy.AttributeIndices().Append(2)
	locKernel := locTable.AppendEmpty()
	locKernel.AttributeIndices().Append(3)

	locNative := locTable.AppendEmpty()
	locNative.AttributeIndices().Append(4)
	locNative = locTable.AppendEmpty()
	locNative.AttributeIndices().Append(4)

	locNative.SetMappingIndex(int32(mappingTable.Len()))
	mapping := mappingTable.AppendEmpty()
	mapping.SetFilenameStrindex(int32(strTable.Len()))
	strTable.Append("libc.so.6")

	/// Five stacks
	stackGo := stackTable.AppendEmpty()
	stackPy := stackTable.AppendEmpty()
	stackKernel := stackTable.AppendEmpty()
	stackNative := stackTable.AppendEmpty()
	stackNative2 := stackTable.AppendEmpty()

	// Add location indices to the stack location indices.
	// Some samples have multiple locations.
	stackGo.LocationIndices().Append(1)
	stackPy.LocationIndices().Append(2)
	stackKernel.LocationIndices().Append(3)
	stackNative.LocationIndices().Append(4)
	stackNative.LocationIndices().Append(2)
	stackNative2.LocationIndices().Append(5)
	stackNative2.LocationIndices().Append(1)

	// Eight samples
	sampleKernel := prof.Sample().AppendEmpty()
	sampleKernel.SetStackIndex(3)
	sampleNative := prof.Sample().AppendEmpty()
	sampleNative.SetStackIndex(4)
	sampleNative = prof.Sample().AppendEmpty()
	sampleNative.SetStackIndex(5)
	sampleGo := prof.Sample().AppendEmpty()
	sampleGo.SetStackIndex(1)
	samplePy := prof.Sample().AppendEmpty()
	samplePy.SetStackIndex(2)
	samplePy = prof.Sample().AppendEmpty()
	samplePy.SetStackIndex(2)
	samplePy = prof.Sample().AppendEmpty()
	samplePy.SetStackIndex(2)
	sampleGo = prof.Sample().AppendEmpty()
	sampleGo.SetStackIndex(1)

	err := conn.ConsumeProfiles(context.Background(), profiles)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), m.execCount.Load())
	assert.Equal(t, map[string]int64{
		"frametest.samples.go.count":          2,
		"frametest.samples.cpython.count":     3,
		"frametest.samples.user.count":        7,
		"frametest.samples.kernel.count":      1,
		"frametest.samples.native.count":      1,
		"frametest.samples.native.count/libc": 1,
	},
		m.counts)
}
