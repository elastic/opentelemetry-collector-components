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
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

func TestConsumeProfiles_WithMetrics(t *testing.T) {
	mockConsumer := new(consumertest.MetricsSink)
	cfg := &Config{
		ByFrameType: true,
	}
	conn := &profilesToMetricsConnector{
		nextConsumer: mockConsumer,
		config:       cfg,
	}

	// Create a profiles object that will result in at least one metric.
	profiles := pprofile.NewProfiles()
	profiles.Dictionary().StringTable().Append("sample")
	profiles.Dictionary().StringTable().Append("count")

	// Add a ResourceProfile with one ScopeProfile and one Profile with one sample.
	resProf := profiles.ResourceProfiles().AppendEmpty()
	scopeProf := resProf.ScopeProfiles().AppendEmpty()
	prof := scopeProf.Profiles().AppendEmpty()
	st := prof.SampleType()
	st.SetTypeStrindex(0)
	st.SetUnitStrindex(1)
	prof.Samples().AppendEmpty() // Add a sample to ensure metric count > 0

	err := conn.ConsumeProfiles(context.Background(), profiles)
	assert.NoError(t, err)
	metrics := mockConsumer.AllMetrics()
	assert.Len(t, metrics, 1)
}

func TestConsumeProfiles_FrameTypeMetrics(t *testing.T) {
	mockConsumer := new(consumertest.MetricsSink)
	cfg := &Config{
		ByFrameType: true,
	}
	conn := &profilesToMetricsConnector{
		nextConsumer: mockConsumer,
		config:       cfg,
	}

	// Create a profiles object with a sample that has a location with a frame type attribute.
	profiles := pprofile.NewProfiles()
	profiles.Dictionary().StringTable().Append("sample")
	profiles.Dictionary().StringTable().Append("count")
	resProf := profiles.ResourceProfiles().AppendEmpty()
	scopeProf := resProf.ScopeProfiles().AppendEmpty()
	prof := scopeProf.Profiles().AppendEmpty()
	st := prof.SampleType()
	st.SetTypeStrindex(0)
	st.SetUnitStrindex(1)
	sample := prof.Samples().AppendEmpty()

	// Setup dictionary tables
	dict := profiles.Dictionary()
	locTable := dict.LocationTable()
	attrTable := dict.AttributeTable()
	strTable := dict.StringTable()
	stackTable := dict.StackTable()

	strTable.Append("")

	// Add an attribute for frame type
	attr := attrTable.AppendEmpty()
	attr.SetKeyStrindex(int32(strTable.Len()))
	strTable.Append(string(semconv.ProfileFrameTypeKey))
	attr.Value().SetStr("go")

	// Add a location referencing the attribute
	loc := locTable.AppendEmpty()
	loc.AttributeIndices().Append(0)

	stackTable.AppendEmpty()

	// Set sample to reference the stack
	sample.SetStackIndex(int32(stackTable.Len()))

	stack := stackTable.AppendEmpty()
	// Add location index to the stack's location indices
	stack.LocationIndices().Append(0)

	// Expect ConsumeMetrics to be called with metrics containing frame type metric
	assertMetricType := func(md pmetric.Metrics) bool {
		found := false
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sm := rms.At(i).ScopeMetrics()
			for j := 0; j < sm.Len(); j++ {
				metrics := sm.At(j).Metrics()
				for k := 0; k < metrics.Len(); k++ {
					metric := metrics.At(k)
					name := metric.Name()
					if name == "samples.frame_type" {
						// Verify it's a Gauge metric
						if metric.Type() == pmetric.MetricTypeGauge {
							gauge := metric.Gauge()
							// Check if any data point has the expected frame_type attribute
							for dp := 0; dp < gauge.DataPoints().Len(); dp++ {
								dataPoint := gauge.DataPoints().At(dp)
								if frameType, exists := dataPoint.Attributes().Get("frame_type"); exists && frameType.Str() == "go" {
									found = true
									break
								}
							}
						}
					}
				}
			}
		}
		return found
	}

	err := conn.ConsumeProfiles(context.Background(), profiles)
	assert.NoError(t, err)

	metrics := mockConsumer.AllMetrics()
	assert.Len(t, metrics, 1)
	assert.True(t, assertMetricType(metrics[0]))
}

func TestConsumeProfiles_MultipleSamplesAndFrameTypes(t *testing.T) {
	mockConsumer := new(consumertest.MetricsSink)
	cfg := &Config{
		ByFrameType: true,
	}
	conn := &profilesToMetricsConnector{
		nextConsumer: mockConsumer,
		config:       cfg,
	}

	profiles := pprofile.NewProfiles()
	profiles.Dictionary().StringTable().Append("sample")
	profiles.Dictionary().StringTable().Append("count")
	resProf := profiles.ResourceProfiles().AppendEmpty()
	scopeProf := resProf.ScopeProfiles().AppendEmpty()
	prof := scopeProf.Profiles().AppendEmpty()
	st := prof.SampleType()
	st.SetTypeStrindex(0)
	st.SetUnitStrindex(1)

	dict := profiles.Dictionary()
	locTable := dict.LocationTable()
	attrTable := dict.AttributeTable()
	strTable := dict.StringTable()
	stackTable := dict.StackTable()

	strTable.Append("")

	// Add two attributes for frame types
	attrGo := attrTable.AppendEmpty()
	attrGo.SetKeyStrindex(int32(strTable.Len()))
	strTable.Append(string(semconv.ProfileFrameTypeKey))
	attrGo.Value().SetStr("go")
	attrPy := attrTable.AppendEmpty()
	attrPy.SetKeyStrindex(int32(strTable.Len()))
	strTable.Append(string(semconv.ProfileFrameTypeKey))
	attrPy.Value().SetStr("python")

	// Add two locations, each referencing a different attribute
	locGo := locTable.AppendEmpty()
	locGo.AttributeIndices().Append(0)
	locPy := locTable.AppendEmpty()
	locPy.AttributeIndices().Append(1)

	stackTable.AppendEmpty()
	stackGo := stackTable.AppendEmpty()
	stackPy := stackTable.AppendEmpty()

	// Add location indices to the stack's location indices
	stackGo.LocationIndices().Append(0)
	stackPy.LocationIndices().Append(1)

	// Add two samples, each referencing a different location
	sampleGo := prof.Samples().AppendEmpty()
	sampleGo.SetStackIndex(1)
	samplePy := prof.Samples().AppendEmpty()
	samplePy.SetStackIndex(2)

	// Expect ConsumeMetrics to be called with both frame type metrics
	assertMetricsType := func(md pmetric.Metrics) bool {
		foundGo := false
		foundPy := false
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sm := rms.At(i).ScopeMetrics()
			for j := 0; j < sm.Len(); j++ {
				metrics := sm.At(j).Metrics()
				for k := 0; k < metrics.Len(); k++ {
					metric := metrics.At(k)
					name := metric.Name()
					if name == "samples.frame_type" {
						// Verify it's a Gauge metric
						if metric.Type() == pmetric.MetricTypeGauge {
							gauge := metric.Gauge()
							// Check data points for both frame types
							for dp := 0; dp < gauge.DataPoints().Len(); dp++ {
								dataPoint := gauge.DataPoints().At(dp)
								if frameType, exists := dataPoint.Attributes().Get("frame_type"); exists {
									if frameType.Str() == "go" {
										foundGo = true
									}
									if frameType.Str() == "python" {
										foundPy = true
									}
								}
							}
						}
					}
				}
			}
		}
		return foundGo && foundPy
	}

	err := conn.ConsumeProfiles(context.Background(), profiles)
	assert.NoError(t, err)
	metrics := mockConsumer.AllMetrics()
	assert.Len(t, metrics, 1)
	assert.True(t, assertMetricsType(metrics[0]))
}

func TestConsumeProfiles_NoMetrics(t *testing.T) {
	mockConsumer := new(consumertest.MetricsSink)
	cfg := &Config{}
	conn := &profilesToMetricsConnector{
		nextConsumer: mockConsumer,
		config:       cfg,
	}

	// Create a profiles object that will result in zero metrics.
	profiles := pprofile.NewProfiles()
	// No ResourceProfiles added, so no metrics.

	// Expect ConsumeMetrics NOT to be called.
	err := conn.ConsumeProfiles(context.Background(), profiles)
	assert.NoError(t, err)
	assert.Len(t, mockConsumer.AllMetrics(), 0)
}

func TestCollectClassificationCounts_GoFrameType(t *testing.T) {
	cfg := &Config{
		ByClassification: true,
	}
	conn := &profilesToMetricsConnector{
		config: cfg,
	}

	// Setup dictionary tables
	profiles := pprofile.NewProfiles()
	dict := profiles.Dictionary()
	strTable := dict.StringTable()
	locTable := dict.LocationTable()
	attrTable := dict.AttributeTable()
	funcTable := dict.FunctionTable()
	stackTable := dict.StackTable()

	// Add strings for function name and package
	fnNameIdx := strTable.Len()
	strTable.Append("mypkg.myfunc")

	// Add function entry
	fnEntry := funcTable.AppendEmpty()
	fnEntry.SetNameStrindex(int32(fnNameIdx))

	// Add attribute for frame type "go"
	attrIdx := attrTable.Len()
	attr := attrTable.AppendEmpty()
	attr.SetKeyStrindex(int32(strTable.Len()))
	strTable.Append(string(semconv.ProfileFrameTypeKey))
	attr.Value().SetStr("go")

	// Add location referencing the attribute and function
	locIdx := locTable.Len()
	loc := locTable.AppendEmpty()
	loc.AttributeIndices().Append(int32(attrIdx))
	line := loc.Lines().AppendEmpty()
	line.SetFunctionIndex(int32(fnNameIdx))

	stackTable.AppendEmpty()

	// Prepare sample referencing the location
	sample := pprofile.NewSample()
	sample.SetStackIndex(int32(stackTable.Len()))
	stackTable.AppendEmpty()

	// Prepare location indices
	locationIndices := pcommon.NewInt32Slice()
	locationIndices.Append(int32(locIdx))

	// Prepare classificationCounts map
	classificationCounts := make(map[string]map[string]int64)

	// Call collectClassificationCounts
	conn.collectClassificationCounts(dict, locationIndices, sample, classificationCounts)

	// Should have one entry for frameTypeGo and package "mypkg"
	if assert.Contains(t, classificationCounts, frameTypeGo) {
		// The extractGolangInfo should extract "mypkg" as package
		found := false
		for k := range classificationCounts[frameTypeGo] {
			if k == "mypkg" {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected package 'mypkg' in classificationCounts[frameTypeGo]")
	}
}
