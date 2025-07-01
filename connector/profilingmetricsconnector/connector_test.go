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

package profilingmetricsconnector

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
)

// mockMetricsConsumer is a mock implementation of consumer.Metrics.
type mockMetricsConsumer struct {
	mock.Mock
}

func (m *mockMetricsConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (m *mockMetricsConsumer) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	args := m.Called(ctx, md)
	return args.Error(0)
}

func TestConsumeProfiles_WithMetrics(t *testing.T) {
	mockConsumer := new(mockMetricsConsumer)
	cfg := &Config{MetricsPrefix: "test."}
	conn := &profilesToMetricsConnector{
		nextConsumer: mockConsumer,
		config:       cfg,
	}

	// Create a profiles object that will result in at least one metric.
	profiles := pprofile.NewProfiles()
	// Add a ResourceProfile with one ScopeProfile and one Profile with one sample.
	resProf := profiles.ResourceProfiles().AppendEmpty()
	scopeProf := resProf.ScopeProfiles().AppendEmpty()
	prof := scopeProf.Profiles().AppendEmpty()
	prof.Sample().AppendEmpty() // Add a sample to ensure metric count > 0

	// Expect ConsumeMetrics to be called once.
	mockConsumer.On("ConsumeMetrics", mock.Anything, mock.MatchedBy(func(md pmetric.Metrics) bool {
		return md.MetricCount() > 0
	})).Return(nil).Once()

	err := conn.ConsumeProfiles(context.Background(), profiles)
	assert.NoError(t, err)
	mockConsumer.AssertExpectations(t)
}

func TestConsumeProfiles_FrameTypeMetrics(t *testing.T) {
	mockConsumer := new(mockMetricsConsumer)
	cfg := &Config{MetricsPrefix: "test."}
	conn := &profilesToMetricsConnector{
		nextConsumer: mockConsumer,
		config:       cfg,
	}

	// Create a profiles object with a sample that has a location with a frame type attribute.
	profiles := pprofile.NewProfiles()
	resProf := profiles.ResourceProfiles().AppendEmpty()
	scopeProf := resProf.ScopeProfiles().AppendEmpty()
	prof := scopeProf.Profiles().AppendEmpty()
	sample := prof.Sample().AppendEmpty()

	// Setup dictionary tables
	dict := profiles.ProfilesDictionary()
	locTable := dict.LocationTable()
	attrTable := dict.AttributeTable()

	// Add an attribute for frame type
	attr := attrTable.AppendEmpty()
	attr.SetKey("profile.frame.type")
	attr.Value().SetStr("go")

	// Add a location referencing the attribute
	loc := locTable.AppendEmpty()
	loc.AttributeIndices().Append(0)

	// Set sample to reference the location
	sample.SetLocationsStartIndex(0)
	sample.SetLocationsLength(1)

	// Expect ConsumeMetrics to be called with metrics containing frame type metric
	mockConsumer.On("ConsumeMetrics", mock.Anything, mock.MatchedBy(func(md pmetric.Metrics) bool {
		found := false
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sm := rms.At(i).ScopeMetrics()
			for j := 0; j < sm.Len(); j++ {
				metrics := sm.At(j).Metrics()
				for k := 0; k < metrics.Len(); k++ {
					name := metrics.At(k).Name()
					if name == "test.samples.frame_type.go" {
						found = true
					}
				}
			}
		}
		return found
	})).Return(nil).Once()

	err := conn.ConsumeProfiles(context.Background(), profiles)
	assert.NoError(t, err)
	mockConsumer.AssertExpectations(t)
}

func TestConsumeProfiles_MultipleSamplesAndFrameTypes(t *testing.T) {
	mockConsumer := new(mockMetricsConsumer)
	cfg := &Config{MetricsPrefix: "test."}
	conn := &profilesToMetricsConnector{
		nextConsumer: mockConsumer,
		config:       cfg,
	}

	profiles := pprofile.NewProfiles()
	resProf := profiles.ResourceProfiles().AppendEmpty()
	scopeProf := resProf.ScopeProfiles().AppendEmpty()
	prof := scopeProf.Profiles().AppendEmpty()

	dict := profiles.ProfilesDictionary()
	locTable := dict.LocationTable()
	attrTable := dict.AttributeTable()

	// Add two attributes for frame types
	attrGo := attrTable.AppendEmpty()
	attrGo.SetKey("profile.frame.type")
	attrGo.Value().SetStr("go")
	attrPy := attrTable.AppendEmpty()
	attrPy.SetKey("profile.frame.type")
	attrPy.Value().SetStr("python")

	// Add two locations, each referencing a different attribute
	locGo := locTable.AppendEmpty()
	locGo.AttributeIndices().Append(0)
	locPy := locTable.AppendEmpty()
	locPy.AttributeIndices().Append(1)

	// Add two samples, each referencing a different location
	sampleGo := prof.Sample().AppendEmpty()
	sampleGo.SetLocationsStartIndex(0)
	sampleGo.SetLocationsLength(1)
	samplePy := prof.Sample().AppendEmpty()
	samplePy.SetLocationsStartIndex(1)
	samplePy.SetLocationsLength(1)

	// Expect ConsumeMetrics to be called with both frame type metrics
	mockConsumer.On("ConsumeMetrics", mock.Anything, mock.MatchedBy(func(md pmetric.Metrics) bool {
		foundGo := false
		foundPy := false
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sm := rms.At(i).ScopeMetrics()
			for j := 0; j < sm.Len(); j++ {
				metrics := sm.At(j).Metrics()
				for k := 0; k < metrics.Len(); k++ {
					name := metrics.At(k).Name()
					if name == "test.samples.frame_type.go" {
						foundGo = true
					}
					if name == "test.samples.frame_type.python" {
						foundPy = true
					}
				}
			}
		}
		return foundGo && foundPy
	})).Return(nil).Once()

	err := conn.ConsumeProfiles(context.Background(), profiles)
	assert.NoError(t, err)
	mockConsumer.AssertExpectations(t)
}

func TestConsumeProfiles_NoMetrics(t *testing.T) {
	mockConsumer := new(mockMetricsConsumer)
	cfg := &Config{MetricsPrefix: "test."}
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
	mockConsumer.AssertNotCalled(t, "ConsumeMetrics", mock.Anything, mock.Anything)
}

func TestConsumeProfiles_ConsumeMetricsError(t *testing.T) {
	mockConsumer := new(mockMetricsConsumer)
	cfg := &Config{MetricsPrefix: "test."}
	conn := &profilesToMetricsConnector{
		nextConsumer: mockConsumer,
		config:       cfg,
	}

	// Create a profiles object that will result in at least one metric.
	profiles := pprofile.NewProfiles()
	resProf := profiles.ResourceProfiles().AppendEmpty()
	scopeProf := resProf.ScopeProfiles().AppendEmpty()
	prof := scopeProf.Profiles().AppendEmpty()
	prof.Sample().AppendEmpty()

	mockConsumer.On("ConsumeMetrics", mock.Anything, mock.Anything).Return(errors.New("consume error")).Once()

	err := conn.ConsumeProfiles(context.Background(), profiles)
	assert.Error(t, err)
	assert.EqualError(t, err, "consume error")
	mockConsumer.AssertExpectations(t)
}
