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

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"

	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

// profilesToMetricsConnector implements xconnector.Profiles
type profilesToMetricsConnector struct {
	nextConsumer consumer.Metrics
	config       *Config
}

// Capabilities returns the consumer capabilities.
func (c *profilesToMetricsConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{
		MutatesData: false,
	}
}

// ConsumeProfiles processes profiles data and extracts metrics.
func (c *profilesToMetricsConnector) ConsumeProfiles(ctx context.Context, profiles pprofile.Profiles) error {
	metrics := c.extractMetricsFromProfiles(profiles)

	if metrics.MetricCount() > 0 {
		return c.nextConsumer.ConsumeMetrics(ctx, metrics)
	}

	return nil
}

func (c *profilesToMetricsConnector) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (c *profilesToMetricsConnector) Shutdown(ctx context.Context) error {
	return nil
}

// extractMetricsFromProfiles extracts basic metrics from the profiles data.
func (c *profilesToMetricsConnector) extractMetricsFromProfiles(profiles pprofile.Profiles) pmetric.Metrics {
	metrics := pmetric.NewMetrics()

	dictionary := profiles.ProfilesDictionary()
	resourceProfiles := profiles.ResourceProfiles().All()
	for _, resourceProfile := range resourceProfiles {
		resourceMetrics := metrics.ResourceMetrics().AppendEmpty()

		// Copy resource attributes
		resourceProfile.Resource().Attributes().CopyTo(resourceMetrics.Resource().Attributes())

		// Process each scope's profiles
		scopeProfiles := resourceProfile.ScopeProfiles().All()
		for _, scopeProfile := range scopeProfiles {
			scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()

			// Copy scope information
			scopeProfile.Scope().CopyTo(scopeMetrics.Scope())

			// Extract metrics from profiles in this scope
			c.extractMetricsFromScopeProfiles(dictionary, scopeProfile, scopeMetrics)
		}
	}

	return metrics
}

// extractMetricsFromScopeProfiles extracts basic metrics from scope-level profile data.
func (c *profilesToMetricsConnector) extractMetricsFromScopeProfiles(dictionary pprofile.ProfilesDictionary, scopeProfile pprofile.ScopeProfiles,
	scopeMetrics pmetric.ScopeMetrics) {
	profiles := scopeProfile.Profiles().All()

	for _, profile := range profiles {
		// Add basic sample count metric.
		c.addSampleCountMetric(profile, scopeMetrics)

		// Collect frame type information.
		frameTypeCounts := make(map[string]int64)
		for _, sample := range profile.Sample().All() {
			c.collectFrameTypeCounts(dictionary, sample, frameTypeCounts)
		}

		// Add metric for frame types.
		c.addAggregatedFrameTypeMetrics(frameTypeCounts, scopeMetrics, profile.Time())
	}
}

// collectFrameTypeCounts walks all locations/frames of a sample and collects the frame type information.
func (c *profilesToMetricsConnector) collectFrameTypeCounts(dictionary pprofile.ProfilesDictionary, sample pprofile.Sample, frameTypeCounts map[string]int64) {
	locationTable := dictionary.LocationTable()
	attrTable := dictionary.AttributeTable()

	for li := sample.LocationsStartIndex(); li < sample.LocationsStartIndex()+sample.LocationsLength(); li++ {
		if int(li) >= locationTable.Len() {
			continue
		}
		loc := locationTable.At(int(li))

		for _, idx := range loc.AttributeIndices().All() {
			if int(idx) >= attrTable.Len() {
				continue
			}
			attr := attrTable.At(int(idx))
			if attr.Key() == string(semconv.ProfileFrameTypeKey) {
				language := attr.Value().Str()
				frameTypeCounts[language]++
				break
			}
		}
	}
}

// addAggregatedFrameTypeMetrics converts and adds frame type information as metric to the scopeMetrics.
func (c *profilesToMetricsConnector) addAggregatedFrameTypeMetrics(frameTypeCounts map[string]int64, scopeMetrics pmetric.ScopeMetrics, ts pcommon.Timestamp) {
	for lang, count := range frameTypeCounts {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName(c.config.MetricsPrefix + "samples.frame_type." + lang)
		metric.SetDescription("")
		metric.SetUnit("1")

		sum := metric.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

		dataPoint := sum.DataPoints().AppendEmpty()
		dataPoint.SetIntValue(count)
		dataPoint.SetTimestamp(ts)
	}
}

// addSampleCountMetric adds a metric for the total number of samples.
func (c *profilesToMetricsConnector) addSampleCountMetric(profile pprofile.Profile, scopeMetrics pmetric.ScopeMetrics) {
	metric := scopeMetrics.Metrics().AppendEmpty()
	metric.SetName(c.config.MetricsPrefix + "samples.count")
	metric.SetDescription("Total number of profiling samples")
	metric.SetUnit("1")

	sum := metric.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	dataPoint := sum.DataPoints().AppendEmpty()
	dataPoint.SetTimestamp(profile.Time())
	dataPoint.SetIntValue(int64(profile.Sample().Len()))
}
