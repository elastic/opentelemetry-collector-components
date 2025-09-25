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
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"

	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

type metric struct {
	name string
	desc string
}

var (
	metricUser   = metric{name: "samples.user.count", desc: "Number of samples executing userspace code (self)"}
	metricKernel = metric{name: "samples.kernel.count", desc: "Number of samples executing kernel code (self)"}
	metricNative = metric{name: "samples.native.count", desc: "Number of samples executing native code (self)"}
	metricJVM    = metric{name: "samples.jvm.count", desc: "Number of samples executing HotSpot code (self)"}
	metricPython = metric{name: "samples.cpython.count", desc: "Number of samples executing Python code (self)"}
	metricGo     = metric{name: "samples.go.count", desc: "Number of samples executing Go code (self)"}
	metricV8JS   = metric{name: "samples.v8js.count", desc: "Number of samples executing V8 JS code (self)"}
	metricPHP    = metric{name: "samples.php.count", desc: "Number of samples executing PHP code (self)"}
	metricPerl   = metric{name: "samples.perl.count", desc: "Number of samples executing Perl code (self)"}
	metricRuby   = metric{name: "samples.ruby.count", desc: "Number of samples executing Ruby code (self)"}
	metricDotnet = metric{name: "samples.dotnet.count", desc: "Number of samples executing Dotnet code (self)"}
	metricRust   = metric{name: "samples.rust.count", desc: "Number of samples executing Rust code (self)"}
	metricBeam   = metric{name: "samples.beam.count", desc: "Number of samples executing Beam code (self)"}
)

var allowedFrameTypes = map[string]metric{
	frameTypeNative: metricNative,
	frameTypeKernel: metricKernel,
	frameTypeJVM:    metricJVM,
	frameTypePython: metricPython,
	frameTypeGo:     metricGo,
	frameTypeV8JS:   metricV8JS,
	frameTypePHP:    metricPHP,
	frameTypePerl:   metricPerl,
	frameTypeRuby:   metricRuby,
	frameTypeDotnet: metricDotnet,
	frameTypeRust:   metricRust,
	frameTypeBeam:   metricBeam,
}

func fetchFrameType(dictionary pprofile.ProfilesDictionary,
	locationIndices pcommon.Int32Slice,
	sampleLocationIndex int,
) (string, error) {
	attrTable := dictionary.AttributeTable()
	locationTable := dictionary.LocationTable()
	strTable := dictionary.StringTable()

	lilen := locationIndices.Len()
	if sampleLocationIndex >= lilen {
		return "", fmt.Errorf("fetchFrameType: sli %d >= %d",
			sampleLocationIndex, lilen)
	}

	li := locationIndices.At(sampleLocationIndex)
	ltlen := locationTable.Len()
	if int(li) >= ltlen {
		return "", fmt.Errorf("fetchFrameType: li %d >= %d",
			li, ltlen)
	}

	leaf := locationTable.At(int(li))
	frameType := ""
	alen := attrTable.Len()
	for _, idx := range leaf.AttributeIndices().All() {
		if int(idx) >= alen {
			return "", fmt.Errorf("fetchFrameType: ai %d >= %d",
				idx, alen)
		}
		attr := attrTable.At(int(idx))
		if strTable.At(int(attr.KeyStrindex())) == string(semconv.ProfileFrameTypeKey) {
			frameType = attr.Value().Str()
			if _, ok := allowedFrameTypes[frameType]; !ok {
				return "", fmt.Errorf("fetchFrameType: unknown frame type %v",
					frameType)
			}
			break
		}
	}
	if frameType == "" {
		return "", fmt.Errorf("fetchFrameType: empty frame type")
	}

	// End validation
	return frameType, nil
}

// classify classifies sample into one or more categories.
// This takes place by increasing the associated metric count.
func classify(dictionary pprofile.ProfilesDictionary,
	locationIndices pcommon.Int32Slice,
	sample pprofile.Sample, counts map[metric]int64,
) error {
	stackTable := dictionary.StackTable()

	// Fetch leaf frame which is always stored first
	sli := stackTable.At(int(sample.StackIndex())).LocationIndices().At(0)
	leafFrameType, err := fetchFrameType(dictionary, locationIndices, int(sli))
	if err != nil {
		return err
	}

	// We don't need a separate metric for total number of samples, as this can always be
	// derived from summing the metricKernel and metricUser counts.
	metric := allowedFrameTypes[leafFrameType]
	counts[metric]++

	if leafFrameType != frameTypeKernel {
		counts[metricUser]++
	}

	return nil
}

func (c *profilesToMetricsConnector) addFrameMetrics(dictionary pprofile.ProfilesDictionary,
	profile pprofile.Profile, scopeMetrics pmetric.ScopeMetrics,
) {
	stackTable := dictionary.StackTable()

	// Process all samples and extract metric counts
	counts := make(map[metric]int64)
	for _, sample := range profile.Sample().All() {
		stack := stackTable.At(int(sample.StackIndex()))
		if err := classify(dictionary, stack.LocationIndices(), sample, counts); err != nil {
			// Should not happen with well-formed profile data
			// TODO: Add error metric or log error
			continue
		}
	}

	for metric, count := range counts {
		m := scopeMetrics.Metrics().AppendEmpty()
		m.SetName(c.config.MetricsPrefix + metric.name)
		m.SetDescription(metric.desc)
		m.SetUnit("1")

		sum := m.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

		dp := sum.DataPoints().AppendEmpty()
		dp.SetTimestamp(profile.Time())
		dp.SetIntValue(count)
	}
}
