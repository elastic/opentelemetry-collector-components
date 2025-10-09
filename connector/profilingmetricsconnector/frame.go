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
	"regexp"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"

	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

type metric struct {
	name string
	desc string
}

type frameInfo struct {
	typ      string
	filename string
}

const (
	nativeLibraryAttrName = "shlib_name"
	syscallAttrName       = "syscall_name"
)

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

	allowedFrameTypes = map[string]metric{
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

	// match shared libraries
	rx = regexp.MustCompile(`(?:.*/)?(.+)\.so`)

	// match syscalls
	syscallRx = regexp.MustCompile(`^__(?:x64|arm64)_sys_(\w+)`)
)

func fetchLeafFrameInfo(dictionary pprofile.ProfilesDictionary,
	locationIndices pcommon.Int32Slice,
	sampleLocationIndex int,
) (frameInfo, error) {
	attrTable := dictionary.AttributeTable()
	locationTable := dictionary.LocationTable()
	strTable := dictionary.StringTable()

	strLen := strTable.Len()

	liLen := locationIndices.Len()
	if sampleLocationIndex >= liLen {
		return frameInfo{}, fmt.Errorf("fetchFrameInfo: sli %d >= %d",
			sampleLocationIndex, liLen)
	}

	li := locationIndices.At(sampleLocationIndex)
	ltLen := locationTable.Len()
	if int(li) >= ltLen {
		return frameInfo{}, fmt.Errorf("fetchFrameInfo: li %d >= %d",
			li, ltLen)
	}

	leaf := locationTable.At(int(li))
	frameType := ""
	aLen := attrTable.Len()
	for _, idx := range leaf.AttributeIndices().All() {
		if int(idx) >= aLen {
			return frameInfo{}, fmt.Errorf("fetchFrameInfo: ai %d >= %d",
				idx, aLen)
		}
		attr := attrTable.At(int(idx))
		if int(attr.KeyStrindex()) >= strLen {
			return frameInfo{}, fmt.Errorf("fetchFrameInfo: attr.KeyStrindex %d >= %d",
				attr.KeyStrindex(), strLen)
		}

		if strTable.At(int(attr.KeyStrindex())) == string(semconv.ProfileFrameTypeKey) {
			frameType = attr.Value().Str()
			if _, ok := allowedFrameTypes[frameType]; !ok {
				return frameInfo{}, fmt.Errorf("fetchFrameInfo: unknown frame type %v",
					frameType)
			}
			break
		}
	}
	if frameType == "" {
		return frameInfo{}, fmt.Errorf("fetchFrameInfo: empty frame type")
	}

	var fname string
	if frameType == frameTypeNative {
		// Extract mapping filename
		mappingTable := dictionary.MappingTable()
		mtLen := mappingTable.Len()
		mi := leaf.MappingIndex()
		if int(mi) >= mtLen {
			return frameInfo{}, fmt.Errorf("fetchFrameInfo: mi %d >= %d",
				mi, mtLen)
		}
		mapFilenameStri := int(mappingTable.At(int(mi)).FilenameStrindex())
		if mapFilenameStri >= strLen {
			return frameInfo{}, fmt.Errorf("fetchFrameInfo: mapping.FilenameStridex %d >= %d",
				mapFilenameStri, strLen)
		}
		fname = strTable.At(mapFilenameStri)
	}

	// End validation
	return frameInfo{typ: frameType, filename: fname}, nil
}

// classifyFrame classifies sample into one or more categories based on frame type.
// This takes place by incrementing the associated metric count.
func classifyFrame(dictionary pprofile.ProfilesDictionary,
	locationIndices pcommon.Int32Slice,
	sample pprofile.Sample,
	counts map[metric]int64,
	nativeCounts map[string]int64,
) error {
	leaf, err := fetchLeafFrameInfo(dictionary, locationIndices, 0)
	if err != nil {
		return err
	}

	leafFrameType := leaf.typ
	// We don't need a separate metric for total number of samples, as this can always be
	// derived from summing the metricKernel and metricUser counts.
	metric := allowedFrameTypes[leafFrameType]

	// TODO: Scale all counts by number of events in each Sample. Currently,
	// this logic assumes 1 event per Sample (thus the increments by 1 below),
	// which isn't necessarily the case.

	if leafFrameType != frameTypeKernel {
		counts[metricUser]++
	}

	if leafFrameType != frameTypeNative {
		counts[metric]++
		return nil
	}

	// Extract native library name and increment associated count
	if sm := rx.FindStringSubmatch(leaf.filename); sm != nil {
		nativeCounts[sm[1]]++
	} else {
		counts[metric]++
	}

	return nil
}

// identifySyscall walks the frames and extracts the syscall information.
func identifySyscall(dictionary pprofile.ProfilesDictionary,
	locationIndices pcommon.Int32Slice,
	syscallCounts map[string]int64,
) error {
	// TODO: Scale syscallCounts by number of events in each Sample. Currently,
	// this logic assumes 1 event per Sample (thus the increments by 1 below),
	// which isn't necessarily the case.
	attrTable := dictionary.AttributeTable()
	locationTable := dictionary.LocationTable()
	strTable := dictionary.StringTable()
	funcTable := dictionary.FunctionTable()

	attrTblLen := attrTable.Len()
	locTblLen := locationTable.Len()
	strTblLen := strTable.Len()
	funcTblLen := funcTable.Len()

	for _, li := range locationIndices.All() {
		if li >= int32(locTblLen) {
			// log error
			continue
		}
		loc := locationTable.At(int(li))
		for _, attrIdx := range loc.AttributeIndices().All() {
			if attrIdx >= int32(attrTblLen) {
				// log error
				continue
			}
			attr := attrTable.At(int(attrIdx))
			if int(attr.KeyStrindex()) >= strTblLen {
				// log error
				continue
			}

			if strTable.At(int(attr.KeyStrindex())) == string(semconv.ProfileFrameTypeKey) {
				frameType := attr.Value().Str()
				if frameType == frameTypeKernel {
					for _, ln := range loc.Line().All() {
						if ln.FunctionIndex() >= int32(funcTblLen) {
							// log error
							continue
						}
						fn := funcTable.At(int(ln.FunctionIndex()))
						if fn.NameStrindex() >= int32(strTblLen) {
							// log error
							continue
						}
						fnName := strTable.At(int(fn.NameStrindex()))

						// Avoid string allocations by using indices to string location.
						indices := syscallRx.FindStringSubmatchIndex(fnName)
						if len(indices) == 4 {
							syscall := fnName[indices[2]:indices[3]]
							syscallCounts[syscall]++
							return nil
						}
					}

				}
			}

		}

	}
	return nil
}

func (c *profilesToMetricsConnector) addFrameMetrics(dictionary pprofile.ProfilesDictionary,
	profile pprofile.Profile, scopeMetrics pmetric.ScopeMetrics,
) {
	stackTable := dictionary.StackTable()

	counts := make(map[metric]int64)
	nativeCounts := make(map[string]int64)
	syscallCounts := make(map[string]int64)

	// Process all samples and extract metric counts
	for _, sample := range profile.Sample().All() {
		stack := stackTable.At(int(sample.StackIndex()))
		if err := classifyFrame(dictionary, stack.LocationIndices(),
			sample, counts, nativeCounts); err != nil {
			// Should not happen with well-formed profile data
			// TODO: Add error metric or log error
		}

		if err := identifySyscall(dictionary, stack.LocationIndices(),
			syscallCounts); err != nil {
			// Should not happen with well-formed profile data
			// TODO: Add error metric or log error
		}
	}

	// Generate metrics
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

	for libraryName, count := range nativeCounts {
		m := scopeMetrics.Metrics().AppendEmpty()
		m.SetName(c.config.MetricsPrefix + metricNative.name)
		m.SetDescription(metricNative.desc)
		m.SetUnit("1")

		sum := m.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

		dp := sum.DataPoints().AppendEmpty()
		dp.SetTimestamp(profile.Time())
		dp.SetIntValue(count)
		dp.Attributes().PutStr(nativeLibraryAttrName, libraryName)
	}

	for sysCall, count := range syscallCounts {
		m := scopeMetrics.Metrics().AppendEmpty()
		m.SetName(c.config.MetricsPrefix + metricNative.name)
		m.SetDescription(metricNative.desc)
		m.SetUnit("1")

		sum := m.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

		dp := sum.DataPoints().AppendEmpty()
		dp.SetTimestamp(profile.Time())
		dp.SetIntValue(count)
		dp.Attributes().PutStr(syscallAttrName, sysCall)
	}
}
