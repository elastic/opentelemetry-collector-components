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
	"strings"

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
	typ          string
	fileName     string
	functionName string
}

type class struct {
	name string
	rx   *regexp.Regexp
}

const (
	nativeLibraryAttrName = "shlib_name"
)

var kernelClassAttrNames = [...]string{
	"kernel_0",
	"kernel_1",
	"kernel_2",
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

	shlibRx = regexp.MustCompile(`(?:.*/)?(.+)\.so`)

	// The following regular expressions try to match certain kernel functions
	// and derive a classification for an entire kernel stacktrace based on the match.
	// The ordering is *significant* as some of the following functions may appear
	// in stacktraces that result in multiple classifications *for the same*
	// stacktrace which would be incorrect. The elements are ordered in decreasing
	// priority: if there are multiple classifications, the element with the lower
	// slice index always wins.
	classes = []class{
		{name: "network/tcp/read",
			rx: regexp.MustCompile(`^(?:tcp_recvmsg|tcp_read|tcp_rcv|tcp_v4_rcv|tcp_v6_rcv)`)},
		{name: "network/tcp/write",
			rx: regexp.MustCompile(`^(?:tcp_sendmsg|tcp_transmit|tcp_write|tcp_push|tcp_sendpage)`)},
		{name: "network/udp/read",
			rx: regexp.MustCompile(`^(?:udp_recvmsg|udp_read|udp_rcv|udp_v4_rcv|udp_v6_rcv)`)},
		{name: "network/udp/write",
			rx: regexp.MustCompile(`^(?:udp_sendmsg|udp_write|udp_push|udp_sendpage)`)},
		{name: "ipc/read",
			rx: regexp.MustCompile(`^(?:pipe_read|pipe_read_iter|eventfd_read` +
				`|unix_stream_read_generic|unix_stream_recvmsg)`)},
		{name: "ipc/write",
			rx: regexp.MustCompile(`^(?:pipe_write|pipe_write_iter|eventfd_write` +
				`|unix_stream_write_generic|unix_stream_sendmsg)`)},
		{name: "network/other/read",
			rx: regexp.MustCompile(`^(?:sock_recvmsg|__sock_recvmsg|sock_read_iter)`)},
		{name: "network/other/write",
			rx: regexp.MustCompile(`^(?:sock_sendmsg|__sock_sendmsg|sock_write_iter)`)},
		{name: "disk/read",
			rx: regexp.MustCompile(`^(?:ext4_file_read_iter|xfs_file_read_iter` +
				`|btrfs_file_read_iter|filemap_read|generic_file_read_iter)`)},
		{name: "disk/write",
			rx: regexp.MustCompile(`^(?:ext4_file_write_iter|xfs_file_write_iter` +
				`|btrfs_file_write_iter|filemap_write|generic_file_write_iter` +
				`|writeback_sb_inodes)`)},
		{name: "memory",
			rx: regexp.MustCompile(`^do_mmap|do_munmap|^do_anonymous_page|mmap_|madvise` +
				`|^handle_mm_fault|^alloc_pages|^free_pages`)},
		{name: "synchronization",
			rx: regexp.MustCompile(`futex_|^schedule|__schedule|^wake_up_|^wake_q_`)},
	}
)

func fetchFrameInfo(dictionary pprofile.ProfilesDictionary,
	locationIndices pcommon.Int32Slice,
	sampleLocationIndex int,
) (frameInfo, error) {
	attrTable := dictionary.AttributeTable()
	locTable := dictionary.LocationTable()
	strTable := dictionary.StringTable()
	funcTable := dictionary.FunctionTable()

	strTbLen := strTable.Len()
	locTbLen := locTable.Len()
	attrTbLen := attrTable.Len()
	funcTbLen := funcTable.Len()
	liLen := locationIndices.Len()

	if sampleLocationIndex >= liLen {
		return frameInfo{}, fmt.Errorf("fetchFrameInfo: sli %d >= %d",
			sampleLocationIndex, liLen)
	}

	li := locationIndices.At(sampleLocationIndex)
	if int(li) >= locTbLen {
		return frameInfo{}, fmt.Errorf("fetchFrameInfo: li %d >= %d",
			li, locTbLen)
	}

	loc := locTable.At(int(li))
	frameType := ""

	for _, idx := range loc.AttributeIndices().All() {
		if int(idx) >= attrTbLen {
			return frameInfo{}, fmt.Errorf("fetchFrameInfo: ai %d >= %d",
				idx, attrTbLen)
		}
		attr := attrTable.At(int(idx))
		if int(attr.KeyStrindex()) >= strTbLen {
			return frameInfo{}, fmt.Errorf("fetchFrameInfo: attr.KeyStrindex %d >= %d",
				attr.KeyStrindex(), strTbLen)
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

	fileName := ""
	funcName := ""

	switch frameType {
	case frameTypeNative:
		// Extract mapping filename
		mapTable := dictionary.MappingTable()
		mapTbLen := mapTable.Len()
		mi := loc.MappingIndex()
		if int(mi) >= mapTbLen {
			// TODO: log error
			// return frameInfo{}, fmt.Errorf("fetchFrameInfo: mi %d >= %d",
			//	mi, mapTbLen)
			break
		}
		mapFilenameStri := int(mapTable.At(int(mi)).FilenameStrindex())
		if mapFilenameStri >= strTbLen {
			// TODO: log error
			// return frameInfo{}, fmt.Errorf("fetchFrameInfo: mapping.FilenameStridex %d >= %d",
			//	mapFilenameStri, strTbLen)
			break
		}
		fileName = strTable.At(mapFilenameStri)
	case frameTypeKernel:
		// Extract function name
		for _, ln := range loc.Line().All() {
			if ln.FunctionIndex() >= int32(funcTbLen) {
				// TODO: log error
				continue
			}
			fn := funcTable.At(int(ln.FunctionIndex()))
			if fn.NameStrindex() >= int32(strTbLen) {
				// TODO: log error
				continue
			}
			funcName = strTable.At(int(fn.NameStrindex()))
		}
	}

	// End validation
	return frameInfo{
		typ:          frameType,
		fileName:     fileName,
		functionName: funcName,
	}, nil
}

func classifyLeaf(fi frameInfo,
	counts map[metric]int64,
	nativeCounts map[string]int64) {
	ft := fi.typ

	// We don't need a separate metric for total number of samples, as this can always be
	// derived from summing the metricKernel and metricUser counts.
	metric := allowedFrameTypes[ft]

	// TODO: Scale all counts by number of events in each Sample. Currently,
	// this logic assumes 1 event per Sample (thus the increments by 1 below),
	// which isn't necessarily the case.
	if ft != frameTypeKernel {
		counts[metricUser]++
	}

	if ft == frameTypeNative && fi.fileName != "" {
		// Extract native library name and increment associated count
		if sm := shlibRx.FindStringSubmatch(fi.fileName); sm != nil {
			nativeCounts[sm[1]]++
			return
		}
	}
	if ft != frameTypeKernel {
		counts[metric]++
	}
}

// classifyFrames classifies a sample into one or more categories based on frame information.
// This takes place by incrementing the associated metric count.
func classifyFrames(dictionary pprofile.ProfilesDictionary,
	locationIndices pcommon.Int32Slice,
	counts map[metric]int64,
	nativeCounts map[string]int64,
	kernelCounts map[string]int64,
) error {
	var err error

	className := ""
	lastMatchIdx := len(classes)
	hasKernel := false
	for idx := range locationIndices.Len() {
		fi, err := fetchFrameInfo(dictionary, locationIndices, idx)
		if err != nil {
			break
		}

		if idx == 0 {
			// Only need to call this once per stacktrace
			classifyLeaf(fi, counts, nativeCounts)
		}

		// Kernel frame specific logic follows
		if fi.typ != frameTypeKernel {
			break
		}

		hasKernel = true
		if fi.functionName == "" {
			// No classification possible for this frame
			continue
		}

		for cIdx, class := range classes {
			// Priority matching logic
			if cIdx == lastMatchIdx {
				// Early exit if there's no higher priority match possible
				break
			}
			if class.rx.MatchString(fi.functionName) {
				className = class.name
				lastMatchIdx = cIdx
				break
			}
		}
	}

	if !hasKernel {
		return err
	}

	if className == "" {
		// No kernel category match found
		counts[metricKernel]++
	} else {
		kernelCounts[className]++
	}

	return err
}

func (c *profilesToMetricsConnector) addFrameMetrics(dictionary pprofile.ProfilesDictionary,
	profile pprofile.Profile, scopeMetrics pmetric.ScopeMetrics,
) {
	stackTable := dictionary.StackTable()

	counts := make(map[metric]int64)
	nativeCounts := make(map[string]int64)
	kernelCounts := make(map[string]int64)

	// Process all samples and extract metric counts
	for _, sample := range profile.Sample().All() {
		stack := stackTable.At(int(sample.StackIndex()))
		if err := classifyFrames(dictionary, stack.LocationIndices(),
			counts, nativeCounts, kernelCounts); err != nil {
			// Should not happen with well-formed profile data
			// TODO: Add error metric or log error
			continue
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

	for className, count := range kernelCounts {
		m := scopeMetrics.Metrics().AppendEmpty()
		m.SetName(c.config.MetricsPrefix + metricKernel.name)
		m.SetDescription(metricKernel.desc)
		m.SetUnit("1")

		sum := m.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

		dp := sum.DataPoints().AppendEmpty()
		dp.SetTimestamp(profile.Time())
		dp.SetIntValue(count)

		for i, name := range strings.Split(className, "/") {
			if name != "" {
				dp.Attributes().PutStr(kernelClassAttrNames[i], name)
			}
		}
	}
}
