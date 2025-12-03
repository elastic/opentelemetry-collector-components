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
	"go.uber.org/zap"

	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

type metric struct {
	name string
	desc string
}

type frameInfo struct {
	typ      string
	fileName string
	funcName string
}

type class struct {
	name string
	rx   *regexp.Regexp
}

type attrInfo struct {
	// Only attached to kernel metric
	class   string
	syscall string
	// Only attached to native metric
	shlib string
}

const (
	nativeLibraryAttrName = "shlib_name"
	syscallAttrName       = "syscall_name"
	kernelClassAttrPrefix = "kernel_"
)

var kernelClassAttrNames = [...]string{
	// network/ipc/disk/memory/synchronization
	kernelClassAttrPrefix + "area",
	// tcp/udp/other
	kernelClassAttrPrefix + "proto",
	// read/write
	kernelClassAttrPrefix + "io",
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

	// Match a shared library name (e.g. libc.so.6)
	shlibRx = regexp.MustCompile(`(?:.*/)?(.+)\.so`)
	// Match a system call and extract its name (e.g. __arm64_sys_write)
	syscallRx = regexp.MustCompile(`^(?:__x64_sys|__arm64_sys|ksys)_(\w+)`)

	// The following regular expressions try to match certain kernel functions
	// and derive a classification for an entire kernel stacktrace based on the match.
	// The ordering is *significant* as some of the following functions may appear
	// in stacktraces that result in multiple classifications *for the same*
	// stacktrace which would be incorrect. The elements are ordered in decreasing
	// priority: if there are multiple classifications, the element with the lower
	// slice index always wins.
	//
	// TODO: CNIs like Cilium may introduce complications and the following list
	// may need to be updated.
	classes = []class{
		{
			name: "network/tcp/read",
			rx:   regexp.MustCompile(`^(?:tcp_recvmsg|tcp_read|tcp_rcv|tcp_v4_rcv|tcp_v6_rcv)`),
		},
		{
			name: "network/tcp/write",
			rx:   regexp.MustCompile(`^(?:tcp_sendmsg|tcp_transmit|tcp_write|tcp_push|tcp_sendpage)`),
		},
		{
			name: "network/udp/read",
			rx:   regexp.MustCompile(`^(?:udp_recvmsg|udp_read|udp_rcv|udp_v4_rcv|udp_v6_rcv)`),
		},
		{
			name: "network/udp/write",
			rx:   regexp.MustCompile(`^(?:udp_sendmsg|udp_write|udp_push|udp_sendpage)`),
		},
		{
			name: "ipc/read",
			rx: regexp.MustCompile(`^(?:pipe_read|pipe_read_iter|eventfd_read` +
				`|unix_stream_read_generic|unix_stream_recvmsg)`),
		},
		{
			name: "ipc/write",
			rx: regexp.MustCompile(`^(?:pipe_write|pipe_write_iter|eventfd_write` +
				`|unix_stream_write_generic|unix_stream_sendmsg)`),
		},
		{
			name: "network/other/read",
			rx:   regexp.MustCompile(`^(?:sock_recvmsg|__sock_recvmsg|sock_read_iter)`),
		},
		{
			name: "network/other/write",
			rx:   regexp.MustCompile(`^(?:sock_sendmsg|__sock_sendmsg|sock_write_iter)`),
		},
		{
			name: "disk/read",
			rx: regexp.MustCompile(`^(?:ext4_file_read_iter|xfs_file_read_iter` +
				`|btrfs_file_read_iter|filemap_read|generic_file_read_iter)`),
		},
		{
			name: "disk/write",
			rx: regexp.MustCompile(`^(?:ext4_file_write_iter|xfs_file_write_iter` +
				`|btrfs_file_write_iter|filemap_write|generic_file_write_iter` +
				`|writeback_sb_inodes)`),
		},
		{
			name: "memory",
			rx: regexp.MustCompile(`^do_mmap|do_munmap|^do_anonymous_page|mmap_|madvise` +
				`|^handle_mm_fault|^alloc_pages|^free_pages`),
		},
		{
			name: "synchronization",
			rx:   regexp.MustCompile(`futex_|^schedule|__schedule|^wake_up_|^wake_q_`),
		},
	}
)

func (c *profilesToMetricsConnector) fetchFrameInfo(dictionary pprofile.ProfilesDictionary,
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
		// Return error if we can't extract frame type in order to signal caller
		// to stop further processing, as the current sample can not be relied upon.
		return frameInfo{}, fmt.Errorf("fetchFrameInfo: empty frame type")
	}

	fileName := ""
	funcName := ""

	// Errors related to extracting filename and function name are logged but not
	// returned, as classification can still take place and the caller may continue
	// to iterate frames.
	switch frameType {
	case frameTypeNative:
		// Extract mapping filename
		mapTable := dictionary.MappingTable()
		mapTbLen := mapTable.Len()
		mi := int(loc.MappingIndex())
		if mi >= mapTbLen {
			c.logger.Error("fetchFrameInfo",
				zap.Int("mapIdx", mi),
				zap.Int("mapTbLen", mapTbLen))
			break
		}
		mapFileStrIdx := int(mapTable.At(int(mi)).FilenameStrindex())
		if mapFileStrIdx >= strTbLen {
			c.logger.Error("fetchFrameInfo",
				zap.Int("mapFileStrIdx", mapFileStrIdx),
				zap.Int("strTbLen", strTbLen))
			break
		}
		fileName = strTable.At(mapFileStrIdx)
	case frameTypeKernel:
		// Extract function name
		for _, ln := range loc.Lines().All() {
			funcIdx := int(ln.FunctionIndex())
			if funcIdx >= funcTbLen {
				c.logger.Error("fetchFrameInfo",
					zap.Int("funcIdx", funcIdx),
					zap.Int("funcTbLen", funcTbLen))
				continue
			}
			fn := funcTable.At(funcIdx)
			nameStrIdx := int(fn.NameStrindex())
			if nameStrIdx >= strTbLen {
				c.logger.Error("fetchFrameInfo",
					zap.Int("nameStrIdx", nameStrIdx),
					zap.Int("strTbLen", strTbLen))
				continue
			}
			funcName = strTable.At(int(fn.NameStrindex()))
			if funcName != "" {
				break
			}
		}
	}

	// End validation
	return frameInfo{
		typ:      frameType,
		fileName: fileName,
		funcName: funcName,
	}, nil
}

// classifyUser classifies a non-kernel frame by increasing relevant metric counts.
// Must NOT be called for kernel frames.
func classifyUser(fi frameInfo,
	counts map[metric]int64,
	nativeCounts map[attrInfo]int64,
	multiplier int64,
) {
	ft := fi.typ

	// We have a distinct metricUser (even if there's no specific frame type for it)
	// so that we can calculate the total number of stacktraces for any given time
	// interval: metricUser.count + metricKernel.count
	counts[metricUser] += multiplier
	metric := allowedFrameTypes[ft]
	if ft != frameTypeNative {
		counts[metric] += multiplier
		return
	}

	// Native frame specific logic follows
	if fi.fileName != "" {
		// Extract native library name and increment associated count
		sm := shlibRx.FindStringSubmatchIndex(fi.fileName)
		if len(sm) == 4 {
			nativeCounts[attrInfo{
				shlib: fi.fileName[sm[2]:sm[3]],
			}] += multiplier
			return
		}
	}

	nativeCounts[attrInfo{}] += multiplier
}

// classifyFrames classifies a sample into one or more categories based on frame information.
// This takes place by incrementing the associated metric count.
func (c *profilesToMetricsConnector) classifyFrames(dictionary pprofile.ProfilesDictionary,
	locationIndices pcommon.Int32Slice,
	counts map[metric]int64,
	nativeCounts map[attrInfo]int64,
	kernelCounts map[attrInfo]int64,
	multiplier int64,
) error {
	var err error

	className := ""
	syscall := ""
	lastMatchIdx := len(classes)
	hasKernel := false
	for idx := range locationIndices.Len() {
		fi, err := c.fetchFrameInfo(dictionary, locationIndices, idx)
		if err != nil {
			break
		}

		if idx == 0 && fi.typ != frameTypeKernel {
			// Only need to call this once per stacktrace, for the leaf frame,
			// which is the first location indexed.
			classifyUser(fi, counts, nativeCounts, multiplier)
		}

		// Kernel frame specific logic follows
		if fi.typ != frameTypeKernel {
			break
		}

		hasKernel = true
		if fi.funcName == "" {
			// No classification possible for this frame
			continue
		}

		for cIdx, class := range classes {
			// Priority matching logic
			if cIdx == lastMatchIdx {
				// Early exit if there's no higher priority match possible
				break
			}
			if class.rx.MatchString(fi.funcName) {
				className = class.name
				lastMatchIdx = cIdx
				// If we find a match we stop the iteration as there's not going to
				// be a higher priority match (decreasing priority ordering).
				break
			}
		}

		// Try to match a syscall and avoid string allocations by using indices
		// to string location.
		sysIndices := syscallRx.FindStringSubmatchIndex(fi.funcName)
		if len(sysIndices) == 4 {
			syscall = fi.funcName[sysIndices[2]:sysIndices[3]]
			// If we match a syscall, we don't need to keep iterating frames
			// as there are no further (syscall or category) matches possible.
			break
		}
	}

	if hasKernel {
		kernelCounts[attrInfo{class: className, syscall: syscall}] += multiplier
	}
	return err
}

func (c *profilesToMetricsConnector) addFrameMetrics(dictionary pprofile.ProfilesDictionary,
	profile pprofile.Profile, scopeMetrics pmetric.ScopeMetrics,
) {
	stackTable := dictionary.StackTable()

	counts := make(map[metric]int64)
	nativeCounts := make(map[attrInfo]int64)
	kernelCounts := make(map[attrInfo]int64)

	// Process all samples and increment metric counts
	for _, sample := range profile.Samples().All() {
		multiplier := max(int64(sample.TimestampsUnixNano().Len()), 1)
		stack := stackTable.At(int(sample.StackIndex()))
		if err := c.classifyFrames(dictionary, stack.LocationIndices(),
			counts, nativeCounts, kernelCounts, multiplier); err != nil {
			c.logger.Error("classifyFrames", zap.Error(err))
		}
	}

	// Generate metrics
	for metric, count := range counts {
		m := scopeMetrics.Metrics().AppendEmpty()
		m.SetName(metric.name)
		m.SetDescription(metric.desc)
		m.SetUnit("1")

		sum := m.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

		dp := sum.DataPoints().AppendEmpty()
		dp.SetTimestamp(profile.Time())
		dp.SetIntValue(count)
	}

	for aInfo, count := range nativeCounts {
		m := scopeMetrics.Metrics().AppendEmpty()
		m.SetName(metricNative.name)
		m.SetDescription(metricNative.desc)
		m.SetUnit("1")

		sum := m.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

		dp := sum.DataPoints().AppendEmpty()
		dp.SetTimestamp(profile.Time())
		dp.SetIntValue(count)
		if aInfo.shlib != "" {
			dp.Attributes().PutStr(nativeLibraryAttrName, aInfo.shlib)
		}
	}

	for aInfo, count := range kernelCounts {
		m := scopeMetrics.Metrics().AppendEmpty()
		m.SetName(metricKernel.name)
		m.SetDescription(metricKernel.desc)
		m.SetUnit("1")

		sum := m.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

		dp := sum.DataPoints().AppendEmpty()
		dp.SetTimestamp(profile.Time())
		dp.SetIntValue(count)

		if aInfo.syscall != "" {
			dp.Attributes().PutStr(syscallAttrName, aInfo.syscall)
		}

		if aInfo.class != "" {
			cSplit := strings.Split(aInfo.class, "/")
			cSplitLen := len(cSplit)
			kClassAttrNamesLen := len(kernelClassAttrNames)
			if cSplitLen > kClassAttrNamesLen {
				c.logger.Error("addFrameMetrics",
					zap.Int("cSplitLen", cSplitLen),
					zap.Int("kClassAttrNamesLen", kClassAttrNamesLen))
				continue
			}

			for i, v := range cSplit {
				if v != "" {
					// Add kernel classifications as separate attributes
					dp.Attributes().PutStr(kernelClassAttrNames[i], v)
				}
			}
		}
	}
}
