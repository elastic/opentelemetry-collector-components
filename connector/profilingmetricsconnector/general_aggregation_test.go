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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractHotspotInfo(t *testing.T) {
	tests := []struct {
		input  string
		output *hotspotInfo
		err    error
	}{
		{
			input: "void sun.security.provider.DigestBase.engineUpdate(byte[], int, int)",
			output: &hotspotInfo{
				pack:  "sun.security.provider",
				class: "DigestBase",
			},
		},
		{
			input: "int org.apache.lucene.search.Weight$DefaultBulkScorer.score(org.apache.lucene.search.LeafCollector, org.apache.lucene.util.Bits, int, int)",
			output: &hotspotInfo{
				pack:  "org.apache.lucene.search",
				class: "Weight$DefaultBulkScorer",
			},
		},
		{
			input: "void org.apache.lucene.search.IndexSearcher.search(org.apache.lucene.search.IndexSearcher$LeafReaderContextPartition[], org.apache.lucene.search.Weight, org.apache.lucene.search.Collector)",
			output: &hotspotInfo{
				pack:  "org.apache.lucene.search",
				class: "IndexSearcher",
			},
		},
		{
			input: "void org.elasticsearch.blobcache.shared.SharedBlobCacheService$CacheFileRegion.lambda$fillGapRunnable$13(org.elasticsearch.blobcache.common.SparseFileTracker$Gap, org.elasticsearch.blobcache.shared.SharedBlobCacheService$RangeMissingHandler, org.elasticsearch.blobcache.shared.SharedBlobCacheService$SourceInputStreamFactory, org.elasticsearch.action.ActionListener)",
			output: &hotspotInfo{
				pack:  "org.elasticsearch.blobcache.shared",
				class: "SharedBlobCacheService$CacheFileRegion",
			},
		},
		{
			input: "int software.amazon.awssdk.core.internal.metrics.BytesReadTrackingInputStream.read(byte[], int, int)",
			output: &hotspotInfo{
				pack:  "software.amazon.awssdk.core.internal.metrics",
				class: "BytesReadTrackingInputStream",
			},
		},
		{
			input: "void org.apache.lucene.search.DocIdStream.forEach(org.apache.lucene.search.CheckedIntConsumer)",
			output: &hotspotInfo{
				pack:  "org.apache.lucene.search",
				class: "DocIdStream",
			},
		},
		{
			input: "void org.apache.lucene.search.LeafCollector$$Lambda+<hidden>.accept(int)",
			output: &hotspotInfo{
				pack:  "org.apache.lucene.search",
				class: "LeafCollector$$Lambda+<hidden>",
			},
		},
		{
			input: "void org.apache.lucene.index.SortedSetDocValuesWriter.addOneValue(org.apache.lucene.util.BytesRef)",
			output: &hotspotInfo{
				pack:  "org.apache.lucene.index",
				class: "SortedSetDocValuesWriter",
			},
		},
		{
			input: "int com.sun.crypto.provider.GaloisCounterMode$GCMEngine.implGCMCrypt(java.nio.ByteBuffer, java.nio.ByteBuffer)",
			output: &hotspotInfo{
				pack:  "com.sun.crypto.provider",
				class: "GaloisCounterMode$GCMEngine",
			},
		},
		{
			input: "long org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator$Balancer.getShardDiskUsageInBytes(org.elasticsearch.cluster.routing.ShardRouting, org.elasticsearch.cluster.metadata.IndexMetadata, org.elasticsearch.cluster.ClusterInfo)",
			output: &hotspotInfo{
				pack:  "org.elasticsearch.cluster.routing.allocation.allocator",
				class: "BalancedShardsAllocator$Balancer",
			},
		},
		{input: "void MyClass.myMethod()", output: &hotspotInfo{class: "MyClass"}},
		{input: "void OuterClass$InnerClass.innerMethod()", output: &hotspotInfo{class: "OuterClass$InnerClass"}},
		{input: "void some.package.MyClass.myMethod()",
			output: &hotspotInfo{
				pack:  "some.package",
				class: "MyClass",
			},
		},
		{input: "MyClass.constructor()",
			output: &hotspotInfo{
				class: "MyClass",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			output, err := extractHotspotInfo(tc.input)
			require.ErrorIs(t, err, tc.err)
			assert.Equal(t, tc.output, output)
		})
	}
}

func TestExtractGolangInfo(t *testing.T) {
	tests := []struct {
		input  string
		output *golangInfo
	}{
		{input: "runtime.gcBgMarkWorker.func2",
			output: &golangInfo{
				pack: "runtime",
			},
		},
		{input: "runtime.gcDrain",
			output: &golangInfo{
				pack: "runtime",
			},
		},
		{input: "runtime.(*mspan).typePointersOfUnchecked",
			output: &golangInfo{
				pack: "runtime",
			},
		},
		{input: "github.com/elastic/elastic-agent-libs/mapstr.M.GetValue",
			output: &golangInfo{
				pack: "github.com/elastic/elastic-agent-libs/mapstr",
			},
		},
		{input: "github.com/elastic/beats/v7/metricbeat/helper/prometheus.(*prometheus).ProcessMetrics",
			output: &golangInfo{
				pack: "github.com/elastic/beats/v7/metricbeat/helper/prometheus",
			},
		},
		{input: "internal/runtime/maps.(*Map).getWithoutKeySmallFastStr",
			output: &golangInfo{
				pack: "internal/runtime/maps",
			},
		},
		{input: "sigs.k8s.io/json/internal/golang/encoding/json.(*decodeState).object",
			output: &golangInfo{
				pack: "sigs.k8s.io/json/internal/golang/encoding/json",
			},
		},
		{input: "crypto/internal/fips140/rsa.verifyPKCS1v15",
			output: &golangInfo{
				pack: "crypto/internal/fips140/rsa",
			},
		},
		{input: "k8s.io/apimachinery/pkg/runtime/serializer/json.(*Serializer).Decode",
			output: &golangInfo{
				pack: "k8s.io/apimachinery/pkg/runtime/serializer/json",
			},
		},
		{input: "github.com/google/uuid.NewRandom",
			output: &golangInfo{
				pack: "github.com/google/uuid",
			},
		},
		{input: "github.com/cilium/cilium/pkg/hubble/monitor.(*consumer).NotifyPerfEvent",
			output: &golangInfo{
				pack: "github.com/cilium/cilium/pkg/hubble/monitor",
			},
		},
		{input: "syscall.RawSyscall6",
			output: &golangInfo{
				pack: "syscall",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			output := extractGolangInfo(tc.input)
			assert.Equal(t, tc.output, output)
		})
	}
}
