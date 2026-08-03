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

package awss3exporter

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
)

// contextWithTarget attaches the Elastic target type/ID to the context's
// client metadata the way upstream pipeline components do, so consumeLogs can
// resolve the tenant identity via targetFromContext.
func contextWithTarget(ctx context.Context, typ, id string) context.Context {
	info := client.FromContext(ctx)
	info.Metadata = client.NewMetadata(map[string][]string{
		xElasticTargetTypeHeader: {typ},
		xElasticTargetIDHeader:   {id},
	})
	return client.NewContext(ctx, info)
}

type capturedPut struct {
	bucket string
	key    string
	body   []byte
	region string
}

type fakeS3 struct {
	mu     sync.Mutex
	region string
	puts   []capturedPut
	err    error
}

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	body, _ := io.ReadAll(in.Body)
	f.puts = append(f.puts, capturedPut{
		bucket: aws.ToString(in.Bucket),
		key:    aws.ToString(in.Key),
		body:   body,
		region: f.region,
	})
	return &s3.PutObjectOutput{}, nil
}

// newTestExporter builds an exporter whose client factory records the created
// destinations and hands back the provided fakes keyed by region.
func newTestExporter(t *testing.T, cfg *Config) (*s3Exporter, *sync.Map) {
	t.Helper()
	set := exportertest.NewNopSettings(typ)
	exp := newS3Exporter(cfg, set)
	exp.now = func() time.Time { return time.Date(2026, 7, 23, 14, 5, 0, 0, time.UTC) }
	exp.newUUID = func() string { return "fixed-uuid" }

	created := &sync.Map{} // clientCacheKey -> *fakeS3
	exp.newClientFactory = func(_ aws.Config, _ *Config, _ *tokenSourceProvider) func(key clientCacheKey) (s3API, error) {
		return func(key clientCacheKey) (s3API, error) {
			f := &fakeS3{region: key.region}
			created.Store(key, f)
			return f, nil
		}
	}

	clients, err := newClientCache(cfg.ClientCache.Size, exp.newClientFactory(aws.Config{}, cfg, nil))
	require.NoError(t, err)
	exp.clients = clients
	return exp, created
}

func defaultTestConfig(t *testing.T) *Config {
	t.Helper()
	cfg, ok := createDefaultConfig().(*Config)
	require.True(t, ok)
	return cfg
}

func recordWithDestination(logs plog.Logs, body, bucket, role, region, format, tenant, org string) {
	rl := logs.ResourceLogs().AppendEmpty()
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr(body)
	a := lr.Attributes()
	a.PutStr("bucket", bucket)
	a.PutStr("role_arn", role)
	a.PutStr("region", region)
	a.PutStr("org_id", org)
	if format != "" {
		a.PutStr("format", format)
	}
	if tenant != "" {
		a.PutStr("tenant_id", tenant)
	}
}

func TestConsumeLogsFanOut(t *testing.T) {
	cfg := defaultTestConfig(t)
	exp, created := newTestExporter(t, cfg)

	logs := plog.NewLogs()
	// two records to bucket-a / role-a / us-east-1
	recordWithDestination(logs, "a1", "bucket-a", "arn:aws:iam::111:role/a", "us-east-1", "", "tenant-1", "org-1")
	recordWithDestination(logs, "a2", "bucket-a", "arn:aws:iam::111:role/a", "us-east-1", "", "tenant-1", "org-1")
	// one record to a different bucket/account/region, but same tenant (the
	// target is resolved once from context, not per record)
	recordWithDestination(logs, "b1", "bucket-b", "arn:aws:iam::222:role/b", "eu-west-1", "", "tenant-2", "org-2")

	ctx := contextWithTarget(context.Background(), "serverless", "proj-1")
	require.NoError(t, exp.consumeLogs(ctx, logs))

	// Two distinct destinations => two cached clients, one PutObject each.
	var totalPuts int
	created.Range(func(_, v any) bool {
		f := v.(*fakeS3)
		f.mu.Lock()
		totalPuts += len(f.puts)
		f.mu.Unlock()
		return true
	})
	assert.Equal(t, 2, totalPuts)

	fa, ok := created.Load(clientCacheKey{targetType: targetTypeServerless, targetID: "proj-1", orgID: "org-1", roleARN: "arn:aws:iam::111:role/a", region: "us-east-1", bucket: "bucket-a"})
	require.True(t, ok)
	putsA := fa.(*fakeS3).puts
	require.Len(t, putsA, 1)
	assert.Equal(t, "bucket-a", putsA[0].bucket)
	assert.Equal(t, "logs/tenant-1/2026/07/23/14/fixed-uuid.json", putsA[0].key)

	fb, ok := created.Load(clientCacheKey{targetType: targetTypeServerless, targetID: "proj-1", orgID: "org-2", roleARN: "arn:aws:iam::222:role/b", region: "eu-west-1", bucket: "bucket-b"})
	require.True(t, ok)
	putsB := fb.(*fakeS3).puts
	require.Len(t, putsB, 1)
	assert.Equal(t, "bucket-b", putsB[0].bucket)
	assert.Equal(t, "logs/tenant-2/2026/07/23/14/fixed-uuid.json", putsB[0].key)
}

func TestConsumeLogsReusesCachedClient(t *testing.T) {
	cfg := defaultTestConfig(t)
	exp, created := newTestExporter(t, cfg)

	ctx := contextWithTarget(context.Background(), "serverless", "proj-1")
	for i := 0; i < 3; i++ {
		logs := plog.NewLogs()
		recordWithDestination(logs, "x", "bucket-a", "arn:aws:iam::111:role/a", "us-east-1", "", "tenant-1", "org-1")
		require.NoError(t, exp.consumeLogs(ctx, logs))
	}

	count := 0
	created.Range(func(_, _ any) bool { count++; return true })
	assert.Equal(t, 1, count, "a single destination must reuse one cached client across batches")
}

func TestConsumeLogsDropsRecordsMissingRoutingAttributes(t *testing.T) {
	cfg := defaultTestConfig(t)
	exp, created := newTestExporter(t, cfg)

	logs := plog.NewLogs()
	// missing region attribute -> dropped, no upload, no error
	rl := logs.ResourceLogs().AppendEmpty()
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Attributes().PutStr("bucket", "bucket-a")
	lr.Attributes().PutStr("role_arn", "arn:aws:iam::111:role/a")

	ctx := contextWithTarget(context.Background(), "serverless", "proj-1")
	require.NoError(t, exp.consumeLogs(ctx, logs))

	count := 0
	created.Range(func(_, _ any) bool { count++; return true })
	assert.Equal(t, 0, count)
}

func TestConsumeLogsDropsAllRecordsMissingTargetContext(t *testing.T) {
	cfg := defaultTestConfig(t)
	exp, created := newTestExporter(t, cfg)

	logs := plog.NewLogs()
	recordWithDestination(logs, "x", "bucket-a", "arn:aws:iam::111:role/a", "us-east-1", "", "tenant-1", "org-1")

	// No X-Elastic-Target-Id/Type in context -> the whole batch is dropped,
	// since there is no tenant identity to authenticate the upload with.
	require.NoError(t, exp.consumeLogs(context.Background(), logs))

	count := 0
	created.Range(func(_, _ any) bool { count++; return true })
	assert.Equal(t, 0, count)
}

func TestObjectKeyDefaultsTenant(t *testing.T) {
	cfg := defaultTestConfig(t)
	exp, _ := newTestExporter(t, cfg)
	key := exp.objectKey(destination{bucket: "b"}, "json")
	assert.Equal(t, "logs/unknown/2026/07/23/14/fixed-uuid.json", key)
}
