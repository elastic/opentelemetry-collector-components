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

package awss3exporter // import "github.com/elastic/opentelemetry-collector-components/exporter/awss3exporter"

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

const signalLogs = "logs"

// destination is the fully resolved S3 target for a set of log records.
// bucket/roleARN/region/format/tenantID/orgID come from event attributes;
// targetType/targetID are resolved once per export call from the request
// context (see targetFromContext) rather than per-record attributes.
type destination struct {
	bucket     string
	roleARN    string
	region     string
	format     string
	tenantID   string
	targetType targetType
	targetID   string
	orgID      string
}

type s3Exporter struct {
	cfg    *Config
	logger *zap.Logger

	clients *clientCache

	// now and newUUID are indirected for deterministic tests.
	now     func() time.Time
	newUUID func() string

	// newClientFactory builds the cache's client factory from the resolved base
	// AWS config and per-tenant token source provider. It is a field so tests
	// can inject fakes; it defaults to newWebIdentityS3Client.
	newClientFactory func(base aws.Config, cfg *Config, tokens *tokenSourceProvider) func(key clientCacheKey) (s3API, error)
}

func newS3Exporter(cfg *Config, set exporter.Settings) *s3Exporter {
	return &s3Exporter{
		cfg:              cfg,
		logger:           set.Logger,
		now:              time.Now,
		newUUID:          func() string { return uuid.NewString() },
		newClientFactory: newWebIdentityS3Client,
	}
}

func (*s3Exporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *s3Exporter) start(ctx context.Context, _ component.Host) error {
	base, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to load AWS configuration: %w", err)
	}

	tokens := newTokenSourceProvider(e.cfg.TokenEndpoint, e.cfg.TokenEndpoint.CertTTL, e.now)

	clients, err := newClientCache(e.cfg.ClientCache.Size, e.newClientFactory(base, e.cfg, tokens))
	if err != nil {
		return fmt.Errorf("failed to create S3 client cache: %w", err)
	}
	e.clients = clients
	return nil
}

// consumeLogs groups the incoming records by their resolved destination and
// uploads one object per destination. Records missing required routing
// attributes or requesting an unsupported format are dropped (permanent
// failures); only upload errors, which may be transient, are returned so that
// the exporterhelper retry/queue machinery can act on them.
//
// The tenant's target type/ID (hosted deployment or serverless project) is
// resolved once from the request context: it is constant for the whole
// call, since it identifies who is calling, not what they are sending. A
// batch arriving without that context cannot be attributed to any tenant's
// mTLS identity, so it is dropped in full rather than partially uploaded
// under the wrong (or no) identity.
func (e *s3Exporter) consumeLogs(ctx context.Context, logs plog.Logs) error {
	tgt, ok := targetFromContext(ctx)
	if !ok {
		e.logger.Warn("dropping records: missing or invalid Elastic target context (X-Elastic-Target-Id / X-Elastic-Target-Type)")
		return nil
	}

	groups := e.splitByDestination(logs, tgt)

	var uploadErrs []error
	for _, g := range groups {
		if err := e.upload(ctx, g.dest, g.logs); err != nil {
			uploadErrs = append(uploadErrs, err)
		}
	}
	return errors.Join(uploadErrs...)
}

type logGroup struct {
	dest destination
	logs plog.Logs
	// scopeFor reuses a destination ScopeLogs for a given source
	// (resourceIndex, scopeIndex) so the resource/scope structure is preserved.
	scopeFor map[[2]int]plog.ScopeLogs
}

func (e *s3Exporter) splitByDestination(logs plog.Logs, tgt target) []*logGroup {
	byDest := map[destination]*logGroup{}
	var order []*logGroup

	rls := logs.ResourceLogs()
	for ri := 0; ri < rls.Len(); ri++ {
		rl := rls.At(ri)
		res := rl.Resource()
		sls := rl.ScopeLogs()
		for si := 0; si < sls.Len(); si++ {
			sl := sls.At(si)
			lrs := sl.LogRecords()
			for li := 0; li < lrs.Len(); li++ {
				lr := lrs.At(li)
				dest, ok := e.resolveDestination(lr.Attributes(), res.Attributes(), tgt)
				if !ok {
					continue
				}

				g := byDest[dest]
				if g == nil {
					g = &logGroup{dest: dest, logs: plog.NewLogs(), scopeFor: map[[2]int]plog.ScopeLogs{}}
					byDest[dest] = g
					order = append(order, g)
				}

				scope, ok := g.scopeFor[[2]int{ri, si}]
				if !ok {
					destRL := g.logs.ResourceLogs().AppendEmpty()
					res.CopyTo(destRL.Resource())
					destRL.SetSchemaUrl(rl.SchemaUrl())
					scope = destRL.ScopeLogs().AppendEmpty()
					sl.Scope().CopyTo(scope.Scope())
					scope.SetSchemaUrl(sl.SchemaUrl())
					g.scopeFor[[2]int{ri, si}] = scope
				}
				lr.CopyTo(scope.LogRecords().AppendEmpty())
			}
		}
	}
	return order
}

// resolveDestination extracts the routing fields from the record attributes,
// falling back to resource attributes, and combines them with the tenant
// target resolved once for the whole call. It returns ok=false when a
// required field (bucket, role_arn, region, org_id) is missing.
func (e *s3Exporter) resolveDestination(record, resource pcommon.Map, tgt target) (destination, bool) {
	attrs := e.cfg.Attributes

	bucket, ok := lookup(attrs.Bucket, record, resource)
	if !ok {
		e.logger.Warn("dropping records: missing bucket attribute", zap.String("attribute", attrs.Bucket))
		return destination{}, false
	}
	roleARN, ok := lookup(attrs.RoleARN, record, resource)
	if !ok {
		e.logger.Warn("dropping records: missing role_arn attribute", zap.String("attribute", attrs.RoleARN))
		return destination{}, false
	}
	region, ok := lookup(attrs.Region, record, resource)
	if !ok {
		e.logger.Warn("dropping records: missing region attribute", zap.String("attribute", attrs.Region))
		return destination{}, false
	}
	orgID, ok := lookup(attrs.OrgID, record, resource)
	if !ok {
		e.logger.Warn("dropping records: missing org_id attribute", zap.String("attribute", attrs.OrgID))
		return destination{}, false
	}

	format := e.cfg.DefaultFormat
	if attrs.Format != "" {
		if v, ok := lookup(attrs.Format, record, resource); ok {
			format = v
		}
	}
	tenantID := ""
	if attrs.TenantID != "" {
		tenantID, _ = lookup(attrs.TenantID, record, resource)
	}

	return destination{
		bucket:     bucket,
		roleARN:    roleARN,
		region:     region,
		format:     format,
		tenantID:   tenantID,
		targetType: tgt.targetType,
		targetID:   tgt.targetID,
		orgID:      orgID,
	}, true
}

func lookup(key string, record, resource pcommon.Map) (string, bool) {
	if key == "" {
		return "", false
	}
	if v, ok := record.Get(key); ok {
		return v.AsString(), true
	}
	if v, ok := resource.Get(key); ok {
		return v.AsString(), true
	}
	return "", false
}

func (e *s3Exporter) upload(ctx context.Context, dest destination, logs plog.Logs) error {
	m, err := marshalerForFormat(dest.format)
	if err != nil {
		e.logger.Warn("dropping records: unsupported format",
			zap.String("format", dest.format), zap.String("bucket", dest.bucket))
		return nil
	}

	buf, err := m.marshaler.MarshalLogs(logs)
	if err != nil {
		e.logger.Warn("dropping records: failed to marshal logs", zap.Error(err))
		return nil
	}

	client, err := e.clients.get(dest)
	if err != nil {
		return fmt.Errorf("failed to obtain S3 client for role %q: %w", dest.roleARN, err)
	}

	key := e.objectKey(dest, m.extension)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(dest.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buf),
	})
	if err != nil {
		return fmt.Errorf("failed to upload object to s3://%s/%s: %w", dest.bucket, key, err)
	}

	e.logger.Debug("uploaded object",
		zap.String("bucket", dest.bucket),
		zap.String("key", key),
		zap.Int("bytes", len(buf)))
	return nil
}

// objectKey builds a deterministic, collision-resistant object key of the form
// <signal>/<tenant>/YYYY/MM/DD/HH/<uuid>.<ext>.
func (e *s3Exporter) objectKey(dest destination, extension string) string {
	tenant := dest.tenantID
	if tenant == "" {
		tenant = "unknown"
	}
	t := e.now().UTC()
	return fmt.Sprintf("%s/%s/%04d/%02d/%02d/%02d/%s.%s",
		signalLogs, tenant, t.Year(), t.Month(), t.Day(), t.Hour(), e.newUUID(), extension)
}
