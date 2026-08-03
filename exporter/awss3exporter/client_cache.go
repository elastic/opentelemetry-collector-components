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
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	lru "github.com/hashicorp/golang-lru/v2"
)

type s3API interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// clientCacheKey identifies a cached S3 client. Credentials are a function of
// the tenant's web identity token (targetType/targetID, org_id) and the
// assumed role, and an S3 client is bound to a region, so the identity is
// fully described by (targetType, targetID, org_id, role_arn, region). The
// bucket is included to mirror the destination identity described in the RFC
// and keep the key aligned with a single Streams output.
type clientCacheKey struct {
	targetType targetType
	targetID   string
	orgID      string
	roleARN    string
	region     string
	bucket     string
}

// clientCache is a bounded, concurrency-safe LRU of assumed-role S3 clients.
// Instantiating a client and assuming a role on every event would be expensive,
// so clients are reused across events sharing a destination.
type clientCache struct {
	mu    sync.Mutex
	cache *lru.Cache[clientCacheKey, s3API]

	// newClient builds a client for a cache miss. It is a field so tests can
	// substitute a fake without reaching AWS.
	newClient func(key clientCacheKey) (s3API, error)
}

func newClientCache(size int, newClient func(key clientCacheKey) (s3API, error)) (*clientCache, error) {
	cache, err := lru.New[clientCacheKey, s3API](size)
	if err != nil {
		return nil, err
	}
	return &clientCache{cache: cache, newClient: newClient}, nil
}

// get returns the client for the given destination, creating and caching it on
// a miss.
func (c *clientCache) get(dest destination) (s3API, error) {
	key := clientCacheKey{
		targetType: dest.targetType,
		targetID:   dest.targetID,
		orgID:      dest.orgID,
		roleARN:    dest.roleARN,
		region:     dest.region,
		bucket:     dest.bucket,
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if client, ok := c.cache.Get(key); ok {
		return client, nil
	}
	client, err := c.newClient(key)
	if err != nil {
		return nil, err
	}
	c.cache.Add(key, client)
	return client, nil
}

// newWebIdentityS3Client returns a factory that builds an S3 client which
// assumes the destination role via STS AssumeRoleWithWebIdentity, using the
// tenant's web identity token, and caches the resulting temporary credentials.
// The base config supplies the region resolution and HTTP client for the STS
// calls.
func newWebIdentityS3Client(base aws.Config, cfg *Config, tokens *tokenSourceProvider) func(key clientCacheKey) (s3API, error) {
	return func(key clientCacheKey) (s3API, error) {
		stsClient := sts.NewFromConfig(base, func(o *sts.Options) {
			if cfg.STS.Region != "" {
				o.Region = cfg.STS.Region
			}
		})

		source, err := tokens.get(target{targetType: key.targetType, targetID: key.targetID}, key.orgID)
		if err != nil {
			return nil, fmt.Errorf("failed to build token source: %w", err)
		}
		provider := stscreds.NewWebIdentityRoleProvider(stsClient, key.roleARN, source,
			func(o *stscreds.WebIdentityRoleOptions) {
				if cfg.STS.SessionName != "" {
					o.RoleSessionName = cfg.STS.SessionName
				}
			})

		// Wrap so that when STS rejects the token the cached token is dropped and
		// refetched on the next attempt, then cache the resulting credentials.
		creds := aws.NewCredentialsCache(&invalidateOnErrorProvider{inner: provider, source: source})

		client := s3.NewFromConfig(base, func(o *s3.Options) {
			o.Region = key.region
			o.Credentials = creds
			o.UsePathStyle = cfg.S3ForcePathStyle
			o.EndpointOptions.DisableHTTPS = cfg.DisableSSL
			if cfg.Endpoint != "" {
				o.BaseEndpoint = aws.String(cfg.Endpoint)
			}
		})
		return client, nil
	}
}

// invalidateOnErrorProvider wraps a credentials provider and, when STS rejects
// the web identity token, invalidates the token source so a fresh token is
// fetched on the next retrieval.
type invalidateOnErrorProvider struct {
	inner  aws.CredentialsProvider
	source *tokenSource
}

func (p *invalidateOnErrorProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	creds, err := p.inner.Retrieve(ctx)
	if err != nil && isInvalidTokenError(err) {
		p.source.invalidate()
	}
	return creds, err
}

// isInvalidTokenError reports whether the STS error indicates the web identity
// token was rejected (expired or otherwise invalid), which is recoverable by
// fetching a new token.
func isInvalidTokenError(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "ExpiredTokenException", "InvalidIdentityToken", "IDPRejectedClaim":
			return true
		}
	}
	return false
}
