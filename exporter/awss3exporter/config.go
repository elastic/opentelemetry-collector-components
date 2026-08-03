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
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config is the configuration for the multi-tenant AWS S3 exporter.
//
// Unlike the upstream awss3exporter, the destination (bucket, region, role to
// assume and payload format) is not fixed at startup. Instead it is resolved
// per log record from attributes, which lets a single exporter fan out to many
// buckets owned by different accounts.
//
// Authentication uses STS AssumeRoleWithWebIdentity: for each tenant the
// exporter fetches a short-lived web identity (JWT) token from a local token
// endpoint and exchanges it for temporary credentials scoped to the
// destination role. Tokens are cached until they expire and refetched on expiry
// or when STS rejects them.
//
// The tenant's identity (hosted deployment or serverless project, plus org)
// is conveyed to the token endpoint via mTLS rather than a request header:
// the exporter presents a self-signed client certificate whose DNS SAN
// encodes that identity. In production the token endpoint is the ECP proxy,
// which terminates that connection and derives the X-Forwarded-Client-Cert
// header itself from the verified SAN — the exporter never builds that
// header directly. The target type/ID come from the request's client
// metadata (X-Elastic-Target-Type / X-Elastic-Target-Id, see
// AttributesConfig.OrgID for the org half of the identity).
type Config struct {
	// TimeoutSettings, QueueSettings and BackOffConfig are the standard
	// exporterhelper knobs governing per-request timeouts, the sending queue
	// and retries on transient failures.
	TimeoutSettings exporterhelper.TimeoutConfig                             `mapstructure:",squash"`
	QueueSettings   configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
	BackOffConfig   configretry.BackOffConfig                                `mapstructure:"retry_on_failure"`

	// Attributes configures which log-record (or resource) attribute keys carry
	// the per-event S3 routing information.
	Attributes AttributesConfig `mapstructure:"attributes"`

	// DefaultFormat is the payload format used when a record does not carry the
	// format attribute. See the format marshaler for supported values.
	DefaultFormat string `mapstructure:"default_format"`

	// TokenEndpoint configures the local endpoint used to fetch per-tenant web
	// identity tokens for AssumeRoleWithWebIdentity.
	TokenEndpoint TokenEndpointConfig `mapstructure:"token_endpoint"`

	// STS configures the shared knobs applied to every AssumeRoleWithWebIdentity
	// call.
	STS STSConfig `mapstructure:"sts"`

	// ClientCache bounds the in-memory cache of assumed-role S3 clients.
	ClientCache ClientCacheConfig `mapstructure:"client_cache"`

	// Endpoint optionally overrides the S3 endpoint, primarily for tests and
	// S3-compatible object stores.
	Endpoint string `mapstructure:"endpoint"`
	// DisableSSL disables HTTPS for the S3 endpoint (test / custom endpoints).
	DisableSSL bool `mapstructure:"disable_ssl"`
	// S3ForcePathStyle uses path-style addressing (test / custom endpoints).
	S3ForcePathStyle bool `mapstructure:"s3_force_path_style"`
}

// AttributesConfig maps the routing fields defined by the Streams S3
// destination schema (bucket, role_arn, region, format) plus the tenant
// identifier onto the attribute keys that carry them.
type AttributesConfig struct {
	// Bucket is the attribute key holding the destination bucket name.
	Bucket string `mapstructure:"bucket"`
	// RoleARN is the attribute key holding the IAM role ARN to assume.
	RoleARN string `mapstructure:"role_arn"`
	// Region is the attribute key holding the destination bucket region.
	Region string `mapstructure:"region"`
	// Format is the attribute key holding the payload format. Optional: when
	// unset on a record, DefaultFormat is used.
	Format string `mapstructure:"format"`
	// TenantID is the attribute key holding the tenant/target identifier used
	// in the generated object key. Optional.
	TenantID string `mapstructure:"tenant_id"`
	// OrgID is the attribute key holding the tenant's organization ID, used
	// (together with the target type/ID resolved from the request context)
	// to build the mTLS identity certificate presented to the token endpoint.
	OrgID string `mapstructure:"org_id"`
}

// TokenEndpointConfig configures the local endpoint that mints per-tenant web
// identity tokens used for AssumeRoleWithWebIdentity.
type TokenEndpointConfig struct {
	// URL is the token endpoint. In production this must be reached through
	// the ECP proxy (not the workload-identity-issuer service directly), since
	// the proxy is what terminates the mTLS connection carrying the tenant's
	// identity certificate and translates it into the X-Forwarded-Client-Cert
	// header the issuer trusts. The request also carries the region in the
	// X-Elastic-Region header.
	URL string `mapstructure:"url"`
	// Region is sent as the X-Elastic-Region header. Optional.
	Region string `mapstructure:"region"`
	// Audience is the token audience sent in the request body ("aud"). Defaults
	// to sts.amazonaws.com.
	Audience string `mapstructure:"audience"`
	// RefreshSkew refetches the token this long before its expires_at, avoiding
	// races against near-expiry tokens.
	RefreshSkew time.Duration `mapstructure:"refresh_skew"`
	// Timeout bounds each token HTTP request.
	Timeout time.Duration `mapstructure:"timeout"`
	// CertTTL is the validity window of the self-signed mTLS identity
	// certificate generated per tenant to authenticate to the token endpoint.
	// The certificate is regenerated automatically once it is within
	// RefreshSkew of CertTTL.
	CertTTL time.Duration `mapstructure:"cert_ttl"`
}

// STSConfig configures shared behavior for STS role assumption. The role ARN
// itself is per-event and comes from the RoleARN attribute.
type STSConfig struct {
	// SessionName is the role session name used for assumed-role sessions.
	SessionName string `mapstructure:"session_name"`
	// Region is the region for the STS client used to assume roles. When empty
	// the SDK default region resolution applies.
	Region string `mapstructure:"region"`
}

// ClientCacheConfig configures the assumed-role S3 client cache.
type ClientCacheConfig struct {
	// Size is the maximum number of cached S3 clients (LRU). Each unique
	// (targetType, targetID, org_id, role_arn, region, bucket) destination occupies one
	// entry.
	Size int `mapstructure:"size"`
}

const (
	defaultSessionName  = "elastic-streams-s3-exporter"
	defaultCacheSize    = 128
	defaultAudience     = "sts.amazonaws.com"
	defaultTokenURL     = "http://localhost:8443/token"
	defaultRefreshSkew  = 5 * time.Minute
	defaultTokenTimeout = 10 * time.Second
	defaultCertTTL      = 24 * time.Hour
)

var (
	errNoBucketAttr = errors.New("attributes::bucket must be set")
	errNoRoleAttr   = errors.New("attributes::role_arn must be set")
	errNoRegionAttr = errors.New("attributes::region must be set")
	errNoOrgIDAttr  = errors.New("attributes::org_id must be set")
	errBadCacheSize = errors.New("client_cache::size must be greater than 0")
	errBadFormat    = errors.New("default_format is not a supported format")
	errNoTokenURL   = errors.New("token_endpoint::url must be set")
	errNoAudience   = errors.New("token_endpoint::audience must be set")
	errBadCertTTL   = errors.New("token_endpoint::cert_ttl must be greater than 0")
)

// Validate checks that the configuration is well formed.
func (c *Config) Validate() error {
	if c.Attributes.Bucket == "" {
		return errNoBucketAttr
	}
	if c.Attributes.RoleARN == "" {
		return errNoRoleAttr
	}
	if c.Attributes.Region == "" {
		return errNoRegionAttr
	}
	if c.Attributes.OrgID == "" {
		return errNoOrgIDAttr
	}
	if c.ClientCache.Size <= 0 {
		return errBadCacheSize
	}
	if c.TokenEndpoint.URL == "" {
		return errNoTokenURL
	}
	if c.TokenEndpoint.Audience == "" {
		return errNoAudience
	}
	if c.TokenEndpoint.CertTTL <= 0 {
		return errBadCertTTL
	}
	if _, err := marshalerForFormat(c.DefaultFormat); err != nil {
		return fmt.Errorf("%w: %q", errBadFormat, c.DefaultFormat)
	}
	return nil
}
