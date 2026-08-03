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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	base := func() *Config {
		c, ok := createDefaultConfig().(*Config)
		require.True(t, ok)
		return c
	}

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr error
	}{
		{
			name:   "default is valid",
			mutate: func(*Config) {},
		},
		{
			name:    "missing bucket attribute",
			mutate:  func(c *Config) { c.Attributes.Bucket = "" },
			wantErr: errNoBucketAttr,
		},
		{
			name:    "missing role_arn attribute",
			mutate:  func(c *Config) { c.Attributes.RoleARN = "" },
			wantErr: errNoRoleAttr,
		},
		{
			name:    "missing region attribute",
			mutate:  func(c *Config) { c.Attributes.Region = "" },
			wantErr: errNoRegionAttr,
		},
		{
			name:    "missing org_id attribute",
			mutate:  func(c *Config) { c.Attributes.OrgID = "" },
			wantErr: errNoOrgIDAttr,
		},
		{
			name:    "non-positive cache size",
			mutate:  func(c *Config) { c.ClientCache.Size = 0 },
			wantErr: errBadCacheSize,
		},
		{
			name:    "missing token endpoint url",
			mutate:  func(c *Config) { c.TokenEndpoint.URL = "" },
			wantErr: errNoTokenURL,
		},
		{
			name:    "missing token audience",
			mutate:  func(c *Config) { c.TokenEndpoint.Audience = "" },
			wantErr: errNoAudience,
		},
		{
			name:    "non-positive cert ttl",
			mutate:  func(c *Config) { c.TokenEndpoint.CertTTL = 0 },
			wantErr: errBadCertTTL,
		},
		{
			name:    "unsupported default format",
			mutate:  func(c *Config) { c.DefaultFormat = "csv" },
			wantErr: errBadFormat,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base()
			tt.mutate(cfg)
			err := cfg.Validate()
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			assert.NoError(t, err)
		})
	}
}
