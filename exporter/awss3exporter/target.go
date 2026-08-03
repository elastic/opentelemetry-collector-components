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
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/client"
)

// targetType identifies the kind of Elasticsearch target data is destined
// for. Upstream pipeline components stamp this (and the target ID) onto the
// request's client metadata from the X-Elastic-Target-Type / X-Elastic-Target-Id
// headers as the request enters the collector.
type targetType string

const (
	targetTypeHosted     targetType = "hosted"
	targetTypeServerless targetType = "serverless"

	// xElasticTargetIDHeader carries the deployment ID (hosted) or project ID
	// (serverless) that identifies the tenant a request belongs to.
	xElasticTargetIDHeader = "x-elastic-target-id"
	// xElasticTargetTypeHeader carries the target type: "hosted" or "serverless".
	xElasticTargetTypeHeader = "x-elastic-target-type"

	// observabilityProjectType is the only serverless project type this
	// exporter serves today.
	observabilityProjectType = "observability"
)

// target identifies the tenant a batch of records is destined for, resolved
// once per export call from the request context rather than per-record
// attributes.
type target struct {
	targetType targetType
	targetID   string
}

// targetFromContext extracts the target type and ID that upstream pipeline
// components attach to the request's client metadata (client.Info) via the
// X-Elastic-Target-Type / X-Elastic-Target-Id headers.
func targetFromContext(ctx context.Context) (target, bool) {
	info := client.FromContext(ctx)

	ids := info.Metadata.Get(xElasticTargetIDHeader)
	types := info.Metadata.Get(xElasticTargetTypeHeader)
	if len(ids) == 0 || len(types) == 0 || ids[0] == "" || types[0] == "" {
		return target{}, false
	}

	switch strings.ToLower(types[0]) {
	case string(targetTypeHosted):
		return target{targetType: targetTypeHosted, targetID: ids[0]}, true
	case string(targetTypeServerless):
		return target{targetType: targetTypeServerless, targetID: ids[0]}, true
	default:
		return target{}, false
	}
}

// clientCertSAN builds the DNS Subject Alternative Name that the ECP proxy's
// XFCC builder recognizes to derive tenant identity for the mTLS connection
// used to reach the token endpoint, mirroring the patterns implemented by
// github.com/elastic/proxy/xfcc:
//
//	hosted:     <deploymentID>.deployment.<orgID>.account
//	serverless: EIDENT.<projectID>.project.<projectType>.type.<orgID>.organization
//
// The proxy only treats a SAN as safe to embed in the XFCC header (and only
// extracts identity fields from it) when the captured ID/org segments match
// [a-z0-9-]+, so IDs are lower-cased defensively before being embedded.
func clientCertSAN(t target, orgID string) (string, error) {
	id := strings.ToLower(t.targetID)
	org := strings.ToLower(orgID)

	switch t.targetType {
	case targetTypeHosted:
		return fmt.Sprintf("%s.deployment.%s.account", id, org), nil
	case targetTypeServerless:
		return fmt.Sprintf("EIDENT.%s.project.%s.type.%s.organization", id, observabilityProjectType, org), nil
	default:
		return "", fmt.Errorf("unsupported target type %q", t.targetType)
	}
}
