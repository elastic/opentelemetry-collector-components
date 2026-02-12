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

//go:generate mdatagen metadata.yaml

// Package verifierreceiver provides an OTEL receiver that verifies
// permissions for cloud integrations and reports the results as OTEL logs.
//
// # Overview
//
// The receiver uses a registry-based architecture to support multiple
// cloud and identity providers:
//   - AWS (active): CloudTrail, GuardDuty, Security Hub, S3, EC2, etc.
//   - Azure (planned): Activity Logs, Audit Logs, Blob Storage
//   - GCP (planned): Audit Logs, Cloud Storage, Pub/Sub
//   - Okta (planned): System Logs, User Events
//
// # Architecture
//
// The receiver consists of two main registries:
//   - Permission Registry: Maps integration types and versions to required permissions.
//     Each integration type can have multiple versioned permission sets matched via
//     semver constraints (e.g., ">=2.0.0", ">=1.0.0,<2.0.0").
//   - Verifier Registry: Manages provider-specific verifiers (AWS, Azure, etc.)
//
// Each verifier implements the Verifier interface and is responsible for
// making API calls to verify that permissions are granted.
//
// # Configuration
//
// The receiver is configured with:
//   - Cloud Connector identification (ID, name)
//   - Verification session (ID, type)
//   - Provider credentials (AWS, Azure, GCP, Okta)
//   - Policies containing integrations to verify, each with an optional
//     integration_version for version-aware permission lookup
//
// Example:
//
//	receivers:
//	  verifier:
//	    cloud_connector_id: "cc-12345"
//	    verification_id: "verify-001"
//	    providers:
//	      aws:
//	        credentials:
//	          role_arn: "arn:aws:iam::123456789012:role/Role"
//	          external_id: "external-id"
//	    policies:
//	      - policy_id: "policy-1"
//	        integrations:
//	          - integration_type: "aws_cloudtrail"
//	            integration_version: "2.17.0"
//
// # Output
//
// The receiver emits OTEL logs with structured attributes following the
// RFC specification for Cloud Connector Permission Verification.
package verifierreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/verifierreceiver"
