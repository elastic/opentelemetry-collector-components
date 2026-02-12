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

package verifierreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/verifierreceiver"

import (
	"github.com/elastic/opentelemetry-collector-components/receiver/verifierreceiver/internal/verifier"
)

// VerificationMethod indicates how a permission should be verified.
type VerificationMethod string

const (
	// MethodAPICall makes an actual API call with minimal scope.
	MethodAPICall VerificationMethod = "api_call"
	// MethodDryRun uses provider's DryRun parameter where supported (e.g., AWS EC2).
	MethodDryRun VerificationMethod = "dry_run"
	// MethodHTTPProbe uses HTTP HEAD/GET request to check connectivity.
	MethodHTTPProbe VerificationMethod = "http_probe"
	// MethodGraphQL uses GraphQL introspection or minimal query.
	MethodGraphQL VerificationMethod = "graphql_query"
)

// PermissionStatus represents the result of a permission verification.
type PermissionStatus string

const (
	StatusGranted PermissionStatus = "granted"
	StatusDenied  PermissionStatus = "denied"
	StatusError   PermissionStatus = "error"
	StatusSkipped PermissionStatus = "skipped"
	StatusPending PermissionStatus = "pending"
)

// Permission represents a single permission to verify.
type Permission struct {
	// Action is the permission action (e.g., "cloudtrail:LookupEvents", "s3:GetObject").
	Action string

	// Required indicates if this permission is required for the integration to function.
	Required bool

	// Method is the verification method to use.
	Method VerificationMethod

	// APIEndpoint is the API endpoint to call (for http_probe, graphql_query methods).
	APIEndpoint string

	// Category is an optional categorization (e.g., "data_access", "management").
	Category string
}

// IntegrationPermissions defines the permissions required by an integration type.
type IntegrationPermissions struct {
	// Provider identifies the cloud/service provider.
	Provider verifier.ProviderType

	// Permissions is the list of permissions required by this integration.
	Permissions []Permission
}

// PermissionRegistry maintains the mapping of integration types to their required permissions.
// The receiver owns this mapping - Fleet API only provides the integration context.
type PermissionRegistry struct {
	integrations map[string]IntegrationPermissions
}

// NewPermissionRegistry creates a new permission registry with default mappings.
func NewPermissionRegistry() *PermissionRegistry {
	registry := &PermissionRegistry{
		integrations: make(map[string]IntegrationPermissions),
	}

	// Register all provider integrations
	registry.registerAWSIntegrations()
	registry.registerAzureIntegrations()
	registry.registerGCPIntegrations()
	registry.registerOktaIntegrations()

	return registry
}

// GetPermissions returns the permissions required for an integration type.
// Returns nil if the integration type is not registered.
func (r *PermissionRegistry) GetPermissions(integrationType string) *IntegrationPermissions {
	if perms, ok := r.integrations[integrationType]; ok {
		return &perms
	}
	return nil
}

// IsSupported returns true if the integration type is registered in the registry.
func (r *PermissionRegistry) IsSupported(integrationType string) bool {
	_, ok := r.integrations[integrationType]
	return ok
}

// SupportedIntegrations returns a list of all supported integration types.
func (r *PermissionRegistry) SupportedIntegrations() []string {
	integrations := make([]string, 0, len(r.integrations))
	for k := range r.integrations {
		integrations = append(integrations, k)
	}
	return integrations
}

// SupportedIntegrationsByProvider returns integration types grouped by provider.
func (r *PermissionRegistry) SupportedIntegrationsByProvider() map[verifier.ProviderType][]string {
	byProvider := make(map[verifier.ProviderType][]string)
	for integrationType, perms := range r.integrations {
		byProvider[perms.Provider] = append(byProvider[perms.Provider], integrationType)
	}
	return byProvider
}

// registerAWSIntegrations registers all AWS-based integrations.
func (r *PermissionRegistry) registerAWSIntegrations() {
	// AWS CloudTrail - commonly used for security auditing
	// https://www.elastic.co/docs/current/integrations/aws/cloudtrail
	r.integrations["aws_cloudtrail"] = IntegrationPermissions{
		Provider: verifier.ProviderAWS,
		Permissions: []Permission{
			{
				Action:   "cloudtrail:LookupEvents",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
			{
				Action:   "cloudtrail:DescribeTrails",
				Required: true,
				Method:   MethodAPICall,
				Category: "management",
			},
			{
				Action:   "cloudtrail:GetTrailStatus",
				Required: false,
				Method:   MethodAPICall,
				Category: "management",
			},
			{
				Action:   "s3:GetObject",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
			{
				Action:   "s3:ListBucket",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
			{
				Action:   "sqs:ReceiveMessage",
				Required: false,
				Method:   MethodAPICall,
				Category: "data_access",
			},
			{
				Action:   "sqs:DeleteMessage",
				Required: false,
				Method:   MethodAPICall,
				Category: "data_access",
			},
		},
	}

	// AWS GuardDuty - threat detection service
	r.integrations["aws_guardduty"] = IntegrationPermissions{
		Provider: verifier.ProviderAWS,
		Permissions: []Permission{
			{
				Action:   "guardduty:ListDetectors",
				Required: true,
				Method:   MethodAPICall,
				Category: "management",
			},
			{
				Action:   "guardduty:GetFindings",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
			{
				Action:   "guardduty:ListFindings",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
		},
	}

	// AWS Security Hub - security findings aggregation
	r.integrations["aws_securityhub"] = IntegrationPermissions{
		Provider: verifier.ProviderAWS,
		Permissions: []Permission{
			{
				Action:   "securityhub:GetFindings",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
			{
				Action:   "securityhub:BatchGetSecurityControls",
				Required: false,
				Method:   MethodAPICall,
				Category: "data_access",
			},
			{
				Action:   "securityhub:DescribeHub",
				Required: true,
				Method:   MethodAPICall,
				Category: "management",
			},
		},
	}

	// AWS S3 - storage access logs
	r.integrations["aws_s3"] = IntegrationPermissions{
		Provider: verifier.ProviderAWS,
		Permissions: []Permission{
			{
				Action:   "s3:ListBucket",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
			{
				Action:   "s3:GetObject",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
			{
				Action:   "s3:GetBucketLocation",
				Required: false,
				Method:   MethodAPICall,
				Category: "management",
			},
		},
	}

	// AWS EC2 - compute instance metrics
	r.integrations["aws_ec2"] = IntegrationPermissions{
		Provider: verifier.ProviderAWS,
		Permissions: []Permission{
			{
				Action:   "ec2:DescribeInstances",
				Required: true,
				Method:   MethodDryRun,
				Category: "data_access",
			},
			{
				Action:   "ec2:DescribeRegions",
				Required: true,
				Method:   MethodAPICall,
				Category: "management",
			},
			{
				Action:   "cloudwatch:GetMetricData",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
		},
	}

	// AWS VPC Flow Logs
	r.integrations["aws_vpcflow"] = IntegrationPermissions{
		Provider: verifier.ProviderAWS,
		Permissions: []Permission{
			{
				Action:   "logs:FilterLogEvents",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
			{
				Action:   "logs:DescribeLogGroups",
				Required: true,
				Method:   MethodAPICall,
				Category: "management",
			},
			{
				Action:   "logs:DescribeLogStreams",
				Required: true,
				Method:   MethodAPICall,
				Category: "management",
			},
			{
				Action:   "ec2:DescribeFlowLogs",
				Required: false,
				Method:   MethodAPICall,
				Category: "management",
			},
		},
	}

	// AWS WAF - Web Application Firewall logs
	r.integrations["aws_waf"] = IntegrationPermissions{
		Provider: verifier.ProviderAWS,
		Permissions: []Permission{
			{
				Action:   "wafv2:GetWebACL",
				Required: true,
				Method:   MethodAPICall,
				Category: "management",
			},
			{
				Action:   "wafv2:ListWebACLs",
				Required: true,
				Method:   MethodAPICall,
				Category: "management",
			},
			{
				Action:   "s3:GetObject",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
			{
				Action:   "s3:ListBucket",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
		},
	}

	// AWS Route53 - DNS query logs
	r.integrations["aws_route53"] = IntegrationPermissions{
		Provider: verifier.ProviderAWS,
		Permissions: []Permission{
			{
				Action:   "logs:FilterLogEvents",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
			{
				Action:   "logs:DescribeLogGroups",
				Required: true,
				Method:   MethodAPICall,
				Category: "management",
			},
			{
				Action:   "route53:ListHostedZones",
				Required: false,
				Method:   MethodAPICall,
				Category: "management",
			},
		},
	}

	// AWS ELB - Elastic Load Balancer access logs
	r.integrations["aws_elb"] = IntegrationPermissions{
		Provider: verifier.ProviderAWS,
		Permissions: []Permission{
			{
				Action:   "s3:GetObject",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
			{
				Action:   "s3:ListBucket",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
			{
				Action:   "elasticloadbalancing:DescribeLoadBalancers",
				Required: false,
				Method:   MethodAPICall,
				Category: "management",
			},
		},
	}

	// AWS CloudFront - CDN access logs
	r.integrations["aws_cloudfront"] = IntegrationPermissions{
		Provider: verifier.ProviderAWS,
		Permissions: []Permission{
			{
				Action:   "s3:GetObject",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
			{
				Action:   "s3:ListBucket",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
			{
				Action:   "cloudfront:ListDistributions",
				Required: false,
				Method:   MethodAPICall,
				Category: "management",
			},
		},
	}
}

// registerAzureIntegrations registers all Azure-based integrations.
// TODO: Implement Azure verifier and add actual permission mappings.
func (r *PermissionRegistry) registerAzureIntegrations() {
	// Azure Activity Logs
	r.integrations["azure_activitylogs"] = IntegrationPermissions{
		Provider: verifier.ProviderAzure,
		Permissions: []Permission{
			{
				Action:   "Microsoft.Insights/eventtypes/values/Read",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
		},
	}

	// Azure Audit Logs
	r.integrations["azure_auditlogs"] = IntegrationPermissions{
		Provider: verifier.ProviderAzure,
		Permissions: []Permission{
			{
				Action:   "Microsoft.Insights/eventtypes/values/Read",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
		},
	}

	// Azure Blob Storage
	r.integrations["azure_blob_storage"] = IntegrationPermissions{
		Provider: verifier.ProviderAzure,
		Permissions: []Permission{
			{
				Action:   "Microsoft.Storage/storageAccounts/blobServices/containers/read",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
		},
	}
}

// registerGCPIntegrations registers all GCP-based integrations.
// TODO: Implement GCP verifier and add actual permission mappings.
func (r *PermissionRegistry) registerGCPIntegrations() {
	// GCP Audit Logs
	r.integrations["gcp_audit"] = IntegrationPermissions{
		Provider: verifier.ProviderGCP,
		Permissions: []Permission{
			{
				Action:   "logging.logEntries.list",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
		},
	}

	// GCP Cloud Storage
	r.integrations["gcp_storage"] = IntegrationPermissions{
		Provider: verifier.ProviderGCP,
		Permissions: []Permission{
			{
				Action:   "storage.objects.get",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
			{
				Action:   "storage.objects.list",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
		},
	}

	// GCP Pub/Sub
	r.integrations["gcp_pubsub"] = IntegrationPermissions{
		Provider: verifier.ProviderGCP,
		Permissions: []Permission{
			{
				Action:   "pubsub.subscriptions.consume",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
		},
	}
}

// registerOktaIntegrations registers all Okta-based integrations.
// TODO: Implement Okta verifier and add actual permission mappings.
func (r *PermissionRegistry) registerOktaIntegrations() {
	// Okta System Logs
	r.integrations["okta_system"] = IntegrationPermissions{
		Provider: verifier.ProviderOkta,
		Permissions: []Permission{
			{
				Action:   "okta.logs.read",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
		},
	}

	// Okta User Events
	r.integrations["okta_users"] = IntegrationPermissions{
		Provider: verifier.ProviderOkta,
		Permissions: []Permission{
			{
				Action:   "okta.users.read",
				Required: true,
				Method:   MethodAPICall,
				Category: "data_access",
			},
		},
	}
}
