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

package verifierreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/opentelemetry-collector-components/receiver/verifierreceiver/internal/verifier"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{
			name: "valid config with AWS credentials",
			config: Config{
				CloudConnectorID:   "cc-12345",
				CloudConnectorName: "Production Connector",
				VerificationID:     "verify-abc123",
				VerificationType:   "on_demand",
				Providers: ProvidersConfig{
					AWS: AWSProviderConfig{
						Credentials: AWSCredentials{
							RoleARN:       "arn:aws:iam::123456789012:role/ElasticAgentRole",
							ExternalID:    "elastic-external-id-12345",
							DefaultRegion: "us-east-1",
						},
					},
				},
				Policies: []PolicyConfig{
					{
						PolicyID:   "policy-1",
						PolicyName: "AWS Security Monitoring",
						Integrations: []IntegrationConfig{
							{
								IntegrationID:   "int-cloudtrail-001",
								IntegrationType: "aws_cloudtrail",
								IntegrationName: "AWS CloudTrail",
							},
						},
					},
				},
			},
			wantErr: "",
		},
		{
			name: "valid config with integration version",
			config: Config{
				CloudConnectorID: "cc-12345",
				VerificationID:   "verify-abc123",
				Policies: []PolicyConfig{
					{
						PolicyID: "policy-1",
						Integrations: []IntegrationConfig{
							{
								IntegrationType:    "aws_cloudtrail",
								IntegrationVersion: "2.17.0",
							},
						},
					},
				},
			},
			wantErr: "",
		},
		{
			name: "valid config without AWS credentials (non-AWS integrations)",
			config: Config{
				CloudConnectorID: "cc-12345",
				VerificationID:   "verify-abc123",
				Policies: []PolicyConfig{
					{
						PolicyID: "policy-1",
						Integrations: []IntegrationConfig{
							{IntegrationType: "okta_system"},
						},
					},
				},
			},
			wantErr: "",
		},
		{
			name: "valid config with AWS integration but no credentials (credentials optional at config level)",
			config: Config{
				CloudConnectorID: "cc-12345",
				VerificationID:   "verify-abc123",
				Policies: []PolicyConfig{
					{
						PolicyID: "policy-1",
						Integrations: []IntegrationConfig{
							{IntegrationType: "aws_cloudtrail"},
						},
					},
				},
			},
			wantErr: "",
		},
		// Note: Provider credentials validation is handled by their respective Validate() methods.
		{
			name: "invalid config - missing cloud_connector_id",
			config: Config{
				VerificationID: "verify-abc123",
				Policies: []PolicyConfig{
					{
						PolicyID: "policy-1",
						Integrations: []IntegrationConfig{
							{IntegrationType: "aws_cloudtrail"},
						},
					},
				},
			},
			wantErr: "cloud_connector_id must be specified",
		},
		{
			name: "invalid config - no policies",
			config: Config{
				CloudConnectorID: "cc-12345",
				VerificationID:   "verify-abc123",
				Policies:         []PolicyConfig{},
			},
			wantErr: "at least one policy must be specified",
		},
		{
			name: "invalid config - policy without policy_id",
			config: Config{
				CloudConnectorID: "cc-12345",
				VerificationID:   "verify-abc123",
				Policies: []PolicyConfig{
					{
						Integrations: []IntegrationConfig{
							{IntegrationType: "aws_cloudtrail"},
						},
					},
				},
			},
			wantErr: "policies[0]: policy_id must be specified",
		},
		{
			name: "invalid config - policy without integrations",
			config: Config{
				CloudConnectorID: "cc-12345",
				VerificationID:   "verify-abc123",
				Policies: []PolicyConfig{
					{
						PolicyID:     "policy-1",
						Integrations: []IntegrationConfig{},
					},
				},
			},
			wantErr: "policies[0]: at least one integration must be specified",
		},
		{
			name: "invalid config - integration without type",
			config: Config{
				CloudConnectorID: "cc-12345",
				VerificationID:   "verify-abc123",
				Policies: []PolicyConfig{
					{
						PolicyID: "policy-1",
						Integrations: []IntegrationConfig{
							{IntegrationName: "Some Integration"},
						},
					},
				},
			},
			wantErr: "policies[0].integrations[0]: integration_type must be specified",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAWSCredentials_Validate(t *testing.T) {
	tests := []struct {
		name        string
		credentials AWSCredentials
		wantErr     string
	}{
		{
			name: "valid - fully configured",
			credentials: AWSCredentials{
				RoleARN:       "arn:aws:iam::123456789012:role/ElasticAgentRole",
				ExternalID:    "test-external-id",
				DefaultRegion: "us-east-1",
			},
			wantErr: "",
		},
		{
			name:        "valid - empty (not configured)",
			credentials: AWSCredentials{},
			wantErr:     "",
		},
		{
			name: "valid - only default_region (considered empty)",
			credentials: AWSCredentials{
				DefaultRegion: "us-east-1",
			},
			wantErr: "",
		},
		{
			name: "valid - use_default_credentials",
			credentials: AWSCredentials{
				UseDefaultCredentials: true,
			},
			wantErr: "",
		},
		{
			name: "invalid - role_arn without external_id",
			credentials: AWSCredentials{
				RoleARN: "arn:aws:iam::123456789012:role/ElasticAgentRole",
			},
			wantErr: "external_id must be specified when role_arn is set",
		},
		{
			name: "invalid - external_id without role_arn",
			credentials: AWSCredentials{
				ExternalID: "test-external-id",
			},
			wantErr: "role_arn must be specified when external_id is set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.credentials.Validate()
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAWSCredentials_IsConfigured(t *testing.T) {
	tests := []struct {
		name        string
		credentials AWSCredentials
		want        bool
	}{
		{
			name: "fully configured",
			credentials: AWSCredentials{
				RoleARN:    "arn:aws:iam::123456789012:role/ElasticAgentRole",
				ExternalID: "test-external-id",
			},
			want: true,
		},
		{
			name: "use_default_credentials",
			credentials: AWSCredentials{
				UseDefaultCredentials: true,
			},
			want: true,
		},
		{
			name: "missing external_id",
			credentials: AWSCredentials{
				RoleARN: "arn:aws:iam::123456789012:role/ElasticAgentRole",
			},
			want: false,
		},
		{
			name: "missing role_arn",
			credentials: AWSCredentials{
				ExternalID: "test-external-id",
			},
			want: false,
		},
		{
			name:        "empty",
			credentials: AWSCredentials{},
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.credentials.IsConfigured())
		})
	}
}

func TestGetProviderForIntegration(t *testing.T) {
	tests := []struct {
		name            string
		integrationType string
		want            verifier.ProviderType
	}{
		{
			name:            "AWS CloudTrail",
			integrationType: "aws_cloudtrail",
			want:            verifier.ProviderAWS,
		},
		{
			name:            "AWS GuardDuty",
			integrationType: "aws_guardduty",
			want:            verifier.ProviderAWS,
		},
		{
			name:            "Azure Activity Logs",
			integrationType: "azure_activitylogs",
			want:            verifier.ProviderAzure,
		},
		{
			name:            "GCP Audit",
			integrationType: "gcp_audit",
			want:            verifier.ProviderGCP,
		},
		{
			name:            "Okta System",
			integrationType: "okta_system",
			want:            verifier.ProviderOkta,
		},
		{
			name:            "Unknown",
			integrationType: "unknown_integration",
			want:            "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetProviderForIntegration(tt.integrationType)
			assert.Equal(t, tt.want, got)
		})
	}
}
