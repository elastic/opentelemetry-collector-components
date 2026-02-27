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
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/elastic/opentelemetry-collector-components/receiver/verifierreceiver/internal/verifier"
)

// Config defines configuration for the permission verifier receiver.
// The receiver owns the mapping between integrations and their required permissions.
// Fleet API provides the policy/integration context; the receiver determines what
// permissions each integration needs and how to verify them.
type Config struct {
	// CloudConnectorID identifies the Cloud Connector being verified.
	CloudConnectorID string `mapstructure:"cloud_connector_id"`

	// CloudConnectorName is the human-readable name of the Cloud Connector.
	CloudConnectorName string `mapstructure:"cloud_connector_name"`

	// VerificationID is a unique identifier for this verification session.
	VerificationID string `mapstructure:"verification_id"`

	// VerificationType indicates the type of verification: "on_demand" or "scheduled".
	VerificationType string `mapstructure:"verification_type"`

	// Providers contains authentication configuration for each cloud/identity provider.
	Providers ProvidersConfig `mapstructure:"providers"`

	// Policies is the list of agent policies to verify.
	// Each policy contains integrations that need permission verification.
	Policies []PolicyConfig `mapstructure:"policies"`
}

// ProvidersConfig contains authentication configuration for all supported providers.
type ProvidersConfig struct {
	// CloudConnector contains shared OIDC authentication used by the cloud connector flow.
	CloudConnector CloudConnectorConfig `mapstructure:"cloud_connector"`

	AWS   AWSProviderConfig   `mapstructure:"aws"`
	Azure AzureProviderConfig `mapstructure:"azure"`
	GCP   GCPProviderConfig   `mapstructure:"gcp"`
	Okta  OktaProviderConfig  `mapstructure:"okta"`
}

// CloudConnectorConfig contains shared OIDC fields for the cloud connector
// authentication flow. These are typically injected as environment variables
// by the agentless controller.
type CloudConnectorConfig struct {
	// IDTokenFile is the path to the OIDC JWT token file.
	// Env fallback: CLOUD_CONNECTORS_ID_TOKEN_FILE
	IDTokenFile string `mapstructure:"id_token_file"`

	// GlobalRoleARN is the Elastic global IAM role used in the AWS/GCP auth chains.
	// Env fallback: CLOUD_CONNECTORS_GLOBAL_ROLE
	GlobalRoleARN string `mapstructure:"global_role_arn"`

	// CloudResourceID identifies the cloud resource, used as SourceIdentity.
	// Env fallback: CLOUD_RESOURCE_ID
	CloudResourceID string `mapstructure:"cloud_resource_id"`
}

// LoadFromEnv populates empty fields from well-known environment variables
// set by the agentless controller.
func (cfg *CloudConnectorConfig) LoadFromEnv() {
	if cfg.IDTokenFile == "" {
		cfg.IDTokenFile = os.Getenv("CLOUD_CONNECTORS_ID_TOKEN_FILE")
	}
	if cfg.GlobalRoleARN == "" {
		cfg.GlobalRoleARN = os.Getenv("CLOUD_CONNECTORS_GLOBAL_ROLE")
	}
	if cfg.CloudResourceID == "" {
		cfg.CloudResourceID = os.Getenv("CLOUD_RESOURCE_ID")
	}
}

// IsConfigured returns true if the cloud connector OIDC token file is available.
func (cfg *CloudConnectorConfig) IsConfigured() bool {
	return cfg.IDTokenFile != ""
}

// AWSProviderConfig contains AWS authentication configuration.
type AWSProviderConfig struct {
	// Credentials contains the Cloud Connector authentication credentials.
	Credentials AWSCredentials `mapstructure:"credentials"`
}

// AWSCredentials contains the AWS credentials for Cloud Connector mode.
type AWSCredentials struct {
	// RoleARN is the ARN of the IAM role to assume in the customer's AWS account.
	RoleARN string `mapstructure:"role_arn"`

	// ExternalID is used to prevent confused deputy attacks.
	ExternalID string `mapstructure:"external_id"`

	// DefaultRegion is the default AWS region to use for API calls.
	DefaultRegion string `mapstructure:"default_region"`

	// UseDefaultCredentials enables using default AWS credentials (for testing).
	UseDefaultCredentials bool `mapstructure:"use_default_credentials"`
}

// Validate validates the AWS credentials.
func (cfg *AWSCredentials) Validate() error {
	if cfg.UseDefaultCredentials {
		return nil
	}
	if cfg.RoleARN == "" && cfg.ExternalID == "" {
		return nil // Not configured; cloud connector config may supply the rest
	}
	if cfg.RoleARN == "" {
		return errors.New("role_arn must be specified when external_id is set")
	}
	// external_id is optional in cloud connector mode (only required for direct mode)
	return nil
}

// IsConfigured returns true if AWS credentials are configured (with or without
// cloud connector config -- the cloud connector fields are checked separately
// during verifier initialization).
func (cfg *AWSCredentials) IsConfigured() bool {
	return cfg.RoleARN != "" || cfg.UseDefaultCredentials
}

// ToAuthConfig converts the config to a verifier.AWSAuthConfig, merging in
// the shared cloud connector OIDC configuration.
func (cfg *AWSCredentials) ToAuthConfig(cc CloudConnectorConfig) verifier.AWSAuthConfig {
	return verifier.AWSAuthConfig{
		IDTokenFile:           cc.IDTokenFile,
		GlobalRoleARN:         cc.GlobalRoleARN,
		CloudResourceID:       cc.CloudResourceID,
		RoleARN:               cfg.RoleARN,
		ExternalID:            cfg.ExternalID,
		DefaultRegion:         cfg.DefaultRegion,
		UseDefaultCredentials: cfg.UseDefaultCredentials,
	}
}

// AzureProviderConfig contains Azure authentication configuration.
type AzureProviderConfig struct {
	// Credentials contains the Azure authentication credentials.
	Credentials AzureCredentials `mapstructure:"credentials"`
}

// AzureCredentials contains the Azure credentials.
type AzureCredentials struct {
	TenantID       string `mapstructure:"tenant_id"`
	ClientID       string `mapstructure:"client_id"`
	SubscriptionID string `mapstructure:"subscription_id"`

	// UseDefaultCredentials uses DefaultAzureCredential which chains env vars,
	// workload identity, managed identity, Azure CLI (az login), and azd CLI.
	UseDefaultCredentials bool `mapstructure:"use_default_credentials"`
}

// Validate validates the Azure credentials.
func (cfg *AzureCredentials) Validate() error {
	if cfg.UseDefaultCredentials {
		return nil
	}
	if cfg.TenantID == "" && cfg.ClientID == "" {
		return nil // Not configured; cloud connector may provide the JWT
	}
	if cfg.TenantID == "" {
		return errors.New("tenant_id must be specified")
	}
	if cfg.ClientID == "" {
		return errors.New("client_id must be specified")
	}
	return nil
}

// IsConfigured returns true if Azure credentials are configured.
func (cfg *AzureCredentials) IsConfigured() bool {
	return cfg.UseDefaultCredentials || (cfg.TenantID != "" && cfg.ClientID != "")
}

// ToAuthConfig converts the config to a verifier.AzureAuthConfig, merging in
// the shared cloud connector OIDC configuration.
func (cfg *AzureCredentials) ToAuthConfig(cc CloudConnectorConfig) verifier.AzureAuthConfig {
	return verifier.AzureAuthConfig{
		IDTokenFile:           cc.IDTokenFile,
		TenantID:              cfg.TenantID,
		ClientID:              cfg.ClientID,
		SubscriptionID:        cfg.SubscriptionID,
		UseDefaultCredentials: cfg.UseDefaultCredentials,
	}
}

// GCPProviderConfig contains GCP authentication configuration.
type GCPProviderConfig struct {
	// Credentials contains the GCP authentication credentials.
	Credentials GCPCredentials `mapstructure:"credentials"`
}

// GCPCredentials contains the GCP credentials.
type GCPCredentials struct {
	ProjectID string `mapstructure:"project_id"`

	// Cloud Connector WIF fields
	// WorkloadIdentityProvider is the full resource name of the GCP WIF provider
	// used as the audience for STS token exchange.
	// Example: //iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider
	WorkloadIdentityProvider string `mapstructure:"workload_identity_provider"`

	// ServiceAccountEmail is the GCP service account to impersonate via WIF.
	ServiceAccountEmail string `mapstructure:"service_account_email"`

	// UseDefaultCredentials uses Application Default Credentials (for testing).
	UseDefaultCredentials bool `mapstructure:"use_default_credentials"`
}

// Validate validates the GCP credentials.
func (cfg *GCPCredentials) Validate() error {
	return nil
}

// IsConfigured returns true if GCP credentials are configured.
func (cfg *GCPCredentials) IsConfigured() bool {
	return cfg.WorkloadIdentityProvider != "" || cfg.UseDefaultCredentials
}

// ToAuthConfig converts the config to a verifier.GCPAuthConfig, merging in
// the shared cloud connector OIDC configuration.
func (cfg *GCPCredentials) ToAuthConfig(cc CloudConnectorConfig) verifier.GCPAuthConfig {
	return verifier.GCPAuthConfig{
		IDTokenFile:              cc.IDTokenFile,
		WorkloadIdentityProvider: cfg.WorkloadIdentityProvider,
		ServiceAccountEmail:      cfg.ServiceAccountEmail,
		ProjectID:                cfg.ProjectID,
		UseDefaultCredentials:    cfg.UseDefaultCredentials,
	}
}

// OktaProviderConfig contains Okta authentication configuration.
type OktaProviderConfig struct {
	// Credentials contains the Okta authentication credentials.
	Credentials OktaCredentials `mapstructure:"credentials"`
}

// OktaCredentials contains the Okta credentials.
type OktaCredentials struct {
	// Domain is the Okta domain (e.g., dev-123456.okta.com).
	Domain string `mapstructure:"domain"`

	// APIToken is the Okta API token.
	APIToken string `mapstructure:"api_token"`

	// ClientID is the OAuth 2.0 client ID (for OAuth authentication).
	ClientID string `mapstructure:"client_id"`

	// PrivateKey is the private key for OAuth authentication.
	PrivateKey string `mapstructure:"private_key"`
}

// Validate validates the Okta credentials.
func (cfg *OktaCredentials) Validate() error {
	if cfg.Domain == "" && cfg.APIToken == "" && cfg.ClientID == "" {
		return nil // Not configured
	}
	if cfg.Domain == "" {
		return errors.New("domain must be specified")
	}
	if cfg.APIToken == "" && cfg.ClientID == "" {
		return errors.New("either api_token or client_id must be specified")
	}
	if cfg.ClientID != "" && cfg.PrivateKey == "" {
		return errors.New("private_key must be specified when using client_id")
	}
	return nil
}

// IsConfigured returns true if Okta credentials are configured.
func (cfg *OktaCredentials) IsConfigured() bool {
	return cfg.Domain != "" && (cfg.APIToken != "" || (cfg.ClientID != "" && cfg.PrivateKey != ""))
}

// ToAuthConfig converts the config to a verifier.OktaAuthConfig.
func (cfg *OktaCredentials) ToAuthConfig() verifier.OktaAuthConfig {
	return verifier.OktaAuthConfig{
		Domain:     cfg.Domain,
		APIToken:   cfg.APIToken,
		ClientID:   cfg.ClientID,
		PrivateKey: cfg.PrivateKey,
	}
}

// PolicyConfig represents an agent policy with its integrations.
type PolicyConfig struct {
	// PolicyID is the unique identifier for the policy.
	PolicyID string `mapstructure:"policy_id"`

	// PolicyName is the human-readable name of the policy.
	PolicyName string `mapstructure:"policy_name"`

	// Integrations is the list of integrations within this policy.
	Integrations []IntegrationConfig `mapstructure:"integrations"`
}

// IntegrationConfig represents an integration within a policy.
type IntegrationConfig struct {
	// IntegrationID is the unique identifier for the integration instance.
	IntegrationID string `mapstructure:"integration_id"`

	// IntegrationType is the package/integration type (e.g., "aws_cloudtrail", "okta").
	// This is used to look up required permissions from the registry.
	IntegrationType string `mapstructure:"integration_type"`

	// IntegrationName is the human-readable name of the integration.
	IntegrationName string `mapstructure:"integration_name"`

	// IntegrationVersion is the semantic version of the integration package (e.g., "2.17.0").
	// Different versions may require different permissions. When empty, the latest
	// registered permission set is used.
	IntegrationVersion string `mapstructure:"integration_version"`

	// Config contains provider-specific configuration.
	// For AWS: may include regions, account_id, etc.
	Config map[string]interface{} `mapstructure:"config"`
}

// Validate validates the configuration.
func (cfg *Config) Validate() error {
	if cfg.CloudConnectorID == "" {
		return errors.New("cloud_connector_id must be specified")
	}
	if cfg.VerificationID == "" {
		return errors.New("verification_id must be specified")
	}
	if len(cfg.Policies) == 0 {
		return errors.New("at least one policy must be specified")
	}

	for i, policy := range cfg.Policies {
		if policy.PolicyID == "" {
			return fmt.Errorf("policies[%d]: policy_id must be specified", i)
		}
		if len(policy.Integrations) == 0 {
			return fmt.Errorf("policies[%d]: at least one integration must be specified", i)
		}
		for j, integration := range policy.Integrations {
			if integration.IntegrationType == "" {
				return fmt.Errorf("policies[%d].integrations[%d]: integration_type must be specified", i, j)
			}
		}
	}

	// Provider credentials validation is handled by their respective Validate() methods
	// which are called automatically by the OTel framework.

	return nil
}

// GetProviderForIntegration returns the provider type for a given integration type.
func GetProviderForIntegration(integrationType string) verifier.ProviderType {
	if strings.HasPrefix(integrationType, "aws_") {
		return verifier.ProviderAWS
	}
	if strings.HasPrefix(integrationType, "azure_") {
		return verifier.ProviderAzure
	}
	if strings.HasPrefix(integrationType, "gcp_") {
		return verifier.ProviderGCP
	}
	if strings.HasPrefix(integrationType, "okta_") {
		return verifier.ProviderOkta
	}
	return ""
}
