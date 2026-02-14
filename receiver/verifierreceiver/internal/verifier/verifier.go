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

// Package verifier provides permission verification for cloud providers.
// It defines interfaces and types for verifying permissions across different
// cloud providers (AWS, Azure, GCP) and identity providers (Okta, etc.).
package verifier // import "github.com/elastic/opentelemetry-collector-components/receiver/verifierreceiver/internal/verifier"

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// ProviderType represents the type of cloud/identity provider.
type ProviderType string

const (
	ProviderAWS   ProviderType = "aws"
	ProviderAzure ProviderType = "azure"
	ProviderGCP   ProviderType = "gcp"
	ProviderOkta  ProviderType = "okta"
)

// Result represents the result of a permission verification.
type Result struct {
	// Status is the verification result status.
	Status Status

	// ErrorCode is the error code returned by the provider (if any).
	ErrorCode string

	// ErrorMessage is the error message returned by the provider (if any).
	ErrorMessage string

	// Duration is how long the verification took.
	Duration time.Duration

	// Endpoint is the API endpoint that was called (if applicable).
	Endpoint string
}

// Status represents the result status of a permission verification.
type Status string

const (
	StatusGranted Status = "granted"
	StatusDenied  Status = "denied"
	StatusError   Status = "error"
	StatusSkipped Status = "skipped"
)

// VerificationMethod indicates how a permission should be verified.
type VerificationMethod string

const (
	MethodAPICall   VerificationMethod = "api_call"
	MethodDryRun    VerificationMethod = "dry_run"
	MethodHTTPProbe VerificationMethod = "http_probe"
)

// Permission represents a permission to verify.
type Permission struct {
	Action   string
	Method   VerificationMethod
	Required bool
	Category string
}

// ProviderConfig contains provider-specific configuration passed during verification.
type ProviderConfig struct {
	// AWS configuration
	Region    string
	AccountID string

	// Azure configuration
	SubscriptionID string
	ResourceGroup  string
	TenantID       string

	// GCP configuration
	ProjectID string

	// Okta configuration
	OktaDomain string

	// Generic configuration
	Endpoint string
}

// AuthConfig is the interface for provider-specific authentication configuration.
// Each provider implements its own auth config struct.
type AuthConfig interface {
	// ProviderType returns the provider type this auth config is for.
	ProviderType() ProviderType
	// IsConfigured returns true if the auth config has the required fields.
	IsConfigured() bool
}

// AWSAuthConfig contains AWS authentication configuration for Cloud Connector.
// This enables assuming a role in the customer's AWS account using STS AssumeRole.
type AWSAuthConfig struct {
	// RoleARN is the ARN of the IAM role to assume in the customer's AWS account.
	RoleARN string

	// ExternalID is used to prevent confused deputy attacks.
	ExternalID string

	// SessionName is an optional name for the assumed role session.
	SessionName string

	// AssumeRoleDuration is the duration for the assumed role credentials.
	AssumeRoleDuration time.Duration

	// DefaultRegion is the default AWS region to use for API calls.
	DefaultRegion string

	// UseDefaultCredentials uses default AWS credentials (for testing).
	UseDefaultCredentials bool
}

// ProviderType implements AuthConfig.
func (c AWSAuthConfig) ProviderType() ProviderType { return ProviderAWS }

// IsConfigured implements AuthConfig.
func (c AWSAuthConfig) IsConfigured() bool {
	return (c.RoleARN != "" && c.ExternalID != "") || c.UseDefaultCredentials
}

// AzureAuthConfig contains Azure authentication configuration.
type AzureAuthConfig struct {
	// TenantID is the Azure AD tenant ID.
	TenantID string

	// ClientID is the Azure AD application (client) ID.
	ClientID string

	// ClientSecret is the Azure AD application secret.
	ClientSecret string

	// SubscriptionID is the Azure subscription ID.
	SubscriptionID string

	// UseManagedIdentity uses Azure managed identity for authentication.
	UseManagedIdentity bool
}

// ProviderType implements AuthConfig.
func (c AzureAuthConfig) ProviderType() ProviderType { return ProviderAzure }

// IsConfigured implements AuthConfig.
func (c AzureAuthConfig) IsConfigured() bool {
	return c.UseManagedIdentity || (c.TenantID != "" && c.ClientID != "" && c.ClientSecret != "")
}

// GCPAuthConfig contains GCP authentication configuration.
type GCPAuthConfig struct {
	// ProjectID is the GCP project ID.
	ProjectID string

	// ServiceAccountKey is the JSON key for the service account.
	ServiceAccountKey string

	// UseDefaultCredentials uses application default credentials.
	UseDefaultCredentials bool

	// ImpersonateServiceAccount is the service account to impersonate.
	ImpersonateServiceAccount string
}

// ProviderType implements AuthConfig.
func (c GCPAuthConfig) ProviderType() ProviderType { return ProviderGCP }

// IsConfigured implements AuthConfig.
func (c GCPAuthConfig) IsConfigured() bool {
	return c.UseDefaultCredentials || c.ServiceAccountKey != "" || c.ImpersonateServiceAccount != ""
}

// OktaAuthConfig contains Okta authentication configuration.
type OktaAuthConfig struct {
	// Domain is the Okta domain (e.g., dev-123456.okta.com).
	Domain string

	// APIToken is the Okta API token.
	APIToken string

	// ClientID is the OAuth 2.0 client ID (for OAuth authentication).
	ClientID string

	// PrivateKey is the private key for OAuth authentication.
	PrivateKey string
}

// ProviderType implements AuthConfig.
func (c OktaAuthConfig) ProviderType() ProviderType { return ProviderOkta }

// IsConfigured implements AuthConfig.
func (c OktaAuthConfig) IsConfigured() bool {
	return c.Domain != "" && (c.APIToken != "" || (c.ClientID != "" && c.PrivateKey != ""))
}

// Verifier is the interface for permission verifiers.
// Each cloud/identity provider implements this interface.
type Verifier interface {
	// Verify checks if a permission is granted.
	Verify(ctx context.Context, permission Permission, config ProviderConfig) Result

	// ProviderType returns the provider type this verifier handles.
	ProviderType() ProviderType

	// Close releases any resources held by the verifier.
	Close() error
}

// VerifierFactory is a function that creates a new Verifier instance.
type VerifierFactory func(ctx context.Context, logger *zap.Logger, authConfig AuthConfig) (Verifier, error)

// Registry manages verifier factories and instances.
// It allows registration of new verifier types and creation of verifier instances.
type Registry struct {
	mu        sync.RWMutex
	factories map[ProviderType]VerifierFactory
	verifiers map[ProviderType]Verifier
	logger    *zap.Logger
}

// NewRegistry creates a new verifier registry.
func NewRegistry(logger *zap.Logger) *Registry {
	return &Registry{
		factories: make(map[ProviderType]VerifierFactory),
		verifiers: make(map[ProviderType]Verifier),
		logger:    logger,
	}
}

// RegisterFactory registers a verifier factory for a provider type.
func (r *Registry) RegisterFactory(providerType ProviderType, factory VerifierFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[providerType] = factory
	r.logger.Debug("Registered verifier factory", zap.String("provider", string(providerType)))
}

// InitializeVerifier creates and stores a verifier for the given provider type and auth config.
func (r *Registry) InitializeVerifier(ctx context.Context, authConfig AuthConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	providerType := authConfig.ProviderType()
	factory, ok := r.factories[providerType]
	if !ok {
		return fmt.Errorf("no factory registered for provider type: %s", providerType)
	}

	verifier, err := factory(ctx, r.logger, authConfig)
	if err != nil {
		return fmt.Errorf("failed to create verifier for %s: %w", providerType, err)
	}

	r.verifiers[providerType] = verifier
	r.logger.Info("Initialized verifier", zap.String("provider", string(providerType)))
	return nil
}

// GetVerifier returns the verifier for a provider type, or nil if not initialized.
func (r *Registry) GetVerifier(providerType ProviderType) Verifier {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.verifiers[providerType]
}

// HasVerifier returns true if a verifier is initialized for the provider type.
func (r *Registry) HasVerifier(providerType ProviderType) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.verifiers[providerType]
	return ok
}

// Close closes all initialized verifiers.
func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var errs []error
	for providerType, verifier := range r.verifiers {
		if err := verifier.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close %s verifier: %w", providerType, err))
		}
	}
	r.verifiers = make(map[ProviderType]Verifier)

	if len(errs) > 0 {
		return fmt.Errorf("errors closing verifiers: %v", errs)
	}
	return nil
}

// RegisteredProviders returns the list of provider types with registered factories.
func (r *Registry) RegisteredProviders() []ProviderType {
	r.mu.RLock()
	defer r.mu.RUnlock()

	providers := make([]ProviderType, 0, len(r.factories))
	for p := range r.factories {
		providers = append(providers, p)
	}
	return providers
}

// InitializedProviders returns the list of provider types with initialized verifiers.
func (r *Registry) InitializedProviders() []ProviderType {
	r.mu.RLock()
	defer r.mu.RUnlock()

	providers := make([]ProviderType, 0, len(r.verifiers))
	for p := range r.verifiers {
		providers = append(providers, p)
	}
	return providers
}
