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
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/receiver/verifierreceiver/internal/verifier"
)

const (
	scopeName    = "elastic.permission_verification"
	scopeVersion = "1.0.0"
	serviceName  = "permission-verifier"
)

// verifierReceiver implements the receiver.Logs interface.
// It verifies permissions for cloud integrations and reports results as OTEL logs.
// The receiver owns the mapping between integrations and their required permissions.
type verifierReceiver struct {
	params             receiver.Settings
	config             *Config
	consumer           consumer.Logs
	logger             *zap.Logger
	permissionRegistry *PermissionRegistry

	// Verifier registry manages verifiers for all cloud/identity providers
	verifierRegistry *verifier.Registry

	cancelFn context.CancelFunc
	wg       sync.WaitGroup

	// done is closed when verification completes (used for testing)
	done chan struct{}
}

// newVerifierReceiver creates a new verifier receiver.
func newVerifierReceiver(
	params receiver.Settings,
	config *Config,
	consumer consumer.Logs,
) *verifierReceiver {
	// Create verifier registry and register available factories
	verifierRegistry := verifier.NewRegistry(params.Logger)

	// Register AWS verifier factory (always available)
	verifierRegistry.RegisterFactory(verifier.ProviderAWS, verifier.NewAWSVerifierFactory())

	// Future: Register other provider factories here
	// verifierRegistry.RegisterFactory(verifier.ProviderAzure, verifier.NewAzureVerifierFactory())
	// verifierRegistry.RegisterFactory(verifier.ProviderGCP, verifier.NewGCPVerifierFactory())
	// verifierRegistry.RegisterFactory(verifier.ProviderOkta, verifier.NewOktaVerifierFactory())

	return &verifierReceiver{
		params:             params,
		config:             config,
		consumer:           consumer,
		logger:             params.Logger,
		permissionRegistry: NewPermissionRegistry(),
		verifierRegistry:   verifierRegistry,
		done:               make(chan struct{}),
	}
}

// Start begins the permission verification process.
func (r *verifierReceiver) Start(ctx context.Context, _ component.Host) error {
	r.logger.Info("Starting verifier receiver",
		zap.String("cloud_connector_id", r.config.CloudConnectorID),
		zap.String("verification_id", r.config.VerificationID),
		zap.Int("policy_count", len(r.config.Policies)),
	)

	// Initialize verifiers for configured providers
	r.initializeVerifiers(ctx)

	// Use context.Background() as parent because the Start() ctx is startup-scoped
	// and may be cancelled after Start returns, which would abort verification.
	startCtx, cancelFn := context.WithCancel(context.Background())
	r.cancelFn = cancelFn

	// Run verification
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.runVerification(startCtx)
	}()

	return nil
}

// initializeVerifiers initializes verifiers for all configured providers.
func (r *verifierReceiver) initializeVerifiers(ctx context.Context) {
	// Initialize AWS verifier if configured
	if r.config.Providers.AWS.Credentials.IsConfigured() {
		creds := r.config.Providers.AWS.Credentials
		if creds.UseDefaultCredentials {
			r.logger.Info("Initializing AWS verifier with default credentials (AWS_PROFILE or environment)",
				zap.String("default_region", creds.DefaultRegion),
			)
		} else {
			r.logger.Info("Initializing AWS verifier with Cloud Connector authentication",
				zap.String("role_arn", creds.RoleARN),
				zap.Bool("has_external_id", creds.ExternalID != ""),
				zap.String("default_region", creds.DefaultRegion),
			)
		}

		if err := r.verifierRegistry.InitializeVerifier(ctx, creds.ToAuthConfig()); err != nil {
			r.logger.Warn("Failed to initialize AWS verifier", zap.Error(err))
		} else {
			r.logger.Info("AWS verifier initialized successfully")
		}
	} else {
		r.logger.Debug("AWS credentials not configured")
	}

	// Initialize Azure verifier if configured
	if r.config.Providers.Azure.Credentials.IsConfigured() {
		r.logger.Info("Initializing Azure verifier",
			zap.String("tenant_id", r.config.Providers.Azure.Credentials.TenantID),
			zap.Bool("use_managed_identity", r.config.Providers.Azure.Credentials.UseManagedIdentity),
		)

		if err := r.verifierRegistry.InitializeVerifier(ctx, r.config.Providers.Azure.Credentials.ToAuthConfig()); err != nil {
			r.logger.Warn("Failed to initialize Azure verifier", zap.Error(err))
		} else {
			r.logger.Info("Azure verifier initialized successfully")
		}
	} else {
		r.logger.Debug("Azure credentials not configured")
	}

	// Initialize GCP verifier if configured
	if r.config.Providers.GCP.Credentials.IsConfigured() {
		r.logger.Info("Initializing GCP verifier",
			zap.String("project_id", r.config.Providers.GCP.Credentials.ProjectID),
			zap.Bool("use_default_credentials", r.config.Providers.GCP.Credentials.UseDefaultCredentials),
		)

		if err := r.verifierRegistry.InitializeVerifier(ctx, r.config.Providers.GCP.Credentials.ToAuthConfig()); err != nil {
			r.logger.Warn("Failed to initialize GCP verifier", zap.Error(err))
		} else {
			r.logger.Info("GCP verifier initialized successfully")
		}
	} else {
		r.logger.Debug("GCP credentials not configured")
	}

	// Initialize Okta verifier if configured
	if r.config.Providers.Okta.Credentials.IsConfigured() {
		r.logger.Info("Initializing Okta verifier",
			zap.String("domain", r.config.Providers.Okta.Credentials.Domain),
		)

		if err := r.verifierRegistry.InitializeVerifier(ctx, r.config.Providers.Okta.Credentials.ToAuthConfig()); err != nil {
			r.logger.Warn("Failed to initialize Okta verifier", zap.Error(err))
		} else {
			r.logger.Info("Okta verifier initialized successfully")
		}
	} else {
		r.logger.Debug("Okta credentials not configured")
	}

	// Log summary of initialized verifiers
	initialized := r.verifierRegistry.InitializedProviders()
	if len(initialized) > 0 {
		providers := make([]string, len(initialized))
		for i, p := range initialized {
			providers[i] = string(p)
		}
		r.logger.Info("Verifiers initialized", zap.Strings("providers", providers))
	} else {
		r.logger.Warn("No verifiers initialized - permission verification will be limited")
	}
}

// Shutdown stops the permission verification process.
func (r *verifierReceiver) Shutdown(ctx context.Context) error {
	r.logger.Info("Shutting down verifier receiver")
	if r.cancelFn != nil {
		r.cancelFn()
	}
	r.wg.Wait()

	// Close all verifiers
	if err := r.verifierRegistry.Close(); err != nil {
		r.logger.Warn("Error closing verifiers", zap.Error(err))
	}

	return nil
}

// runVerification runs the permission verification for all configured policies.
func (r *verifierReceiver) runVerification(ctx context.Context) {
	defer close(r.done)
	if err := r.verifyPermissions(ctx); err != nil {
		r.logger.Error("Failed to verify permissions", zap.Error(err))
	}
}

// verifyPermissions performs permission verification for all policies and integrations.
// For each integration, it looks up required permissions from the registry and emits
// OTEL log records with structured results.
func (r *verifierReceiver) verifyPermissions(ctx context.Context) error {
	r.logger.Info("Starting permission verification",
		zap.String("cloud_connector_id", r.config.CloudConnectorID),
		zap.String("verification_id", r.config.VerificationID),
		zap.Int("policy_count", len(r.config.Policies)),
	)

	now := time.Now()
	timestamp := pcommon.NewTimestampFromTime(now)
	verificationTimestamp := now.UTC().Format(time.RFC3339)

	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()

	// Set resource attributes per RFC specification
	resource := resourceLogs.Resource()
	resource.Attributes().PutStr("cloud_connector.id", r.config.CloudConnectorID)
	if r.config.CloudConnectorName != "" {
		resource.Attributes().PutStr("cloud_connector.name", r.config.CloudConnectorName)
	}
	resource.Attributes().PutStr("verification.id", r.config.VerificationID)
	resource.Attributes().PutStr("verification.timestamp", verificationTimestamp)
	verificationType := r.config.VerificationType
	if verificationType == "" {
		verificationType = "on_demand"
	}
	resource.Attributes().PutStr("verification.type", verificationType)
	resource.Attributes().PutStr("service.name", serviceName)
	resource.Attributes().PutStr("service.version", scopeVersion)

	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	scopeLogs.Scope().SetName(scopeName)
	scopeLogs.Scope().SetVersion(scopeVersion)

	// Iterate through all policies and their integrations
	for _, policy := range r.config.Policies {
		r.logger.Debug("Processing policy",
			zap.String("policy_id", policy.PolicyID),
			zap.String("policy_name", policy.PolicyName),
			zap.Int("integration_count", len(policy.Integrations)),
		)

		for _, integration := range policy.Integrations {
			r.logger.Debug("Processing integration",
				zap.String("integration_id", integration.IntegrationID),
				zap.String("integration_type", integration.IntegrationType),
				zap.String("integration_name", integration.IntegrationName),
				zap.String("integration_version", integration.IntegrationVersion),
			)

			// Look up required permissions from registry (version-aware)
			integrationPerms := r.permissionRegistry.GetPermissions(integration.IntegrationType, integration.IntegrationVersion)
			if integrationPerms == nil {
				// Unknown integration type or unsupported version - emit a warning log
				r.emitUnsupportedIntegrationLog(
					scopeLogs,
					timestamp,
					policy,
					integration,
				)
				continue
			}

			// Verify and emit a log record for each permission
			for _, perm := range integrationPerms.Permissions {
				result := r.verifyPermission(ctx, integrationPerms.Provider, perm, integration)
				r.emitPermissionCheckLog(
					scopeLogs,
					timestamp,
					policy,
					integration,
					integrationPerms.Provider,
					perm,
					result,
				)
			}
		}
	}

	// Send logs to the consumer
	if scopeLogs.LogRecords().Len() > 0 {
		if err := r.consumer.ConsumeLogs(ctx, logs); err != nil {
			return fmt.Errorf("failed to consume logs: %w", err)
		}
		r.logger.Info("Permission verification logs emitted",
			zap.Int("log_count", scopeLogs.LogRecords().Len()),
		)
	}

	return nil
}

// verifyPermission verifies a single permission using the appropriate provider verifier.
func (r *verifierReceiver) verifyPermission(
	ctx context.Context,
	provider verifier.ProviderType,
	perm Permission,
	integration IntegrationConfig,
) verifier.Result {
	// Build provider config from integration config
	providerCfg := verifier.ProviderConfig{}

	// AWS-specific config
	if region, ok := integration.Config["region"].(string); ok {
		providerCfg.Region = region
	}
	if accountID, ok := integration.Config["account_id"].(string); ok {
		providerCfg.AccountID = accountID
	}

	// Azure-specific config
	if subscriptionID, ok := integration.Config["subscription_id"].(string); ok {
		providerCfg.SubscriptionID = subscriptionID
	}
	if resourceGroup, ok := integration.Config["resource_group"].(string); ok {
		providerCfg.ResourceGroup = resourceGroup
	}
	if tenantID, ok := integration.Config["tenant_id"].(string); ok {
		providerCfg.TenantID = tenantID
	}

	// GCP-specific config
	if projectID, ok := integration.Config["project_id"].(string); ok {
		providerCfg.ProjectID = projectID
	}

	// Okta-specific config
	if domain, ok := integration.Config["domain"].(string); ok {
		providerCfg.OktaDomain = domain
	}

	// Get the verifier for this provider
	v := r.verifierRegistry.GetVerifier(provider)
	if v == nil {
		return verifier.Result{
			Status:       verifier.StatusError,
			ErrorCode:    "VerifierNotInitialized",
			ErrorMessage: fmt.Sprintf("%s verifier not initialized - credentials not configured", provider),
		}
	}

	return v.Verify(ctx, verifier.Permission{
		Action:   perm.Action,
		Method:   verifier.VerificationMethod(perm.Method),
		Required: perm.Required,
		Category: perm.Category,
	}, providerCfg)
}

// emitPermissionCheckLog emits a log record for a single permission check.
// The log record follows the RFC structure with all required attributes.
func (r *verifierReceiver) emitPermissionCheckLog(
	scopeLogs plog.ScopeLogs,
	timestamp pcommon.Timestamp,
	policy PolicyConfig,
	integration IntegrationConfig,
	provider verifier.ProviderType,
	perm Permission,
	result verifier.Result,
) {
	logRecord := scopeLogs.LogRecords().AppendEmpty()
	logRecord.SetTimestamp(timestamp)
	logRecord.SetObservedTimestamp(timestamp)

	// Determine severity based on verification result
	var severityNumber plog.SeverityNumber
	var severityText string
	var status PermissionStatus

	switch result.Status {
	case verifier.StatusGranted:
		severityNumber = plog.SeverityNumberInfo
		severityText = "INFO"
		status = StatusGranted
	case verifier.StatusDenied:
		if perm.Required {
			severityNumber = plog.SeverityNumberError
			severityText = "ERROR"
		} else {
			severityNumber = plog.SeverityNumberWarn
			severityText = "WARN"
		}
		status = StatusDenied
	case verifier.StatusError:
		severityNumber = plog.SeverityNumberError
		severityText = "ERROR"
		status = StatusError
	case verifier.StatusSkipped:
		severityNumber = plog.SeverityNumberInfo
		severityText = "INFO"
		status = StatusSkipped
	default:
		severityNumber = plog.SeverityNumberInfo
		severityText = "INFO"
		status = StatusPending
	}

	logRecord.SetSeverityNumber(severityNumber)
	logRecord.SetSeverityText(severityText)

	// Set the log body with human-readable summary
	body := fmt.Sprintf("Permission check: %s/%s - %s", provider, perm.Action, status)
	logRecord.Body().SetStr(body)

	// Set log attributes per RFC specification
	attrs := logRecord.Attributes()

	// Policy context
	attrs.PutStr("policy.id", policy.PolicyID)
	if policy.PolicyName != "" {
		attrs.PutStr("policy.name", policy.PolicyName)
	}

	// Integration context
	if integration.IntegrationID != "" {
		attrs.PutStr("integration.id", integration.IntegrationID)
	}
	if integration.IntegrationName != "" {
		attrs.PutStr("integration.name", integration.IntegrationName)
	}
	attrs.PutStr("integration.type", integration.IntegrationType)
	if integration.IntegrationVersion != "" {
		attrs.PutStr("integration.version", integration.IntegrationVersion)
	} else {
		attrs.PutStr("integration.version", "unspecified")
	}

	// Provider context
	attrs.PutStr("provider.type", string(provider))
	if accountID, ok := integration.Config["account_id"].(string); ok && accountID != "" {
		attrs.PutStr("provider.account", accountID)
	}
	if region, ok := integration.Config["region"].(string); ok && region != "" {
		attrs.PutStr("provider.region", region)
	}
	if subscriptionID, ok := integration.Config["subscription_id"].(string); ok && subscriptionID != "" {
		attrs.PutStr("provider.subscription_id", subscriptionID)
	}
	if projectID, ok := integration.Config["project_id"].(string); ok && projectID != "" {
		attrs.PutStr("provider.project_id", projectID)
	}

	// Permission details
	attrs.PutStr("permission.action", perm.Action)
	if perm.Category != "" {
		attrs.PutStr("permission.category", perm.Category)
	}
	attrs.PutStr("permission.status", string(status))
	attrs.PutBool("permission.required", perm.Required)

	// Error details (if any)
	if result.ErrorCode != "" {
		attrs.PutStr("permission.error_code", result.ErrorCode)
	}
	if result.ErrorMessage != "" {
		attrs.PutStr("permission.error_message", result.ErrorMessage)
	}

	// Verification metadata
	attrs.PutStr("verification.method", string(perm.Method))
	if result.Endpoint != "" {
		attrs.PutStr("verification.endpoint", result.Endpoint)
	}
	attrs.PutInt("verification.duration_ms", result.Duration.Milliseconds())

	r.logger.Debug("Emitted permission check log",
		zap.String("policy_id", policy.PolicyID),
		zap.String("integration_type", integration.IntegrationType),
		zap.String("permission", perm.Action),
		zap.String("status", string(status)),
		zap.Duration("duration", result.Duration),
	)
}

// emitUnsupportedIntegrationLog emits a warning log for unsupported integration types.
func (r *verifierReceiver) emitUnsupportedIntegrationLog(
	scopeLogs plog.ScopeLogs,
	timestamp pcommon.Timestamp,
	policy PolicyConfig,
	integration IntegrationConfig,
) {
	logRecord := scopeLogs.LogRecords().AppendEmpty()
	logRecord.SetTimestamp(timestamp)
	logRecord.SetObservedTimestamp(timestamp)
	logRecord.SetSeverityNumber(plog.SeverityNumberWarn)
	logRecord.SetSeverityText("WARN")

	// Distinguish between unsupported type and unsupported version
	var body string
	var errorCode string
	if r.permissionRegistry.IsSupported(integration.IntegrationType) {
		// Type is known but version doesn't match any constraint
		body = fmt.Sprintf("Unsupported integration version: %s@%s - skipping permission verification",
			integration.IntegrationType, integration.IntegrationVersion)
		errorCode = "UnsupportedVersion"
	} else {
		body = fmt.Sprintf("Unsupported integration type: %s - skipping permission verification",
			integration.IntegrationType)
		errorCode = "UnsupportedIntegration"
	}
	logRecord.Body().SetStr(body)

	attrs := logRecord.Attributes()
	attrs.PutStr("policy.id", policy.PolicyID)
	if policy.PolicyName != "" {
		attrs.PutStr("policy.name", policy.PolicyName)
	}
	if integration.IntegrationID != "" {
		attrs.PutStr("integration.id", integration.IntegrationID)
	}
	if integration.IntegrationName != "" {
		attrs.PutStr("integration.name", integration.IntegrationName)
	}
	attrs.PutStr("integration.type", integration.IntegrationType)
	if integration.IntegrationVersion != "" {
		attrs.PutStr("integration.version", integration.IntegrationVersion)
	} else {
		attrs.PutStr("integration.version", "unspecified")
	}
	attrs.PutStr("permission.status", string(StatusSkipped))
	attrs.PutStr("permission.error_code", errorCode)

	r.logger.Warn("Unsupported integration",
		zap.String("integration_type", integration.IntegrationType),
		zap.String("integration_version", integration.IntegrationVersion),
		zap.String("error_code", errorCode),
		zap.String("policy_id", policy.PolicyID),
	)
}
