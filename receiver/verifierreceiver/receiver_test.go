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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/elastic/opentelemetry-collector-components/receiver/verifierreceiver/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/receiver/verifierreceiver/internal/verifier"
)

func TestReceiver_StartShutdown(t *testing.T) {
	config := &Config{
		CloudConnectorID:   "cc-12345",
		CloudConnectorName: "Test Connector",
		VerificationID:     "verify-test-001",
		VerificationType:   "on_demand",
		Providers: ProvidersConfig{
			AWS: AWSProviderConfig{
				Credentials: AWSCredentials{
					RoleARN:       "arn:aws:iam::123456789012:role/ElasticAgentRole",
					ExternalID:    "elastic-test-external-id",
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
						Config: map[string]interface{}{
							"account_id": "123456789012",
							"region":     "us-east-1",
						},
					},
				},
			},
		},
	}

	consumer := &consumertest.LogsSink{}
	receiver := newVerifierReceiver(
		receivertest.NewNopSettings(metadata.Type),
		config,
		consumer,
	)

	ctx := context.Background()

	// Start the receiver
	err := receiver.Start(ctx, nil)
	require.NoError(t, err)

	// Give it time to run the verification
	time.Sleep(100 * time.Millisecond)

	// Shutdown the receiver
	err = receiver.Shutdown(ctx)
	require.NoError(t, err)

	// Verify logs were emitted
	logs := consumer.AllLogs()
	require.NotEmpty(t, logs, "expected logs to be emitted")

	// Check the first log batch
	firstLog := logs[0]
	require.Equal(t, 1, firstLog.ResourceLogs().Len())

	resourceLog := firstLog.ResourceLogs().At(0)

	// Verify resource attributes per RFC specification
	attrs := resourceLog.Resource().Attributes()
	serviceName, ok := attrs.Get("service.name")
	require.True(t, ok)
	assert.Equal(t, "permission-verifier", serviceName.Str())

	cloudConnectorID, ok := attrs.Get("cloud_connector.id")
	require.True(t, ok)
	assert.Equal(t, "cc-12345", cloudConnectorID.Str())

	verificationID, ok := attrs.Get("verification.id")
	require.True(t, ok)
	assert.Equal(t, "verify-test-001", verificationID.Str())

	// Verify log records
	scopeLogs := resourceLog.ScopeLogs()
	require.Equal(t, 1, scopeLogs.Len())

	// Check scope name per RFC
	assert.Equal(t, "elastic.permission_verification", scopeLogs.At(0).Scope().Name())

	logRecords := scopeLogs.At(0).LogRecords()
	assert.GreaterOrEqual(t, logRecords.Len(), 1, "expected log records for permissions")

	// Verify first log record attributes
	record := logRecords.At(0)

	// Policy context
	policyID, ok := record.Attributes().Get("policy.id")
	require.True(t, ok)
	assert.Equal(t, "policy-1", policyID.Str())

	// Integration context
	integrationType, ok := record.Attributes().Get("integration.type")
	require.True(t, ok)
	assert.Equal(t, "aws_cloudtrail", integrationType.Str())

	// Provider context
	providerType, ok := record.Attributes().Get("provider.type")
	require.True(t, ok)
	assert.Equal(t, "aws", providerType.Str())

	// Permission status
	status, ok := record.Attributes().Get("permission.status")
	require.True(t, ok)
	assert.Contains(t, []string{"pending", "granted", "denied", "error", "skipped"}, status.Str())
}

func TestReceiver_WithoutAWSCredentials(t *testing.T) {
	config := &Config{
		CloudConnectorID: "cc-12345",
		VerificationID:   "verify-test-002",
		// No provider credentials configured
		Policies: []PolicyConfig{
			{
				PolicyID: "policy-1",
				Integrations: []IntegrationConfig{
					{
						IntegrationType: "aws_cloudtrail",
						IntegrationName: "CloudTrail",
					},
				},
			},
		},
	}

	consumer := &consumertest.LogsSink{}
	receiver := newVerifierReceiver(
		receivertest.NewNopSettings(metadata.Type),
		config,
		consumer,
	)

	ctx := context.Background()

	err := receiver.Start(ctx, nil)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = receiver.Shutdown(ctx)
	require.NoError(t, err)

	// Should still emit logs but with error status
	logs := consumer.AllLogs()
	require.NotEmpty(t, logs)

	logRecords := logs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	assert.GreaterOrEqual(t, logRecords.Len(), 1)

	// First record should have error status since no credentials
	record := logRecords.At(0)
	status, ok := record.Attributes().Get("permission.status")
	require.True(t, ok)
	assert.Equal(t, "error", status.Str())

	errorCode, ok := record.Attributes().Get("permission.error_code")
	require.True(t, ok)
	assert.Equal(t, "VerifierNotInitialized", errorCode.Str())
}

func TestReceiver_UnsupportedIntegration(t *testing.T) {
	config := &Config{
		CloudConnectorID: "cc-12345",
		VerificationID:   "verify-test-003",
		Policies: []PolicyConfig{
			{
				PolicyID: "policy-1",
				Integrations: []IntegrationConfig{
					{
						IntegrationType: "unknown_integration",
						IntegrationName: "Unknown Integration",
					},
				},
			},
		},
	}

	consumer := &consumertest.LogsSink{}
	receiver := newVerifierReceiver(
		receivertest.NewNopSettings(metadata.Type),
		config,
		consumer,
	)

	ctx := context.Background()

	err := receiver.Start(ctx, nil)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = receiver.Shutdown(ctx)
	require.NoError(t, err)

	logs := consumer.AllLogs()
	require.NotEmpty(t, logs)

	logRecords := logs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	require.Equal(t, 1, logRecords.Len())

	record := logRecords.At(0)
	assert.Equal(t, "WARN", record.SeverityText())
	assert.Contains(t, record.Body().Str(), "Unsupported integration type")

	status, ok := record.Attributes().Get("permission.status")
	require.True(t, ok)
	assert.Equal(t, "skipped", status.Str())
}

func TestReceiver_MultipleIntegrations(t *testing.T) {
	config := &Config{
		CloudConnectorID: "cc-12345",
		VerificationID:   "verify-test-004",
		Providers: ProvidersConfig{
			AWS: AWSProviderConfig{
				Credentials: AWSCredentials{
					RoleARN:       "arn:aws:iam::123456789012:role/ElasticAgentRole",
					ExternalID:    "elastic-test-external-id",
					DefaultRegion: "us-east-1",
				},
			},
		},
		Policies: []PolicyConfig{
			{
				PolicyID:   "policy-1",
				PolicyName: "AWS Security",
				Integrations: []IntegrationConfig{
					{IntegrationType: "aws_cloudtrail"},
					{IntegrationType: "aws_guardduty"},
				},
			},
			{
				PolicyID:   "policy-2",
				PolicyName: "AWS Storage",
				Integrations: []IntegrationConfig{
					{IntegrationType: "aws_s3"},
				},
			},
		},
	}

	consumer := &consumertest.LogsSink{}
	receiver := newVerifierReceiver(
		receivertest.NewNopSettings(metadata.Type),
		config,
		consumer,
	)

	ctx := context.Background()

	err := receiver.Start(ctx, nil)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = receiver.Shutdown(ctx)
	require.NoError(t, err)

	logs := consumer.AllLogs()
	require.NotEmpty(t, logs)

	logRecords := logs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	assert.GreaterOrEqual(t, logRecords.Len(), 10, "expected log records for all integration permissions")

	// Collect unique policy IDs and integration types
	policyIDs := make(map[string]bool)
	integrationTypes := make(map[string]bool)

	for i := 0; i < logRecords.Len(); i++ {
		record := logRecords.At(i)
		if policyID, ok := record.Attributes().Get("policy.id"); ok {
			policyIDs[policyID.Str()] = true
		}
		if intType, ok := record.Attributes().Get("integration.type"); ok {
			integrationTypes[intType.Str()] = true
		}
	}

	assert.True(t, policyIDs["policy-1"])
	assert.True(t, policyIDs["policy-2"])
	assert.True(t, integrationTypes["aws_cloudtrail"])
	assert.True(t, integrationTypes["aws_guardduty"])
	assert.True(t, integrationTypes["aws_s3"])
}

func TestPermissionRegistry(t *testing.T) {
	registry := NewPermissionRegistry()

	t.Run("supported integration - no version (latest)", func(t *testing.T) {
		perms := registry.GetPermissions("aws_cloudtrail", "")
		require.NotNil(t, perms)
		assert.Equal(t, verifier.ProviderAWS, perms.Provider)
		assert.NotEmpty(t, perms.Permissions)

		actionFound := false
		for _, p := range perms.Permissions {
			if p.Action == "cloudtrail:LookupEvents" {
				actionFound = true
				assert.True(t, p.Required)
				assert.Equal(t, MethodAPICall, p.Method)
				break
			}
		}
		assert.True(t, actionFound, "expected cloudtrail:LookupEvents permission")
	})

	t.Run("unsupported integration", func(t *testing.T) {
		perms := registry.GetPermissions("unknown_integration", "")
		assert.Nil(t, perms)
		assert.False(t, registry.IsSupported("unknown_integration"))
	})

	t.Run("all AWS integrations registered", func(t *testing.T) {
		awsIntegrations := []string{
			"aws_cloudtrail",
			"aws_guardduty",
			"aws_securityhub",
			"aws_s3",
			"aws_ec2",
			"aws_vpcflow",
			"aws_waf",
			"aws_route53",
			"aws_elb",
			"aws_cloudfront",
		}

		for _, integration := range awsIntegrations {
			assert.True(t, registry.IsSupported(integration), "expected %s to be supported", integration)
			perms := registry.GetPermissions(integration, "")
			require.NotNil(t, perms, "expected permissions for %s", integration)
			assert.Equal(t, verifier.ProviderAWS, perms.Provider, "expected AWS provider for %s", integration)
		}
	})

	t.Run("Azure integrations registered", func(t *testing.T) {
		azureIntegrations := []string{
			"azure_activitylogs",
			"azure_auditlogs",
			"azure_blob_storage",
		}

		for _, integration := range azureIntegrations {
			assert.True(t, registry.IsSupported(integration), "expected %s to be supported", integration)
			perms := registry.GetPermissions(integration, "")
			require.NotNil(t, perms, "expected permissions for %s", integration)
			assert.Equal(t, verifier.ProviderAzure, perms.Provider, "expected Azure provider for %s", integration)
		}
	})

	t.Run("GCP integrations registered", func(t *testing.T) {
		gcpIntegrations := []string{
			"gcp_audit",
			"gcp_storage",
			"gcp_pubsub",
		}

		for _, integration := range gcpIntegrations {
			assert.True(t, registry.IsSupported(integration), "expected %s to be supported", integration)
			perms := registry.GetPermissions(integration, "")
			require.NotNil(t, perms, "expected permissions for %s", integration)
			assert.Equal(t, verifier.ProviderGCP, perms.Provider, "expected GCP provider for %s", integration)
		}
	})

	t.Run("Okta integrations registered", func(t *testing.T) {
		oktaIntegrations := []string{
			"okta_system",
			"okta_users",
		}

		for _, integration := range oktaIntegrations {
			assert.True(t, registry.IsSupported(integration), "expected %s to be supported", integration)
			perms := registry.GetPermissions(integration, "")
			require.NotNil(t, perms, "expected permissions for %s", integration)
			assert.Equal(t, verifier.ProviderOkta, perms.Provider, "expected Okta provider for %s", integration)
		}
	})

	t.Run("supported integrations by provider", func(t *testing.T) {
		byProvider := registry.SupportedIntegrationsByProvider()
		assert.NotEmpty(t, byProvider[verifier.ProviderAWS])
		assert.NotEmpty(t, byProvider[verifier.ProviderAzure])
		assert.NotEmpty(t, byProvider[verifier.ProviderGCP])
		assert.NotEmpty(t, byProvider[verifier.ProviderOkta])
	})

	// Version-aware permission lookup tests
	t.Run("cloudtrail v2 - SQS permissions required", func(t *testing.T) {
		perms := registry.GetPermissions("aws_cloudtrail", "2.17.0")
		require.NotNil(t, perms)
		assert.Equal(t, verifier.ProviderAWS, perms.Provider)

		// In v2+, sqs:ReceiveMessage and sqs:DeleteMessage should be required
		for _, p := range perms.Permissions {
			if p.Action == "sqs:ReceiveMessage" {
				assert.True(t, p.Required, "sqs:ReceiveMessage should be required in v2+")
			}
			if p.Action == "sqs:DeleteMessage" {
				assert.True(t, p.Required, "sqs:DeleteMessage should be required in v2+")
			}
		}
	})

	t.Run("cloudtrail v1 - SQS permissions optional", func(t *testing.T) {
		perms := registry.GetPermissions("aws_cloudtrail", "1.5.0")
		require.NotNil(t, perms)
		assert.Equal(t, verifier.ProviderAWS, perms.Provider)

		// In v1.x, sqs:ReceiveMessage and sqs:DeleteMessage should be optional
		for _, p := range perms.Permissions {
			if p.Action == "sqs:ReceiveMessage" {
				assert.False(t, p.Required, "sqs:ReceiveMessage should be optional in v1.x")
			}
			if p.Action == "sqs:DeleteMessage" {
				assert.False(t, p.Required, "sqs:DeleteMessage should be optional in v1.x")
			}
		}
	})

	t.Run("cloudtrail no version - defaults to latest (v2+)", func(t *testing.T) {
		perms := registry.GetPermissions("aws_cloudtrail", "")
		require.NotNil(t, perms)

		// Should get v2+ permissions (latest)
		for _, p := range perms.Permissions {
			if p.Action == "sqs:ReceiveMessage" {
				assert.True(t, p.Required, "default (latest) should have sqs:ReceiveMessage required")
			}
		}
	})

	t.Run("cloudtrail invalid version - falls back to latest", func(t *testing.T) {
		perms := registry.GetPermissions("aws_cloudtrail", "not-a-version")
		require.NotNil(t, perms)
		// Should fall back to the first (latest) entry
		for _, p := range perms.Permissions {
			if p.Action == "sqs:ReceiveMessage" {
				assert.True(t, p.Required, "invalid version should fall back to latest")
			}
		}
	})

	t.Run("guardduty with version - matches >=0.0.0", func(t *testing.T) {
		perms := registry.GetPermissions("aws_guardduty", "3.0.0")
		require.NotNil(t, perms)
		assert.Equal(t, verifier.ProviderAWS, perms.Provider)
	})

	t.Run("version constraints are returned", func(t *testing.T) {
		constraints := registry.GetVersionConstraints("aws_cloudtrail")
		require.NotNil(t, constraints)
		assert.Len(t, constraints, 2)
		assert.Equal(t, ">=2.0.0", constraints[0])
		assert.Equal(t, ">=1.0.0,<2.0.0", constraints[1])
	})

	t.Run("version constraints for unknown integration", func(t *testing.T) {
		constraints := registry.GetVersionConstraints("unknown_integration")
		assert.Nil(t, constraints)
	})
}
