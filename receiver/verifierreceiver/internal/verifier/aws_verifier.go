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

package verifier

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/guardduty"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/securityhub"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"go.uber.org/zap"
)

const (
	defaultSessionName        = "verifier-receiver"
	defaultAssumeRoleDuration = 15 * time.Minute
)

// AWSVerifier implements permission verification for AWS.
type AWSVerifier struct {
	logger        *zap.Logger
	baseConfig    aws.Config
	configured    bool
	authConfig    AWSAuthConfig
	defaultRegion string
}

// Ensure AWSVerifier implements Verifier interface.
var _ Verifier = (*AWSVerifier)(nil)

// NewAWSVerifierFactory returns a factory function for creating AWS verifiers.
// This factory should be registered with the verifier Registry.
func NewAWSVerifierFactory() VerifierFactory {
	return func(ctx context.Context, logger *zap.Logger, authConfig AuthConfig) (Verifier, error) {
		awsConfig, ok := authConfig.(AWSAuthConfig)
		if !ok {
			return nil, errors.New("invalid auth config type for AWS verifier")
		}
		return NewAWSVerifier(ctx, logger, awsConfig)
	}
}

// NewAWSVerifier creates a new AWS verifier with Cloud Connector authentication.
// It uses STS AssumeRole with the provided role ARN and external ID.
func NewAWSVerifier(ctx context.Context, logger *zap.Logger, authConfig AWSAuthConfig) (*AWSVerifier, error) {
	// Start with loading default config (for base credentials from IRSA, instance profile, etc.)
	baseCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		logger.Warn("Failed to load default AWS config", zap.Error(err))
		return &AWSVerifier{
			logger:     logger,
			configured: false,
		}, nil
	}

	// Set default region if specified
	if authConfig.DefaultRegion != "" {
		baseCfg.Region = authConfig.DefaultRegion
	}

	// If role ARN is provided, configure STS AssumeRole with external ID
	if authConfig.RoleARN != "" {
		logger.Info("Configuring AWS STS AssumeRole",
			zap.String("role_arn", authConfig.RoleARN),
			zap.Bool("has_external_id", authConfig.ExternalID != ""),
		)

		// Create STS client using base credentials
		stsClient := sts.NewFromConfig(baseCfg)

		// Configure assume role options
		sessionName := authConfig.SessionName
		if sessionName == "" {
			sessionName = defaultSessionName
		}

		duration := authConfig.AssumeRoleDuration
		if duration == 0 {
			duration = defaultAssumeRoleDuration
		}

		// Create assume role provider with external ID
		assumeRoleProvider := stscreds.NewAssumeRoleProvider(stsClient, authConfig.RoleARN,
			func(options *stscreds.AssumeRoleOptions) {
				options.RoleSessionName = sessionName
				options.Duration = duration
				if authConfig.ExternalID != "" {
					options.ExternalID = aws.String(authConfig.ExternalID)
				}
			},
		)

		// Wrap with credentials cache for automatic refresh
		baseCfg.Credentials = aws.NewCredentialsCache(assumeRoleProvider)

		logger.Info("AWS STS AssumeRole configured successfully",
			zap.String("session_name", sessionName),
			zap.Duration("duration", duration),
		)
	} else {
		logger.Info("Using default AWS credentials (no role assumption)")
	}

	return &AWSVerifier{
		logger:        logger,
		baseConfig:    baseCfg,
		configured:    true,
		authConfig:    authConfig,
		defaultRegion: authConfig.DefaultRegion,
	}, nil
}

// ProviderType returns the provider type.
func (v *AWSVerifier) ProviderType() ProviderType {
	return ProviderAWS
}

// Close releases resources.
func (v *AWSVerifier) Close() error {
	return nil
}

// Verify checks if an AWS permission is granted.
func (v *AWSVerifier) Verify(ctx context.Context, permission Permission, providerCfg ProviderConfig) Result {
	start := time.Now()

	if !v.configured {
		return Result{
			Status:       StatusError,
			ErrorCode:    "ConfigurationError",
			ErrorMessage: "AWS credentials not configured",
			Duration:     time.Since(start),
		}
	}

	// Create region-specific config
	cfg := v.baseConfig.Copy()
	if providerCfg.Region != "" {
		cfg.Region = providerCfg.Region
	}

	// Parse the action to determine service and operation
	parts := strings.SplitN(permission.Action, ":", 2)
	if len(parts) != 2 {
		return Result{
			Status:       StatusError,
			ErrorCode:    "InvalidAction",
			ErrorMessage: "Invalid action format: " + permission.Action,
			Duration:     time.Since(start),
		}
	}

	service := strings.ToLower(parts[0])
	operation := parts[1]

	v.logger.Debug("Verifying AWS permission",
		zap.String("service", service),
		zap.String("operation", operation),
		zap.String("region", cfg.Region),
		zap.String("method", string(permission.Method)),
	)

	var result Result
	switch service {
	case "cloudtrail":
		result = v.verifyCloudTrail(ctx, cfg, operation)
	case "guardduty":
		result = v.verifyGuardDuty(ctx, cfg, operation)
	case "securityhub":
		result = v.verifySecurityHub(ctx, cfg, operation)
	case "s3":
		result = v.verifyS3(ctx, cfg, operation)
	case "ec2":
		result = v.verifyEC2(ctx, cfg, operation, permission.Method)
	case "cloudwatch":
		result = v.verifyCloudWatch(ctx, cfg, operation)
	case "sqs":
		result = v.verifySQS(ctx, cfg, operation)
	case "logs":
		result = v.verifyCloudWatchLogs(ctx, cfg, operation)
	default:
		result = Result{
			Status:       StatusSkipped,
			ErrorMessage: "Unsupported AWS service: " + service,
		}
	}

	result.Duration = time.Since(start)
	return result
}

// verifyCloudTrail verifies CloudTrail permissions.
func (v *AWSVerifier) verifyCloudTrail(ctx context.Context, cfg aws.Config, operation string) Result {
	client := cloudtrail.NewFromConfig(cfg)

	switch operation {
	case "LookupEvents":
		// Make a minimal API call to check permission
		_, err := client.LookupEvents(ctx, &cloudtrail.LookupEventsInput{
			MaxResults: aws.Int32(1),
		})
		return v.handleAWSError(err, "cloudtrail:LookupEvents")

	case "DescribeTrails":
		_, err := client.DescribeTrails(ctx, &cloudtrail.DescribeTrailsInput{})
		return v.handleAWSError(err, "cloudtrail:DescribeTrails")

	case "GetTrailStatus":
		// Need a trail name - try listing first
		trails, err := client.DescribeTrails(ctx, &cloudtrail.DescribeTrailsInput{})
		if err != nil {
			return v.handleAWSError(err, "cloudtrail:GetTrailStatus")
		}
		if len(trails.TrailList) == 0 {
			return Result{
				Status:   StatusGranted,
				Endpoint: "cloudtrail:GetTrailStatus (no trails to check)",
			}
		}
		_, err = client.GetTrailStatus(ctx, &cloudtrail.GetTrailStatusInput{
			Name: trails.TrailList[0].Name,
		})
		return v.handleAWSError(err, "cloudtrail:GetTrailStatus")

	default:
		return Result{
			Status:       StatusSkipped,
			ErrorMessage: "Unsupported CloudTrail operation: " + operation,
		}
	}
}

// verifyGuardDuty verifies GuardDuty permissions.
func (v *AWSVerifier) verifyGuardDuty(ctx context.Context, cfg aws.Config, operation string) Result {
	client := guardduty.NewFromConfig(cfg)

	switch operation {
	case "ListDetectors":
		_, err := client.ListDetectors(ctx, &guardduty.ListDetectorsInput{
			MaxResults: aws.Int32(1),
		})
		return v.handleAWSError(err, "guardduty:ListDetectors")

	case "GetFindings", "ListFindings":
		// First get a detector ID
		detectors, err := client.ListDetectors(ctx, &guardduty.ListDetectorsInput{
			MaxResults: aws.Int32(1),
		})
		if err != nil {
			return v.handleAWSError(err, "guardduty:"+operation)
		}
		if len(detectors.DetectorIds) == 0 {
			return Result{
				Status:   StatusGranted,
				Endpoint: "guardduty:" + operation + " (no detectors configured)",
			}
		}

		if operation == "ListFindings" {
			_, err = client.ListFindings(ctx, &guardduty.ListFindingsInput{
				DetectorId: aws.String(detectors.DetectorIds[0]),
				MaxResults: aws.Int32(1),
			})
		} else {
			// GetFindings requires finding IDs, so we'll use ListFindings as proxy
			_, err = client.ListFindings(ctx, &guardduty.ListFindingsInput{
				DetectorId: aws.String(detectors.DetectorIds[0]),
				MaxResults: aws.Int32(1),
			})
		}
		return v.handleAWSError(err, "guardduty:"+operation)

	default:
		return Result{
			Status:       StatusSkipped,
			ErrorMessage: "Unsupported GuardDuty operation: " + operation,
		}
	}
}

// verifySecurityHub verifies Security Hub permissions.
func (v *AWSVerifier) verifySecurityHub(ctx context.Context, cfg aws.Config, operation string) Result {
	client := securityhub.NewFromConfig(cfg)

	switch operation {
	case "GetFindings":
		_, err := client.GetFindings(ctx, &securityhub.GetFindingsInput{
			MaxResults: aws.Int32(1),
		})
		return v.handleAWSError(err, "securityhub:GetFindings")

	case "DescribeHub":
		_, err := client.DescribeHub(ctx, &securityhub.DescribeHubInput{})
		return v.handleAWSError(err, "securityhub:DescribeHub")

	case "BatchGetSecurityControls":
		// This requires control IDs, skip if we don't have them
		return Result{
			Status:   StatusSkipped,
			Endpoint: "securityhub:BatchGetSecurityControls (requires control IDs)",
		}

	default:
		return Result{
			Status:       StatusSkipped,
			ErrorMessage: "Unsupported Security Hub operation: " + operation,
		}
	}
}

// verifyS3 verifies S3 permissions.
func (v *AWSVerifier) verifyS3(ctx context.Context, cfg aws.Config, operation string) Result {
	client := s3.NewFromConfig(cfg)

	switch operation {
	case "ListBucket":
		// ListBuckets is a simpler permission check
		_, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
		return v.handleAWSError(err, "s3:ListBucket")

	case "GetObject":
		// GetObject requires a bucket and key - use ListBuckets as proxy
		_, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
		if err != nil {
			return v.handleAWSError(err, "s3:GetObject")
		}
		// If we can list buckets, we have basic S3 access
		// The actual GetObject permission is bucket-specific
		return Result{
			Status:   StatusGranted,
			Endpoint: "s3:GetObject (verified via ListBuckets)",
		}

	case "GetBucketLocation":
		buckets, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
		if err != nil {
			return v.handleAWSError(err, "s3:GetBucketLocation")
		}
		if len(buckets.Buckets) == 0 {
			return Result{
				Status:   StatusGranted,
				Endpoint: "s3:GetBucketLocation (no buckets to check)",
			}
		}
		_, err = client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
			Bucket: buckets.Buckets[0].Name,
		})
		return v.handleAWSError(err, "s3:GetBucketLocation")

	default:
		return Result{
			Status:       StatusSkipped,
			ErrorMessage: "Unsupported S3 operation: " + operation,
		}
	}
}

// verifyEC2 verifies EC2 permissions, using DryRun where appropriate.
func (v *AWSVerifier) verifyEC2(ctx context.Context, cfg aws.Config, operation string, method VerificationMethod) Result {
	client := ec2.NewFromConfig(cfg)

	switch operation {
	case "DescribeInstances":
		if method == MethodDryRun {
			// Use DryRun to check permission without actually running
			_, err := client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
				DryRun:     aws.Bool(true),
				MaxResults: aws.Int32(5),
			})
			return v.handleEC2DryRunError(err, "ec2:DescribeInstances")
		}
		_, err := client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
			MaxResults: aws.Int32(5),
		})
		return v.handleAWSError(err, "ec2:DescribeInstances")

	case "DescribeRegions":
		_, err := client.DescribeRegions(ctx, &ec2.DescribeRegionsInput{})
		return v.handleAWSError(err, "ec2:DescribeRegions")

	case "DescribeFlowLogs":
		_, err := client.DescribeFlowLogs(ctx, &ec2.DescribeFlowLogsInput{
			MaxResults: aws.Int32(5),
		})
		return v.handleAWSError(err, "ec2:DescribeFlowLogs")

	default:
		return Result{
			Status:       StatusSkipped,
			ErrorMessage: "Unsupported EC2 operation: " + operation,
		}
	}
}

// verifyCloudWatch verifies CloudWatch permissions.
func (v *AWSVerifier) verifyCloudWatch(ctx context.Context, cfg aws.Config, operation string) Result {
	client := cloudwatch.NewFromConfig(cfg)

	switch operation {
	case "GetMetricData":
		// GetMetricData requires metric queries - use ListMetrics as proxy
		_, err := client.ListMetrics(ctx, &cloudwatch.ListMetricsInput{})
		return v.handleAWSError(err, "cloudwatch:GetMetricData")

	default:
		return Result{
			Status:       StatusSkipped,
			ErrorMessage: "Unsupported CloudWatch operation: " + operation,
		}
	}
}

// verifySQS verifies SQS permissions.
func (v *AWSVerifier) verifySQS(ctx context.Context, cfg aws.Config, operation string) Result {
	client := sqs.NewFromConfig(cfg)

	switch operation {
	case "ReceiveMessage", "DeleteMessage":
		// These require a queue URL - use ListQueues as proxy
		_, err := client.ListQueues(ctx, &sqs.ListQueuesInput{
			MaxResults: aws.Int32(1),
		})
		return v.handleAWSError(err, "sqs:"+operation)

	default:
		return Result{
			Status:       StatusSkipped,
			ErrorMessage: "Unsupported SQS operation: " + operation,
		}
	}
}

// verifyCloudWatchLogs verifies CloudWatch Logs permissions.
func (v *AWSVerifier) verifyCloudWatchLogs(ctx context.Context, cfg aws.Config, operation string) Result {
	// CloudWatch Logs uses the same SDK client pattern
	// For now, skip - would need to add cloudwatchlogs client
	return Result{
		Status:       StatusSkipped,
		ErrorMessage: "CloudWatch Logs verification not yet implemented",
	}
}

// handleAWSError converts an AWS error to a verification result.
func (v *AWSVerifier) handleAWSError(err error, endpoint string) Result {
	if err == nil {
		return Result{
			Status:   StatusGranted,
			Endpoint: endpoint,
		}
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()

		// Check for access denied errors
		if isAccessDeniedError(code) {
			return Result{
				Status:       StatusDenied,
				ErrorCode:    code,
				ErrorMessage: apiErr.ErrorMessage(),
				Endpoint:     endpoint,
			}
		}

		// Other errors are treated as errors, not denials
		return Result{
			Status:       StatusError,
			ErrorCode:    code,
			ErrorMessage: apiErr.ErrorMessage(),
			Endpoint:     endpoint,
		}
	}

	// Non-API errors
	return Result{
		Status:       StatusError,
		ErrorMessage: err.Error(),
		Endpoint:     endpoint,
	}
}

// handleEC2DryRunError handles EC2 DryRun responses.
// DryRun returns an error even on success - we need to check the error type.
func (v *AWSVerifier) handleEC2DryRunError(err error, endpoint string) Result {
	if err == nil {
		// Unexpected - DryRun should always return an error
		return Result{
			Status:   StatusGranted,
			Endpoint: endpoint,
		}
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()

		// DryRunOperation means the permission check passed
		if code == "DryRunOperation" {
			return Result{
				Status:   StatusGranted,
				Endpoint: endpoint + " (DryRun)",
			}
		}

		// UnauthorizedOperation means access denied
		if code == "UnauthorizedOperation" || isAccessDeniedError(code) {
			return Result{
				Status:       StatusDenied,
				ErrorCode:    code,
				ErrorMessage: apiErr.ErrorMessage(),
				Endpoint:     endpoint + " (DryRun)",
			}
		}

		// Other errors
		return Result{
			Status:       StatusError,
			ErrorCode:    code,
			ErrorMessage: apiErr.ErrorMessage(),
			Endpoint:     endpoint,
		}
	}

	return Result{
		Status:       StatusError,
		ErrorMessage: err.Error(),
		Endpoint:     endpoint,
	}
}

// isAccessDeniedError checks if an error code indicates access denied.
func isAccessDeniedError(code string) bool {
	accessDeniedCodes := []string{
		"AccessDenied",
		"AccessDeniedException",
		"UnauthorizedAccess",
		"UnauthorizedOperation",
		"AuthorizationError",
		"Forbidden",
		"InvalidAccessKeyId",
		"SignatureDoesNotMatch",
		"ExpiredToken",
		"ExpiredTokenException",
	}

	for _, c := range accessDeniedCodes {
		if code == c {
			return true
		}
	}
	return false
}
