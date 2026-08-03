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
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/elastic/opentelemetry-collector-components/exporter/awss3exporter/internal/metadata"
)

// NewFactory creates a factory for the multi-tenant AWS S3 exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		TimeoutSettings: exporterhelper.NewDefaultTimeoutConfig(),
		QueueSettings:   configoptional.Default(exporterhelper.NewDefaultQueueConfig()),
		BackOffConfig:   configretry.NewDefaultBackOffConfig(),
		Attributes: AttributesConfig{
			Bucket:   "bucket",
			RoleARN:  "role_arn",
			Region:   "region",
			Format:   "format",
			TenantID: "tenant_id",
			OrgID:    "org_id",
		},
		DefaultFormat: "otlp_json",
		TokenEndpoint: TokenEndpointConfig{
			URL:         defaultTokenURL,
			Audience:    defaultAudience,
			RefreshSkew: defaultRefreshSkew,
			Timeout:     defaultTokenTimeout,
			CertTTL:     defaultCertTTL,
		},
		STS: STSConfig{
			SessionName: defaultSessionName,
		},
		ClientCache: ClientCacheConfig{
			Size: defaultCacheSize,
		},
	}
}

func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	config component.Config,
) (exporter.Logs, error) {
	cfg, ok := config.(*Config)
	if !ok {
		return nil, errors.New("config is not of type *awss3exporter.Config")
	}

	s3Exp := newS3Exporter(cfg, set)

	return exporterhelper.NewLogs(
		ctx,
		set,
		config,
		s3Exp.consumeLogs,
		exporterhelper.WithStart(s3Exp.start),
		exporterhelper.WithCapabilities(s3Exp.Capabilities()),
		exporterhelper.WithQueue(cfg.QueueSettings),
		exporterhelper.WithRetry(cfg.BackOffConfig),
		exporterhelper.WithTimeout(cfg.TimeoutSettings),
	)
}
