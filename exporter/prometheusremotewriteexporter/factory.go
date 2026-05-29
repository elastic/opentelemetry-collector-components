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

package prometheusremotewriteexporter // import "github.com/elastic/opentelemetry-collector-components/exporter/prometheusremotewriteexporter"

import (
	"context"
	"errors"
	"time"

	remoteapi "github.com/prometheus/client_golang/exp/api/remote"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/featuregate"

	"github.com/elastic/opentelemetry-collector-components/exporter/prometheusremotewriteexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/resourcetotelemetry"
)

var retryOn429FeatureGate = featuregate.GlobalRegistry().MustRegister(
	"exporter.prometheusremotewritexporter.RetryOn429",
	featuregate.StageAlpha,
	featuregate.WithRegisterFromVersion("v0.101.0"),
	featuregate.WithRegisterDescription("When enabled, the Prometheus remote write exporter will retry 429 http status code. Requires exporter.prometheusremotewritexporter.metrics.RetryOn429 to be enabled."),
)

var enableSendingRW2FeatureGate = featuregate.GlobalRegistry().MustRegister(
	"exporter.prometheusremotewritexporter.enableSendingRW2",
	featuregate.StageAlpha,
	featuregate.WithRegisterFromVersion("v0.125.0"),
	featuregate.WithRegisterDescription("When enabled, the Prometheus remote write exporter will support sending rw2. Extra configuration is still required besides enabling this feature gate."),
)

// NewFactory creates a new Prometheus Remote Write exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability))
}

func createMetricsExporter(ctx context.Context, set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	prwCfg, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("invalid configuration")
	}

	prwe, err := newPRWExporter(prwCfg, set)
	if err != nil {
		return nil, err
	}

	// Pass the SendingQueue config (configoptional.Optional[QueueBatchConfig]) directly
	// to exporterhelper.
	exp, err := exporterhelper.NewMetrics(
		ctx,
		set,
		cfg,
		prwe.PushMetrics,
		exporterhelper.WithTimeout(prwCfg.TimeoutSettings),
		exporterhelper.WithQueue(prwCfg.SendingQueue),
		exporterhelper.WithStart(prwe.Start),
		exporterhelper.WithShutdown(prwe.Shutdown),
	)
	if err != nil {
		return nil, err
	}
	return resourcetotelemetry.WrapMetricsExporter(prwCfg.ResourceToTelemetrySettings, exp), nil
}

func createDefaultConfig() component.Config {
	retrySettings := configretry.NewDefaultBackOffConfig()
	retrySettings.InitialInterval = 50 * time.Millisecond
	clientConfig := confighttp.NewDefaultClientConfig()
	clientConfig.Endpoint = "http://some.url:9411/api/prom/push"
	// We almost read 0 bytes, so no need to tune ReadBufferSize.
	clientConfig.ReadBufferSize = 0
	clientConfig.WriteBufferSize = 512 * 1024
	clientConfig.Timeout = exporterhelper.NewDefaultTimeoutConfig().Timeout

	return &Config{
		Namespace:           "",
		ExternalLabels:      map[string]string{},
		MaxBatchSizeBytes:   3000000,
		TimeoutSettings:     exporterhelper.NewDefaultTimeoutConfig(),
		BackOffConfig:       retrySettings,
		AddMetricSuffixes:   true,
		SendMetadata:        false,
		RemoteWriteProtoMsg: remoteapi.WriteV1MessageType,
		ClientConfig:        clientConfig,
		// Use sending_queue.batch.partition.metadata_keys
		SendingQueue: configoptional.Optional[exporterhelper.QueueBatchConfig]{},
		TargetInfo: TargetInfo{
			Enabled: true,
		},
	}
}
