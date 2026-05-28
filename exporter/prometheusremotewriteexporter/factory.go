// ELASTICSEARCH CONFIDENTIAL
// __________________
//
//  Copyright Elasticsearch B.V. All rights reserved.
//
// NOTICE:  All information contained herein is, and remains
// the property of Elasticsearch B.V. and its suppliers, if any.
// The intellectual and technical concepts contained herein
// are proprietary to Elasticsearch B.V. and its suppliers and
// may be covered by U.S. and Foreign Patents, patents in
// process, and are protected by trade secret or copyright
// law.  Dissemination of this information or reproduction of
// this material is strictly forbidden unless prior written
// permission is obtained from Elasticsearch B.V.

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
