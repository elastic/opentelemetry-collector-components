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

package prometheusscrapereceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/prometheusscrapereceiver"

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	commonconfig "github.com/prometheus/common/config"
	promconfig "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/scrape"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
	"go.uber.org/zap/exp/zapslog"
)

type prometheusScrapeReceiver struct {
	settings receiver.Settings
	cfg      *Config
	consumer consumer.Metrics
	obsrecv  *receiverhelper.ObsReport

	cancel            context.CancelFunc
	wg                sync.WaitGroup
	scrapeManager     *scrape.Manager
	discoveryManager  *discovery.Manager
	unregisterMetrics func()
}

func newReceiver(set receiver.Settings, cfg *Config, next consumer.Metrics) (*prometheusScrapeReceiver, error) {
	if err := cfg.PrometheusConfig.Reload(); err != nil {
		return nil, fmt.Errorf("failed to reload Prometheus config: %w", err)
	}
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             set.ID,
		Transport:              "http",
		ReceiverCreateSettings: set,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create obsreport: %w", err)
	}
	return &prometheusScrapeReceiver{settings: set, cfg: cfg, consumer: next, obsrecv: obsrecv}, nil
}

// Start implements component.Component.
func (r *prometheusScrapeReceiver) Start(_ context.Context, host component.Host) error {
	// The managers outlive the Start context.
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	if err := r.initPrometheusComponents(ctx, host); err != nil {
		cancel()
		r.settings.Logger.Error("Failed to initialize Prometheus components", zap.Error(err))
		return err
	}
	return nil
}

func (r *prometheusScrapeReceiver) initPrometheusComponents(ctx context.Context, host component.Host) error {
	logger := slog.New(zapslog.NewHandler(r.settings.Logger.Core()))
	promCfg := (*promconfig.Config)(r.cfg.PrometheusConfig)

	registry := prometheus.NewRegistry()
	registerer := prometheus.WrapRegistererWith(prometheus.Labels{"receiver": r.settings.ID.String()}, registry)

	sdMetrics, err := discovery.CreateAndRegisterSDMetrics(registerer)
	if err != nil {
		return fmt.Errorf("failed to register service discovery metrics: %w", err)
	}
	r.discoveryManager = discovery.NewManager(ctx, logger, registerer, sdMetrics)
	if r.discoveryManager == nil {
		return errors.New("failed to create discovery manager")
	}

	store := newAppendable(r.settings.Logger, r.consumer, r.obsrecv, promCfg.GlobalConfig.ExternalLabels)
	scrapeOpts := &scrape.Options{
		// Needed to pass the family type, HELP and UNIT to the appender.
		AppendMetadata: true,
		// Keep `_created` series as ordinary samples instead of consuming them.
		ParseST: false,
		HTTPClientOptions: []commonconfig.HTTPClientOption{
			commonconfig.WithUserAgent(r.settings.BuildInfo.Command + "/" + r.settings.BuildInfo.Version),
		},
	}
	r.scrapeManager, err = scrape.NewManager(scrapeOpts, logger, nil, nil, store, registerer)
	if err != nil {
		return err
	}

	r.unregisterMetrics = func() {
		sdMetrics.RefreshManager.Unregister()
		for _, m := range sdMetrics.MechanismMetrics {
			m.Unregister()
		}
		r.discoveryManager.UnregisterMetrics()
		r.scrapeManager.UnregisterMetrics()
	}

	scrapeConfigs, err := promCfg.GetScrapeConfigs()
	if err != nil {
		return fmt.Errorf("could not get scrape configs: %w", err)
	}
	if err := r.scrapeManager.ApplyConfig(promCfg); err != nil {
		return err
	}
	discoveryCfg := make(map[string]discovery.Configs, len(scrapeConfigs))
	for _, sc := range scrapeConfigs {
		discoveryCfg[sc.JobName] = sc.ServiceDiscoveryConfigs
		r.settings.Logger.Info("Scrape job added", zap.String("jobName", sc.JobName))
	}
	if err := r.discoveryManager.ApplyConfig(discoveryCfg); err != nil {
		return err
	}

	r.wg.Add(2)
	go func() {
		defer r.wg.Done()
		r.settings.Logger.Info("Starting discovery manager")
		if err := r.discoveryManager.Run(); err != nil && !errors.Is(err, context.Canceled) {
			r.settings.Logger.Error("Discovery manager failed", zap.Error(err))
			componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(err))
		}
	}()
	go func() {
		defer r.wg.Done()
		r.settings.Logger.Info("Starting scrape manager")
		if err := r.scrapeManager.Run(r.discoveryManager.SyncCh()); err != nil {
			r.settings.Logger.Error("Scrape manager failed", zap.Error(err))
			componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(err))
		}
	}()
	return nil
}

// Shutdown implements component.Component.
func (r *prometheusScrapeReceiver) Shutdown(context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	if r.scrapeManager != nil {
		r.scrapeManager.Stop()
	}
	r.wg.Wait()
	if r.unregisterMetrics != nil {
		r.unregisterMetrics()
	}
	return nil
}
