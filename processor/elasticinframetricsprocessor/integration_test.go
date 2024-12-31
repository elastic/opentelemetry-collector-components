// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticinframetricsprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestProcessorHostMetrics(t *testing.T) {
	tests := []struct {
		name                string
		hostmetricsRoopath  string
		scrapersConfig      string
		processorConfig     *Config
		expectedMetricsPath string
	}{
		{
			name:               "all scrapers",
			hostmetricsRoopath: "./testdata/hostmetrics_integration/e2e/",
			scrapersConfig: `
scrapers:
  cpu:
    metrics:
      system.cpu.utilization:
        enabled: true
      system.cpu.logical.count:
        enabled: true
  memory:
    metrics:
      system.memory.utilization:
        enabled: true
  process:
    mute_process_exe_error: true
    mute_process_io_error: true
    mute_process_user_error: true
    metrics:
      process.threads:
        enabled: true
      process.open_file_descriptors:
        enabled: true
      process.memory.utilization:
        enabled: true
      process.disk.operations:
        enabled: true
  network: {}
  processes: {}
  load: {}
  disk: {}
  filesystem:
    exclude_mount_points:
      mount_points:
        - /dev/*
        - /proc/*
        - /sys/*
        - /run/k3s/containerd/*
        - /var/lib/docker/*
        - /var/lib/kubelet/*
        - /snap/*
      match_type: regexp
    exclude_fs_types:
      fs_types:
        - autofs
        - binfmt_misc
        - bpf
        - cgroup2
        - configfs
        - debugfs
        - devpts
        - devtmpfs
        - fusectl
        - hugetlbfs
        - iso9660
        - mqueue
        - nsfs
        - overlay
        - proc
        - procfs
        - pstore
        - rpc_pipefs
        - securityfs
        - selinuxfs
        - squashfs
        - sysfs
        - tracefs
      match_type: strict
`,
			processorConfig: &Config{
				DropOriginal:     true,
				AddSystemMetrics: true,
			},
			expectedMetricsPath: "./testdata/hostmetrics_integration/output-metrics-drop.yaml",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			metricsProvider := hostmetricsreceiver.NewFactory()
			cfg := metricsProvider.CreateDefaultConfig().(*hostmetricsreceiver.Config)
			cfg.RootPath = tt.hostmetricsRoopath
			cfg.CollectionInterval = 1 * time.Second

			hostCfg2, err := confmap.NewRetrievedFromYAML([]byte(tt.scrapersConfig))
			require.NoError(t, err)
			hostCfg, err := hostCfg2.AsConf()
			require.NoError(t, err)

			err = cfg.Unmarshal(hostCfg)
			require.NoError(t, err)

			nextMetrics := new(consumertest.MetricsSink)

			hostComponent, err := metricsProvider.CreateMetrics(ctx, receivertest.NewNopSettings(), cfg, nextMetrics)
			require.NoError(t, err)
			err = hostComponent.Start(ctx, componenttest.NewNopHost())
			require.NoError(t, err)
			defer hostComponent.Shutdown(ctx)

			var inputMetrics []pmetric.Metrics
			require.Eventually(t, func() bool {
				inputMetrics = nextMetrics.AllMetrics()
				return len(inputMetrics) > 0
			}, 5*time.Second, 2*time.Second)

			p := newProcessor(processortest.NewNopSettings(), tt.processorConfig)

			actualMetrics, err := p.processMetrics(context.Background(), inputMetrics[0])
			assert.NoError(t, err)

			// golden.WriteMetrics(t, tt.expectedMetricsPath, actualMetrics)

			expectedMetrics, err := golden.ReadMetrics(tt.expectedMetricsPath)
			require.NoError(t, err)
			require.NoError(t, pmetrictest.CompareMetrics(expectedMetrics, actualMetrics, pmetrictest.IgnoreMetricDataPointsOrder(), pmetrictest.IgnoreStartTimestamp(), pmetrictest.IgnoreTimestamp()))
		})
	}
}
