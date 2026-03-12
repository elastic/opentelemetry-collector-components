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

package certscannerreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/certscannerreceiver"

import (
	"context"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/receiver/certscannerreceiver/discovery"
	"github.com/elastic/opentelemetry-collector-components/receiver/certscannerreceiver/internal"
	"github.com/elastic/opentelemetry-collector-components/receiver/certscannerreceiver/scanner"
)

type certScannerReceiver struct {
	config       *Config
	logger       *zap.Logger
	logsConsumer consumer.Logs

	discovery discovery.PortDiscoverer
	scanner   *scanner.TLSScanner

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newCertScannerReceiver(
	settings receiver.Settings,
	cfg *Config,
	logsConsumer consumer.Logs,
) (*certScannerReceiver, error) {
	r := &certScannerReceiver{
		config:       cfg,
		logger:       settings.Logger,
		logsConsumer: logsConsumer,
		scanner:      scanner.NewTLSScanner(cfg.Timeout),
		discovery:    discovery.NewDiscoverer(),
	}

	return r, nil
}

// Start begins the certificate scanning loop.
func (r *certScannerReceiver) Start(ctx context.Context, _ component.Host) error {
	ctx, r.cancel = context.WithCancel(ctx)

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.runScanLoop(ctx)
	}()

	r.logger.Info("Certificate scanner receiver started",
		zap.Duration("interval", r.config.Interval),
		zap.Duration("timeout", r.config.Timeout))

	return nil
}

// Shutdown stops the receiver gracefully.
func (r *certScannerReceiver) Shutdown(_ context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
	r.logger.Info("Certificate scanner receiver stopped")
	return nil
}

// runScanLoop runs the main scan loop at the configured interval.
func (r *certScannerReceiver) runScanLoop(ctx context.Context) {
	// Run immediately on start
	r.performScan(ctx)

	ticker := time.NewTicker(r.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.performScan(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// performScan executes a single scan cycle.
func (r *certScannerReceiver) performScan(ctx context.Context) {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	// Discover listening ports with process info
	discoveredPorts, err := r.discovery.DiscoverListeningPorts(ctx)
	if err != nil {
		r.logger.Error("Failed to discover listening ports", zap.Error(err))
		return
	}

	// Filter ports based on configuration
	portsToScan := r.filterPorts(discoveredPorts)

	r.logger.Info("Starting certificate scan",
		zap.Int("discovered_ports", len(discoveredPorts)),
		zap.Int("ports_to_scan", len(portsToScan)))

	scannedCount := 0
	tlsCount := 0

	// Scan each port
	for _, lp := range portsToScan {
		select {
		case <-ctx.Done():
			return
		default:
		}

		scannedCount++
		result, err := r.scanner.ScanPort(ctx, "127.0.0.1", lp.Port)
		if err != nil {
			// Debug level since many ports won't have TLS
			r.logger.Debug("Port scan failed",
				zap.Int("port", lp.Port),
				zap.Error(err))
			continue
		}

		// Add process information from port discovery
		result.ProcessPID = lp.PID
		result.ProcessName = lp.ProcessName
		result.ProcessExecutable = lp.Executable

		tlsCount++
		r.logger.Debug("TLS certificate found",
			zap.Int("port", lp.Port),
			zap.String("subject_cn", result.LeafCertificate.SubjectCN),
			zap.Int("process_pid", lp.PID),
			zap.String("process_name", lp.ProcessName))

		// Emit logs if consumer is available
		if r.logsConsumer != nil {
			logs := internal.ConvertToLogs(result, hostname)
			if err := r.logsConsumer.ConsumeLogs(ctx, logs); err != nil {
				r.logger.Error("Failed to emit logs", zap.Error(err))
			}
		}
	}

	r.logger.Info("Certificate scan completed",
		zap.Int("ports_scanned", scannedCount),
		zap.Int("tls_services_found", tlsCount))
}

// filterPorts applies include/exclude configuration to discovered ports.
func (r *certScannerReceiver) filterPorts(discovered []discovery.ListeningPort) []discovery.ListeningPort {
	// Build exclude set for fast lookup
	excludeSet := make(map[int]struct{}, len(r.config.Ports.Exclude))
	for _, p := range r.config.Ports.Exclude {
		excludeSet[p] = struct{}{}
	}

	var result []discovery.ListeningPort

	if r.config.Ports.ScanAll {
		// Scan all discovered ports except those in exclude list
		for _, lp := range discovered {
			if _, excluded := excludeSet[lp.Port]; !excluded {
				result = append(result, lp)
			}
		}
	} else {
		// Only scan ports that are both discovered AND in include list
		includeSet := make(map[int]struct{}, len(r.config.Ports.Include))
		for _, p := range r.config.Ports.Include {
			includeSet[p] = struct{}{}
		}

		for _, lp := range discovered {
			_, included := includeSet[lp.Port]
			_, excluded := excludeSet[lp.Port]
			if included && !excluded {
				result = append(result, lp)
			}
		}
	}

	return result
}
