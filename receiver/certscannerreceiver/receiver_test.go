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

package certscannerreceiver

import (
	"sort"
	"testing"
	"time"

	"github.com/elastic/opentelemetry-collector-components/receiver/certscannerreceiver/discovery"
)

// Helper to convert []int to []discovery.ListeningPort
func toListeningPorts(ports []int) []discovery.ListeningPort {
	result := make([]discovery.ListeningPort, len(ports))
	for i, p := range ports {
		result[i] = discovery.ListeningPort{Port: p}
	}
	return result
}

// Helper to extract ports from []discovery.ListeningPort
func extractPorts(lps []discovery.ListeningPort) []int {
	result := make([]int, len(lps))
	for i, lp := range lps {
		result[i] = lp.Port
	}
	return result
}

func TestFilterPorts(t *testing.T) {
	tests := []struct {
		name       string
		config     *Config
		discovered []int
		want       []int
	}{
		{
			name: "scan_all with no excludes",
			config: &Config{
				Ports: PortsConfig{
					ScanAll: true,
				},
			},
			discovered: []int{22, 80, 443, 8080},
			want:       []int{22, 80, 443, 8080},
		},
		{
			name: "scan_all with excludes",
			config: &Config{
				Ports: PortsConfig{
					ScanAll: true,
					Exclude: []int{22, 80},
				},
			},
			discovered: []int{22, 80, 443, 8080},
			want:       []int{443, 8080},
		},
		{
			name: "include list only",
			config: &Config{
				Ports: PortsConfig{
					ScanAll: false,
					Include: []int{443, 8443},
				},
			},
			discovered: []int{22, 80, 443, 8080},
			want:       []int{443},
		},
		{
			name: "include with excludes",
			config: &Config{
				Ports: PortsConfig{
					ScanAll: false,
					Include: []int{443, 8443, 22},
					Exclude: []int{22},
				},
			},
			discovered: []int{22, 80, 443, 8080},
			want:       []int{443},
		},
		{
			name: "include list - nothing discovered",
			config: &Config{
				Ports: PortsConfig{
					ScanAll: false,
					Include: []int{443, 8443},
				},
			},
			discovered: []int{22, 80, 8080},
			want:       []int{},
		},
		{
			name: "empty discovered",
			config: &Config{
				Ports: PortsConfig{
					ScanAll: true,
				},
			},
			discovered: []int{},
			want:       []int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &certScannerReceiver{config: tt.config}
			got := extractPorts(r.filterPorts(toListeningPorts(tt.discovered)))

			// Sort both for comparison
			sort.Ints(got)
			sort.Ints(tt.want)

			// Handle nil vs empty slice
			if len(got) == 0 && len(tt.want) == 0 {
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("filterPorts() = %v, want %v", got, tt.want)
				return
			}

			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("filterPorts() = %v, want %v", got, tt.want)
					return
				}
			}
		})
	}
}

func TestNewCertScannerReceiver(t *testing.T) {
	// Placeholder - in real tests you'd use receivertest.NewNopSettings()
	// This test is here to ensure the struct compiles correctly
	_ = &Config{
		Interval:    time.Hour,
		Timeout:     3 * time.Second,
		Ports:       PortsConfig{Include: []int{443}},
		EmitMetrics: true,
	}
}
