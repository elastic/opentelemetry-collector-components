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
	"strings"
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config with include list",
			config: Config{
				Interval: time.Hour,
				Timeout:  3 * time.Second,
				Ports:    PortsConfig{Include: []int{443, 8443}, ScanAll: false},
			},
			wantErr: false,
		},
		{
			name: "valid config with scan_all",
			config: Config{
				Interval: time.Hour,
				Timeout:  3 * time.Second,
				Ports:    PortsConfig{ScanAll: true},
			},
			wantErr: false,
		},
		{
			name: "interval too short",
			config: Config{
				Interval: 30 * time.Second,
				Timeout:  3 * time.Second,
				Ports:    PortsConfig{Include: []int{443}},
			},
			wantErr: true,
			errMsg:  "interval must be at least 1 minute",
		},
		{
			name: "timeout too short",
			config: Config{
				Interval: time.Hour,
				Timeout:  50 * time.Millisecond,
				Ports:    PortsConfig{Include: []int{443}},
			},
			wantErr: true,
			errMsg:  "timeout must be at least 100ms",
		},
		{
			name: "timeout too long",
			config: Config{
				Interval: time.Hour,
				Timeout:  2 * time.Minute,
				Ports:    PortsConfig{Include: []int{443}},
			},
			wantErr: true,
			errMsg:  "timeout must not exceed 1 minute",
		},
		{
			name: "missing include when scan_all is false",
			config: Config{
				Interval: time.Hour,
				Timeout:  3 * time.Second,
				Ports:    PortsConfig{ScanAll: false},
			},
			wantErr: true,
			errMsg:  "ports.include must be specified",
		},
		{
			name: "invalid port in include (too high)",
			config: Config{
				Interval: time.Hour,
				Timeout:  3 * time.Second,
				Ports:    PortsConfig{Include: []int{443, 70000}},
			},
			wantErr: true,
			errMsg:  "invalid port 70000",
		},
		{
			name: "invalid port in include (zero)",
			config: Config{
				Interval: time.Hour,
				Timeout:  3 * time.Second,
				Ports:    PortsConfig{Include: []int{0, 443}},
			},
			wantErr: true,
			errMsg:  "invalid port 0",
		},
		{
			name: "invalid port in exclude",
			config: Config{
				Interval: time.Hour,
				Timeout:  3 * time.Second,
				Ports:    PortsConfig{Include: []int{443}, Exclude: []int{-1}},
			},
			wantErr: true,
			errMsg:  "invalid port -1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errMsg)
				} else if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}
