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

//go:build windows

package discovery

import (
	"sort"
	"testing"
)

func TestParseNetstatOutput(t *testing.T) {
	input := `
Active Connections

  Proto  Local Address          Foreign Address        State           PID
  TCP    0.0.0.0:135            0.0.0.0:0              LISTENING       1100
  TCP    0.0.0.0:443            0.0.0.0:0              LISTENING       4567
  TCP    0.0.0.0:8080           0.0.0.0:0              ESTABLISHED     9999
  TCP    [::]:445               [::]:0                 LISTENING       4
  TCP    127.0.0.1:9200         0.0.0.0:0              LISTENING       8888
`

	ports := parseNetstatOutput(input)

	// Should find 4 LISTENING entries (8080 is ESTABLISHED, skip)
	if len(ports) != 4 {
		t.Fatalf("expected 4 listening ports, got %d", len(ports))
	}

	// Sort for deterministic checks
	sort.Slice(ports, func(i, j int) bool { return ports[i].Port < ports[j].Port })

	tests := []struct {
		port int
		addr string
		pid  int
	}{
		{135, "0.0.0.0", 1100},
		{443, "0.0.0.0", 4567},
		{445, "::", 4},
		{9200, "127.0.0.1", 8888},
	}

	for i, tt := range tests {
		if ports[i].Port != tt.port {
			t.Errorf("port[%d]: expected port %d, got %d", i, tt.port, ports[i].Port)
		}
		if ports[i].Address != tt.addr {
			t.Errorf("port[%d]: expected addr %q, got %q", i, tt.addr, ports[i].Address)
		}
		if ports[i].PID != tt.pid {
			t.Errorf("port[%d]: expected PID %d, got %d", i, tt.pid, ports[i].PID)
		}
	}
}

func TestParseNetstatOutput_Empty(t *testing.T) {
	ports := parseNetstatOutput("")
	if len(ports) != 0 {
		t.Errorf("expected 0 ports from empty input, got %d", len(ports))
	}
}

func TestParseNetstatOutput_Deduplication(t *testing.T) {
	// Same port on IPv4 and IPv6 should be deduplicated
	input := `
Active Connections

  Proto  Local Address          Foreign Address        State           PID
  TCP    0.0.0.0:443            0.0.0.0:0              LISTENING       100
  TCP    [::]:443               [::]:0                 LISTENING       100
`
	ports := parseNetstatOutput(input)
	if len(ports) != 1 {
		t.Errorf("expected 1 deduplicated port, got %d", len(ports))
	}
}
