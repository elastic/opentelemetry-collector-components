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

//go:build !linux && !windows

package discovery

import (
	"sort"
	"testing"
)

func TestParseGenericNetstatOutput(t *testing.T) {
	// macOS-style netstat output
	input := `Active Internet connections (including servers)
Proto Recv-Q Send-Q  Local Address          Foreign Address        (state)
tcp4       0      0  *.443                  *.*                    LISTEN
tcp6       0      0  *.443                  *.*                    LISTEN
tcp4       0      0  127.0.0.1.9200         *.*                    LISTEN
tcp4       0      0  *.80                   *.*                    ESTABLISHED
tcp4       0      0  *.8443                 *.*                    LISTEN
`

	ports := parseGenericNetstatOutput(input)

	// 443 deduplicated (tcp4+tcp6), 9200, 8443 = 3 LISTEN entries (80 is ESTABLISHED)
	if len(ports) != 3 {
		t.Fatalf("expected 3 listening ports, got %d", len(ports))
	}

	sort.Slice(ports, func(i, j int) bool { return ports[i].Port < ports[j].Port })

	expected := []struct {
		port int
		addr string
	}{
		{443, "0.0.0.0"},
		{8443, "0.0.0.0"},
		{9200, "127.0.0.1"},
	}

	for i, tt := range expected {
		if ports[i].Port != tt.port {
			t.Errorf("port[%d]: expected port %d, got %d", i, tt.port, ports[i].Port)
		}
		if ports[i].Address != tt.addr {
			t.Errorf("port[%d]: expected addr %q, got %q", i, tt.addr, ports[i].Address)
		}
	}
}

func TestExtractPortAndAddr(t *testing.T) {
	tests := []struct {
		input    string
		wantPort int
		wantAddr string
	}{
		// BSD/macOS dot notation
		{"*.443", 443, "0.0.0.0"},
		{"127.0.0.1.9200", 9200, "127.0.0.1"},
		// Colon notation
		{"0.0.0.0:443", 443, "0.0.0.0"},
		// IPv6 bracket notation
		{"[::]:443", 443, "::"},
		// Invalid
		{"noport", 0, ""},
	}

	for _, tt := range tests {
		port, addr := extractPortAndAddr(tt.input)
		if port != tt.wantPort {
			t.Errorf("extractPortAndAddr(%q) port = %d, want %d", tt.input, port, tt.wantPort)
		}
		if addr != tt.wantAddr {
			t.Errorf("extractPortAndAddr(%q) addr = %q, want %q", tt.input, addr, tt.wantAddr)
		}
	}
}

func TestParseGenericNetstatOutput_Empty(t *testing.T) {
	ports := parseGenericNetstatOutput("")
	if len(ports) != 0 {
		t.Errorf("expected 0 ports from empty input, got %d", len(ports))
	}
}
