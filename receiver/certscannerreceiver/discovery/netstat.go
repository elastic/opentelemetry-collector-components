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

package discovery // import "github.com/elastic/opentelemetry-collector-components/receiver/certscannerreceiver/discovery"

import (
	"context"
	"os/exec"
	"strconv"
	"strings"
)

type netstatDiscoverer struct{}

// NewDiscoverer creates a generic port discoverer that parses netstat output.
// Used on platforms other than Linux and Windows (e.g., macOS, BSD).
func NewDiscoverer() PortDiscoverer {
	return &netstatDiscoverer{}
}

func (d *netstatDiscoverer) DiscoverListeningPorts(ctx context.Context) ([]ListeningPort, error) {
	// Generic netstat command that works on most Unix-like systems (macOS, BSD)
	cmd := exec.CommandContext(ctx, "netstat", "-an", "-p", "tcp")
	output, err := cmd.Output()
	if err != nil {
		// Try alternative syntax used by some systems
		cmd = exec.CommandContext(ctx, "netstat", "-an")
		output, err = cmd.Output()
		if err != nil {
			return nil, err
		}
	}

	return parseGenericNetstatOutput(string(output)), nil
}

// parseGenericNetstatOutput extracts listening TCP ports from netstat output.
//
// macOS/BSD netstat -an output format:
//
//	Active Internet connections (including servers)
//	Proto Recv-Q Send-Q  Local Address          Foreign Address        (state)
//	tcp4       0      0  *.443                  *.*                    LISTEN
//	tcp6       0      0  *.443                  *.*                    LISTEN
func parseGenericNetstatOutput(output string) []ListeningPort {
	ports := make(map[int]ListeningPort)
	lines := strings.Split(output, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Skip non-TCP lines and non-listening sockets
		if !strings.HasPrefix(line, "tcp") {
			continue
		}
		if !strings.Contains(line, "LISTEN") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}

		// Local address field position varies, find it by looking for the pattern
		var localAddr string
		for i, field := range fields {
			if i == 0 {
				continue
			}
			if strings.Contains(field, ".") || strings.Contains(field, ":") || strings.Contains(field, "*") {
				localAddr = field
				break
			}
		}

		if localAddr == "" {
			continue
		}

		port, addr := extractPortAndAddr(localAddr)
		if port > 0 {
			if _, exists := ports[port]; !exists {
				ports[port] = ListeningPort{
					Port:    port,
					Address: addr,
					// PID not available via generic netstat
				}
			}
		}
	}

	result := make([]ListeningPort, 0, len(ports))
	for _, lp := range ports {
		result = append(result, lp)
	}
	return result
}

// extractPortAndAddr extracts the port number and address from various formats.
func extractPortAndAddr(addrStr string) (int, string) {
	// Handle IPv6 bracket notation: [::]:443
	if strings.HasPrefix(addrStr, "[") {
		bracketEnd := strings.Index(addrStr, "]")
		if bracketEnd != -1 && len(addrStr) > bracketEnd+2 {
			addr := addrStr[1:bracketEnd]
			portStr := addrStr[bracketEnd+2:]
			if port, err := strconv.Atoi(portStr); err == nil {
				return port, addr
			}
		}
		return 0, ""
	}

	// Handle colon notation: 0.0.0.0:443
	if colonIdx := strings.LastIndex(addrStr, ":"); colonIdx != -1 {
		portStr := addrStr[colonIdx+1:]
		if port, err := strconv.Atoi(portStr); err == nil {
			return port, addrStr[:colonIdx]
		}
	}

	// Handle dot notation (BSD/macOS): *.443, 127.0.0.1.443
	if dotIdx := strings.LastIndex(addrStr, "."); dotIdx != -1 {
		portStr := addrStr[dotIdx+1:]
		if port, err := strconv.Atoi(portStr); err == nil && port > 0 && port <= 65535 {
			addr := addrStr[:dotIdx]
			if addr == "*" {
				addr = "0.0.0.0"
			}
			return port, addr
		}
	}

	return 0, ""
}
