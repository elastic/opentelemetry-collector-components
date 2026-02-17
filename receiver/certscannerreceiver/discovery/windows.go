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
	"context"
	"os/exec"
	"strconv"
	"strings"
)

type windowsDiscoverer struct{}

// NewDiscoverer creates a Windows-specific port discoverer that parses netstat output.
func NewDiscoverer() PortDiscoverer {
	return &windowsDiscoverer{}
}

func (d *windowsDiscoverer) DiscoverListeningPorts(ctx context.Context) ([]ListeningPort, error) {
	// Use netstat -ano to include PID
	// -a: all connections
	// -n: numeric addresses
	// -o: include PID
	cmd := exec.CommandContext(ctx, "netstat", "-ano", "-p", "TCP")
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	return parseNetstatOutput(string(output)), nil
}

// parseNetstatOutput extracts listening ports from netstat -ano output.
//
// Windows netstat -ano -p TCP output format:
//
//	Active Connections
//
//	  Proto  Local Address          Foreign Address        State           PID
//	  TCP    0.0.0.0:135            0.0.0.0:0              LISTENING       1234
//	  TCP    [::]:445               [::]:0                 LISTENING       4
func parseNetstatOutput(output string) []ListeningPort {
	ports := make(map[int]ListeningPort)
	lines := strings.Split(output, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, "LISTENING") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}

		// Local address is field[1], format: IP:PORT or [::]:PORT
		localAddr := fields[1]
		lastColon := strings.LastIndex(localAddr, ":")
		if lastColon == -1 {
			continue
		}

		// Extract address part
		addr := localAddr[:lastColon]
		if strings.HasPrefix(addr, "[") && strings.HasSuffix(addr, "]") {
			addr = addr[1 : len(addr)-1] // Remove brackets from IPv6
		}

		// Extract port
		portStr := localAddr[lastColon+1:]
		port, err := strconv.Atoi(portStr)
		if err != nil {
			continue
		}

		// PID is the last field
		pid, err := strconv.Atoi(fields[len(fields)-1])
		if err != nil {
			pid = 0
		}

		if _, exists := ports[port]; !exists {
			ports[port] = ListeningPort{
				Port:    port,
				Address: addr,
				PID:     pid,
				// ProcessName and Executable would require additional Windows API calls
				// Could use tasklist /FI "PID eq <pid>" but keeping it simple for now
			}
		}
	}

	result := make([]ListeningPort, 0, len(ports))
	for _, lp := range ports {
		result = append(result, lp)
	}
	return result
}
