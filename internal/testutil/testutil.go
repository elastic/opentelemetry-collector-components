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

// This is a copy of the internal module from opentelemetry-collector:
// https://github.com/open-telemetry/opentelemetry-collector/tree/main/internal/testutil

package testutil // import "github.com/elastic/opentelemetry-collector-components/internal/testutil"

import (
	"net"
	"os/exec"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type portpair struct {
	first string
	last  string
}

// GetAvailableLocalAddress finds an available local port and returns an endpoint
// describing it. The port is available for opening when this function returns
// provided that there is no race by some other code to grab the same port
// immediately.
func GetAvailableLocalAddress(t testing.TB) string {
	return findAvailable(t, "tcp4")
}

func findAvailable(t testing.TB, network string) string {
	// Retry has been added for windows as net.Listen can return a port that is not actually available. Details can be
	// found in https://github.com/docker/for-win/issues/3171 but to summarize Hyper-V will reserve ranges of ports
	// which do not show up under the "netstat -ano" but can only be found by
	// "netsh interface ipv4 show excludedportrange protocol=tcp".  We'll use []exclusions to hold those ranges and
	// retry if the port returned by GetAvailableLocalAddress falls in one of those them.
	var exclusions []portpair
	portFound := false
	if runtime.GOOS == "windows" {
		exclusions = getExclusionsList(network, t)
	}

	var endpoint string
	for !portFound {
		endpoint = findAvailableAddress(network, t)
		_, port, err := net.SplitHostPort(endpoint)
		require.NoError(t, err)
		portFound = true
		if runtime.GOOS == "windows" {
			for _, pair := range exclusions {
				if port >= pair.first && port <= pair.last {
					portFound = false
					break
				}
			}
		}
	}

	return endpoint
}

func findAvailableAddress(network string, t testing.TB) string {
	var host string
	switch network {
	case "tcp", "tcp4":
		host = "localhost"
	case "tcp6":
		host = "[::1]"
	}
	require.NotZero(t, host, "network must be either of tcp, tcp4 or tcp6")

	ln, err := net.Listen("tcp", host+":0")
	require.NoError(t, err, "Failed to get a free local port")
	// There is a possible race if something else takes this same port before
	// the test uses it, however, that is unlikely in practice.
	defer func() {
		assert.NoError(t, ln.Close())
	}()
	return ln.Addr().String()
}

// Get excluded ports on Windows from the command: netsh interface ipv4 show excludedportrange protocol=tcp
func getExclusionsList(network string, t testing.TB) []portpair {
	var cmdTCP *exec.Cmd
	switch network {
	case "tcp", "tcp4":
		cmdTCP = exec.Command("netsh", "interface", "ipv4", "show", "excludedportrange", "protocol=tcp")
	case "tcp6":
		cmdTCP = exec.Command("netsh", "interface", "ipv6", "show", "excludedportrange", "protocol=tcp")
	}
	require.NotZero(t, cmdTCP, "network must be either of tcp, tcp4 or tcp6")

	outputTCP, errTCP := cmdTCP.CombinedOutput()
	require.NoError(t, errTCP)
	exclusions := createExclusionsList(t, string(outputTCP))

	cmdUDP := exec.Command("netsh", "interface", "ipv4", "show", "excludedportrange", "protocol=udp")
	outputUDP, errUDP := cmdUDP.CombinedOutput()
	require.NoError(t, errUDP)
	exclusions = append(exclusions, createExclusionsList(t, string(outputUDP))...)

	return exclusions
}

func createExclusionsList(t testing.TB, exclusionsText string) []portpair {
	var exclusions []portpair

	parts := strings.Split(exclusionsText, "--------")
	require.Len(t, parts, 3)
	portsText := strings.Split(parts[2], "*")
	require.Greater(t, len(portsText), 1) // original text may have a suffix like " - Administered port exclusions."
	lines := strings.Split(portsText[0], "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			entries := strings.Fields(strings.TrimSpace(line))
			require.Len(t, entries, 2)
			pair := portpair{entries[0], entries[1]}
			exclusions = append(exclusions, pair)
		}
	}
	return exclusions
}
