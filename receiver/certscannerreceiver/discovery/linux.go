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

//go:build linux

package discovery

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	// TCP_LISTEN state in hex as shown in /proc/net/tcp
	tcpListenState = "0A"
)

// socketInfo holds parsed info from /proc/net/tcp
type socketInfo struct {
	port  int
	addr  string
	inode uint64
}

type linuxDiscoverer struct{}

// NewDiscoverer creates a Linux-specific port discoverer that parses /proc/net/tcp.
func NewDiscoverer() PortDiscoverer {
	return &linuxDiscoverer{}
}

func (d *linuxDiscoverer) DiscoverListeningPorts(ctx context.Context) ([]ListeningPort, error) {
	// Step 1: Build inode -> PID mapping
	inodeToPID := d.buildInodePIDMap()

	// Step 2: Parse /proc/net/tcp and /proc/net/tcp6 for listening sockets
	sockets := make(map[int]socketInfo)

	if err := d.parseProcNetTCP("/proc/net/tcp", sockets); err != nil {
		return nil, err
	}
	// IPv6 - ignore errors if unavailable
	_ = d.parseProcNetTCP("/proc/net/tcp6", sockets)

	// Step 3: Build result with process info
	result := make([]ListeningPort, 0, len(sockets))
	for _, sock := range sockets {
		lp := ListeningPort{
			Port:    sock.port,
			Address: sock.addr,
		}

		// Look up process info from inode
		if pid, ok := inodeToPID[sock.inode]; ok {
			lp.PID = pid
			lp.ProcessName = d.readProcessName(pid)
			lp.Executable = d.readExecutablePath(pid)
		}

		result = append(result, lp)
	}

	return result, nil
}

// buildInodePIDMap scans /proc/[pid]/fd/ to build inode -> PID mapping
func (d *linuxDiscoverer) buildInodePIDMap() map[uint64]int {
	inodeToPID := make(map[uint64]int)

	procDirs, err := os.ReadDir("/proc")
	if err != nil {
		return inodeToPID
	}

	for _, entry := range procDirs {
		if !entry.IsDir() {
			continue
		}

		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue // Not a PID directory
		}

		fdPath := filepath.Join("/proc", entry.Name(), "fd")
		fds, err := os.ReadDir(fdPath)
		if err != nil {
			continue // Permission denied or process exited
		}

		for _, fd := range fds {
			linkPath := filepath.Join(fdPath, fd.Name())
			link, err := os.Readlink(linkPath)
			if err != nil {
				continue
			}

			// Socket links look like: socket:[12345]
			if strings.HasPrefix(link, "socket:[") && strings.HasSuffix(link, "]") {
				inodeStr := link[8 : len(link)-1]
				inode, err := strconv.ParseUint(inodeStr, 10, 64)
				if err != nil {
					continue
				}
				inodeToPID[inode] = pid
			}
		}
	}

	return inodeToPID
}

// parseProcNetTCP reads a /proc/net/tcp[6] file and extracts listening sockets.
//
// Format of /proc/net/tcp:
//
//	sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
//	0: 00000000:0016 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 12345 ...
//
// Field 1 (local_address): IP:PORT in hex
// Field 3 (st): Connection state in hex (0A = LISTEN)
// Field 9 (inode): Socket inode number
func (d *linuxDiscoverer) parseProcNetTCP(path string, sockets map[int]socketInfo) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)

	// Skip header line
	if !scanner.Scan() {
		return scanner.Err()
	}

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 10 {
			continue
		}

		// Field index 3 is the connection state
		state := fields[3]
		if state != tcpListenState {
			continue
		}

		// Field index 1 is local_address in format IP:PORT (hex)
		localAddr := fields[1]
		colonIdx := strings.LastIndex(localAddr, ":")
		if colonIdx == -1 {
			continue
		}

		// Extract IP address
		ipHex := localAddr[:colonIdx]
		ip := parseHexIP(ipHex)

		// Extract port
		portHex := localAddr[colonIdx+1:]
		port, err := strconv.ParseInt(portHex, 16, 32)
		if err != nil {
			continue
		}

		// Field index 9 is the inode
		inode, err := strconv.ParseUint(fields[9], 10, 64)
		if err != nil {
			continue
		}

		// Use port as key to deduplicate (same port on IPv4 and IPv6)
		if _, exists := sockets[int(port)]; !exists {
			sockets[int(port)] = socketInfo{
				port:  int(port),
				addr:  ip,
				inode: inode,
			}
		}
	}

	return scanner.Err()
}

// parseHexIP converts a hex IP address from /proc/net/tcp to a string
func parseHexIP(hexIP string) string {
	if len(hexIP) == 8 {
		// IPv4: stored in little-endian
		b := make([]byte, 4)
		for i := 0; i < 4; i++ {
			val, err := strconv.ParseUint(hexIP[i*2:i*2+2], 16, 8)
			if err != nil {
				return "0.0.0.0"
			}
			b[3-i] = byte(val)
		}
		return fmt.Sprintf("%d.%d.%d.%d", b[0], b[1], b[2], b[3])
	}
	if len(hexIP) == 32 {
		// IPv6: stored as 4 groups of 4 bytes, each group in little-endian
		b := make(net.IP, 16)
		for i := 0; i < 4; i++ {
			for j := 0; j < 4; j++ {
				val, err := strconv.ParseUint(hexIP[(i*8)+(j*2):(i*8)+(j*2)+2], 16, 8)
				if err != nil {
					return "[::]"
				}
				b[i*4+(3-j)] = byte(val)
			}
		}
		return b.String()
	}
	return "[::]"
}

// readProcessName reads the process name from /proc/[pid]/comm
func (d *linuxDiscoverer) readProcessName(pid int) string {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// readExecutablePath reads the executable path from /proc/[pid]/exe
func (d *linuxDiscoverer) readExecutablePath(pid int) string {
	link, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil {
		return ""
	}
	return link
}
