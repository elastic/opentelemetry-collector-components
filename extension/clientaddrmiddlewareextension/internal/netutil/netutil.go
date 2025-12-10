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

package netutil

import (
	"net"
	"net/http"
	"strconv"
	"strings"
)

// ClientAddrFromHeaders returns the IP address of the client
// for an HTTP request from one of various headers in order: Forwarded, X-Real-IP,
// X-Forwarded-For. For the multi-valued Forwarded and X-Forwarded-For headers, the
// first value in the list is returned.
//
// If the client is able to control the headers, they can control the result of this
// function. The result should therefore not necessarily be trusted to be correct;
// that depends on the presence and configuration of proxies in front of the server/collector.
//
// This code is from: https://github.com/elastic/apm-server/blob/main/internal/netutil/netutil.go
// and has been updated to return a `*net.IPAddr ` and no port.
func ClientAddrFromHeaders(header http.Header) *net.IPAddr {
	for _, parse := range parseHeadersInOrder {
		if ip := parse(header); ip != nil {
			return ip
		}
	}
	return nil
}

var parseHeadersInOrder = []func(http.Header) *net.IPAddr{
	parseForwardedHeader,
	parseXRealIP,
	parseXForwardedFor,
}

func parseForwardedHeader(header http.Header) *net.IPAddr {
	forwarded := parseForwarded(getHeader(header, "Forwarded", "forwarded"))
	if forwarded.For == "" {
		return nil
	}

	return SplitAddrPort(forwarded.For)
}

func parseXRealIP(header http.Header) *net.IPAddr {
	return SplitAddrPort(getHeader(header, "X-Real-Ip", "x-real-ip"))
}

func parseXForwardedFor(header http.Header) *net.IPAddr {
	if xff := getHeader(header, "X-Forwarded-For", "x-forwarded-for"); xff != "" {
		if sep := strings.IndexRune(xff, ','); sep > 0 {
			xff = xff[:sep]
		}
		return SplitAddrPort(strings.TrimSpace(xff))
	}
	return nil
}

func getHeader(header http.Header, key, keyLower string) string {
	if v := header.Get(key); v != "" {
		return v
	}

	// header.Get() internally canonicalizes key names, but metadata.Pairs uses
	// lowercase keys. Using the lowercase key name allows this function to be
	// used for gRPC metadata.
	if v, ok := header[keyLower]; ok && len(v) > 0 {
		return v[0]
	}
	return ""
}

// forwardedHeader holds information extracted from a "Forwarded" HTTP header.
type forwardedHeader struct {
	For   string
	Host  string
	Proto string
}

// parseForwarded parses a "Forwarded" HTTP header.
func parseForwarded(f string) forwardedHeader {
	// We only consider the first value in the sequence,
	// if there are multiple. Disregard everything after
	// the first comma.
	if comma := strings.IndexRune(f, ','); comma != -1 {
		f = f[:comma]
	}
	var result forwardedHeader
	for f != "" {
		field := f
		if semi := strings.IndexRune(f, ';'); semi != -1 {
			field = f[:semi]
			f = f[semi+1:]
		} else {
			f = ""
		}
		eq := strings.IndexRune(field, '=')
		if eq == -1 {
			// Malformed field, ignore.
			continue
		}
		key := strings.TrimSpace(field[:eq])
		value := strings.TrimSpace(field[eq+1:])
		if len(value) > 0 && value[0] == '"' {
			var err error
			value, err = strconv.Unquote(value)
			if err != nil {
				// Malformed, ignore
				continue
			}
		}
		switch {
		case strings.EqualFold(key, "for"):
			result.For = value
		case strings.EqualFold(key, "host"):
			result.Host = value
		case strings.EqualFold(key, "proto"):
			result.Proto = value
		}
	}
	return result
}

// SplitAddrPort splits a network address of the form "host",
// "host:port", "[host]:port" or "[host]:port" into a net.IPAddr.
//
// The port portion is ignored. If input cannot be parsed or it is empty,
// nil will be returned.
func SplitAddrPort(in string) *net.IPAddr {
	if in == "" {
		return nil
	}

	var host string
	// Try to split host and port using net.SplitHostPort
	// This handles: "host:port", "[host]:port", and "host" correctly
	if h, _, err := net.SplitHostPort(in); err == nil {
		host = h
	} else {
		// If SplitHostPort fails, it might be just a host without port
		// Try parsing the whole string as an IP
		host = in
	}

	// Parse the IP address
	ip := net.ParseIP(host)
	if ip == nil {
		return nil
	}

	return &net.IPAddr{IP: ip}
}
