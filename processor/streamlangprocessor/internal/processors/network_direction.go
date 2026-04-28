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

package processors // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/processors"

import (
	"errors"
	"fmt"
	"net/netip"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

type compiledNetworkDirection struct {
	sourceIP              string
	destIP                string
	target                string
	prefixes              []netip.Prefix // resolved at compile time when networks are static
	namedGroups           []string       // names of static groups: private/public/loopback/link_local/etc.
	internalNetworksField string         // dynamic: read at execute time
	ignoreMissing         *bool
	ignoreFailure         bool
}

func compileNetworkDirection(p *dsl.NetworkDirectionProcessor) (Compiled, error) {
	if p.SourceIP == "" || p.DestinationIP == "" {
		return nil, errors.New("streamlang/network_direction: 'source_ip' and 'destination_ip' are required")
	}
	target := p.TargetField
	if target == "" {
		target = "network.direction"
	}
	c := &compiledNetworkDirection{
		sourceIP:              p.SourceIP,
		destIP:                p.DestinationIP,
		target:                target,
		internalNetworksField: p.InternalNetworksField,
		ignoreMissing:         p.IgnoreMissing,
		ignoreFailure:         p.IgnoreFailure,
	}
	for _, n := range p.InternalNetworks {
		if isNamedGroup(n) {
			c.namedGroups = append(c.namedGroups, n)
			continue
		}
		pfx, err := netip.ParsePrefix(n)
		if err != nil {
			return nil, fmt.Errorf("streamlang/network_direction: invalid CIDR %q: %w", n, err)
		}
		c.prefixes = append(c.prefixes, pfx)
	}
	return c, nil
}

func (c *compiledNetworkDirection) Action() string      { return "network_direction" }
func (c *compiledNetworkDirection) IgnoreFailure() bool { return c.ignoreFailure }

func (c *compiledNetworkDirection) Execute(d document.Document) error {
	srcStr, ok := readIPField(d, c.sourceIP)
	if !ok {
		if c.ignoreMissing != nil && *c.ignoreMissing {
			return &SkipError{Reason: "source_ip missing"}
		}
		return fmt.Errorf("streamlang/network_direction: field %q not found", c.sourceIP)
	}
	dstStr, ok := readIPField(d, c.destIP)
	if !ok {
		if c.ignoreMissing != nil && *c.ignoreMissing {
			return &SkipError{Reason: "destination_ip missing"}
		}
		return fmt.Errorf("streamlang/network_direction: field %q not found", c.destIP)
	}
	src, err := netip.ParseAddr(srcStr)
	if err != nil {
		return fmt.Errorf("streamlang/network_direction: invalid source IP %q", srcStr)
	}
	dst, err := netip.ParseAddr(dstStr)
	if err != nil {
		return fmt.Errorf("streamlang/network_direction: invalid destination IP %q", dstStr)
	}

	prefixes := c.prefixes
	groups := c.namedGroups
	if c.internalNetworksField != "" {
		dynamic, err := c.dynamicNetworks(d)
		if err != nil {
			return err
		}
		prefixes, groups = dynamic.prefixes, dynamic.groups
	}

	srcInternal := isInternal(src, prefixes, groups)
	dstInternal := isInternal(dst, prefixes, groups)

	var direction string
	switch {
	case srcInternal && dstInternal:
		direction = "internal"
	case srcInternal && !dstInternal:
		direction = "outbound"
	case !srcInternal && dstInternal:
		direction = "inbound"
	default:
		direction = "external"
	}
	return d.Set(c.target, document.StringValue(direction))
}

type networkSet struct {
	prefixes []netip.Prefix
	groups   []string
}

func (c *compiledNetworkDirection) dynamicNetworks(d document.Document) (networkSet, error) {
	v, ok := d.Get(c.internalNetworksField)
	if !ok {
		return networkSet{}, fmt.Errorf("streamlang/network_direction: internal_networks_field %q not found", c.internalNetworksField)
	}
	var entries []string
	if v.Type() == document.ValueTypeSlice {
		for _, it := range v.Slice() {
			entries = append(entries, it.Str())
		}
	} else {
		entries = append(entries, v.Str())
	}
	var ns networkSet
	for _, e := range entries {
		if isNamedGroup(e) {
			ns.groups = append(ns.groups, e)
			continue
		}
		pfx, err := netip.ParsePrefix(e)
		if err != nil {
			return networkSet{}, fmt.Errorf("streamlang/network_direction: invalid CIDR %q from %s: %w", e, c.internalNetworksField, err)
		}
		ns.prefixes = append(ns.prefixes, pfx)
	}
	return ns, nil
}

func readIPField(d document.Document, path string) (string, bool) {
	v, ok := d.Get(path)
	if !ok {
		return "", false
	}
	return v.Str(), true
}

func isNamedGroup(s string) bool {
	switch s {
	case "private", "public", "loopback", "link_local", "interface_local_multicast",
		"link_local_multicast", "global_unicast", "unspecified", "multicast", "broadcast":
		return true
	}
	return false
}

func isInternal(addr netip.Addr, prefixes []netip.Prefix, groups []string) bool {
	for _, p := range prefixes {
		if p.Contains(addr) {
			return true
		}
	}
	for _, g := range groups {
		if matchNamedGroup(addr, g) {
			return true
		}
	}
	return false
}

func matchNamedGroup(addr netip.Addr, group string) bool {
	switch group {
	case "private":
		return addr.IsPrivate()
	case "public":
		// "public" is the inverse of private + reserved spaces — treat as
		// "anything globally routable" via Is4() && !Private && !Loopback etc.
		return addr.IsGlobalUnicast() && !addr.IsPrivate()
	case "loopback":
		return addr.IsLoopback()
	case "link_local", "link_local_multicast":
		return addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast()
	case "multicast":
		return addr.IsMulticast()
	case "interface_local_multicast":
		return addr.IsInterfaceLocalMulticast()
	case "global_unicast":
		return addr.IsGlobalUnicast()
	case "unspecified":
		return addr.IsUnspecified()
	case "broadcast":
		// IPv4 broadcast is 255.255.255.255 (no Go stdlib helper).
		if !addr.Is4() {
			return false
		}
		b := addr.As4()
		return b == [4]byte{255, 255, 255, 255}
	}
	return false
}
