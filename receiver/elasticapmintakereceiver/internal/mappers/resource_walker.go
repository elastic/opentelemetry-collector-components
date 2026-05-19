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

package mappers // import "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal/mappers"

import (
	"net/netip"
	"slices"
	"sort"

	semconv22 "go.opentelemetry.io/otel/semconv/v1.22.0"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"

	"github.com/elastic/apm-data/model/modelpb"
	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
)

// ResourceAttrVisitor is the abstract sink for resource-attribute writes.
// WalkResourceAttributes is the SINGLE source of truth for which APM event
// fields contribute to resource attributes; both setResourceAttributes
// (which writes into a pcommon.Map) and resourceFingerprint (which writes
// into an xxhash.Digest) are implemented as visitors over the same walker.
//
// Adding a new resource attribute: edit WalkResourceAttributes only — both
// consumers pick the change up automatically.
type ResourceAttrVisitor interface {
	// PutStr writes a string attribute. Walker callers skip empty values
	// where the historical pcommon behaviour was "set only if non-empty".
	PutStr(key, value string)
	// PutInt writes an integer attribute.
	PutInt(key string, value int64)
	// PutBool writes a bool attribute.
	PutBool(key string, value bool)
	// PutDouble writes a float64 attribute (used for numeric labels).
	PutDouble(key string, value float64)
	// PutStrSlice writes a string-slice attribute (used for Process.Argv
	// and multi-valued labels).
	PutStrSlice(key string, values []string)
}

// WalkResourceAttributes invokes v with every (key, value) pair that the
// receiver writes onto the OTel Resource for an APM event. Order is
// deterministic for stable hashing — Labels and NumericLabels are visited
// in sorted-key order; everything else is in source order.
func WalkResourceAttributes(event *modelpb.APMEvent, v ResourceAttrVisitor) {
	walkServiceResource(event, v)
	walkHostAndUserAgentResource(event, v)
	walkCloudResource(event, v)
	walkContainerKubernetesResource(event, v)
	walkProcessUserNetworkResource(event, v)
	walkFaasResource(event, v)
	walkAgentResource(event, v)
	walkLabelsResource(event, v)
}

func walkServiceResource(event *modelpb.APMEvent, v ResourceAttrVisitor) {
	s := event.Service
	if s == nil {
		return
	}
	putNonEmpty(v, string(semconv.ServiceNameKey), s.Name)
	putNonEmpty(v, string(semconv.ServiceVersionKey), s.Version)
	if l := s.Language; l != nil {
		putNonEmpty(v, string(semconv.TelemetrySDKLanguageKey), l.Name)
		putNonEmpty(v, string(semconv.TelemetrySDKVersionKey), l.Version)
	}
	if rt := s.Runtime; rt != nil {
		putNonEmpty(v, string(semconv.ProcessRuntimeNameKey), rt.Name)
		putNonEmpty(v, string(semconv.ProcessRuntimeVersionKey), rt.Version)
	}
	v.PutStr(string(semconv.TelemetrySDKNameKey), "ElasticAPM")
	if s.Environment != "" {
		// elasticsearchexporter currently uses v1.22.0 of the OTel
		// SemConv, so we need to include the v1.22.0 attribute alongside
		// the current name.
		v.PutStr(string(semconv22.DeploymentEnvironmentKey), s.Environment)
		v.PutStr(string(semconv.DeploymentEnvironmentNameKey), s.Environment)
	}
	if n := s.Node; n != nil {
		putNonEmpty(v, string(semconv.ServiceInstanceIDKey), n.Name)
	}
	if f := s.Framework; f != nil {
		putNonEmpty(v, elasticattr.ServiceFrameworkName, f.Name)
		putNonEmpty(v, elasticattr.ServiceFrameworkVersion, f.Version)
	}
	if o := s.Origin; o != nil {
		putNonEmpty(v, elasticattr.ServiceOriginID, o.Id)
		putNonEmpty(v, elasticattr.ServiceOriginName, o.Name)
		putNonEmpty(v, elasticattr.ServiceOriginVersion, o.Version)
	}
}

func walkHostAndUserAgentResource(event *modelpb.APMEvent, v ResourceAttrVisitor) {
	if h := event.Host; h != nil {
		putNonEmpty(v, string(semconv.HostNameKey), h.Name)
		putNonEmpty(v, string(semconv.HostIDKey), h.Id)
		putNonEmpty(v, string(semconv.HostArchKey), h.Architecture)
		if os := h.Os; os != nil {
			putNonEmpty(v, string(semconv.OSNameKey), os.Name)
			putNonEmpty(v, string(semconv.OSTypeKey), os.Platform)
			putNonEmpty(v, string(semconv.OSVersionKey), os.Version)
		}
		putNonEmpty(v, elasticattr.HostHostName, h.Hostname)
	}

	// UserAgent fields are only expected to be available for error and
	// transaction events. Translating here since fields should be present
	// at the resource level. https://opentelemetry.io/docs/specs/semconv/registry/attributes/user-agent
	if ua := event.UserAgent; ua != nil {
		putNonEmpty(v, string(semconv.UserAgentOriginalKey), ua.Original)
	}
}

func walkCloudResource(event *modelpb.APMEvent, v ResourceAttrVisitor) {
	c := event.Cloud
	if c == nil {
		return
	}
	putNonEmpty(v, string(semconv.CloudProviderKey), c.Provider)
	putNonEmpty(v, string(semconv.CloudRegionKey), c.Region)
	putNonEmpty(v, string(semconv.CloudAvailabilityZoneKey), c.AvailabilityZone)
	putNonEmpty(v, string(semconv.CloudAccountIDKey), c.AccountId)
	putNonEmpty(v, string(semconv.CloudPlatformKey), c.ServiceName)
	if o := c.Origin; o != nil {
		putNonEmpty(v, elasticattr.CloudOriginAccountID, o.AccountId)
		putNonEmpty(v, elasticattr.CloudOriginProvider, o.Provider)
		putNonEmpty(v, elasticattr.CloudOriginRegion, o.Region)
		putNonEmpty(v, elasticattr.CloudOriginServiceName, o.ServiceName)
	}
	putNonEmpty(v, elasticattr.CloudAccountName, c.AccountName)
	putNonEmpty(v, elasticattr.CloudInstanceID, c.InstanceId)
	putNonEmpty(v, elasticattr.CloudInstanceName, c.InstanceName)
	putNonEmpty(v, elasticattr.CloudMachineType, c.MachineType)
	putNonEmpty(v, elasticattr.CloudProjectID, c.ProjectId)
	putNonEmpty(v, elasticattr.CloudProjectName, c.ProjectName)
}

func walkContainerKubernetesResource(event *modelpb.APMEvent, v ResourceAttrVisitor) {
	if c := event.Container; c != nil {
		putNonEmpty(v, string(semconv.ContainerIDKey), c.Id)
		putNonEmpty(v, string(semconv.ContainerNameKey), c.Name)
		putNonEmpty(v, string(semconv.ContainerRuntimeKey), c.Runtime)
		putNonEmpty(v, string(semconv.ContainerImageNameKey), c.ImageName)
		putNonEmpty(v, string(semconv.ContainerImageTagsKey), c.ImageTag)
	}
	if k := event.Kubernetes; k != nil {
		putNonEmpty(v, string(semconv.K8SNamespaceNameKey), k.Namespace)
		putNonEmpty(v, string(semconv.K8SNodeNameKey), k.NodeName)
		putNonEmpty(v, string(semconv.K8SPodNameKey), k.PodName)
		putNonEmpty(v, string(semconv.K8SPodUIDKey), k.PodUid)
	}
}

func walkProcessUserNetworkResource(event *modelpb.APMEvent, v ResourceAttrVisitor) {
	if p := event.Process; p != nil {
		if p.Pid != 0 {
			v.PutInt(string(semconv.ProcessPIDKey), int64(p.Pid))
		}
		if p.Ppid != 0 {
			v.PutInt(string(semconv.ProcessParentPIDKey), int64(p.Ppid))
		}
		putNonEmpty(v, string(semconv.ProcessExecutableNameKey), p.Title)
		if len(p.Argv) > 0 {
			v.PutStrSlice(string(semconv.ProcessCommandLineKey), p.Argv)
		}
		putNonEmpty(v, string(semconv.ProcessExecutablePathKey), p.Executable)
	}

	if u := event.User; u != nil {
		// translate user fields defined here:
		// https://opentelemetry.io/docs/specs/semconv/registry/attributes/user
		putNonEmpty(v, string(semconv.UserIDKey), u.Id)
		putNonEmpty(v, string(semconv.UserEmailKey), u.Email)
		putNonEmpty(v, string(semconv.UserNameKey), u.Name)
		putNonEmpty(v, elasticattr.UserDomain, u.Domain)
	}

	if n := event.Network; n != nil {
		// translate network fields:
		// https://opentelemetry.io/docs/specs/semconv/registry/attributes/network
		if c := n.Connection; c != nil {
			putNonEmpty(v, string(semconv.NetworkConnectionTypeKey), c.Type)
			putNonEmpty(v, string(semconv.NetworkConnectionSubtypeKey), c.Subtype)
		}
		if cr := n.Carrier; cr != nil {
			putNonEmpty(v, string(semconv.NetworkCarrierNameKey), cr.Name)
			putNonEmpty(v, string(semconv.NetworkCarrierMccKey), cr.Mcc)
			putNonEmpty(v, string(semconv.NetworkCarrierMncKey), cr.Mnc)
			putNonEmpty(v, string(semconv.NetworkCarrierIccKey), cr.Icc)
		}
	}

	if c := event.Client; c != nil {
		putIPAddress(v, string(semconv.ClientAddressKey), c.Ip)
		if c.Port != 0 {
			v.PutInt(string(semconv.ClientPortKey), int64(c.Port))
		}
	}

	if s := event.Source; s != nil {
		putIPAddress(v, string(semconv.SourceAddressKey), s.Ip)
		if s.Port != 0 {
			v.PutInt(string(semconv.SourcePortKey), int64(s.Port))
		}
		if nat := s.Nat; nat != nil {
			putIPAddress(v, elasticattr.SourceNATIP, nat.Ip)
		}
	}

	if d := event.Destination; d != nil && d.Address != "" {
		if ip, err := netip.ParseAddr(d.Address); err == nil {
			v.PutStr(elasticattr.DestinationIP, ip.String())
		}
	}
}

func walkFaasResource(event *modelpb.APMEvent, v ResourceAttrVisitor) {
	f := event.Faas
	if f == nil {
		return
	}
	putNonEmpty(v, string(semconv.FaaSInstanceKey), f.Id)
	putNonEmpty(v, string(semconv.FaaSNameKey), f.Name)
	putNonEmpty(v, string(semconv.FaaSVersionKey), f.Version)
	putNonEmpty(v, string(semconv.FaaSTriggerKey), f.TriggerType)
	if f.ColdStart != nil {
		v.PutBool(string(semconv.FaaSColdstartKey), *f.ColdStart)
	}
	putNonEmpty(v, elasticattr.FaaSTriggerRequestID, f.TriggerRequestId)
	putNonEmpty(v, elasticattr.FaaSExecution, f.Execution)
}

func walkAgentResource(event *modelpb.APMEvent, v ResourceAttrVisitor) {
	a := event.Agent
	if a == nil {
		return
	}
	v.PutStr(elasticattr.AgentName, a.Name)
	v.PutStr(elasticattr.AgentVersion, a.Version)
	putNonEmpty(v, elasticattr.AgentEphemeralID, a.EphemeralId)
	putNonEmpty(v, elasticattr.AgentActivationMethod, a.ActivationMethod)
}

// walkLabelsResource visits Labels and NumericLabels in sorted-key order so
// hash-based visitors produce deterministic output. The pcommon visitor
// does not depend on order; the hash visitor relies on it.
//
// A 16-entry stack buffer covers typical agent label counts; slices.Grow
// promotes to the heap only when an event carries an unusually large set.
func walkLabelsResource(event *modelpb.APMEvent, v ResourceAttrVisitor) {
	if len(event.Labels) > 0 {
		var buf [16]string
		keys := slices.Grow(buf[:0], len(event.Labels))
		for k := range event.Labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			lv := event.Labels[k]
			if k == "" || lv == nil {
				continue
			}
			// apm-data decode paths normally set either LabelValue.Value or
			// LabelValue.Values for a key, not both. If both are present
			// (e.g. from a non-standard producer), prefer Values.
			if len(lv.Values) > 0 {
				v.PutStrSlice("labels."+k, lv.Values)
				continue
			}
			if lv.Value != "" {
				v.PutStr("labels."+k, lv.Value)
			}
		}
	}

	if len(event.NumericLabels) > 0 {
		var buf [16]string
		keys := slices.Grow(buf[:0], len(event.NumericLabels))
		for k := range event.NumericLabels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			nv := event.NumericLabels[k]
			if k == "" || nv == nil {
				continue
			}
			v.PutDouble("numeric_labels."+k, nv.Value)
		}
	}
}

// putNonEmpty mirrors the pre-walker putNonEmptyStr helper: write only
// when value is non-empty. Skipping on empty makes "field present but
// blank" indistinguishable from "field absent", which preserves the
// receiver's historical pcommon behaviour. Hash visitors rely on this
// uniform skipping to produce identical fingerprints for events that
// would produce identical resource attribute maps.
func putNonEmpty(v ResourceAttrVisitor, key, value string) {
	if value != "" {
		v.PutStr(key, value)
	}
}

// putIPAddress is the walker analogue of translateIPAddress. The IP is
// formatted exactly the way the receiver does today.
func putIPAddress(v ResourceAttrVisitor, key string, ip *modelpb.IP) {
	if ip == nil {
		return
	}
	addr := modelpb.IP2Addr(ip)
	s := addr.String()
	if s != "" {
		v.PutStr(key, s)
	}
}
