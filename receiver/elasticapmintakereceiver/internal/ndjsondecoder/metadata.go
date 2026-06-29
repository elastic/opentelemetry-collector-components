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

package ndjsondecoder

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv22 "go.opentelemetry.io/otel/semconv/v1.22.0"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
)

// DecodeMetadata reads the next NDJSON line from dec and decodes it as a metadata event.
func DecodeMetadata(dec *NDJSONStreamDecoder) (*metadata, error) {
	var root metadataRoot
	if err := dec.Decode(&root); err != nil {
		return nil, err
	}
	if err := root.validate(); err != nil {
		return nil, fmt.Errorf("validation error: %w", err)
	}
	if err := root.processNestedSource(); err != nil {
		return nil, fmt.Errorf("validation error: %w", err)
	}
	return &root.Metadata, nil
}

// GlobalLabelKeys returns the sorted keys from m.Labels. All metadata labels
// are global; any event whose tags shadow these keys is routed to a shadow batch.
func GlobalLabelKeys(m *metadata) []string {
	if len(m.Labels) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m.Labels))
	for k := range m.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// WriteResourceAttributes writes the resource attributes for m into attrs.
// svc, if non-nil, carries per-event service overrides that take precedence
// over the metadata service fields.
func WriteResourceAttributes(m *metadata, svc *contextService, attrs pcommon.Map) {
	walkMetadataResourceAttributes(m, svc, mapWriter{m: attrs})
}

// FingerprintMetadata returns a stable hash of the resource attributes that
// WriteResourceAttributes would produce for the same inputs. h is reset before use.
func FingerprintMetadata(m *metadata, svc *contextService, h *xxhash.Digest) uint64 {
	h.Reset()
	walkMetadataResourceAttributes(m, svc, hashWriter{h: h})
	return h.Sum64()
}

// attrWriter is the local visitor used by both WriteResourceAttributes (via
// mapWriter) and FingerprintMetadata (via hashWriter).
type attrWriter interface {
	putStr(key, value string)
	putInt(key string, value int64)
	putDouble(key string, value float64)
	putStrSlice(key string, values []string)
}

// putStrNonEmpty mirrors the resource_walker helper: skip empty strings so
// "field present but blank" is indistinguishable from "field absent", matching
// the receiver's historical pcommon behaviour.
func putStrNonEmpty(v attrWriter, key, value string) {
	if value != "" {
		v.putStr(key, value)
	}
}

type mapWriter struct{ m pcommon.Map }

func (w mapWriter) putStr(key, value string)        { w.m.PutStr(key, value) }
func (w mapWriter) putInt(key string, v int64)      { w.m.PutInt(key, v) }
func (w mapWriter) putDouble(key string, v float64) { w.m.PutDouble(key, v) }
func (w mapWriter) putStrSlice(key string, values []string) {
	sl := w.m.PutEmptySlice(key)
	sl.EnsureCapacity(len(values))
	for _, s := range values {
		sl.AppendEmpty().SetStr(s)
	}
}

// fpKVSep and fpEntrySep separate key/value pairs and entries in the hash stream.
var (
	fpKVSep    = []byte{0x00}
	fpEntrySep = []byte{0x01}
)

type hashWriter struct{ h *xxhash.Digest }

func (w hashWriter) putStr(key, value string) {
	_, _ = w.h.WriteString(key)
	_, _ = w.h.Write(fpKVSep)
	_, _ = w.h.WriteString(value)
	_, _ = w.h.Write(fpEntrySep)
}

func (w hashWriter) putInt(key string, value int64) {
	_, _ = w.h.WriteString(key)
	_, _ = w.h.Write(fpKVSep)
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(value))
	_, _ = w.h.Write(buf[:])
	_, _ = w.h.Write(fpEntrySep)
}

func (w hashWriter) putDouble(key string, value float64) {
	_, _ = w.h.WriteString(key)
	_, _ = w.h.Write(fpKVSep)
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(value))
	_, _ = w.h.Write(buf[:])
	_, _ = w.h.Write(fpEntrySep)
}

func (w hashWriter) putStrSlice(key string, values []string) {
	_, _ = w.h.WriteString(key)
	_, _ = w.h.Write(fpKVSep)
	for _, s := range values {
		_, _ = w.h.WriteString(s)
		_, _ = w.h.Write(fpKVSep)
	}
	_, _ = w.h.Write(fpEntrySep)
}

// walkMetadataResourceAttributes visits every resource attribute that
// WriteResourceAttributes would write. Order mirrors WalkResourceAttributes
// in mappers/resource_walker.go for fingerprint compatibility.
//
// FaaS, UserAgent, Client, Source, and Destination are per-event only and
// are not present in metadata; they are handled by the per-event converters.
func walkMetadataResourceAttributes(m *metadata, svc *contextService, v attrWriter) {
	walkMetadataService(m, svc, v)
	walkMetadataHost(m, v)
	walkMetadataCloud(m, v)
	walkMetadataContainerKubernetes(m, v)
	walkMetadataProcessUser(m, v)
	walkMetadataAgent(m, svc, v)
	walkMetadataLabels(m, v)
}

func walkMetadataService(m *metadata, svc *contextService, v attrWriter) {
	// Start with metadata defaults; override with contextService when IsSet.
	name := m.Service.Name.Val
	version := m.Service.Version.Val
	langName := m.Service.Language.Name.Val
	langVer := m.Service.Language.Version.Val
	rtName := m.Service.Runtime.Name.Val
	rtVer := m.Service.Runtime.Version.Val
	environment := m.Service.Environment.Val
	nodeName := m.Service.Node.Name.Val
	fwName := m.Service.Framework.Name.Val
	fwVer := m.Service.Framework.Version.Val
	var originID, originName, originVer string

	if svc != nil {
		if svc.Name.IsSet() {
			name = svc.Name.Val
			if !svc.Version.IsSet() {
				version = "" // don't inherit base version when name is overridden
			}
		}
		if svc.Version.IsSet() {
			version = svc.Version.Val
		}
		if svc.Language.Name.IsSet() {
			langName = svc.Language.Name.Val
		}
		if svc.Language.Version.IsSet() {
			langVer = svc.Language.Version.Val
		}
		if svc.Runtime.Name.IsSet() {
			rtName = svc.Runtime.Name.Val
		}
		if svc.Runtime.Version.IsSet() {
			rtVer = svc.Runtime.Version.Val
		}
		if svc.Environment.IsSet() {
			environment = svc.Environment.Val
		}
		if svc.Node.Name.IsSet() {
			nodeName = svc.Node.Name.Val
		}
		if svc.Framework.Name.IsSet() {
			fwName = svc.Framework.Name.Val
		}
		if svc.Framework.Version.IsSet() {
			fwVer = svc.Framework.Version.Val
		}
		originID = svc.Origin.ID.Val
		originName = svc.Origin.Name.Val
		originVer = svc.Origin.Version.Val
	}

	putStrNonEmpty(v, string(semconv.ServiceNameKey), name)
	putStrNonEmpty(v, string(semconv.ServiceVersionKey), version)
	if langName != "" {
		putStrNonEmpty(v, string(semconv.TelemetrySDKLanguageKey), langName)
		putStrNonEmpty(v, string(semconv.TelemetrySDKVersionKey), langVer)
	}
	if rtName != "" {
		putStrNonEmpty(v, string(semconv.ProcessRuntimeNameKey), rtName)
		putStrNonEmpty(v, string(semconv.ProcessRuntimeVersionKey), rtVer)
	}
	// TelemetrySDKNameKey is always written (matches original walkAgentResource behaviour).
	v.putStr(string(semconv.TelemetrySDKNameKey), "ElasticAPM")
	if environment != "" {
		v.putStr(string(semconv22.DeploymentEnvironmentKey), environment)
		v.putStr(string(semconv.DeploymentEnvironmentNameKey), environment)
	}
	putStrNonEmpty(v, string(semconv.ServiceInstanceIDKey), nodeName)
	if fwName != "" {
		putStrNonEmpty(v, elasticattr.ServiceFrameworkName, fwName)
		putStrNonEmpty(v, elasticattr.ServiceFrameworkVersion, fwVer)
	}
	putStrNonEmpty(v, elasticattr.ServiceOriginID, originID)
	putStrNonEmpty(v, elasticattr.ServiceOriginName, originName)
	putStrNonEmpty(v, elasticattr.ServiceOriginVersion, originVer)
}

func walkMetadataHost(m *metadata, v attrWriter) {
	// host.name: only from configured_hostname (explicit admin name).
	// host.hostname: prefer detected, fall back to deprecated (both are machine-observed names).
	putStrNonEmpty(v, string(semconv.HostNameKey), m.System.ConfiguredHostname.Val)
	putStrNonEmpty(v, string(semconv.HostIDKey), m.System.HostID.Val)
	putStrNonEmpty(v, string(semconv.HostArchKey), m.System.Architecture.Val)
	// os.name and os.version are not available in metadata.system; only os.type (platform) is.
	putStrNonEmpty(v, string(semconv.OSTypeKey), m.System.Platform.Val)
	hostHostname := m.System.DetectedHostname.Val
	if hostHostname == "" {
		hostHostname = m.System.DeprecatedHostname.Val
	}
	putStrNonEmpty(v, elasticattr.HostHostName, hostHostname)
}

func walkMetadataCloud(m *metadata, v attrWriter) {
	c := &m.Cloud
	putStrNonEmpty(v, string(semconv.CloudProviderKey), c.Provider.Val)
	putStrNonEmpty(v, string(semconv.CloudRegionKey), c.Region.Val)
	putStrNonEmpty(v, string(semconv.CloudAvailabilityZoneKey), c.AvailabilityZone.Val)
	putStrNonEmpty(v, string(semconv.CloudAccountIDKey), c.Account.ID.Val)
	putStrNonEmpty(v, string(semconv.CloudPlatformKey), c.Service.Name.Val)
	// Cloud origin is per-event only (not in metadata); handled by event converters.
	putStrNonEmpty(v, elasticattr.CloudAccountName, c.Account.Name.Val)
	putStrNonEmpty(v, elasticattr.CloudInstanceID, c.Instance.ID.Val)
	putStrNonEmpty(v, elasticattr.CloudInstanceName, c.Instance.Name.Val)
	putStrNonEmpty(v, elasticattr.CloudMachineType, c.Machine.Type.Val)
	putStrNonEmpty(v, elasticattr.CloudProjectID, c.Project.ID.Val)
	putStrNonEmpty(v, elasticattr.CloudProjectName, c.Project.Name.Val)
}

func walkMetadataContainerKubernetes(m *metadata, v attrWriter) {
	putStrNonEmpty(v, string(semconv.ContainerIDKey), m.System.Container.ID.Val)
	// Container Name, Runtime, ImageName, ImageTag are not in metadata.system.container.
	putStrNonEmpty(v, string(semconv.K8SNamespaceNameKey), m.System.Kubernetes.Namespace.Val)
	putStrNonEmpty(v, string(semconv.K8SNodeNameKey), m.System.Kubernetes.Node.Name.Val)
	putStrNonEmpty(v, string(semconv.K8SPodNameKey), m.System.Kubernetes.Pod.Name.Val)
	putStrNonEmpty(v, string(semconv.K8SPodUIDKey), m.System.Kubernetes.Pod.UID.Val)
}

func walkMetadataProcessUser(m *metadata, v attrWriter) {
	if m.Process.Pid.Val != 0 {
		v.putInt(string(semconv.ProcessPIDKey), int64(m.Process.Pid.Val))
	}
	if m.Process.Ppid.Val != 0 {
		v.putInt(string(semconv.ProcessParentPIDKey), int64(m.Process.Ppid.Val))
	}
	putStrNonEmpty(v, string(semconv.ProcessExecutableNameKey), m.Process.Title.Val)
	if len(m.Process.Argv) > 0 {
		v.putStrSlice(string(semconv.ProcessCommandLineKey), m.Process.Argv)
	}
	// process.executable.path is not in metadata.process.

	// User fields.
	if m.User.ID.IsSet() {
		switch id := m.User.ID.Val.(type) {
		case string:
			putStrNonEmpty(v, string(semconv.UserIDKey), id)
		case json.Number:
			putStrNonEmpty(v, string(semconv.UserIDKey), id.String())
		}
	}
	putStrNonEmpty(v, string(semconv.UserEmailKey), m.User.Email.Val)
	putStrNonEmpty(v, string(semconv.UserNameKey), m.User.Name.Val)
	putStrNonEmpty(v, elasticattr.UserDomain, m.User.Domain.Val)

	// Network: only connection.type is available in metadata.
	// connection.subtype and carrier fields are per-event only.
	putStrNonEmpty(v, string(semconv.NetworkConnectionTypeKey), m.Network.Connection.Type.Val)
}

func walkMetadataAgent(m *metadata, svc *contextService, v attrWriter) {
	a := &m.Service.Agent
	name := a.Name.Val
	version := a.Version.Val
	ephemeral := a.EphemeralID.Val
	actMethod := a.ActivationMethod.Val
	if svc != nil {
		if svc.Agent.Name.IsSet() {
			name = svc.Agent.Name.Val
		}
		if svc.Agent.Version.IsSet() {
			version = svc.Agent.Version.Val
		}
		if svc.Agent.EphemeralID.IsSet() {
			ephemeral = svc.Agent.EphemeralID.Val
		}
	}
	// AgentName and AgentVersion are always written (even if empty),
	// matching walkAgentResource which uses v.PutStr not putNonEmpty.
	v.putStr(elasticattr.AgentName, name)
	v.putStr(elasticattr.AgentVersion, version)
	putStrNonEmpty(v, elasticattr.AgentEphemeralID, ephemeral)
	putStrNonEmpty(v, elasticattr.AgentActivationMethod, actMethod)
}

// walkMetadataLabels visits Labels in two sorted passes — string/bool then
// numeric — matching the two-pass order in walkLabelsResource.
func walkMetadataLabels(m *metadata, v attrWriter) {
	if len(m.Labels) == 0 {
		return
	}

	strKeys, numKeys := partitionTagKeys(m.Labels)
	for _, k := range strKeys {
		switch sv := m.Labels[k].(type) {
		case string:
			putStrNonEmpty(v, "labels."+k, sv)
		case bool:
			if sv {
				v.putStr("labels."+k, "true")
			} else {
				v.putStr("labels."+k, "false")
			}
		}
	}
	for _, k := range numKeys {
		n := m.Labels[k].(json.Number)
		if f, err := n.Float64(); err == nil {
			v.putDouble("numeric_labels."+k, f)
		}
	}
}
