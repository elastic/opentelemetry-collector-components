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

// Package kubetunnel holds the transport contract shared between the relay
// service and the in-cluster probe extension: the command/result schema that is
// JSON-encoded into the opaque gRPC frames, and the relay-side probe registry.
package kubetunnel // import "github.com/elastic/opentelemetry-collector-components/internal/kubetunnel"

import (
	"encoding/json"
	"time"
)

// Read-only operations the agent is allowed to perform. Anything outside this
// set is rejected before any Kubernetes API call is made.
const (
	OpGet  = "get"
	OpList = "list"
	OpLogs = "logs"
)

// ReadRequest is the command the SRE agent sends. It is JSON-encoded into
// ServerFrame.request_json and decoded by the in-cluster executor.
type ReadRequest struct {
	// Operation is one of OpGet, OpList, OpLogs.
	Operation string `json:"operation"`
	// Resource is the Kubernetes resource, e.g. "pods" or "deployments.apps".
	// Used by get/list.
	Resource string `json:"resource,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	// Name is required for get and logs; empty for list.
	Name string `json:"name,omitempty"`
	LabelSelector string `json:"labelSelector,omitempty"`
	FieldSelector string `json:"fieldSelector,omitempty"`
	// Container and TailLines apply to logs only.
	Container string `json:"container,omitempty"`
	TailLines int64 `json:"tailLines,omitempty"`
}

// ReadResult is the answer streamed back up the tunnel. It is JSON-encoded into
// ClientFrame.result_json.
type ReadResult struct {
	// JSON is the result payload, always valid JSON so it embeds directly in the
	// relay's HTTP response: a Kubernetes object/list for get/list, or a JSON
	// string for logs.
	JSON json.RawMessage `json:"json,omitempty"`
	// Error is non-empty if the read failed.
	Error string `json:"error,omitempty"`
}

// ProbeInfo is the public metadata describing one connected collector ("probe").
// It is the element type returned by the relay's GET /targets discovery endpoint
// (each probe is a candidate target the external AI agent can pick).
type ProbeInfo struct {
	ProbeID          string            `json:"probeId"`
	ClusterID        string            `json:"clusterId"`
	NodeName         string            `json:"nodeName,omitempty"`
	PodName          string            `json:"podName,omitempty"`
	Namespace        string            `json:"namespace,omitempty"`
	CollectorVersion string            `json:"collectorVersion,omitempty"`
	K8sVersion       string            `json:"k8sVersion,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
	ConnectedAt      time.Time         `json:"connectedAt"`
}
