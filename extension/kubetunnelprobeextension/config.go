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

package kubetunnelprobeextension // import "github.com/elastic/opentelemetry-collector-components/extension/kubetunnelprobeextension"

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/config/configopaque"
)

// Config configures the in-cluster probe: how it dials out to the relay, the
// identifying metadata it registers with, and how it reaches the Kubernetes API.
type Config struct {
	// Relay is the outbound gRPC connection to the public relay. At minimum set
	// Relay.Endpoint; use Relay.TLS for transport security.
	Relay configgrpc.ClientConfig `mapstructure:"relay"`

	// AuthToken is the bearer token presented to the relay on registration
	// (stubbed auth for the PoC).
	AuthToken configopaque.String `mapstructure:"auth_token"`

	// ClusterID is the logical cluster identity reported in discovery metadata.
	ClusterID string `mapstructure:"cluster_id"`
	// ProbeID uniquely identifies this probe. If empty it defaults to
	// "<namespace>/<pod_name>" (or a random id when those are unset).
	ProbeID string `mapstructure:"probe_id"`
	// NodeName/PodName/Namespace are reported as discovery metadata. In Kubernetes
	// wire these from the Downward API via ${env:NODE_NAME} etc.
	NodeName  string `mapstructure:"node_name"`
	PodName   string `mapstructure:"pod_name"`
	Namespace string `mapstructure:"namespace"`

	// ReconnectBackoff is how long to wait before redialing the relay after the
	// tunnel drops.
	ReconnectBackoff time.Duration `mapstructure:"reconnect_backoff"`

	// Kubeconfig is the path to a kubeconfig file. Empty means in-cluster config.
	Kubeconfig string `mapstructure:"kubeconfig"`
	// AllowedNamespaces, when non-empty, restricts reads to these namespaces
	// (an extra guardrail on top of the ServiceAccount's RBAC).
	AllowedNamespaces []string `mapstructure:"allowed_namespaces"`
}

// Validate checks the configuration.
func (c *Config) Validate() error {
	if c.Relay.Endpoint == "" {
		return errors.New("relay.endpoint is required")
	}
	if c.ReconnectBackoff < 0 {
		return errors.New("reconnect_backoff must not be negative")
	}
	return nil
}
