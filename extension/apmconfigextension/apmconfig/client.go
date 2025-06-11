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

package apmconfig // import "github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"

import (
	"context"
	"errors"

	"github.com/open-telemetry/opamp-go/protobufs"
)

var UnidentifiedAgent = errors.New("agent could not be identified")

// protobufs.AgentToServer.InstanceUid
type (
	InstanceUid           []byte
	IdentifyingAttributes []*protobufs.KeyValue
)

type Query struct {
	InstanceUid           []byte
	IdentifyingAttributes []*protobufs.KeyValue
	LastConfigHash        []byte
}

// RemoteConfigClient is an adapter interface that can be used between different
// remote configuration providers.
type RemoteConfigClient interface {
	// RemoteConfig returns the upstream remote configuration that needs to be applied. Empty RemoteConfig Attrs if no remote configuration is available for the specified service.
	RemoteConfig(context.Context, Query) (*protobufs.AgentRemoteConfig, error)
}
