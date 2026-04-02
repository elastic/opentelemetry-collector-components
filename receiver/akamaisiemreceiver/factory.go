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

package akamaisiemreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver"

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/sharedcomponent"
)

// receivers holds shared receiver instances keyed by API connection identity.
// Two named instances (e.g. akamai_siem/ecs and akamai_siem/otel) that share
// the same endpoint, config_ids, and auth credentials will share a single
// poller, cursor, and API connection.
var receivers = sharedcomponent.NewSharedMap[connectionKey, *akamaiReceiver]()

// connectionKey identifies a unique Akamai API connection. Instances with the
// same key share a single poller. The mapping mode is intentionally excluded —
// each instance registers its own consumer on the shared receiver.
type connectionKey struct {
	Endpoint  string
	ConfigIDs string
	AuthHash  string
}

// partialKey is endpoint + config_ids without auth, used to detect mismatches.
type partialKey struct {
	Endpoint  string
	ConfigIDs string
}

func keyFromConfig(cfg *Config) connectionKey {
	h := sha256.Sum256([]byte(
		string(cfg.Authentication.ClientToken) +
			string(cfg.Authentication.ClientSecret) +
			string(cfg.Authentication.AccessToken),
	))
	return connectionKey{
		Endpoint:  cfg.Endpoint,
		ConfigIDs: cfg.ConfigIDs,
		AuthHash:  hex.EncodeToString(h[:8]),
	}
}

// NewFactory creates a new factory for the Akamai SIEM receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability),
	)
}

// seenPartialKeys tracks (endpoint, config_ids) pairs to detect auth mismatches.
// seenMu guards concurrent access to seenPartialKeys.
var (
	seenPartialKeysMu sync.Mutex
	seenPartialKeys   = map[partialKey]connectionKey{}
)

func createLogsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	cons consumer.Logs,
) (receiver.Logs, error) {
	oCfg := cfg.(*Config)
	key := keyFromConfig(oCfg)
	pk := partialKey{Endpoint: oCfg.Endpoint, ConfigIDs: oCfg.ConfigIDs}

	// Detect auth mismatch: same endpoint + config_ids but different auth hash
	// means the user likely intended dual mode but has a credential typo, which
	// would silently create two independent receivers instead of sharing.
	warn := false
	seenPartialKeysMu.Lock()
	if prev, seen := seenPartialKeys[pk]; seen && prev.AuthHash != key.AuthHash {
		warn = true
	}
	seenPartialKeys[pk] = key
	seenPartialKeysMu.Unlock()

	if warn {
		set.Logger.Warn(
			"two akamai_siem instances share endpoint and config_ids but have DIFFERENT credentials — "+
				"this creates two separate API connections instead of dual mode. "+
				"Ensure authentication is identical for both instances to share a single connection.",
			zap.String("endpoint", oCfg.Endpoint),
			zap.String("config_ids", oCfg.ConfigIDs),
		)
	}

	r, err := receivers.LoadOrStore(key, func() (*akamaiReceiver, error) {
		return newAkamaiReceiver(oCfg, set)
	})
	if err != nil {
		return nil, err
	}

	// Register this instance's consumer on the shared receiver based on output format.
	rcv := r.Unwrap()
	switch oCfg.OutputFormat {
	case "raw":
		rcv.setRawConsumer(cons)
	case "otel":
		rcv.setOTelConsumer(cons)
	}

	// Log whether this instance is sharing or standalone.
	if rcv.isDualMode() {
		set.Logger.Info("dual mode active — sharing API connection with paired instance",
			zap.String("output_format", oCfg.OutputFormat),
			zap.String("endpoint", oCfg.Endpoint),
		)
	}

	return r, nil
}
