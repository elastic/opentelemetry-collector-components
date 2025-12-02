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

package ecs

import (
	"context"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// SetHostIP sets the `host.ip` attribute using the client address.
// Does not override any existing `host.ip` values.
// Safely handles empty client addresses and empty attribute maps.
func SetHostIP(ctx context.Context, attributesMap pcommon.Map) {
	cl := client.FromContext(ctx)
	if cl.Addr == nil {
		return
	}

	ip := cl.Addr.String()
	if ip == "" {
		return
	}

	// Only set host.ip when it does not exist or is empty
	if value, ok := attributesMap.Get("host.ip"); !ok || value.Str() == "" {
		attributesMap.PutStr("host.ip", ip)
	}
}
