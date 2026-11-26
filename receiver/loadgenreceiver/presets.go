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

package loadgenreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver"

import (
	"bytes"
	_ "embed"
	"fmt"
)

const (
	presetVercelLogs          = "vercel_logs"
	presetVercelSpeedInsights = "vercel_speed_insights"
	presetVercelBoth          = "vercel_both"
)

//go:embed testdata/vercel/logs.jsonl
var vercelLogs []byte

//go:embed testdata/vercel/speed_insights.jsonl
var vercelSpeedInsights []byte

// logsPresetData returns embedded JSONL sample bytes for the given logs preset.
// An empty preset uses the default OpenTelemetry Demo logs.
func logsPresetData(preset string) ([]byte, error) {
	switch preset {
	case "":
		return demoLogs, nil
	case presetVercelLogs:
		return vercelLogs, nil
	case presetVercelSpeedInsights:
		return vercelSpeedInsights, nil
	case presetVercelBoth:
		return concatJSONL(vercelLogs, vercelSpeedInsights), nil
	default:
		return nil, fmt.Errorf("unknown logs preset %q (supported: %q, %q, %q)",
			preset, presetVercelLogs, presetVercelSpeedInsights, presetVercelBoth)
	}
}

func concatJSONL(parts ...[]byte) []byte {
	var b bytes.Buffer
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}
		if b.Len() > 0 && !bytes.HasSuffix(b.Bytes(), []byte("\n")) {
			b.WriteByte('\n')
		}
		b.Write(part)
	}
	return b.Bytes()
}
