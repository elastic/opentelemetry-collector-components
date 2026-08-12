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

package httpexporter // import "github.com/elastic/opentelemetry-collector-components/internal/exporter/httpexporter"

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

const defaultContentType = "application/json"

type httpExporter struct {
	config     *Config
	logger     *zap.Logger
	settings   component.TelemetrySettings
	httpClient *http.Client
}

func newExporter(cfg *Config, set exporter.Settings) (*httpExporter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &httpExporter{
		config:   cfg,
		logger:   set.Logger,
		settings: set.TelemetrySettings,
	}, nil
}

func (e *httpExporter) start(ctx context.Context, host component.Host) error {
	client, err := e.config.ToClient(ctx, host.GetExtensions(), e.settings)
	if err != nil {
		return fmt.Errorf("failed to create HTTP client: %w", err)
	}
	e.httpClient = client
	return nil
}

func (e *httpExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	buf, empty := encodeLogBodies(ld)
	if empty {
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.config.Endpoint, bytes.NewReader(buf.Bytes()))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", defaultContentType)
	}

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to POST to %s: %w", e.config.Endpoint, err)
	}
	defer func() {
		// Drain body so connections can be reused.
		_, _ = io.Copy(io.Discard, resp.Body)
		if err := resp.Body.Close(); err != nil {
			e.logger.Warn("failed to close response body", zap.Error(err))
		}
	}()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	err = fmt.Errorf("POST %s returned status %d", e.config.Endpoint, resp.StatusCode)
	// 5xx and 429 are typically transient; other 4xx (auth, bad request) are not.
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		return err
	}
	return consumererror.NewPermanent(err)
}

// encodeLogBodies joins each log record body as NDJSON (one JSON text per line,
// each terminated by '\n', including after the last record).
func encodeLogBodies(ld plog.Logs) (buf bytes.Buffer, empty bool) {
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		sls := rls.At(i).ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			records := sls.At(j).LogRecords()
			for k := 0; k < records.Len(); k++ {
				line := records.At(k).Body().AsString()
				if line == "" {
					continue
				}
				buf.WriteString(line)
				buf.WriteByte('\n')
			}
		}
	}
	return buf, buf.Len() == 0
}
