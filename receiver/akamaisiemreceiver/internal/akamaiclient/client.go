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

// Package akamaiclient contains the core Akamai SIEM API client and NDJSON
// streaming parser.
package akamaiclient // import "github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/akamaiclient"

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

const siemAPIPath = "/siem/v1/configs/"

// APIError represents a non-200 response from the Akamai SIEM API.
//
// Detail is parsed from the JSON `detail` field per RFC 7807 problem-details
// and is the value matched by IsInvalidTimestamp / IsFromTooOld to drive
// recovery decisions — keep it strictly structured.
//
// Body is the raw response body, useful for debugging when the response is
// not JSON (HTML error page, proxy error, etc.). It is logged at debug level
// from the construction site and never used for state-machine decisions.
type APIError struct {
	StatusCode int
	Status     string
	Detail     string
	Body       string
}

func (e *APIError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("akamai API error: %s (%d): %s", e.Status, e.StatusCode, e.Detail)
	}
	return fmt.Sprintf("akamai API error: %s (%d)", e.Status, e.StatusCode)
}

// IsInvalidTimestamp returns true if the error indicates an invalid HMAC timestamp.
func (e *APIError) IsInvalidTimestamp() bool {
	return e.StatusCode == 400 && strings.Contains(strings.ToLower(e.Detail), "invalid timestamp")
}

// IsOffsetOutOfRange returns true if the offset is expired (416).
func (e *APIError) IsOffsetOutOfRange() bool {
	return e.StatusCode == 416
}

// IsFromTooOld returns true if the from parameter exceeds the max lookback.
func (e *APIError) IsFromTooOld() bool {
	if e.StatusCode != 400 {
		return false
	}
	lower := strings.ToLower(e.Detail)
	return strings.Contains(lower, "out of range") || strings.Contains(lower, "too old")
}

// FetchParams contains parameters for an API fetch request.
type FetchParams struct {
	Offset string
	From   int64
	To     int64
	Limit  int
}

// FetchMode returns "offset" or "time" based on the params.
func (p FetchParams) FetchMode() string {
	if p.Offset != "" {
		return "offset"
	}
	return "time"
}

// OffsetContext is the pagination metadata returned as the last line in
// NDJSON responses. A valid offset context has Limit > 0. The API also
// returns a "total" field which the receiver does not use.
type OffsetContext struct {
	Offset string `json:"offset"`
	Limit  int    `json:"limit"`
}

// Client is the Akamai SIEM API HTTP client.
type Client struct {
	httpClient        *http.Client
	baseURL           *url.URL
	configIDs         string
	log               *zap.Logger
	lastContentLength int64
}

// NewClient creates a new Akamai SIEM API client. The httpClient should
// already have EdgeGrid signing configured on its transport.
func NewClient(httpClient *http.Client, endpoint, configIDs string, log *zap.Logger) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoint URL: %w", err)
	}

	return &Client{
		httpClient: httpClient,
		baseURL:    u,
		configIDs:  configIDs,
		log:        log.Named("client"),
	}, nil
}

// FetchResponse makes the HTTP request and returns the response body.
// On non-200 status, the body is consumed and an *APIError is returned.
// The caller must close the returned body on success.
func (c *Client) FetchResponse(ctx context.Context, params FetchParams) (io.ReadCloser, error) {
	reqURL := c.buildRequestURL(params)
	c.log.Debug("fetching events",
		zap.String("mode", params.FetchMode()),
		zap.Int("limit", params.Limit),
		zap.String("offset", params.Offset),
		zap.Int64("from", params.From),
		zap.Int64("to", params.To),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.log.Error("HTTP request failed",
			zap.Error(err),
			zap.String("url", reqURL),
			zap.String("mode", params.FetchMode()),
		)
		return nil, fmt.Errorf("request failed: %w", err)
	}

	c.log.Debug("API response received",
		zap.Int("status_code", resp.StatusCode),
		zap.String("mode", params.FetchMode()),
	)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		apiErr := &APIError{
			StatusCode: resp.StatusCode,
			Status:     resp.Status,
			Body:       string(body),
		}
		if len(body) > 0 {
			var errResp struct {
				Detail string `json:"detail"`
			}
			if json.Unmarshal(body, &errResp) == nil {
				apiErr.Detail = errResp.Detail
			}
			// Log the raw body at debug level so non-JSON responses (HTML error
			// pages, proxy errors) are visible without polluting Detail, which
			// is substring-matched for recovery decisions.
			c.log.Debug("non-200 response body",
				zap.Int("status_code", resp.StatusCode),
				zap.ByteString("body", body),
			)
		}
		return nil, apiErr
	}

	c.lastContentLength = resp.ContentLength
	return resp.Body, nil
}

// LastContentLength returns the Content-Length from the most recent successful
// response. Returns -1 if unknown (chunked transfer).
func (c *Client) LastContentLength() int64 {
	return c.lastContentLength
}

func (c *Client) buildRequestURL(params FetchParams) string {
	u := *c.baseURL
	u.Path = siemAPIPath + c.configIDs

	query := url.Values{}
	query.Set("limit", strconv.Itoa(params.Limit))

	if params.Offset != "" {
		query.Set("offset", params.Offset)
	} else {
		if params.From > 0 {
			query.Set("from", strconv.FormatInt(params.From, 10))
		}
		if params.To > 0 {
			query.Set("to", strconv.FormatInt(params.To, 10))
		}
	}

	u.RawQuery = query.Encode()
	return u.String()
}

// StreamEvents reads NDJSON lines from body, pushing event lines into eventCh
// using a one-line delay pattern. The last line is checked for offset context
// metadata and returned separately.
//
// The caller must close eventCh after StreamEvents returns.
func StreamEvents(ctx context.Context, body io.Reader, eventCh chan<- string) (pageCtx OffsetContext, count int, err error) {
	scanner := bufio.NewScanner(body)
	const maxTokenSize = 10 * 1024 * 1024 // 10MB max line size
	scanner.Buffer(make([]byte, 64*1024), maxTokenSize)

	// send pushes s onto eventCh and bumps count, returning ctx.Err() if the
	// caller cancelled.
	send := func(s string) error {
		select {
		case eventCh <- s:
			count++
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// 1-line delay: hold back the current line until the next is read.
	// This lets us check if the last line is offset context without emitting it.
	var prev string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		if prev != "" {
			if err = send(prev); err != nil {
				return OffsetContext{}, count, err
			}
		}
		prev = line
	}

	if err = scanner.Err(); err != nil {
		return OffsetContext{}, count, fmt.Errorf("error reading response: %w", err)
	}
	if prev == "" {
		return OffsetContext{}, 0, nil
	}

	// Last line: try to unmarshal as offset context. If valid, don't emit it.
	if err = json.Unmarshal([]byte(prev), &pageCtx); err == nil && pageCtx.Offset != "" && pageCtx.Limit > 0 {
		return pageCtx, count, nil
	}

	// Last line was a regular event, not offset context.
	if err = send(prev); err != nil {
		return OffsetContext{}, count, err
	}
	return OffsetContext{}, count, nil
}

// Close releases resources held by the client.
func (c *Client) Close() {
	c.httpClient.CloseIdleConnections()
}
