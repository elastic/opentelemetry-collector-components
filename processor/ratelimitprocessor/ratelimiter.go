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

package ratelimitprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor"

import (
	"context"
	"errors"
	"strings"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/otel/metric"
)

var (
	errTooManyRequests        = errors.New("too many requests")
	errRateLimitInternalError = errors.New("rate limiter failed")
)

// RateLimiter provides an interface for rate limiting by some number
// of things: requests, records, or bytes.
type RateLimiter interface {
	RateLimit(ctx context.Context, n int) error
}

// getUniqueKey returns a unique key based on client metadata stored
// in ctx with the given metadata keys.
//
// The unique key is built by concatenating the metadata keys and any
// associated values. Being able to link a key back to a data source
// can be useful for observability purposes, so we use the full keys
// and values instead of hashing.
//
// If no metadata keys are specified, a special non-empty value
// "default" is returned.
//
// Metadata keys should be limited to ones that do not have extremely
// high cardinality: tenant ID would be a good choice. For rate
// limiting by IP (e.g. to avoid DDoS), consider running OpenTelemetry
// Collector behind a WAF/API Gateway/proxy.

type metrics struct {
	ratelimitRequests metric.Int64Counter
}

func getUniqueKey(ctx context.Context, metadataKeys []string) string {
	if len(metadataKeys) == 0 {
		return "default"
	}

	// Generate a unique key from client metadata.
	var uniqueKey strings.Builder
	clientInfo := client.FromContext(ctx)
	for i, metadataKey := range metadataKeys {
		values := clientInfo.Metadata.Get(metadataKey)
		if i > 0 {
			uniqueKey.WriteByte(';')
		}
		uniqueKey.WriteString(metadataKey)
		uniqueKey.WriteByte(':')
		for i, value := range values {
			if i > 0 {
				uniqueKey.WriteByte(',')
			}
			uniqueKey.WriteString(value)
		}
	}
	return uniqueKey.String()
}

func newMetrics(mp metric.MeterProvider) (metrics, error) {
	meter := mp.Meter("internal/ratelimit")

	ratelimitRequests, err := meter.Int64Counter("ratelimit.requests",
		metric.WithUnit("1"),
		metric.WithDescription("Number of rate-limiting requests"))
	if err != nil {
		return metrics{}, err
	}

	return metrics{
		ratelimitRequests: ratelimitRequests,
	}, nil
}
