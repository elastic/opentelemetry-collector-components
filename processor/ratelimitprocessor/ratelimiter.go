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
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.opentelemetry.io/collector/client"
)

var (
	errTooManyRequests = errors.New("too many requests")
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
func getUniqueKey(metadata client.Metadata, metadataKeys []string) string {
	if len(metadataKeys) == 0 {
		return "default"
	}

	// Generate a unique key from client metadata.
	var uniqueKey strings.Builder
	for i, metadataKey := range metadataKeys {
		values := metadata.Get(metadataKey)
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

// getAttrsFromContext looks up for the metadata keys in the
// context and returns the values as attributes.
func getAttrsFromContext(ctx context.Context, metadataKeys []string) []attribute.KeyValue {
	clientInfo := client.FromContext(ctx)

	attrs := make([]attribute.KeyValue, 0, len(metadataKeys))
	for _, key := range metadataKeys {
		values := clientInfo.Metadata.Get(key)
		if len(values) > 0 {
			attrs = append(attrs, attribute.String(key, strings.Join(values, ",")))
		}
	}
	return attrs
}

// errorWithDetails provides a user friendly error with additional error details that
// can be later used to provide more detailed error information to the user.
func errorWithDetails(err error, cfg RateLimitSettings) error {
	st := status.New(codes.ResourceExhausted, err.Error())
	if detailedSt, stErr := st.WithDetails(&errdetails.ErrorInfo{
		Domain: "ingest.elastic.co",
		Metadata: map[string]string{
			"component":         "ratelimitprocessor",
			"limit":             fmt.Sprintf("%d", cfg.Rate),
			"throttle_interval": cfg.ThrottleInterval.String(),
		},
	}, &errdetails.RetryInfo{
		RetryDelay: durationpb.New(cfg.RetryDelay),
	}); stErr == nil {
		return detailedSt.Err()
	}
	return st.Err()
}
