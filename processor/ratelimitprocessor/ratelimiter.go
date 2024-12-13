// ELASTICSEARCH CONFIDENTIAL
// __________________
//
//  Copyright Elasticsearch B.V. All rights reserved.
//
// NOTICE:  All information contained herein is, and remains
// the property of Elasticsearch B.V. and its suppliers, if any.
// The intellectual and technical concepts contained herein
// are proprietary to Elasticsearch B.V. and its suppliers and
// may be covered by U.S. and Foreign Patents, patents in
// process, and are protected by trade secret or copyright
// law.  Dissemination of this information or reproduction of
// this material is strictly forbidden unless prior written
// permission is obtained from Elasticsearch B.V.

package ratelimitprocessor

import (
	"context"
	"errors"
	"strings"

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
