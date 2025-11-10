// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package rawsampleexporter

import "errors"

var (
	// ErrMissingEndpoint is returned when endpoint is not configured.
	ErrMissingEndpoint = errors.New("endpoint is required")

	// ErrMissingIndex is returned when index is not configured.
	ErrMissingIndex = errors.New("index is required")
)
