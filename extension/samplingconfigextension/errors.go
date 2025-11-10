// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package samplingconfigextension

import "errors"

var (
	errMissingEndpoint      = errors.New("endpoint is required")
	errInvalidPollInterval  = errors.New("poll_interval must be greater than 0")
	errInvalidSampleRate    = errors.New("sample_rate must be between 0 and 1")
	errExtensionNotStarted  = errors.New("extension not started")
)
