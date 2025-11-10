// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package samplingconfig

import "context"

// Provider is the interface for getting sampling configuration.
// This interface is used by processors to query sampling rules from the extension.
type Provider interface {
	// GetSamplingRule returns the highest priority sampling rule that matches
	// the given stream name and resource attributes.
	// Returns nil if no rule matches.
	GetSamplingRule(ctx context.Context, streamName string, resourceAttrs map[string]any) (*Rule, error)
}

// Rule represents a sampling rule.
type Rule struct {
	// ID is the unique identifier for this rule
	ID string

	// Enabled indicates if this rule is active
	Enabled bool

	// Priority determines the order of evaluation (higher = evaluated first)
	Priority int

	// Match criteria
	Match Match

	// Condition is the OTTL expression to evaluate
	Condition string

	// SampleRate is the sampling rate (0.0 to 1.0)
	SampleRate float64

	// UpdatedAt is when this rule was last updated
	UpdatedAt string
}

// Match defines the matching criteria for a rule.
type Match struct {
	// StreamPattern is a glob pattern to match against stream.name attribute
	// e.g., "logs-nginx*", "logs-*", "*"
	StreamPattern string `json:"stream,omitempty"`

	// ResourceAttrs are key-value pairs that must match resource attributes
	ResourceAttrs map[string]any `json:"resource_attrs,omitempty"`

	// Condition is the OTTL condition (stored in Rule.Condition)
	Condition string `json:"condition,omitempty"`
}
