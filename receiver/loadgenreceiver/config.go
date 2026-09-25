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
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"go.opentelemetry.io/collector/component"
)

const (
	maxScannerBufSize = 1024 * 1024
	// the type of compression codec
	compressionZSTD = "zstd"
)

// JsonlFile is an optional configuration option to specify the path to
// get the base generated signals from.
type JsonlFile struct {
	Path string `mapstructure:"jsonl_file"`
	// Compression Codec used to extract telemetry data
	// Supported compression algorithms:`zstd`
	Compression string `mapstructure:"compression"`
}

// Config defines configuration for loadgen receiver.
type Config struct {
	Metrics  MetricsConfig  `mapstructure:"metrics"`
	Logs     LogsConfig     `mapstructure:"logs"`
	Traces   TracesConfig   `mapstructure:"traces"`
	Profiles ProfilesConfig `mapstructure:"profiles"`

	// Concurrency is the amount of concurrency when sending to next consumer.
	// The concurrent workers share the amount of workload, instead of multiplying the amount of workload,
	// i.e. loadgenreceiver still sends up to the same MaxReplay limit.
	// A higher concurrency translates to a higher load.
	// As requests are synchronous, when concurrency is N, there will be N in-flight requests.
	// This is similar to the `agent_replicas` config in apmsoak.
	Concurrency int `mapstructure:"concurrency"`

	// DisablePdataReuse disables the optimization that reuses pdata structures to reduce allocations.
	// It is useful in cases where the optimization causes problems with certain downstream components, e.g. batchprocessor.
	DisablePdataReuse bool `mapstructure:"disable_pdata_reuse"`
}

type SignalConfig struct {
	// MaxReplay is an optional configuration to specify the number of times the file is replayed.
	MaxReplay int `mapstructure:"max_replay"`

	// MaxBufferSize defines the maximum acceptable size for the file content. Set to 0 if you don't want
	// to set a limit.
	MaxBufferSize int `mapstructure:"max_buffer_size"`

	// Jitter defines the range of random delay the receiver waits between forwarding signals.
	// When set, each forward sleeps for a random duration in [Min, Max].
	Jitter *JitterRange `mapstructure:"jitter"`

	// doneCh is only non-nil when the receiver is created with NewFactoryWithDone.
	// It is to notify the caller of collector that receiver finished replaying the file for MaxReplay number of times.
	doneCh chan Stats
}

type MetricsConfig struct {
	JsonlFile `mapstructure:",squash"`

	SignalConfig `mapstructure:",squash"`

	// AddCounterAttr, if true, adds a loadgenreceiver_counter resource attribute containing increasing counter value to the generated metrics.
	// It can be used to workaround timestamp precision and duplication detection of backends,
	// e.g. Elasticsearch TSDB version_conflict_engine_exception with millisecond-precision timestamp mapping,
	// which will be triggered when loadgenreceiver generates metrics too quickly such that
	// there exists data points of the same metric with the same dimensions within a millisecond but different nanoseconds.
	AddCounterAttr bool `mapstructure:"add_counter_attr"`
}

type LogsConfig struct {
	JsonlFile `mapstructure:",squash"`

	SignalConfig `mapstructure:",squash"`

	// Preset selects embedded JSONL samples for logs. Mutually exclusive with jsonl_file.
	// Supported values: vercel_logs, vercel_speed_insights, vercel_both.
	// Empty uses the default OpenTelemetry Demo logs embed.
	Preset string `mapstructure:"preset"`
}

type TracesConfig struct {
	JsonlFile `mapstructure:",squash"`

	SignalConfig `mapstructure:",squash"`

	// RewriteIDs rewrites trace and span IDs on every replay pass so that each
	// pass over the input file produces unique trace IDs with an identical
	// statistical shape. IDs are remapped deterministically per pass: spans
	// sharing a trace ID keep sharing one, and parent-child span ID links are
	// preserved. Without this, replaying a file re-sends the same trace IDs,
	// which tail-based sampling backends treat as late spans of already-decided
	// traces instead of new traces.
	RewriteIDs bool `mapstructure:"rewrite_ids"`

	// LateSpans holds back spans from emitted traces and re-emits them after a
	// wall-clock delay, to exercise tail-based sampling late-arrival paths
	// (partial trace at decision time, decision cache hits, re-decisions after
	// cache eviction).
	LateSpans *LateSpansConfig `mapstructure:"late_spans"`
}

// LateSpansConfig configures delayed re-emission of spans held back from
// generated traces.
type LateSpansConfig struct {
	// Fraction is the probability in [0, 1] that spans are held back from an
	// emitted payload.
	Fraction float64 `mapstructure:"fraction"`

	// Spans is the maximum number of spans held back per selected payload.
	// Defaults to 1.
	Spans int `mapstructure:"spans"`

	// DelayMin and DelayMax bound the uniform random delay after which the
	// held spans are emitted.
	DelayMin time.Duration `mapstructure:"delay_min"`
	DelayMax time.Duration `mapstructure:"delay_max"`
}

type ProfilesConfig struct {
	JsonlFile `mapstructure:",squash"`

	SignalConfig `mapstructure:",squash"`
}

// JitterRange specifies a uniform random jitter applied between forwarded signals.
type JitterRange struct {
	Min time.Duration `mapstructure:"min"`
	Max time.Duration `mapstructure:"max"`
}

// waitJitter blocks for a random duration in [jitter.Min, jitter.Max].
// Returns false if ctx is canceled before the duration elapses.
func waitJitter(ctx context.Context, jitter *JitterRange) bool {
	if jitter == nil {
		return true
	}
	n := jitter.Max.Nanoseconds() - jitter.Min.Nanoseconds()
	var d time.Duration
	if n > 0 {
		d = time.Duration(rand.Int64N(n) + jitter.Min.Nanoseconds())
	} else {
		d = jitter.Min
	}
	t := time.NewTimer(d)
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		t.Stop()
		return false
	}
}

var _ component.Config = (*Config)(nil)

func validateSignal(sigConfig SignalConfig, file JsonlFile) error {
	if sigConfig.MaxReplay < 0 {
		return fmt.Errorf("max_replay must be >= 0")
	}
	if sigConfig.MaxBufferSize < 0 {
		return fmt.Errorf("max_buffer_size must be >= 0")
	}

	if file.Path != "" && file.Compression != "" && file.Compression != compressionZSTD {
		return errors.New("compression is not supported")
	}
	if j := sigConfig.Jitter; j != nil {
		if j.Min < 0 {
			return fmt.Errorf("jitter.min must be >= 0")
		}
		if j.Max < j.Min {
			return fmt.Errorf("jitter.max must be >= jitter.min")
		}
	}
	return nil
}

// Validate checks the receiver configuration is valid
func (cfg *Config) Validate() error {
	err := validateSignal(cfg.Logs.SignalConfig, cfg.Logs.JsonlFile)
	if err != nil {
		return fmt.Errorf("logs::%w", err)
	}
	if cfg.Logs.Preset != "" && cfg.Logs.Path != "" {
		return fmt.Errorf("logs::preset and logs::jsonl_file are mutually exclusive")
	}
	if _, err := logsPresetData(cfg.Logs.Preset); err != nil {
		return fmt.Errorf("logs::%w", err)
	}

	err = validateSignal(cfg.Metrics.SignalConfig, cfg.Metrics.JsonlFile)
	if err != nil {
		return fmt.Errorf("metrics::%w", err)
	}

	err = validateSignal(cfg.Traces.SignalConfig, cfg.Traces.JsonlFile)
	if err != nil {
		return fmt.Errorf("traces::%w", err)
	}
	if ls := cfg.Traces.LateSpans; ls != nil {
		if ls.Fraction < 0 || ls.Fraction > 1 {
			return fmt.Errorf("traces::late_spans::fraction must be in [0, 1]")
		}
		if ls.Spans < 0 {
			return fmt.Errorf("traces::late_spans::spans must be >= 0")
		}
		if ls.DelayMin < 0 {
			return fmt.Errorf("traces::late_spans::delay_min must be >= 0")
		}
		if ls.DelayMax < ls.DelayMin {
			return fmt.Errorf("traces::late_spans::delay_max must be >= delay_min")
		}
	}

	err = validateSignal(cfg.Profiles.SignalConfig, cfg.Profiles.JsonlFile)
	if err != nil {
		return fmt.Errorf("profiles::%w", err)
	}

	return nil
}
