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

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"
)

// exitAfterEndMarker matches the fatal-error event the metricsgen receiver emits
// once a backfill completes with exit_after_end. It must not be reported as a failure, so we ignore it.
const exitAfterEndMarker = "exit_after_end"

// runMetricsGenerator runs otelbench as a plain metricsgen-based load generator
// metricsgen receiver normally terminates the collector via exit_after_end; the
// optional -duration-metrics flag acts as a safety cap. It returns the process exit code.
func runMetricsGenerator(parent context.Context) int {
	if Config.CollectorConfigPath == "" {
		fmt.Fprintln(os.Stderr, "metrics-generator requires -config with a metricsgen pipeline")
		return 2
	}

	ctx, cancel := signal.NotifyContext(parent, os.Interrupt)
	defer cancel()

	// RunCollector cancels the collector context when stop is closed. With
	// -duration-metrics unset, stop is never closed and the run continues until
	// the collector returns on its own (e.g. metricsgen exit_after_end).
	stop := make(chan struct{})
	if Config.DurationMetrics > 0 {
		timer := time.AfterFunc(Config.DurationMetrics, func() {
			close(stop)
		})
		defer timer.Stop()
	}

	// The loadgen Stats channels are unused because the metricsgen config does
	// not make use of the loadgen receiver. So we pass nil values.
	err := RunCollector(ctx, stop, []string{Config.CollectorConfigPath}, nil, nil, nil, nil)
	if err != nil && !strings.Contains(err.Error(), exitAfterEndMarker) {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}
