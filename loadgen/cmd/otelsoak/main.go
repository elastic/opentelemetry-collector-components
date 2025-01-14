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
	"fmt"
	"log"

	"github.com/elastic/opentelemetry-collector-components/loadgen"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/collector/otelcol"
)

func main() {
	loadgen.Init()

	settings, err := loadgen.NewCollectorSettings(nil)
	if err != nil {
		log.Fatalf("collector new settings error: %v", err)
	}

	cmd := otelcol.NewCommand(settings)
	cmd.Flags().AddGoFlagSet(loadgen.FlagSet)
	runE := cmd.RunE
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		// This is to pass in parsed loadgen flags e.g. api-key, secret-token to otel collector
		sets := loadgen.CollectorSetFromConfig()
		for _, set := range sets {
			if err := cmd.Flags().Set("set", set); err != nil {
				return fmt.Errorf("error passing --set to collector: %w", err)
			}
		}
		return runE(cmd, args)
	}
	if err := cmd.Execute(); err != nil {
		log.Fatalf("collector execute error: %v", err)
	}
}
