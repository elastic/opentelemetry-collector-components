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
	"testing"

	"golang.org/x/mod/semver"
)

func TestLatestComponentVersion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   []string
		want string
	}{
		{
			name: "highest semver across paths",
			in: []string{
				"receiver/foo/v1.0.0",
				"receiver/foo/v1.1.0",
				"receiver/foo/v1.0.1",
				"not-a-semver-tag",
				"no-slash",
				"processor/bar/v2.0.0",
				"deep/nested/path/v0.1.0",
				"invalid/ver/notsemver",
			},
			want: "v2.0.0",
		},
		{
			name: "empty input",
			in:   nil,
			want: "",
		},
		{
			name: "only invalid lines",
			in: []string{
				"",
				"   ",
				"no-slash",
				"nope",
				"bad/tail/notsemver",
			},
			want: "",
		},
		{
			name: "trims whitespace and ignores blank lines",
			in: []string{
				"  a/v1.0.0  ",
				"",
				"b/v1.1.0",
			},
			want: "v1.1.0",
		},
		{
			name: "canonical semver",
			in: []string{
				"x/v1.0.0+build",
				"y/v1.0.1",
			},
			want: semver.Canonical("v1.0.1"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := latestComponentVersion(tt.in)
			if got != tt.want {
				t.Fatalf("latestComponentVersion() = %q, want %q", got, tt.want)
			}
		})
	}
}

// edotBaseCheckWouldError mirrors the gate in run() after latestComponentVersion:
// when there is at least one semver tag, semver.Compare(edotVersion, latestTag) <= 0
// triggers the error return path (edot-base must be strictly greater than latest tag).
func edotBaseCheckWouldError(edotVersion, latestTag string) bool {
	if latestTag == "" {
		return false
	}
	return semver.Compare(edotVersion, latestTag) <= 0
}

func TestEdotBaseVersusLatestTagGate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		edot       string
		latestTag  string
		wantError bool
	}{
		{
			name:       "edot strictly greater than latest tag",
			edot:       "v2.0.0",
			latestTag:  "v1.0.0",
			wantError: false,
		},
		{
			name:       "edot equal to latest tag",
			edot:       "v1.0.0",
			latestTag:  "v1.0.0",
			wantError: true,
		},
		{
			name:       "edot strictly less than latest tag",
			edot:       "v1.0.0",
			latestTag:  "v2.0.0",
			wantError: true,
		},
		{
			name:       "no semver tags (latest empty)",
			edot:       "v1.0.0",
			latestTag:  "",
			wantError: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := edotBaseCheckWouldError(tt.edot, tt.latestTag)
			if got != tt.wantError {
				t.Fatalf("edotBaseCheckWouldError(%q, %q) = %v, want %v", tt.edot, tt.latestTag, got, tt.wantError)
			}
		})
	}
}

func TestParseVersionsYAML(t *testing.T) {
	t.Parallel()
	const sample = `
module-sets:
  edot-base:
    version: v1.2.3
    modules:
      - github.com/elastic/opentelemetry-collector-components/receiver/foo
`
	cfg, err := parseVersionsYAML([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	ms := cfg.ModuleSets["edot-base"]
	if ms.Version != "v1.2.3" || len(ms.Modules) != 1 {
		t.Fatalf("unexpected parse: %#v", ms)
	}
}
