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
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/google/go-github/v84/github"
	"golang.org/x/mod/semver"
)

func repoTag(name string) *github.RepositoryTag {
	return &github.RepositoryTag{Name: github.Ptr(name)}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestLatestComponentVersion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		in          []*github.RepositoryTag
		wantLatest  string
		wantTagName string
	}{
		{
			name: "highest semver across paths",
			in: []*github.RepositoryTag{
				repoTag("receiver/foo/v1.0.0"),
				repoTag("receiver/foo/v1.1.0"),
				repoTag("receiver/foo/v1.0.1"),
				repoTag("not-a-semver-tag"),
				repoTag("no-slash"),
				repoTag("processor/bar/v2.0.0"),
				repoTag("deep/nested/path/v0.1.0"),
				repoTag("invalid/ver/notsemver"),
			},
			wantLatest:  "v2.0.0",
			wantTagName: "processor/bar/v2.0.0",
		},
		{
			name:        "empty input",
			in:          nil,
			wantLatest:  "",
			wantTagName: "",
		},
		{
			name: "only invalid or nil names",
			in: []*github.RepositoryTag{
				nil,
				repoTag(""),
				repoTag("no-slash"),
				repoTag("bad/tail/notsemver"),
			},
			wantLatest:  "",
			wantTagName: "",
		},
		{
			name: "canonical semver",
			in: []*github.RepositoryTag{
				repoTag("x/v1.0.0+build"),
				repoTag("y/v1.0.1"),
			},
			wantLatest:  semver.Canonical("v1.0.1"),
			wantTagName: "y/v1.0.1",
		},
		{
			name: "plain v tag without path",
			in: []*github.RepositoryTag{
				repoTag("v3.0.0"),
				repoTag("other/v2.0.0"),
			},
			wantLatest:  "v3.0.0",
			wantTagName: "v3.0.0",
		},
		{
			name: "tie same semver keeps first winning tag name",
			in: []*github.RepositoryTag{
				repoTag("path/a/v1.0.0"),
				repoTag("path/b/v1.0.0"),
			},
			wantLatest:  "v1.0.0",
			wantTagName: "path/a/v1.0.0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotLatest, gotTag := latestComponentVersion(tt.in)
			if gotLatest != tt.wantLatest || gotTag != tt.wantTagName {
				t.Fatalf("latestComponentVersion() = (%q, %q), want (%q, %q)", gotLatest, gotTag, tt.wantLatest, tt.wantTagName)
			}
		})
	}
}

func TestSplitGitHubRepo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in      string
		wantO   string
		wantR   string
		wantErr bool
	}{
		{"elastic/opentelemetry-collector-components", "elastic", "opentelemetry-collector-components", false},
		{"", "", "", true},
		{"onlyone", "", "", true},
		{"a/b/c", "", "", true},
		{"/repo", "", "", true},
		{"owner/", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			o, r, err := splitGitHubRepo(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if o != tt.wantO || r != tt.wantR {
				t.Fatalf("splitGitHubRepo(%q) = (%q, %q), want (%q, %q)", tt.in, o, r, tt.wantO, tt.wantR)
			}
		})
	}
}

func TestValidateModuleSetVersionAgainstTagList(t *testing.T) {
	t.Parallel()
	const repo = "elastic/opentelemetry-collector-components"
	tests := []struct {
		name          string
		msVersion     string
		latest        string
		tagName       string
		allowEmptyTag bool
		wantErr       bool
		wantErrSubstr string
	}{
		{
			name:          "strictly greater than latest tag",
			msVersion:     "v2.0.0",
			latest:        "v1.0.0",
			tagName:       "processor/bar/v1.0.0",
			allowEmptyTag: false,
			wantErr:       false,
		},
		{
			name:          "equal to latest tag",
			msVersion:     "v1.0.0",
			latest:        "v1.0.0",
			tagName:       "x/v1.0.0",
			allowEmptyTag: false,
			wantErr:       true,
			wantErrSubstr: "must be greater than version",
		},
		{
			name:          "strictly less than latest tag",
			msVersion:     "v1.0.0",
			latest:        "v2.0.0",
			tagName:       "y/v2.0.0",
			allowEmptyTag: false,
			wantErr:       true,
			wantErrSubstr: "must be greater than version",
		},
		{
			name:          "no semver tags and allow empty",
			msVersion:     "v1.0.0",
			latest:        "",
			tagName:       "",
			allowEmptyTag: true,
			wantErr:       false,
		},
		{
			name:          "no semver tags and not allowed",
			msVersion:     "v1.0.0",
			latest:        "",
			tagName:       "",
			allowEmptyTag: false,
			wantErr:       true,
			wantErrSubstr: "no semver tags found for",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateModuleSetVersionAgainstTagList(tt.msVersion, tt.latest, tt.tagName, tt.allowEmptyTag, repo)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				if tt.wantErrSubstr != "" && !strings.Contains(err.Error(), tt.wantErrSubstr) {
					t.Fatalf("error %q should contain %q", err.Error(), tt.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFormatVersionscheckOK(t *testing.T) {
	t.Parallel()
	const key = "edot-base"
	const ver = "v0.42.0"
	const gh = "elastic/opentelemetry-collector-components"
	t.Run("with latest tag", func(t *testing.T) {
		t.Parallel()
		got := formatVersionscheckOK(key, ver, "v0.41.0", "processor/x/v0.41.0", gh)
		want := "versionscheck: ok (edot-base version v0.42.0 is greater than latest v0.41.0 in tag processor/x/v0.41.0)\n"
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
	t.Run("no semver tags", func(t *testing.T) {
		t.Parallel()
		got := formatVersionscheckOK(key, ver, "", "", gh)
		want := "versionscheck: ok (edot-base version v0.42.0; no semver tags in elastic/opentelemetry-collector-components)\n"
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
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

func TestGitTagListFromGitHub_invalidRepo(t *testing.T) {
	t.Parallel()
	_, err := gitTagListFromGitHub("not-a-valid-repo")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), `github-repo must be "owner/repo"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGitTagListFromGitHub_tagsJSON(t *testing.T) {
	orig := httpClient
	t.Cleanup(func() { httpClient = orig })

	httpClient = &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			wantPrefix := "/repos/elastic/collector/tags"
			if !strings.HasPrefix(req.URL.Path, wantPrefix) {
				return nil, fmt.Errorf("unexpected path %q, want prefix %q", req.URL.Path, wantPrefix)
			}
			if req.Method != http.MethodGet {
				return nil, fmt.Errorf("unexpected method %s", req.Method)
			}
			body := `[{"name":"receiver/foo/v1.2.0"},{"name":"not-semver"}]`
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Request:    req,
			}, nil
		}),
	}

	tags, err := gitTagListFromGitHub("elastic/collector")
	if err != nil {
		t.Fatal(err)
	}
	if len(tags) != 2 {
		t.Fatalf("len(tags) = %d, want 2", len(tags))
	}
	if tags[0] == nil || tags[0].Name == nil || *tags[0].Name != "receiver/foo/v1.2.0" {
		t.Fatalf("first tag = %#v, want name receiver/foo/v1.2.0", tags[0])
	}
	if tags[1] == nil || tags[1].Name == nil || *tags[1].Name != "not-semver" {
		t.Fatalf("second tag = %#v, want name not-semver", tags[1])
	}
}

func TestGitTagListFromGitHub_pagination(t *testing.T) {
	orig := httpClient
	t.Cleanup(func() { httpClient = orig })

	var n int
	httpClient = &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			n++
			if !strings.HasPrefix(req.URL.Path, "/repos/elastic/collector/tags") {
				return nil, fmt.Errorf("unexpected path %q", req.URL.Path)
			}
			switch n {
			case 1:
				body := `[{"name":"a/v1.0.0"}]`
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(body)),
					Header: http.Header{
						"Content-Type": []string{"application/json"},
						"Link":         []string{`<https://api.github.com/repos/elastic/collector/tags?per_page=100&page=2>; rel="next"`},
					},
					Request: req,
				}, nil
			case 2:
				body := `[{"name":"b/v2.0.0"}]`
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(body)),
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Request:    req,
				}, nil
			default:
				return nil, fmt.Errorf("unexpected extra request %d", n)
			}
		}),
	}

	tags, err := gitTagListFromGitHub("elastic/collector")
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("HTTP round trips = %d, want 2", n)
	}
	if len(tags) != 2 {
		t.Fatalf("len(tags) = %d, want 2: %#v", len(tags), tags)
	}
}

func TestGitTagListFromGitHub_APIError(t *testing.T) {
	orig := httpClient
	t.Cleanup(func() { httpClient = orig })

	httpClient = &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(strings.NewReader(`{"message":"server error"}`)),
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Request:    req,
			}, nil
		}),
	}

	_, err := gitTagListFromGitHub("elastic/collector")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "list tags for") {
		t.Fatalf("unexpected error: %v", err)
	}
}
