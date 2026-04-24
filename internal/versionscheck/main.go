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

// Versionscheck is a development tool that compares versions.yaml with Git tags.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/go-github/v84/github"
	"golang.org/x/mod/semver"
	"gopkg.in/yaml.v3"
)

type versionsYAML struct {
	ModuleSets      map[string]moduleSet `yaml:"module-sets"`
	ExcludedModules []string             `yaml:"excluded-modules"`
}

type moduleSet struct {
	Version string   `yaml:"version"`
	Modules []string `yaml:"modules"`
}

// tagFetchDeadline bounds how long we wait for paginated ListTags calls to GitHub.
const tagFetchDeadline = 5 * time.Minute

var httpClient = newVersionscheckHTTPClient()

func newVersionscheckHTTPClient() *http.Client {
	return &http.Client{
		Timeout: tagFetchDeadline,
	}
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("versionscheck: ")
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	versionsPath := flag.String("versions", "", "required: path to versions.yaml")
	githubRepo := flag.String("github-repo", "", "required: GitHub repository as owner/repo;")
	moduleKey := flag.String("module-key", "", "required: module-sets key in versions.yaml (e.g. edot-base)")
	allowEmptyTag := flag.Bool("allow-empty-tag", false, "allow a repository with no semver-shaped tags; otherwise fail when GitHub returns no v* tags")

	quiet := flag.Bool("q", false, "suppress success message on stdout")
	flag.Parse()

	if *versionsPath == "" {
		return fmt.Errorf("-versions is required")
	}
	if *githubRepo == "" {
		return fmt.Errorf("-github-repo is required (owner/repo)")
	}
	if *moduleKey == "" {
		return fmt.Errorf("-module-key is required")
	}

	data, err := os.ReadFile(*versionsPath)
	if err != nil {
		return fmt.Errorf("read versions file: %w", err)
	}

	cfg, err := parseVersionsYAML(data)
	if err != nil {
		return err
	}

	ms, ok := cfg.ModuleSets[*moduleKey]
	if !ok {
		return fmt.Errorf("no entry %s in %s", *moduleKey, *versionsPath)
	}
	if ms.Version == "" {
		return fmt.Errorf("empty version for %s", *moduleKey)
	}
	if !semver.IsValid(ms.Version) {
		return fmt.Errorf("invalid semver for %s version: %q", *moduleKey, ms.Version)
	}

	tags, err := gitTagListFromGitHub(*githubRepo)
	if err != nil {
		return err
	}
	latest, tagName := latestComponentVersion(tags)

	if err := validateModuleSetVersionAgainstTagList(ms.Version, latest, tagName, *allowEmptyTag, *githubRepo); err != nil {
		return err
	}

	if !*quiet {
		msg := formatVersionscheckOK(*moduleKey, ms.Version, latest, tagName, *githubRepo)
		if _, err := fmt.Fprint(os.Stdout, msg); err != nil {
			log.Printf("warning: could not write success message: %v", err)
		}
	}
	return nil
}

// splitGitHubRepo parses "owner/repo" for use with the GitHub API.
func splitGitHubRepo(githubRepo string) (owner, repo string, err error) {
	parts := strings.Split(githubRepo, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf(`github-repo must be "owner/repo", got %q`, githubRepo)
	}
	return parts[0], parts[1], nil
}

func gitTagListFromGitHub(githubRepo string) ([]*github.RepositoryTag, error) {
	owner, repo, err := splitGitHubRepo(githubRepo)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), tagFetchDeadline)
	defer cancel()

	client := github.NewClient(httpClient)
	var all []*github.RepositoryTag
	tagsIter := client.Repositories.ListTagsIter(ctx, owner, repo, &github.ListOptions{PerPage: 100})
	for tag, err := range tagsIter {
		if err != nil {
			return nil, fmt.Errorf("list tags for %q on GitHub: %w (check owner/repo, network, and GitHub API availability or rate limits)", githubRepo, err)
		}
		all = append(all, tag)
	}
	return all, nil
}

func parseVersionsYAML(data []byte) (*versionsYAML, error) {
	var cfg versionsYAML
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse versions.yaml: %w", err)
	}
	return &cfg, nil
}

// validateModuleSetVersionAgainstTagList enforces that, if a tag exists (i.e. latest is not empty),
// then the module set version must be greater than the latest tag.
// If latest is empty, an error is returned unless allowEmptyTag flag is set.
func validateModuleSetVersionAgainstTagList(msVersion, latest, tagName string, allowEmptyTag bool, githubRepo string) error {
	if latest == "" && !allowEmptyTag {
		return fmt.Errorf("no semver tags found for repo %s", githubRepo)
	}
	if latest != "" && semver.Compare(msVersion, latest) <= 0 {
		return fmt.Errorf("module-sets version %s must be greater than version %s from tag %s",
			msVersion, latest, tagName)
	}
	return nil
}

func formatVersionscheckOK(moduleKey, msVersion, latest, tagName, githubRepo string) string {
	if latest == "" {
		return fmt.Sprintf("versionscheck: ok (%s version %s; no semver tags in %s)\n", moduleKey, msVersion, githubRepo)
	}
	return fmt.Sprintf("versionscheck: ok (%s version %s is greater than latest %s in tag %s)\n", moduleKey, msVersion, latest, tagName)
}

// latestComponentVersion returns the highest valid version amongst all components and the complete tag name
// The tags must follow the format "v1.2.3" or "component/v1.2.3" or "path/component/v1.2.3"
func latestComponentVersion(tags []*github.RepositoryTag) (string, string) {
	highest := ""
	tagName := ""

	for _, tag := range tags {
		if tag == nil || tag.Name == nil {
			continue
		}
		ver := *tag.Name
		if i := strings.LastIndex(ver, "/"); i != -1 {
			ver = ver[i+1:]
		}
		if !semver.IsValid(ver) {
			continue
		}
		canonical := semver.Canonical(ver)
		if highest == "" || semver.Compare(canonical, highest) > 0 {
			highest = canonical
			tagName = *tag.Name
		}
	}
	return highest, tagName
}
