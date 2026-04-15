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
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"

	"golang.org/x/mod/semver"
	"gopkg.in/yaml.v3"
)

const edotBaseKey = "edot-base"

type versionsYAML struct {
	ModuleSets      map[string]moduleSet `yaml:"module-sets"`
	ExcludedModules []string             `yaml:"excluded-modules"`
}

type moduleSet struct {
	Version string   `yaml:"version"`
	Modules []string `yaml:"modules"`
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("versionscheck: ")
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	versionsPath := flag.String("versions", "../../versions.yaml", "path to versions.yaml")
	quiet := flag.Bool("q", false, "suppress success message on stdout")
	flag.Parse()

	vpath := *versionsPath
	if vpath == "" {
		vpath = "../../versions.yaml"
	}

	data, err := os.ReadFile(vpath)
	if err != nil {
		return fmt.Errorf("read versions file: %w", err)
	}

	cfg, err := parseVersionsYAML(data)
	if err != nil {
		return err
	}

	ms, ok := cfg.ModuleSets[edotBaseKey]
	if !ok {
		return fmt.Errorf("no entry %s in %s", edotBaseKey, vpath)
	}
	if ms.Version == "" {
		return fmt.Errorf("empty version for %s", edotBaseKey)
	}
	if !semver.IsValid(ms.Version) {
		return fmt.Errorf("invalid semver for %s version: %q", edotBaseKey, ms.Version)
	}

	tagOut, err := gitTagList()
	if err != nil {
		return err
	}
	latest := latestComponentVersion(strings.Split(tagOut, "\n"))

	// Require edot-base to be strictly greater than the highest semver tag (if any).
	if latest != "" && semver.Compare(ms.Version, latest) <= 0 {
		return fmt.Errorf("module-sets version %s must be greater than tag %s",
			ms.Version, latest)
	}

	if !*quiet {
		if _, err := fmt.Fprintf(os.Stdout, "versionscheck: ok (%s version %s, %d modules checked)\n", edotBaseKey, ms.Version, len(ms.Modules)); err != nil {
			log.Printf("warning: could not write success message: %v", err)
		}
	}
	return nil
}

func gitTagList() (string, error) {
	cmd := exec.Command("git", "tag", "-l")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git tag -l: %w", err)
	}
	return string(out), nil
}

func parseVersionsYAML(data []byte) (*versionsYAML, error) {
	var cfg versionsYAML
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse versions.yaml: %w", err)
	}
	return &cfg, nil
}

// latestComponentVersion returns the highest valid version amongst all components
// The tags must follow the format "component/v1.2.3" or "path/component/v1.2.3"
func latestComponentVersion(tagLines []string) string {
	highest := ""

	for _, line := range tagLines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		i := strings.LastIndex(line, "/")
		if i <= 0 {
			continue
		}
		ver := line[i+1:]
		if !semver.IsValid(ver) {
			continue
		}
		canonical := semver.Canonical(ver)
		if highest == "" || semver.Compare(canonical, highest) > 0 {
			highest = canonical
		}
	}
	return highest
}
