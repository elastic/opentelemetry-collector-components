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

package profilingmetricsconnector // import "github.com/elastic/opentelemetry-collector-components/connector/profilingmetricsconnector"

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	// Breakdown of the regex:
	// `(?:\S+\s+)?`
	//   - `(?:\S+\s+)`: Non-capturing group for the optional return type (e.g., "void ", "int ", "long ").
	//     - `\S+`: Matches one or more non-whitespace characters (the return type).
	//     - `\s+`: Matches one or more whitespace characters.
	//   - `?`: Makes the entire return type group optional.
	//
	// `(`
	//   - Start of the first capturing group, which will contain the full class path.
	//
	// `[a-zA-Z_][a-zA-Z0-9_.$<>+]+`
	//   - Matches the first segment of the class name (e.g., "org", "sun", "MyClass").
	//   - `[a-zA-Z_]`: Starts with a letter or underscore (standard Java identifier start).
	//   - `[a-zA-Z0-9_.$<>+]+`: Followed by one or more letters, numbers, underscores,
	//     dots (for package separators), dollar signs (for inner classes),
	//     angle brackets (`<`, `>`) and plus signs (`+`) for synthetic class names
	//     like those generated for lambdas (e.g., `$$Lambda+<hidden>`).
	//
	// `(?:\.[a-zA-Z_][a-zA-Z0-9_.$<>+]+)*`
	//   - Non-capturing group for subsequent package/class segments (e.g., ".apache.lucene.search").
	//   - `\.`: Matches a literal dot.
	//   - `[a-zA-Z_][a-zA-Z0-9_.$<>+]+`: Matches another identifier-like segment.
	//   - `*`: Allows zero or more of these segments.
	//
	// `)`
	//   - End of the first capturing group (the full class path).
	//
	// `\.`
	//   - Matches the literal dot that separates the class name from the method name.
	//
	// `([a-zA-Z_][a-zA-Z0-9_.$<>+]+)`
	//   - Second capturing group, for the method name itself.
	//   - `[a-zA-Z_][a-zA-Z0-9_.$<>+]+`: Matches a valid method name, which can also include
	//     `$` for synthetic methods (e.g., `lambda$fillGapRunnable$13`).
	//
	// `\(`
	//   - Matches the literal opening parenthesis that signifies the start of method arguments.
	hotspotRegex = regexp.MustCompile(`(?:\S+\s+)?([a-zA-Z_][a-zA-Z0-9_.$<>+]+(?:\.[a-zA-Z_][a-zA-Z0-9_.$<>+]+)*)\.([a-zA-Z_][a-zA-Z0-9_.$<>+]+)\(`)

	errHotspotNoMatch = errors.New("input does not match hotspot regex")
)

// hotspotInfo holds extracted Java package and class name information.
type hotspotInfo struct {
	pack  string
	class string
}

func extractHotspotInfo(funcString string) (*hotspotInfo, error) {
	matches := hotspotRegex.FindStringSubmatch(funcString)

	if len(matches) < 2 {
		return nil, fmt.Errorf("could not extract class information from string: '%s': %w",
			funcString, errHotspotNoMatch)
	}

	// The first captured group is the fully qualified class name.
	fullClassName := matches[1]

	// Determine the last occurrence of a dot in the full class name.
	// This dot separates the class name from its package path.
	lastDotIndex := strings.LastIndex(fullClassName, ".")

	var packageName string
	var className string

	if lastDotIndex == -1 {
		// If no dot is found, it means the class is in the default package.
		packageName = ""
		className = fullClassName
	} else {
		packageName = fullClassName[:lastDotIndex]
		className = fullClassName[lastDotIndex+1:]
	}

	return &hotspotInfo{
		pack:  packageName,
		class: className,
	}, nil
}

// golangInfo holds extracted Go package information.
type golangInfo struct {
	pack string
}

func extractGolangInfo(funcString string) *golangInfo {
	// Find the last slash. This separates the import path from the final package name segment.
	lastSlashIdx := strings.LastIndex(funcString, "/")

	if lastSlashIdx != -1 {
		basePath := funcString[:lastSlashIdx]
		lastSegment := funcString[lastSlashIdx+1:]

		// Find the first dot in this last segment that separates the package name
		// from the function/method name within that segment.
		firstDotInSegmentIdx := strings.Index(lastSegment, ".")
		if firstDotInSegmentIdx != -1 {
			return &golangInfo{pack: basePath + "/" + lastSegment[:firstDotInSegmentIdx]}
		}

		return &golangInfo{
			pack: funcString,
		}
	} else {
		// If no slash exists, the function string is likely just "package.FunctionName"
		// or "package.(*Type).MethodName".
		firstDotIdx := strings.Index(funcString, ".")
		if firstDotIdx != -1 {
			return &golangInfo{pack: funcString[:firstDotIdx]}
		}

		return &golangInfo{pack: funcString}
	}
}
