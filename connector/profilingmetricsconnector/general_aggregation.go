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
	"strings"
)

type hotspotInfo struct {
	pack  string
	class string
}

func extractHotspotInfo(funcString string) (*hotspotInfo, error) {
	// Get rid of the arguments
	parenthesisIdx := strings.Index(funcString, "(")
	if parenthesisIdx == -1 {
		parenthesisIdx = len(funcString)
	}
	withoutArgs := strings.TrimSpace(funcString[:parenthesisIdx])

	// Extract fully qualified class name and method name.
	lastSpaceIndex := strings.LastIndex(withoutArgs, " ")
	var fullyQualifiedClassAndMethod string
	if lastSpaceIndex != -1 {
		fullyQualifiedClassAndMethod = withoutArgs[lastSpaceIndex+1:]
	} else {
		fullyQualifiedClassAndMethod = withoutArgs
	}

	// Separate fully qualified class name from the method name.
	lastDotIndexForMethod := strings.LastIndex(fullyQualifiedClassAndMethod, ".")
	var fullyQualifiedClassName string
	if lastDotIndexForMethod != -1 {
		fullyQualifiedClassName = fullyQualifiedClassAndMethod[:lastDotIndexForMethod]
	} else {
		fullyQualifiedClassName = fullyQualifiedClassAndMethod
	}

	// Separate package name from the class name.
	var packageName, className string
	lastDotIndexForPackage := strings.LastIndex(fullyQualifiedClassName, ".")
	if lastDotIndexForPackage != -1 {
		packageName = fullyQualifiedClassName[:lastDotIndexForPackage]
		className = fullyQualifiedClassName[lastDotIndexForPackage+1:]
	} else {
		packageName = ""
		className = fullyQualifiedClassName
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
