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

package mobile

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/cespare/xxhash/v2"
)

var (
	// Regex patterns for java stack trace processing
	errorOrCausePattern = regexp.MustCompile(`^((?:Caused\sby:\s[^:]+)|(?:[^\s][^:]+))(:\s.+)?$`)
	callSitePattern     = regexp.MustCompile(`^\s+at\s.+(:\d+)\)$`)

	// Regex patterns for swift stack trace processing
	swiftCrashedThreadPattern = regexp.MustCompile(`\nThread \d+ Crashed:.*\n((?:.+\n)+)\n`)
	swiftCrashLinePattern     = regexp.MustCompile(`^\d+\s+[^\s]+\s+([^\s]+\s[^\s]+\s+\+)\s+[^\s]+$`)

	// Common patterns
	allLinesPattern = regexp.MustCompile(`(m?).+`)
	unwantedPattern = regexp.MustCompile(`\s+`)
)

func CreateJavaStacktraceGroupingKey(stacktrace string) string {
	hash := xxhash.Sum64String(curateJavaStacktrace(stacktrace))
	return fmt.Sprintf("%016x", hash)
}

func curateJavaStacktrace(stacktrace string) string {
	curatedLines := allLinesPattern.ReplaceAllStringFunc(stacktrace, func(s string) string {
		if errorOrCausePattern.MatchString(s) {
			return errorOrCausePattern.ReplaceAllString(s, "$1")
		}
		if callSitePattern.MatchString(s) {
			return strings.Replace(s, callSitePattern.FindStringSubmatch(s)[1], "", 1)
		}
		return s
	})

	return unwantedPattern.ReplaceAllString(curatedLines, "")
}

func CreateSwiftStacktraceGroupingKey(stacktrace string) (string, error) {
	crashThreadContent, err := findAndCurateSwiftStacktrace(stacktrace)

	if err != nil {
		return "", err
	}

	hash := xxhash.Sum64String(crashThreadContent)
	return fmt.Sprintf("%016x", hash), nil
}

func findAndCurateSwiftStacktrace(stacktrace string) (string, error) {
	match := swiftCrashedThreadPattern.FindStringSubmatch(stacktrace)

	if match == nil {
		return "", errors.New("no swift crashed thread found")
	}

	curatedLines := allLinesPattern.ReplaceAllStringFunc(match[1], func(s string) string {
		if swiftCrashLinePattern.MatchString(s) {
			return strings.Replace(s, swiftCrashLinePattern.FindStringSubmatch(s)[1], "", 1)
		}
		return s
	})

	return unwantedPattern.ReplaceAllString(curatedLines, ""), nil
}
