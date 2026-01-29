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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCurateJavaStacktrace(t *testing.T) {
	for _, tc := range []struct {
		name        string
		stacktraces []string
		curated     string
	}{
		{
			name:        "standalone_stacktrace",
			stacktraces: []string{readJavaStacktraceFile(t, "stacktrace1_a.txt"), readJavaStacktraceFile(t, "stacktrace1_b.txt"), readJavaStacktraceFile(t, "stacktrace1_c.txt")},
			curated:     readJavaStacktraceFile(t, "curated_stacktrace1.txt"),
		},
		{
			name:        "stacktrace_with_cause",
			stacktraces: []string{readJavaStacktraceFile(t, "stacktrace2_a.txt"), readJavaStacktraceFile(t, "stacktrace2_b.txt"), readJavaStacktraceFile(t, "stacktrace2_c.txt")},
			curated:     readJavaStacktraceFile(t, "curated_stacktrace2.txt"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, stacktrace := range tc.stacktraces {
				assert.Equal(t, tc.curated, curateJavaStacktrace(stacktrace))
			}
		})
	}
}

func TestCreateJavaStacktraceGroupingKey(t *testing.T) {
	for _, tc := range []struct {
		name        string
		stacktraces []string
		expectedId  string
	}{
		{
			name:        "standalone_stacktrace",
			stacktraces: []string{readJavaStacktraceFile(t, "stacktrace1_a.txt"), readJavaStacktraceFile(t, "stacktrace1_b.txt"), readJavaStacktraceFile(t, "stacktrace1_c.txt")},
			expectedId:  "e3d876640dd47864",
		},
		{
			name:        "stacktrace_with_cause",
			stacktraces: []string{readJavaStacktraceFile(t, "stacktrace2_a.txt"), readJavaStacktraceFile(t, "stacktrace2_b.txt"), readJavaStacktraceFile(t, "stacktrace2_c.txt")},
			expectedId:  "390d84a9a633fa73",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, stacktrace := range tc.stacktraces {
				assert.Equal(t, tc.expectedId, CreateJavaStacktraceGroupingKey(stacktrace))
			}
		})
	}
}

func TestCurateSwiftStacktrace(t *testing.T) {
	for _, tc := range []struct {
		name          string
		crashFile     string
		expectedValue string
	}{
		{
			name:          "thread_0_crash",
			crashFile:     "thread-0-crash.txt",
			expectedValue: readSwiftStacktraceFile(t, "curated-thread-0-crash.txt"),
		},
		{
			name:          "thread_8_crash",
			crashFile:     "thread-8-crash.txt",
			expectedValue: readSwiftStacktraceFile(t, "curated-thread-8-crash.txt"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			crashReport := readSwiftStacktraceFile(t, tc.crashFile)
			curatedValue, err := findAndCurateSwiftStacktrace(crashReport)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedValue, curatedValue)
		})
	}
}

func TestCreateSwiftStacktraceGroupingKey(t *testing.T) {
	for _, tc := range []struct {
		name       string
		crashFile  string
		expectedId string
	}{
		{
			name:       "thread_0_crash",
			crashFile:  "thread-0-crash.txt",
			expectedId: "e73b3795d98d4c79",
		},
		{
			name:       "thread_8_crash",
			crashFile:  "thread-8-crash.txt",
			expectedId: "e737b0da1c8f9d5a",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			crashReport := readSwiftStacktraceFile(t, tc.crashFile)
			actualId, err := CreateSwiftStacktraceGroupingKey(crashReport)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedId, actualId)
		})
	}
}

func readJavaStacktraceFile(t *testing.T, fileName string) string {
	bytes, err := os.ReadFile(filepath.Join("testdata", "java", fileName))
	require.NoError(t, err)
	return string(bytes)
}

func readSwiftStacktraceFile(t *testing.T, fileName string) string {
	bytes, err := os.ReadFile(filepath.Join("testdata", "swift", fileName))
	require.NoError(t, err)
	return string(bytes)
}
