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

package loadgenreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver"

import (
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

type zstdReadCloser struct {
	*zstd.Decoder
	f *os.File
}

func (z *zstdReadCloser) Close() error {
	z.Decoder.Close()
	return z.f.Close()
}

// openJSONLFile opens a JSONL file for reading, decompressing with zstd if specified.
// The caller is responsible for closing the returned ReadCloser when done.
func openJSONLFile(fileConfig JsonlFile) (io.ReadCloser, error) {
	f, err := os.Open(fileConfig.Path)
	if err != nil {
		return nil, err
	}
	if fileConfig.Compression == compressionZSTD {
		r, err := zstd.NewReader(f)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		return &zstdReadCloser{Decoder: r, f: f}, nil
	}
	return f, nil
}
