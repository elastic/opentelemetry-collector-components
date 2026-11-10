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

package compressedotlpencodingextension // import "github.com/elastic/opentelemetry-collector-components/extension/compressedotlpencodingextension"

import "errors"

// Magic encodes a 6-byte header for compressed OTLP streams:
// [0-1] Magic identifier (0x11, 0x22)
// [2-3] Version (0x00, 0x00)
// [4]   Protocol
// [5]   Compression
type Magic struct {
	protocol    protocol
	compression compression
}

func (m Magic) ToBytes() []byte {
	// 2 bytes magic identifier
	// 2 bytes version of magic

	// In version 0, 2 more bytes follow:
	// 1 byte protocol
	// 1 byte compression
	return []byte{0x11, 0x22, 0x00, 0x00, byte(m.protocol), byte(m.compression)}
}

func parseMagic(b []byte) (Magic, []byte, error) {
	if len(b) < 4 {
		return Magic{}, nil, errors.New("bytes too short for magic")
	}
	if b[0] != 0x11 || b[1] != 0x22 {
		return Magic{}, nil, errors.New("magic identifier does not match")
	}
	switch {
	case b[2] == 0x00 && b[3] == 0x00:
		if len(b) < 6 {
			return Magic{}, nil, errors.New("v0: magic content too short")
		}
		p := protocol(b[4])
		if !p.Valid() {
			return Magic{}, nil, errors.New("v0: invalid protocol")
		}
		c := compression(b[5])
		if !c.Valid() {
			return Magic{}, nil, errors.New("v0: invalid compression")
		}
		return Magic{
			protocol:    p,
			compression: c,
		}, b[6:], nil
	default:
		return Magic{}, nil, errors.New("unknown magic version")
	}
}
