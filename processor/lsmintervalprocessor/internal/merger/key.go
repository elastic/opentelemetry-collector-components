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

package merger // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/merger"

import (
	"encoding/binary"
	"errors"
	"slices"
	"time"
)

const KeySizeBinary = 10

// TODO (lahsivjar): Think about multitenancy, should be part of the key
type Key struct {
	Interval       time.Duration
	ProcessingTime time.Time
}

// NewKey creates a new instance of the merger key.
func NewKey(ivl time.Duration, pTime time.Time) Key {
	return Key{
		Interval:       ivl,
		ProcessingTime: pTime,
	}
}

// AppendBinary marshals the key into its binary representation,
// and appends it to to b.
func (k *Key) AppendBinary(b []byte) ([]byte, error) {
	b = slices.Grow(b, KeySizeBinary)

	var buf [8]byte
	ivlSeconds := uint16(k.Interval.Seconds())
	binary.BigEndian.PutUint16(buf[:2], ivlSeconds)
	b = append(b, buf[:2]...)

	binary.BigEndian.PutUint64(buf[:], uint64(k.ProcessingTime.Unix()))
	b = append(b, buf[:]...)

	return b, nil
}

// Unmarshal unmarshals the binary representation of the Key.
func (k *Key) Unmarshal(d []byte) error {
	if len(d) != 10 {
		return errors.New("failed to unmarshal key, invalid sized buffer provided")
	}
	k.Interval = time.Duration(binary.BigEndian.Uint16(d[:2])) * time.Second
	k.ProcessingTime = time.Unix(int64(binary.BigEndian.Uint64(d[2:10])), 0)
	return nil
}
