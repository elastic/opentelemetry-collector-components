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
	"fmt"
	"slices"
	"time"
)

type Key struct {
	Interval       time.Duration
	ProcessingTime time.Time

	// Metadata holds an ordered list of arbitrary keys and associated
	// string values to associate with the interval and processing time.
	Metadata []KeyValues
}

type KeyValues struct {
	Key    string
	Values []string
}

// AppendBinary marshals the key into its binary representation,
// appending it to b.
func (k *Key) AppendBinary(b []byte) ([]byte, error) {
	b = slices.Grow(b, 10)
	b = binary.BigEndian.AppendUint16(b, uint16(k.Interval.Seconds()))
	b = binary.BigEndian.AppendUint64(b, uint64(k.ProcessingTime.Unix()))
	if len(k.Metadata) != 0 {
		b = binary.AppendUvarint(b, uint64(len(k.Metadata)))
		for _, kvs := range k.Metadata {
			mk := kvs.Key
			mvs := kvs.Values

			b = binary.AppendUvarint(b, uint64(len(mk)))
			b = append(b, mk...)
			b = binary.AppendUvarint(b, uint64(len(mvs)))
			for _, mv := range mvs {
				b = binary.AppendUvarint(b, uint64(len(mv)))
				b = append(b, mv...)
			}
		}
	}
	return b, nil
}

// Unmarshal unmarshals the binary representation of the Key.
func (k *Key) Unmarshal(d []byte) error {
	if len(d) < 10 {
		return errors.New("failed to unmarshal key, invalid sized buffer provided")
	}
	k.Interval = time.Duration(binary.BigEndian.Uint16(d[:2])) * time.Second
	k.ProcessingTime = time.Unix(int64(binary.BigEndian.Uint64(d[2:10])), 0)

	d = d[10:]
	if len(d) > 0 {
		numKeys, n := binary.Uvarint(d)
		if n <= 0 {
			return fmt.Errorf("error reading number of metadata keys (n=%d)", n)
		}
		d = d[n:]
		k.Metadata = make([]KeyValues, numKeys)

		for i := range numKeys {
			mklen, n := binary.Uvarint(d)
			if n <= 0 {
				return fmt.Errorf("error reading metadata key length (n=%d)", n)
			}
			d = d[n:]
			mk := string(d[:mklen])
			d = d[mklen:]

			numValues, n := binary.Uvarint(d)
			if n <= 0 {
				return fmt.Errorf("error reading number of metadata values for %q (n=%d)", mk, n)
			}
			d = d[n:]
			mvs := make([]string, numValues)
			for i := range numValues {
				mvlen, n := binary.Uvarint(d)
				if n <= 0 {
					return fmt.Errorf("error reading metadata value length for %q (n=%d)", mk, n)
				}
				d = d[n:]
				mv := string(d[:mvlen])
				d = d[mvlen:]
				mvs[i] = mv
			}

			k.Metadata[i] = KeyValues{Key: mk, Values: mvs}
		}
	}

	return nil
}
