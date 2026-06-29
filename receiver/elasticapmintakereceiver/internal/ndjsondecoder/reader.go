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

// Adapted from github.com/elastic/apm-data/input/elasticapm/internal/decoder.
// Consolidates decoder.go, line_reader.go, limited_reader.go, stream_decoder.go.

package ndjsondecoder

import (
	"bufio"
	"bytes"
	"errors"
	"io"

	jsoniter "github.com/json-iterator/go"
)

// ConfigCompatibleWithStandardLibrary + UseNumber
var jsonConfig = jsoniter.Config{
	EscapeHTML:             true,
	SortMapKeys:            true,
	ValidateJsonRawMessage: true,
	UseNumber:              true,
}.Froze()

// JSONDecodeError is a custom error that can occur during JSON decoding.
type JSONDecodeError string

func (s JSONDecodeError) Error() string { return string(s) }

// ErrLineTooLong is returned when a line exceeds the maximum permitted length.
var ErrLineTooLong = errors.New("line exceeded permitted length")

// LineReader reads length-limited lines from streams using a limited amount of memory.
type LineReader struct {
	br            *bufio.Reader
	maxLineLength int
	skip          bool
}

func newLineReader(reader *bufio.Reader, maxLineLength int) *LineReader {
	return &LineReader{
		br:            reader,
		maxLineLength: maxLineLength,
	}
}

// Reset sets lr's underlying *bufio.Reader to br, and clears any state.
func (lr *LineReader) Reset(br *bufio.Reader) {
	lr.br = br
	lr.skip = false
}

// ReadLine reads the next line from the given reader.
// If it encounters a line longer than maxLineLength it returns the first
// maxLineLength bytes with ErrLineTooLong. On the next call it returns the
// next line.
func (lr *LineReader) ReadLine() ([]byte, error) {
	for {
		prefix := false
		line, err := lr.br.ReadSlice('\n')
		if err == bufio.ErrBufferFull {
			prefix = true
		}

		if !lr.skip {
			if prefix {
				lr.skip = true
				return line[:lr.maxLineLength], ErrLineTooLong
			}

			if len(line) > 0 && line[len(line)-1] == '\n' {
				line = line[:len(line)-1]
			}
			return line, err
		} else if err == io.EOF {
			return nil, io.EOF
		} else if !prefix {
			lr.skip = false
		}
	}
}

// LimitedReader is like io.LimitedReader, but returns an error upon detecting
// a request that is too large. Based on net/http.maxBytesReader.
type LimitedReader struct {
	R   io.Reader
	err error
	N   int64
}

// Read implements the standard Read interface, returning an error if more than
// l.N bytes are read. After each read, l.N is decremented; if the limit is
// exceeded, l.N is set to a negative value.
func (l *LimitedReader) Read(p []byte) (n int, err error) {
	if l.err != nil || len(p) == 0 {
		return 0, l.err
	}
	if int64(len(p)) > l.N+1 {
		p = p[:l.N+1]
	}
	n, err = l.R.Read(p)

	if int64(n) <= l.N {
		l.N -= int64(n)
		l.err = err
		return n, err
	}

	n, l.N = int(l.N), l.N-int64(n)
	l.err = errors.New("too large")
	return n, l.err
}

// NewNDJSONStreamDecoder returns a new NDJSONStreamDecoder which decodes
// ND-JSON lines from r, with a maximum line length of maxLineLength.
func NewNDJSONStreamDecoder(r io.Reader, maxLineLength int) *NDJSONStreamDecoder {
	var dec NDJSONStreamDecoder
	dec.bufioReader = bufio.NewReaderSize(r, maxLineLength)
	dec.lineReader = newLineReader(dec.bufioReader, maxLineLength)
	return &dec
}

// NDJSONStreamDecoder decodes a stream of ND-JSON lines from an io.Reader.
type NDJSONStreamDecoder struct {
	latestError      error
	bufioReader      *bufio.Reader
	lineReader       *LineReader
	latestLine       []byte
	latestLineReader bytes.Reader
	isEOF            bool
}

// Reset sets the underlying io.Reader to r, and resets any reading/decoding state.
func (dec *NDJSONStreamDecoder) Reset(r io.Reader) {
	dec.bufioReader.Reset(r)
	dec.lineReader.Reset(dec.bufioReader)
	dec.isEOF = false
	dec.latestLine = nil
	dec.resetLatestLineReader()
}

// Decode decodes the next line into v.
func (dec *NDJSONStreamDecoder) Decode(v interface{}) error {
	defer dec.resetLatestLineReader()
	if dec.latestLineReader.Size() == 0 {
		_, _ = dec.ReadAhead() // error checked below
	}
	if len(dec.latestLine) == 0 || (dec.latestError != nil && !dec.isEOF) {
		return dec.latestError
	}

	iter := jsonConfig.BorrowIterator(dec.latestLine)
	defer jsonConfig.ReturnIterator(iter)
	iter.ReadVal(v)
	if iter.Error != nil && iter.Error != io.EOF {
		return JSONDecodeError("data read error: " + iter.Error.Error())
	}
	return nil // successful decode; EOF state is tracked via ReadAhead/isEOF
}

// ReadAhead reads the next NDJSON line, buffering it for a subsequent call to Decode.
func (dec *NDJSONStreamDecoder) ReadAhead() ([]byte, error) {
	line, readErr := dec.lineReader.ReadLine()
	dec.latestLine = line
	dec.latestLineReader.Reset(dec.latestLine)
	dec.latestError = readErr
	dec.isEOF = readErr == io.EOF
	return line, readErr
}

func (dec *NDJSONStreamDecoder) resetLatestLineReader() {
	dec.latestLineReader.Reset(nil)
	dec.latestError = nil
}

// IsEOF reports whether the underlying reader reached the end.
func (dec *NDJSONStreamDecoder) IsEOF() bool { return dec.isEOF }

// LatestLine returns the latest line read as []byte.
func (dec *NDJSONStreamDecoder) LatestLine() []byte { return dec.latestLine }
