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

package compressedotlpencodingextension

import (
	"bytes"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

const (
	// zstdPooledBufDefaultCap is the starting capacity for newly allocated pool
	// buffers. 64KiB avoids tiny-buffer grow churn for common payloads while
	// keeping per-entry baseline memory modest.
	zstdPooledBufDefaultCap = 64 << 10 // 64KiB

	// zstdPooledBufMaxCap bounds sync.Pool "contamination" from large outliers.
	// If a request forces a huge EncodeAll output buffer, we keep that buffer
	// out of the pool to avoid permanently inflating baseline RSS.
	//
	// 2MiB is a deliberate midpoint from our benchmark matrix: it keeps strong
	// wins for ~1MiB incompressible payloads, but avoids retaining very large
	// (4MiB+) buffers that materially increase steady-state memory risk.
	zstdPooledBufMaxCap = 2 << 20 // 2MiB
)

var (
	lz4WriterPool = sync.Pool{
		New: func() interface{} {
			return lz4.NewWriter(nil)
		},
	}
	lz4ReaderPool = sync.Pool{
		New: func() interface{} {
			return lz4.NewReader(nil)
		},
	}
)

type compressor interface {
	Compress(io.Writer, []byte) error
	Decompress([]byte) ([]byte, error)
}
type NoneCompressor struct{}

func (NoneCompressor) Compress(out io.Writer, b []byte) error {
	_, err := out.Write(b)
	return err
}

func (c NoneCompressor) Decompress(b []byte) ([]byte, error) {
	return b, nil
}

type LZ4Compressor struct{}

func (LZ4Compressor) Compress(out io.Writer, b []byte) error {
	w := lz4WriterPool.Get().(*lz4.Writer)
	defer func() {
		w.Reset(nil)
		lz4WriterPool.Put(w)
	}()
	w.Reset(out)
	if _, err := w.Write(b); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	} // w.Close must be called before Reset or pending data may be dropped.
	return nil
}

func (LZ4Compressor) Decompress(b []byte) ([]byte, error) {
	r := lz4ReaderPool.Get().(*lz4.Reader)
	defer func() {
		r.Reset(nil)
		lz4ReaderPool.Put(r)
	}()
	r.Reset(bytes.NewReader(b))
	// Pre-allocate buffer assuming ~4x compression ratio to reduce grows.
	buf := bytes.NewBuffer(make([]byte, 0, len(b)*4))
	_, err := io.Copy(buf, r)
	return buf.Bytes(), err
}

type ZSTDCompressor struct {
	// Unlike the streaming API, EncodeAll/DecodeAll are stateless — they don't
	// store references to the input or output buffers inside the encoder/decoder.
	// This means we can skip the Reset(nil) call that the LZ4 pools need: LZ4
	// uses the streaming API, so Reset(nil) is required there to release the
	// io.Writer/io.Reader reference before returning to the pool (otherwise the
	// pooled object pins the caller's buffer and prevents GC).
	//
	// IMPORTANT: do not mix EncodeAll/DecodeAll with the streaming interface on
	// the same Encoder/Decoder — the klauspost/compress library forbids this.
	writer   *zstd.Encoder
	reader   *zstd.Decoder
	bytePool sync.Pool
}

func NewZSTDCompressor() (*ZSTDCompressor, error) {
	w, err := zstd.NewWriter(nil, zstd.WithLowerEncoderMem(true))
	if err != nil {
		return nil, err
	}
	r, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	return &ZSTDCompressor{
		writer: w,
		reader: r,
		bytePool: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, 0, zstdPooledBufDefaultCap)
				return &buf
			},
		},
	}, nil
}

func (c *ZSTDCompressor) close() error {
	if c == nil {
		return nil
	}
	var err error
	if c.writer != nil {
		err = c.writer.Close()
		c.writer = nil
	}
	if c.reader != nil {
		c.reader.Close()
		c.reader = nil
	}
	return err
}

func zstdPoolSanitizeGet(bufPtr *[]byte) []byte {
	buf := *bufPtr
	if cap(buf) <= zstdPooledBufMaxCap {
		return buf
	}
	// Keep pool entries bounded even if an oversized slice is encountered.
	buf = make([]byte, 0, zstdPooledBufDefaultCap)
	*bufPtr = buf
	return buf
}

func zstdPoolBufForPut(dstPtr *[]byte, compressed []byte) *[]byte {
	if cap(compressed) <= zstdPooledBufMaxCap {
		*dstPtr = compressed[:0]
		return dstPtr
	}
	// EncodeAll had to allocate a huge output buffer; keep it out of the pool.
	// Return dst to keep the pool warm without retaining the outlier allocation.
	dst := *dstPtr
	if cap(dst) > zstdPooledBufMaxCap {
		dst = make([]byte, 0, zstdPooledBufDefaultCap)
	}
	*dstPtr = dst[:0]
	return dstPtr
}

func (c *ZSTDCompressor) Compress(out io.Writer, b []byte) error {
	dst := zstdPoolSanitizeGet(c.bytePool.Get().(*[]byte))
	// EncodeAll produces a complete zstd frame with content size in the
	// frame header. This enables DecodeAll on the read side to allocate
	// exactly the right buffer size in one shot.
	compressed := c.writer.EncodeAll(b, dst[:0])
	defer c.bytePool.Put(zstdPoolBufForPut(&dst, compressed))
	_, err := out.Write(compressed)
	return err
}

func (c *ZSTDCompressor) Decompress(b []byte) ([]byte, error) {
	// DecodeAll decodes in a single pass without streaming overhead.
	// When content size is present in the frame header (written by EncodeAll),
	// the decoder allocates exactly the right amount — no intermediate
	// bytes.NewReader, bytes.Buffer, or io.Copy temp buffer.
	// For legacy frames without content size it falls back to 2×input heuristic.
	return c.reader.DecodeAll(b, nil)
}
