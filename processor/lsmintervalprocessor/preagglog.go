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

package lsmintervalprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor"

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/klauspost/compress/s2"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/merger"
)

const (
	// preAggLogDir is the directory, inside the processor's data directory,
	// holding the pre-aggregation log segments.
	preAggLogDir = "preagg-log"

	preAggLogSegmentSuffix = ".log"

	// preAggLogSyncBytes is the number of bytes appended to the active
	// segment between fsyncs. Every append reaches the OS page cache, so a
	// process restart loses nothing either way; the fsync cadence only
	// matters if the host itself fails. It matches the batch commit
	// threshold, which bounds the same exposure without the log.
	preAggLogSyncBytes = dbCommitThresholdBytes

	// preAggLogRecordHeaderSize is the size of the length and CRC fields
	// that precede every record body.
	preAggLogRecordHeaderSize = 8
)

// preAggLogMarkerKey holds the sequence number of the last log segment whose
// records are reflected in the database. It is written in the same batch as
// the values flushed from those records, so both are committed atomically.
// Aggregation keys start with the interval in seconds followed by the
// processing time in seconds, so this key never falls inside an exported
// range.
var preAggLogMarkerKey = []byte("\xff\xff\xffpreagg-log-marker")

var preAggLogCRCTable = crc32.MakeTable(crc32.Castagnoli)

// preAggLog is an append-only log of the requests merged into the
// pre-aggregation buffer. Each segment holds the requests merged since the
// previous flush; a flush starts a new segment and the database records the
// last segment it covers.
//
// A record is laid out as:
//
//	length  uint32 (big endian, length of the body)
//	crc     uint32 (big endian, CRC-32C of the body)
//	body:
//	  processing time  int64 (big endian, unix seconds)
//	  metadata key     uvarint length + merger.Key binary encoding
//	  metrics          S2-compressed OTLP protobuf
//
// Appends use group commit. A caller enqueues its record while holding the
// lock of the value it merges into, so records for a key are logged in merge
// order, and then waits outside that lock until the record is written. The
// first waiter writes every enqueued record with a single write, so
// concurrent callers share one system call.
type preAggLog struct {
	dir string

	// writeMu is held while writing enqueued records to the active segment.
	writeMu  sync.Mutex
	unsynced int64

	// mu guards the fields below. enqueued and written are byte offsets
	// across all segments.
	mu       sync.Mutex
	f        *os.File
	seq      uint64
	size     int64
	buf      []byte
	spare    []byte
	enqueued uint64
	written  uint64
	err      error
}

func newPreAggLog(dir string) (*preAggLog, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create pre-aggregation log directory: %w", err)
	}
	return &preAggLog{dir: dir}, nil
}

func (l *preAggLog) segmentPath(seq uint64) string {
	return filepath.Join(l.dir, fmt.Sprintf("%020d%s", seq, preAggLogSegmentSuffix))
}

// segments returns the sequence numbers of the segments on disk, in order.
func (l *preAggLog) segments() ([]uint64, error) {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return nil, fmt.Errorf("failed to list pre-aggregation log segments: %w", err)
	}
	var seqs []uint64
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), preAggLogSegmentSuffix)
		if !ok || e.IsDir() {
			continue
		}
		seq, err := strconv.ParseUint(name, 10, 64)
		if err != nil {
			continue
		}
		seqs = append(seqs, seq)
	}
	slices.Sort(seqs)
	return seqs, nil
}

// createSegment creates the file for a new segment. It does not make it the
// active segment.
func (l *preAggLog) createSegment(seq uint64) (*os.File, error) {
	f, err := os.OpenFile(l.segmentPath(seq), os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("failed to create pre-aggregation log segment: %w", err)
	}
	if err := syncDir(l.dir); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, err
	}
	return f, nil
}

// activate writes any enqueued records to the active segment, then makes f,
// created for seq, the active segment and closes the previous one. It
// returns the sequence number of the closed segment. The caller must ensure
// that no records for the closed segment are enqueued after activate starts.
func (l *preAggLog) activate(f *os.File, seq uint64) (closed uint64, err error) {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()
	werr := l.writeEnqueuedLocked()
	l.mu.Lock()
	defer l.mu.Unlock()
	closed = l.seq
	if l.f != nil {
		// Records already reached the page cache. Flushed segments are
		// covered by the database commit, so no fsync is needed here.
		err = l.f.Close()
	}
	l.f, l.seq, l.size, l.unsynced = f, seq, 0, 0
	return closed, errors.Join(werr, err)
}

// newRecord encodes md for metaKey, leaving the processing time and the
// header to be set by enqueue. It runs outside of any lock.
func (l *preAggLog) newRecord(metaKey []byte, md pmetric.Metrics) ([]byte, error) {
	var marshaler pmetric.ProtoMarshaler
	raw, err := marshaler.MarshalMetrics(md)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metrics: %w", err)
	}
	// S2 rather than zstd: records are small, and S2 compresses them several
	// times faster for a somewhat lower ratio, which matters on the request
	// path.
	prefix := preAggLogRecordHeaderSize + 8 + binary.MaxVarintLen64 + len(metaKey)
	rec := make([]byte, preAggLogRecordHeaderSize+8, prefix+s2.MaxEncodedLen(len(raw)))
	rec = binary.AppendUvarint(rec, uint64(len(metaKey)))
	rec = append(rec, metaKey...)
	compressed := s2.Encode(rec[len(rec):cap(rec)], raw)
	return rec[:len(rec)+len(compressed)], nil
}

// enqueue adds rec, produced by newRecord, for the given processing time to
// the records waiting to be written, and returns the offset to pass to wait.
func (l *preAggLog) enqueue(rec []byte, processingTime time.Time) (uint64, error) {
	body := rec[preAggLogRecordHeaderSize:]
	binary.BigEndian.PutUint64(body, uint64(processingTime.Unix()))
	binary.BigEndian.PutUint32(rec[0:4], uint32(len(body)))
	binary.BigEndian.PutUint32(rec[4:8], crc32.Checksum(body, preAggLogCRCTable))

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return 0, l.err
	}
	if l.f == nil {
		return 0, errors.New("pre-aggregation log is not open")
	}
	l.buf = append(l.buf, rec...)
	l.enqueued += uint64(len(rec))
	return l.enqueued, nil
}

// wait returns once every record enqueued up to offset end is written to the
// OS, writing the enqueued records itself if no other caller has.
func (l *preAggLog) wait(end uint64) error {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()
	l.mu.Lock()
	written, err := l.written, l.err
	l.mu.Unlock()
	if written >= end {
		return nil
	}
	if err != nil {
		return err
	}
	return l.writeEnqueuedLocked()
}

// writeEnqueuedLocked writes the enqueued records to the active segment.
// The caller must hold writeMu. A failed write is sticky: the log rejects
// further records, because records merged into memory but not written
// cannot be recovered.
func (l *preAggLog) writeEnqueuedLocked() error {
	l.mu.Lock()
	if len(l.buf) == 0 {
		l.mu.Unlock()
		return nil
	}
	// Swap in the spare buffer so callers can keep enqueueing while the
	// write runs.
	buf, f, end, size := l.buf, l.f, l.enqueued, l.size
	l.buf, l.spare = l.spare[:0], nil
	l.mu.Unlock()

	_, err := f.Write(buf)
	if err != nil {
		// Drop a partial write so the records before it stay readable.
		_ = f.Truncate(size)
		err = fmt.Errorf("failed to write pre-aggregation log: %w", err)
	} else if l.unsynced += int64(len(buf)); l.unsynced >= preAggLogSyncBytes {
		if err = f.Sync(); err != nil {
			err = fmt.Errorf("failed to sync pre-aggregation log: %w", err)
		}
		l.unsynced = 0
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if err != nil {
		l.err = err
		return err
	}
	l.size += int64(len(buf))
	l.written = end
	l.spare = buf
	return nil
}

// replay calls fn for every record in the segment, in order. It stops at the
// first incomplete or corrupt record, which is expected at the end of the
// segment that was active when the process stopped.
func (l *preAggLog) replay(seq uint64, fn func(processingTime time.Time, metaKey []byte, md pmetric.Metrics) error) error {
	f, err := os.Open(l.segmentPath(seq))
	if err != nil {
		return fmt.Errorf("failed to open pre-aggregation log segment %d: %w", seq, err)
	}
	defer func() { _ = f.Close() }()

	r := bufio.NewReader(f)
	var (
		header [preAggLogRecordHeaderSize]byte
		body   []byte
	)
	unmarshaler := &pmetric.ProtoUnmarshaler{}
	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return fmt.Errorf("failed to read pre-aggregation log segment %d: %w", seq, err)
		}
		n := binary.BigEndian.Uint32(header[0:4])
		body = slices.Grow(body[:0], int(n))[:n]
		if _, err := io.ReadFull(r, body); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return fmt.Errorf("failed to read pre-aggregation log segment %d: %w", seq, err)
		}
		if crc32.Checksum(body, preAggLogCRCTable) != binary.BigEndian.Uint32(header[4:8]) || len(body) < 8 {
			return nil
		}
		processingTime := time.Unix(int64(binary.BigEndian.Uint64(body[:8])), 0).UTC()
		rest := body[8:]
		keyLen, k := binary.Uvarint(rest)
		if k <= 0 || uint64(len(rest)-k) < keyLen {
			return nil
		}
		metaKey := rest[k : k+int(keyLen)]
		// A fresh buffer per record, so the unmarshaled metrics never alias
		// memory that is reused.
		raw, err := s2.Decode(nil, rest[k+int(keyLen):])
		if err != nil {
			return fmt.Errorf("failed to decompress pre-aggregation log record in segment %d: %w", seq, err)
		}
		md, err := unmarshaler.UnmarshalMetrics(raw)
		if err != nil {
			return fmt.Errorf("failed to unmarshal pre-aggregation log record in segment %d: %w", seq, err)
		}
		if err := fn(processingTime, metaKey, md); err != nil {
			return err
		}
	}
}

// removeThrough deletes every segment with a sequence number up to and
// including seq, except the active one.
func (l *preAggLog) removeThrough(seq uint64) error {
	seqs, err := l.segments()
	if err != nil {
		return err
	}
	l.mu.Lock()
	active, open := l.seq, l.f != nil
	l.mu.Unlock()
	var errs []error
	for _, s := range seqs {
		if s > seq || (open && s == active) {
			continue
		}
		if err := os.Remove(l.segmentPath(s)); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, fmt.Errorf("failed to remove pre-aggregation log segment %d: %w", s, err))
		}
	}
	return errors.Join(errs...)
}

func (l *preAggLog) close() error {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()
	err := l.writeEnqueuedLocked()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f != nil {
		err = errors.Join(err, l.f.Close())
		l.f = nil
	}
	return err
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("failed to open directory for sync: %w", err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("failed to sync directory: %w", err)
	}
	return nil
}

// openPreAggLog replays the log segments not yet reflected in the database,
// removes the covered ones, and opens a new active segment. The caller must
// hold p.mu and the database must be open.
func (p *Processor) openPreAggLog() error {
	l, err := newPreAggLog(filepath.Join(p.dataDir, preAggLogDir))
	if err != nil {
		return err
	}
	covered, err := p.preAggLogMarker()
	if err != nil {
		_ = l.close()
		return err
	}
	seqs, err := l.segments()
	if err != nil {
		_ = l.close()
		return err
	}

	next := covered + 1
	if n := len(seqs); n > 0 && seqs[n-1] >= next {
		next = seqs[n-1] + 1
	}
	if i, _ := slices.BinarySearch(seqs, covered+1); i < len(seqs) {
		uncovered := seqs[i:]
		last := uncovered[len(uncovered)-1]
		batch := p.db.NewBatch()
		records, err := p.replayPreAggLog(l, batch, uncovered)
		if err == nil {
			err = p.setPreAggLogMarker(batch, last)
		}
		if err == nil {
			err = batch.Commit(p.wOpts)
		}
		_ = batch.Close()
		if err != nil {
			_ = l.close()
			return fmt.Errorf("failed to replay pre-aggregation log: %w", err)
		}
		p.logger.Info("replayed pre-aggregation log",
			zap.Int("segments", len(uncovered)),
			zap.Int("records", records),
		)
		covered = last
	}
	if err := l.removeThrough(covered); err != nil {
		p.logger.Warn("failed to remove covered pre-aggregation log segments", zap.Error(err))
	}

	f, err := l.createSegment(next)
	if err != nil {
		_ = l.close()
		return err
	}
	_, _ = l.activate(f, next)
	p.preAggLog = l
	p.preAggLogCovered = covered
	return nil
}

// replayPreAggLog merges the records of the given segments into one value
// per processing time and metadata, and writes those values to batch. Metrics
// that are not aggregated were already passed to the next consumer when the
// records were written, so they are skipped.
func (p *Processor) replayPreAggLog(l *preAggLog, batch *pebble.Batch, seqs []uint64) (int, error) {
	type replayKey struct {
		processingTime int64
		metaKey        string
	}
	values := make(map[replayKey]*pendingValue)
	var records int
	for _, seq := range seqs {
		err := l.replay(seq, func(processingTime time.Time, metaKey []byte, md pmetric.Metrics) error {
			records++
			k := replayKey{processingTime: processingTime.Unix(), metaKey: string(metaKey)}
			pv, ok := values[k]
			if !ok {
				var key merger.Key
				if err := key.Unmarshal(metaKey); err != nil {
					return fmt.Errorf("failed to decode pre-aggregation log metadata: %w", err)
				}
				pv = &pendingValue{
					metadata: key.Metadata,
					window:   processingTime,
					value: merger.NewValue(
						p.cfg.ResourceLimit,
						p.cfg.ScopeLimit,
						p.cfg.MetricLimit,
						p.cfg.DatapointLimit,
						p.cfg.ExponentialHistogramMaxBuckets,
					),
				}
				values[k] = pv
			}
			if _, errs := p.mergeMetrics(pv.value, md); len(errs) > 0 {
				p.logger.Warn("errors merging replayed pre-aggregation log record", zap.Error(errors.Join(errs...)))
			}
			return nil
		})
		if err != nil {
			return records, err
		}
	}
	var valueBuf, keyBuf []byte
	for _, pv := range values {
		var err error
		valueBuf, err = pv.value.AppendBinary(valueBuf[:0])
		if err != nil {
			return records, fmt.Errorf("failed to marshal replayed value: %w", err)
		}
		if keyBuf, err = p.mergeValueToBatch(batch, pv.window, pv.metadata, valueBuf, keyBuf); err != nil {
			return records, err
		}
	}
	return records, nil
}

// commitPreAggLogLocked commits the batch holding the values flushed from
// the segment that was active until the flush, together with the values of
// any earlier segments whose flush failed, and records the covered segment.
// next is the already-created segment that becomes active. The caller must
// hold p.mu, and every pending value must already be marked flushed so no
// appends to the previous segment are in flight.
func (p *Processor) commitPreAggLogLocked(next *os.File, nextSeq uint64) error {
	closed, err := p.preAggLog.activate(next, nextSeq)
	if err != nil {
		p.logger.Warn("failed to close pre-aggregation log segment", zap.Error(err))
	}
	if p.batch == nil {
		p.batch = newBatch(p.db)
	}
	batch := p.batch
	p.batch = nil
	defer func() { _ = batch.Close() }()

	// Segments between the covered one and the one just closed belong to
	// flushes whose commit failed. Their values are no longer in memory, so
	// they are read back from the log.
	if p.preAggLogCovered+1 < closed {
		retry := make([]uint64, 0, closed-p.preAggLogCovered-1)
		for s := p.preAggLogCovered + 1; s < closed; s++ {
			retry = append(retry, s)
		}
		if _, err := p.replayPreAggLog(p.preAggLog, batch, retry); err != nil {
			return fmt.Errorf("failed to replay pre-aggregation log segments of failed flushes: %w", err)
		}
	}
	if err := p.setPreAggLogMarker(batch, closed); err != nil {
		return err
	}
	if err := batch.Commit(p.wOpts); err != nil {
		return fmt.Errorf("failed to commit pre-aggregated values: %w", err)
	}
	p.preAggLogCovered = closed
	if err := p.preAggLog.removeThrough(closed); err != nil {
		p.logger.Warn("failed to remove covered pre-aggregation log segments", zap.Error(err))
	}
	return nil
}

func (p *Processor) preAggLogMarker() (uint64, error) {
	v, closer, err := p.db.Get(preAggLogMarkerKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to read pre-aggregation log marker: %w", err)
	}
	defer func() { _ = closer.Close() }()
	if len(v) != 8 {
		return 0, fmt.Errorf("invalid pre-aggregation log marker of %d bytes", len(v))
	}
	return binary.BigEndian.Uint64(v), nil
}

func (p *Processor) setPreAggLogMarker(batch *pebble.Batch, seq uint64) error {
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], seq)
	if err := batch.Set(preAggLogMarkerKey, v[:], nil); err != nil {
		return fmt.Errorf("failed to write pre-aggregation log marker: %w", err)
	}
	return nil
}
