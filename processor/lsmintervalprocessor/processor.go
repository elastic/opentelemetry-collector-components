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
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/merger"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/metadata"
)

var _ processor.Metrics = (*Processor)(nil)

const (
	// pebbleMemTableSize defines the max steady state size of a memtable.
	// There can be more than 1 memtable in memory at a time as it takes
	// time for old memtable to flush. The memtable size also defines
	// the size for large batches. A large batch is a batch which will
	// take atleast half of the memtable size. Note that the Batch#Len
	// is not the same as the memtable size that the batch will occupy
	// as data in batches are encoded differently. In general, the
	// memtable size of the batch will be higher than the length of the
	// batch data.
	//
	// On commit, data in the large batch maybe kept by pebble and thus
	// large batches will need to be reallocated. Note that large batch
	// classification uses the memtable size that a batch will occupy
	// rather than the length of data slice backing the batch.
	pebbleMemTableSize = 32 << 20 // 32MB

	// pebbleMemTableStopWritesThreshold is the hard limit on the maximum
	// number of memtables that could be queued before which writes are
	// stopped. This value should be at least 2 or writes will stop whenever
	// a MemTable is being flushed.
	pebbleMemTableStopWritesThreshold = 2

	// dbCommitThresholdBytes is a soft limit and the batch is committed
	// to the DB as soon as it crosses this threshold. To make sure that
	// the commit threshold plays well with the max retained batch size
	// the threshold should be kept smaller than the sum of max retained
	// batch size and encoded size of aggregated data to be committed.
	// However, this requires https://github.com/cockroachdb/pebble/pull/3139.
	// So, for now we are only tweaking the available options.
	dbCommitThresholdBytes = 8 << 20 // 8MB
)

type Processor struct {
	telemetryBuilder   *metadata.TelemetryBuilder
	cfg                *config.Config
	sortedMetadataKeys []string

	db      *pebble.DB
	dataDir string
	dbOpts  *pebble.Options
	wOpts   *pebble.WriteOptions

	intervals  []intervalDef
	next       consumer.Metrics
	bufferPool sync.Pool

	mu             sync.Mutex
	batch          *pebble.Batch
	processingTime time.Time

	ctx           context.Context
	cancel        context.CancelFunc
	exportStopped chan struct{}
	logger        *zap.Logger
}

func newProcessor(
	telemetrySettings component.TelemetrySettings,
	cfg *config.Config,
	ivlDefs []intervalDef,
	log *zap.Logger,
	next consumer.Metrics,
) (*Processor, error) {
	telemetryBuilder, err := metadata.NewTelemetryBuilder(telemetrySettings)
	if err != nil {
		return nil, fmt.Errorf("failed to create telemetry builder: %w", err)
	}

	dbOpts := &pebble.Options{
		Merger: &pebble.Merger{
			Name: "pmetrics_merger",
			Merge: func(key, value []byte) (pebble.ValueMerger, error) {
				v := merger.NewValue(
					cfg.ResourceLimit,
					cfg.ScopeLimit,
					cfg.MetricLimit,
					cfg.DatapointLimit,
					cfg.ExponentialHistogramMaxBuckets,
				)
				if err := v.Unmarshal(value); err != nil {
					return nil, fmt.Errorf("failed to unmarshal value from db: %w", err)
				}
				return merger.New(v), nil
			},
		},
		MemTableSize:                pebbleMemTableSize,
		MemTableStopWritesThreshold: pebbleMemTableStopWritesThreshold,
	}
	writeOpts := pebble.Sync
	dataDir := cfg.Directory
	if dataDir == "" {
		log.Info("no directory specified, switching to in-memory mode")
		dbOpts.FS = vfs.NewMem()
		dbOpts.DisableWAL = true
		writeOpts = pebble.NoSync
		dataDir = "/data" // will be created in the in-mem file-system
	}

	sortedMetadataKeys := append([]string{}, cfg.MetadataKeys...)
	sort.Strings(sortedMetadataKeys)

	ctx, cancel := context.WithCancel(context.Background())
	return &Processor{
		telemetryBuilder:   telemetryBuilder,
		cfg:                cfg,
		sortedMetadataKeys: sortedMetadataKeys,
		dataDir:            dataDir,
		dbOpts:             dbOpts,
		wOpts:              writeOpts,
		intervals:          ivlDefs,
		next:               next,
		processingTime:     time.Now().UTC().Truncate(ivlDefs[0].Duration),
		ctx:                ctx,
		cancel:             cancel,
		logger:             log,
	}, nil
}

func (p *Processor) Start(ctx context.Context, host component.Host) error {
	p.mu.Lock()
	if p.db == nil {
		db, err := pebble.Open(p.dataDir, p.dbOpts)
		if err != nil {
			return fmt.Errorf("failed to open database: %w", err)
		}
		p.db = db
	}

	if p.exportStopped == nil {
		p.exportStopped = make(chan struct{})
	}
	p.mu.Unlock()

	go func() {
		defer close(p.exportStopped)
		to := p.processingTime.Add(p.intervals[0].Duration)
		timer := time.NewTimer(time.Until(to))
		defer timer.Stop()

		for {
			select {
			case <-p.ctx.Done():
				return
			case <-timer.C:
			}

			p.mu.Lock()
			batch := p.batch
			p.batch = nil
			p.processingTime = to
			p.mu.Unlock()

			// Export the batch
			if err := p.commitAndExport(p.ctx, batch, to); err != nil {
				p.logger.Warn("failed to export", zap.Error(err), zap.Time("end_time", to))
			}

			to = to.Add(p.intervals[0].Duration)
			timer.Reset(time.Until(to))
		}
	}()
	return p.registerPebbleMetrics()
}

func (p *Processor) Shutdown(ctx context.Context) error {
	defer p.logger.Info("shutdown finished")
	// Signal stop for the exporting goroutine
	p.cancel()

	// Wait for the exporting goroutine to stop. Note that we don't need to acquire
	// mutex here since even if there is a race between Start and Shutdown the
	// processor context is cancelled ensuring export goroutine will be noop.
	if p.exportStopped != nil {
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed to shutdown due to context timeout while waiting for export to stop: %w", ctx.Err())
		case <-p.exportStopped:
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Ensure all data in the database is exported
	if p.db != nil {
		p.logger.Info("exporting all data before shutting down")
		if p.batch != nil {
			if err := p.batch.Commit(p.wOpts); err != nil {
				return fmt.Errorf("failed to commit batch: %w", err)
			}
			if err := p.batch.Close(); err != nil {
				return fmt.Errorf("failed to close batch: %w", err)
			}
			p.batch = nil
		}

		var errs []error
		for _, ivl := range p.intervals {
			// At any particular time there will be 1 export candidate for
			// each aggregation interval. We will align the end time and
			// process each of these.
			to := p.processingTime.Truncate(ivl.Duration).Add(ivl.Duration)
			if err := p.export(ctx, to); err != nil {
				errs = append(errs, fmt.Errorf(
					"failed to export metrics for interval %s: %w", ivl.Duration, err),
				)
			}
		}
		if len(errs) > 0 {
			return fmt.Errorf("failed while running final export: %w", errors.Join(errs...))
		}
		if err := p.db.Close(); err != nil {
			return fmt.Errorf("failed to close database: %w", err)
		}
		// All future operations are invalid after db is closed
		p.db = nil
	}
	if p.telemetryBuilder != nil {
		p.telemetryBuilder.Shutdown()
	}
	return nil
}

func (p *Processor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *Processor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	v := merger.NewValue(
		p.cfg.ResourceLimit,
		p.cfg.ScopeLimit,
		p.cfg.MetricLimit,
		p.cfg.DatapointLimit,
		p.cfg.ExponentialHistogramMaxBuckets,
	)

	var errs []error
	nextMD := pmetric.NewMetrics()
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		var nextMDResourceMetrics pmetric.ResourceMetrics
		rm := rms.At(i)
		sms := rm.ScopeMetrics()
		for i := 0; i < sms.Len(); i++ {
			var nextMDScopeMetrics pmetric.ScopeMetrics
			sm := sms.At(i)
			ms := sm.Metrics()
			for i := 0; i < ms.Len(); i++ {
				m := ms.At(i)
				switch t := m.Type(); t {
				case pmetric.MetricTypeEmpty, pmetric.MetricTypeGauge:
					// TODO (lahsivjar): implement support for gauges
					//
					// For now, pass through by copying across to nextMD below.
					break
				case pmetric.MetricTypeSummary:
					if p.cfg.PassThrough.Summary {
						// Copy across to nextMD below.
						break
					}
					if err := v.MergeMetric(rm, sm, m); err != nil {
						errs = append(errs, err)
					}
					continue
				case pmetric.MetricTypeSum, pmetric.MetricTypeHistogram, pmetric.MetricTypeExponentialHistogram:
					if err := v.MergeMetric(rm, sm, m); err != nil {
						errs = append(errs, err)
					}
					continue
				default:
					// All metric types are handled, this is unexpected
					errs = append(errs, fmt.Errorf("unexpected metric type, dropping: %d", t))
					continue
				}

				if nextMDScopeMetrics == (pmetric.ScopeMetrics{}) {
					if nextMDResourceMetrics == (pmetric.ResourceMetrics{}) {
						nextMDResourceMetrics = nextMD.ResourceMetrics().AppendEmpty()
						rm.Resource().CopyTo(nextMDResourceMetrics.Resource())
						nextMDResourceMetrics.SetSchemaUrl(rm.SchemaUrl())
					}
					nextMDScopeMetrics = nextMDResourceMetrics.ScopeMetrics().AppendEmpty()
					sm.Scope().CopyTo(nextMDScopeMetrics.Scope())
					nextMDScopeMetrics.SetSchemaUrl(sm.SchemaUrl())
				}
				m.CopyTo(nextMDScopeMetrics.Metrics().AppendEmpty())
			}
		}
	}

	mb, ok := p.bufferPool.Get().(*mergeBuffer)
	if !ok {
		mb = &mergeBuffer{}
	}
	defer p.bufferPool.Put(mb)

	var err error
	mb.value, err = v.AppendBinary(mb.value[:0])
	if err != nil {
		return errors.Join(append(errs, fmt.Errorf("failed to marshal value to proto binary: %w", err))...)
	}

	clientInfo := client.FromContext(ctx)
	clientMetadata := make([]merger.KeyValues, 0, len(p.sortedMetadataKeys))
	attributes := make([]attribute.KeyValue, 0, len(p.sortedMetadataKeys))
	for _, k := range p.sortedMetadataKeys {
		if values := clientInfo.Metadata.Get(k); len(values) != 0 {
			clientMetadata = append(clientMetadata, merger.KeyValues{
				Key:    k,
				Values: values,
			})
			attributes = append(attributes, attribute.StringSlice(k, values))
		}
	}

	if err := p.mergeToBatch(mb, clientMetadata); err != nil {
		return fmt.Errorf("failed to merge the value to batch: %w", err)
	}

	p.telemetryBuilder.LsmintervalProcessedDataPoints.Add(
		ctx,
		int64(md.DataPointCount()),
		metric.WithAttributes(attributes...),
	)
	p.telemetryBuilder.LsmintervalProcessedBytes.Add(
		ctx,
		int64(len(mb.value)+len(mb.key)),
		metric.WithAttributes(attributes...),
	)

	// Call next for the metrics remaining in the input if any
	if nextMD.DataPointCount() > 0 {
		if err := p.next.ConsumeMetrics(ctx, nextMD); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (p *Processor) mergeToBatch(mb *mergeBuffer, clientMetadata []merger.KeyValues) (err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.batch == nil {
		p.batch = newBatch(p.db)
	}

	for _, ivl := range p.intervals {
		key := merger.Key{
			Interval:       ivl.Duration,
			ProcessingTime: p.processingTime,
			Metadata:       clientMetadata,
		}
		var err error
		mb.key, err = key.AppendBinary(mb.key[:0])
		if err != nil {
			return fmt.Errorf("failed to marshal key to binary for ivl %s: %w", ivl.Duration, err)
		}
		if err := p.batch.Merge(mb.key, mb.value, nil); err != nil {
			return fmt.Errorf("failed to merge to db: %w", err)
		}
	}

	if p.batch.Len() >= dbCommitThresholdBytes {
		if err := p.batch.Commit(p.wOpts); err != nil {
			return fmt.Errorf("failed to commit a batch to db: %w", err)
		}
		if err := p.batch.Close(); err != nil {
			return fmt.Errorf("failed to close a batch post commit: %w", err)
		}
		p.batch = nil
	}
	return nil
}

type mergeBuffer struct {
	key   []byte
	value []byte
}

// commitAndExport commits the batch to DB and exports all aggregated metrics in the provided range
// bounded by `to. If the batch is not committed then a corresponding error would be returned however
// exports will still proceed.
func (p *Processor) commitAndExport(ctx context.Context, batch *pebble.Batch, to time.Time) error {
	var errs []error
	if batch != nil {
		if err := batch.Commit(p.wOpts); err != nil {
			errs = append(errs, fmt.Errorf("failed to commit batch before export: %w", err))
		}
		if err := batch.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close batch before export: %w", err))
		}
	}
	if err := p.export(ctx, to); err != nil {
		errs = append(errs, fmt.Errorf("failed to export: %w", err))
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (p *Processor) export(ctx context.Context, end time.Time) error {
	snap := p.db.NewSnapshot()
	defer snap.Close()

	var errs []error
	for _, ivl := range p.intervals {
		// Check if the given aggregation interval needs to be exported now
		if end.Truncate(ivl.Duration).Equal(end) {
			start := end.Add(-ivl.Duration)
			exportedCount, err := p.exportForInterval(ctx, snap, start, end, ivl)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to export interval %s for end time %d: %w", ivl.Duration, end.Unix(), err))
			}
			p.logger.Debug(
				"Finished exporting metrics",
				zap.Int("exported_datapoints", exportedCount),
				zap.Duration("interval", ivl.Duration),
				zap.Time("exported_till(exclusive)", end),
				zap.Error(err),
			)
		}
	}
	return errors.Join(errs...)
}

func (p *Processor) exportForInterval(
	ctx context.Context,
	snap *pebble.Snapshot,
	start, end time.Time,
	ivl intervalDef,
) (int, error) {
	var boundsBuffer []byte
	from := merger.Key{Interval: ivl.Duration, ProcessingTime: start}
	boundsBuffer, err := from.AppendBinary(nil)
	if err != nil {
		return 0, fmt.Errorf("failed to encode range: %w", err)
	}
	lb := boundsBuffer[:]

	to := merger.Key{Interval: ivl.Duration, ProcessingTime: end}
	boundsBuffer, err = to.AppendBinary(boundsBuffer)
	if err != nil {
		return 0, fmt.Errorf("failed to encode range: %w", err)
	}
	ub := boundsBuffer[len(lb):]

	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: lb,
		UpperBound: ub,
		KeyTypes:   pebble.IterKeyTypePointsOnly,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	var errs []error
	var exportedDPCount int
	rangeHasData := iter.First()
	for ; iter.Valid(); iter.Next() {
		v := merger.NewValue(
			p.cfg.ResourceLimit,
			p.cfg.ScopeLimit,
			p.cfg.MetricLimit,
			p.cfg.DatapointLimit,
			p.cfg.ExponentialHistogramMaxBuckets,
		)
		var key merger.Key
		if err := key.Unmarshal(iter.Key()); err != nil {
			errs = append(errs, fmt.Errorf("failed to decode key from database: %w", err))
			continue
		}
		if err := v.Unmarshal(iter.Value()); err != nil {
			errs = append(errs, fmt.Errorf("failed to decode value from database: %w", err))
			continue
		}
		finalMetrics, err := v.Finalize()
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to finalize merged metric: %w", err))
			continue
		}
		resourceMetrics := finalMetrics.ResourceMetrics()
		if ivl.Statements != nil {
			for i := 0; i < resourceMetrics.Len(); i++ {
				res := resourceMetrics.At(i)
				scopeMetrics := res.ScopeMetrics()
				for j := 0; j < scopeMetrics.Len(); j++ {
					scope := scopeMetrics.At(j)
					metrics := scope.Metrics()
					for k := 0; k < metrics.Len(); k++ {
						metric := metrics.At(k)
						executeTransform := func(dp any) {
							dCtx := ottldatapoint.NewTransformContext(dp, metric, metrics, scope.Scope(), res.Resource(), scope, res)
							if err := ivl.Statements.Execute(ctx, dCtx); err != nil {
								errs = append(errs, fmt.Errorf("failed to execute ottl statement for interval %s: %w", ivl.Duration, err))
							}
						}
						// TODO (lahsivjar): add exhaustive:enforce lint rule
						//exhaustive:enforce
						switch metric.Type() {
						case pmetric.MetricTypeGauge:
							dps := metric.Gauge().DataPoints()
							for l := 0; l < dps.Len(); l++ {
								executeTransform(dps.At(l))
							}
						case pmetric.MetricTypeSum:
							dps := metric.Sum().DataPoints()
							for l := 0; l < dps.Len(); l++ {
								executeTransform(dps.At(l))
							}
						case pmetric.MetricTypeSummary:
							dps := metric.Summary().DataPoints()
							for l := 0; l < dps.Len(); l++ {
								executeTransform(dps.At(l))
							}
						case pmetric.MetricTypeHistogram:
							dps := metric.Histogram().DataPoints()
							for l := 0; l < dps.Len(); l++ {
								executeTransform(dps.At(l))
							}
						case pmetric.MetricTypeExponentialHistogram:
							dps := metric.ExponentialHistogram().DataPoints()
							for l := 0; l < dps.Len(); l++ {
								executeTransform(dps.At(l))
							}
						}
					}
				}
			}
		}
		var attributes []attribute.KeyValue
		if n := len(key.Metadata); n != 0 {
			attributes = make([]attribute.KeyValue, 0, n)
			metadataMap := make(map[string][]string, n)
			for _, kvs := range key.Metadata {
				metadataMap[kvs.Key] = kvs.Values
				attributes = append(attributes, attribute.StringSlice(kvs.Key, kvs.Values))
			}
			info := client.FromContext(ctx)
			info.Metadata = client.NewMetadata(metadataMap)
			ctx = client.NewContext(ctx, info)
		}
		attributes = append(attributes, attribute.String("interval", ivl.Duration.String()))
		if err := p.next.ConsumeMetrics(ctx, finalMetrics); err != nil {
			errs = append(errs, fmt.Errorf("failed to consume the decoded value: %w", err))
			continue
		}
		cnt := finalMetrics.DataPointCount()
		exportedDPCount += cnt
		p.telemetryBuilder.LsmintervalExportedDataPoints.Add(
			ctx,
			int64(cnt),
			metric.WithAttributes(attributes...),
		)
		p.telemetryBuilder.LsmintervalExportedBytes.Add(
			ctx,
			int64(len(iter.Key())+len(iter.Value())),
			metric.WithAttributes(attributes...),
		)
	}
	if rangeHasData {
		if err := p.db.DeleteRange(lb, ub, p.wOpts); err != nil {
			errs = append(errs, fmt.Errorf("failed to delete exported entries: %w", err))
		}
	}
	if len(errs) > 0 {
		return exportedDPCount, errors.Join(errs...)
	}
	return exportedDPCount, nil
}

func (p *Processor) registerPebbleMetrics() error {
	// Because pebble.DB.Metrics call locks the database
	// cache the result for 1 second to avoid superfluous locks
	// when reporting metrics asynchronously.
	var mu sync.Mutex
	var m *pebble.Metrics
	var t time.Time
	metrics := func() *pebble.Metrics {
		mu.Lock()
		defer mu.Unlock()
		if time.Since(t) < time.Second {
			return m
		}
		m = p.db.Metrics()
		t = time.Now()
		return m
	}
	if err := p.telemetryBuilder.RegisterLsmintervalPebbleFlushesCallback(func(ctx context.Context, io metric.Int64Observer) error {
		io.Observe(metrics().Flush.Count)
		return nil
	}); err != nil {
		return err
	}
	if err := p.telemetryBuilder.RegisterLsmintervalPebbleFlushedBytesCallback(func(ctx context.Context, io metric.Int64Observer) error {
		io.Observe(int64(metrics().Levels[0].BytesFlushed))
		return nil
	}); err != nil {
		return err
	}
	if err := p.telemetryBuilder.RegisterLsmintervalPebbleCompactionsCallback(func(ctx context.Context, io metric.Int64Observer) error {
		io.Observe(metrics().Compact.Count)
		return nil
	}); err != nil {
		return err
	}
	if err := p.telemetryBuilder.RegisterLsmintervalPebbleIngestedBytesCallback(func(ctx context.Context, io metric.Int64Observer) error {
		io.Observe(int64(metrics().Total().BytesIngested))
		return nil
	}); err != nil {
		return err
	}
	if err := p.telemetryBuilder.RegisterLsmintervalPebbleCompactedBytesReadCallback(func(ctx context.Context, io metric.Int64Observer) error {
		io.Observe(int64(metrics().Total().BytesRead))
		return nil
	}); err != nil {
		return err
	}
	if err := p.telemetryBuilder.RegisterLsmintervalPebbleCompactedBytesWrittenCallback(func(ctx context.Context, io metric.Int64Observer) error {
		io.Observe(int64(metrics().Total().BytesCompacted))
		return nil
	}); err != nil {
		return err
	}
	if err := p.telemetryBuilder.RegisterLsmintervalPebbleTotalMemtableSizeCallback(func(ctx context.Context, io metric.Int64Observer) error {
		io.Observe(int64(metrics().MemTable.Size))
		return nil
	}); err != nil {
		return err
	}
	if err := p.telemetryBuilder.RegisterLsmintervalPebbleTotalDiskUsageCallback(func(ctx context.Context, io metric.Int64Observer) error {
		io.Observe(int64(metrics().DiskSpaceUsage()))
		return nil
	}); err != nil {
		return err
	}
	if err := p.telemetryBuilder.RegisterLsmintervalPebbleReadAmplificationCallback(func(ctx context.Context, io metric.Int64Observer) error {
		io.Observe(int64(metrics().Total().Sublevels))
		return nil
	}); err != nil {
		return err
	}
	if err := p.telemetryBuilder.RegisterLsmintervalPebbleSstablesCallback(func(ctx context.Context, io metric.Int64Observer) error {
		io.Observe(metrics().Total().NumFiles)
		return nil
	}); err != nil {
		return err
	}
	if err := p.telemetryBuilder.RegisterLsmintervalPebbleReadersMemoryCallback(func(ctx context.Context, io metric.Int64Observer) error {
		io.Observe(metrics().TableCache.Size)
		return nil
	}); err != nil {
		return err
	}
	if err := p.telemetryBuilder.RegisterLsmintervalPebblePendingCompactionCallback(func(ctx context.Context, io metric.Int64Observer) error {
		io.Observe(int64(metrics().Compact.EstimatedDebt))
		return nil
	}); err != nil {
		return err
	}
	if err := p.telemetryBuilder.RegisterLsmintervalPebbleMarkedForCompactionFilesCallback(func(ctx context.Context, io metric.Int64Observer) error {
		io.Observe(int64(metrics().Compact.MarkedFiles))
		return nil
	}); err != nil {
		return err
	}
	if err := p.telemetryBuilder.RegisterLsmintervalPebbleKeysTombstonesCallback(func(ctx context.Context, io metric.Int64Observer) error {
		io.Observe(int64(metrics().Keys.TombstoneCount))
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func newBatch(db *pebble.DB) *pebble.Batch {
	// TODO (lahsivjar): Optimize batch as per our needs
	// Requires release of https://github.com/cockroachdb/pebble/pull/3139
	return db.NewBatch()
}
