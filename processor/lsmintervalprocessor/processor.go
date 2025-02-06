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
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/merger"
)

var _ processor.Metrics = (*Processor)(nil)
var zeroTime = time.Unix(0, 0).UTC()

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
	pebbleMemTableSize = 64 << 20 // 64MB

	// pebbleMemTableStopWritesThreshold is the hard limit on the maximum
	// number of memtables that could be queued before which writes are
	// stopped. This value should be at least 2 or writes will stop whenever
	// a MemTable is being flushed.
	pebbleMemTableStopWritesThreshold = 2

	// dbCommitDatapointsThresholdCount is the soft limit on the count of
	// the datapoints after which the value is committed to the database.
	// The threshold is not based on size in bytes, however, to make sure
	// that the commit threshold plays well with the max retained batch size
	// the threshold should be kept smaller than the expected sum of max
	// retained batch size and encoded size of aggregated data to be
	// committed. In order to achieve this we can estimate the size of the
	// datapoint.
	dbCommitDatapointsThresholdCount = 16384
)

type Processor struct {
	cfg                *config.Config
	sortedMetadataKeys []string

	db      *pebble.DB
	dataDir string
	dbOpts  *pebble.Options
	wOpts   *pebble.WriteOptions

	intervals []intervalDef
	next      consumer.Metrics

	mu              sync.Mutex
	values          map[attribute.Set][]*merger.Value
	totalDataPoints int
	processingTime  time.Time

	ctx           context.Context
	cancel        context.CancelFunc
	exportStopped chan struct{}
	logger        *zap.Logger
}

func newProcessor(cfg *config.Config, ivlDefs []intervalDef, log *zap.Logger, next consumer.Metrics) (*Processor, error) {
	dbOpts := &pebble.Options{
		Merger: &pebble.Merger{
			Name: "pmetrics_merger",
			Merge: func(key, value []byte) (pebble.ValueMerger, error) {
				v := merger.NewValue(
					cfg.ResourceLimit,
					cfg.ScopeLimit,
					cfg.MetricLimit,
					cfg.DatapointLimit,
				)
				if err := v.Unmarshal(value); err != nil {
					return nil, fmt.Errorf("failed to unmarshal value from db: %w", err)
				}
				return merger.New(
					v,
					cfg.ResourceLimit,
					cfg.ScopeLimit,
					cfg.MetricLimit,
					cfg.DatapointLimit,
				), nil
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
		cfg:                cfg,
		sortedMetadataKeys: sortedMetadataKeys,
		dataDir:            dataDir,
		dbOpts:             dbOpts,
		wOpts:              writeOpts,
		intervals:          ivlDefs,
		next:               next,
		values:             make(map[attribute.Set][]*merger.Value),
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
			p.processingTime = to
			values := p.values
			p.values = make(map[attribute.Set][]*merger.Value)
			p.totalDataPoints = 0
			p.mu.Unlock()

			if len(values) != 0 {
				if err := p.commitValues(values); err != nil {
					p.logger.Warn(
						"failed to commit value to database",
						zap.Error(err), zap.Time("end_time", to),
					)
				}
			}

			// Export the batch
			if err := p.export(ctx, to); err != nil {
				p.logger.Warn("failed to export", zap.Error(err), zap.Time("end_time", to))
			}

			to = to.Add(p.intervals[0].Duration)
			timer.Reset(time.Until(to))
		}
	}()
	return nil
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
		var errs []error
		p.logger.Info("exporting all data before shutting down")
		if len(p.values) != 0 {
			if err := p.commitValues(p.values); err != nil {
				errs = append(errs, fmt.Errorf("failed to commit values: %w", err))
			}
		}
		p.values = nil

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
	return nil
}

func (p *Processor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *Processor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) (errs error) {
	clientInfo := client.FromContext(ctx)
	clientMeta := make([]attribute.KeyValue, 0, len(p.sortedMetadataKeys))
	for _, k := range p.sortedMetadataKeys {
		values := clientInfo.Metadata.Get(k)
		if len(values) == 0 {
			continue
		}
		clientMeta = append(clientMeta, attribute.StringSlice(k, values))
	}
	clientMetaSet := attribute.NewSet(clientMeta...)

	if err := p.mergeMetric(clientMetaSet, md); err != nil {
		errs = errors.Join(errs, fmt.Errorf("failed to merge metric: %w", err))
	}

	// Call next for the metrics remaining in the input
	if err := p.next.ConsumeMetrics(ctx, md); err != nil {
		errs = errors.Join(errs, err)
	}

	return errs
}

func (p *Processor) mergeMetric(meta attribute.Set, md pmetric.Metrics) error {
	value := merger.NewValue(
		p.cfg.ResourceLimit,
		p.cfg.ScopeLimit,
		p.cfg.MetricLimit,
		p.cfg.DatapointLimit,
	)

	var errs []error
	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			sm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				switch t := m.Type(); t {
				case pmetric.MetricTypeEmpty, pmetric.MetricTypeGauge:
					// TODO (lahsivjar): implement support for gauges
					return false
				case pmetric.MetricTypeSummary:
					if p.cfg.PassThrough.Summary {
						return false
					}
					if err := value.MergeMetric(rm, sm, m); err != nil {
						errs = append(errs, err)
					}
					return true
				case pmetric.MetricTypeSum, pmetric.MetricTypeHistogram, pmetric.MetricTypeExponentialHistogram:
					if err := value.MergeMetric(rm, sm, m); err != nil {
						errs = append(errs, err)
					}
					return true
				default:
					// All metric types are handled, this is unexpected
					errs = append(errs, fmt.Errorf("unexpected metric type, dropping: %d", t))
					return true
				}
			})
			return sm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})

	p.mu.Lock()
	defer p.mu.Unlock()

	// Accumulate values until there is enough total data points to commit.
	// We don't use size of the pmetric here as getting the size is costly.
	p.values[meta] = append(p.values[meta], value)
	p.totalDataPoints += value.DatapointsCount()

	if p.totalDataPoints >= dbCommitDatapointsThresholdCount {
		if err := p.commitValues(p.values); err != nil {
			return fmt.Errorf("failed to merge the value to batch: %w", err)
		}
		p.values = make(map[attribute.Set][]*merger.Value)
		p.totalDataPoints = 0
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (p *Processor) commitValues(metaValues map[attribute.Set][]*merger.Value) error {
	batch := newBatch(p.db)
	for meta, values := range metaValues {
		// Merge all values for a given client metadata,
		// so we reduce encoding/decoding round-trips.
		value0 := values[0]
		for _, val := range values[1:] {
			if err := value0.Merge(val); err != nil {
				return fmt.Errorf("failed to merge values: %w", err)
			}
		}

		vb, err := value0.Marshal()
		if err != nil {
			return fmt.Errorf("failed to marshal value to proto binary: %w", err)
		}

		var k []byte
		for _, ivl := range p.intervals {
			key := merger.Key{
				Interval:       ivl.Duration,
				ProcessingTime: p.processingTime,
				Metadata:       meta,
			}
			k, err := key.AppendBinary(k[:0])
			if err != nil {
				return fmt.Errorf("failed to marshal key to binary for ivl %s: %w", ivl.Duration, err)
			}
			if err := batch.Merge(k, vb, nil); err != nil {
				return fmt.Errorf("failed to merge to db: %w", err)
			}
		}
	}
	if err := batch.Commit(p.wOpts); err != nil {
		return fmt.Errorf("failed to commit a batch to db: %w", err)
	}
	if err := batch.Close(); err != nil {
		return fmt.Errorf("failed to close a batch post commit: %w", err)
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
			exportedCount, err := p.exportForInterval(ctx, snap, end, ivl)
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
	end time.Time,
	ivl intervalDef,
) (int, error) {
	var boundsBuffer []byte
	from := merger.Key{Interval: ivl.Duration, ProcessingTime: zeroTime}
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
	for iter.First(); iter.Valid(); iter.Next() {
		v := merger.NewValue(
			p.cfg.ResourceLimit,
			p.cfg.ScopeLimit,
			p.cfg.MetricLimit,
			p.cfg.DatapointLimit,
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
		if n := key.Metadata.Len(); n != 0 {
			metadataMap := make(map[string][]string, n)
			for metaIter := key.Metadata.Iter(); metaIter.Next(); {
				metaAttr := metaIter.Attribute()
				if metaAttr.Value.Type() != attribute.STRINGSLICE {
					errs = append(errs, errors.New("invalid metadata type, only string slice allowed"))
					continue
				}
				metadataMap[string(metaAttr.Key)] = metaAttr.Value.AsStringSlice()
			}
			info := client.FromContext(ctx)
			info.Metadata = client.NewMetadata(metadataMap)
			ctx = client.NewContext(ctx, info)
		}
		if err := p.next.ConsumeMetrics(ctx, finalMetrics); err != nil {
			errs = append(errs, fmt.Errorf("failed to consume the decoded value: %w", err))
			continue
		}
		exportedDPCount += finalMetrics.DataPointCount()
	}
	if err := p.db.DeleteRange(lb, ub, p.wOpts); err != nil {
		errs = append(errs, fmt.Errorf("failed to delete exported entries: %w", err))
	}
	if len(errs) > 0 {
		return exportedDPCount, errors.Join(errs...)
	}
	return exportedDPCount, nil
}

func newBatch(db *pebble.DB) *pebble.Batch {
	// TODO (lahsivjar): Optimize batch as per our needs
	// Requires release of https://github.com/cockroachdb/pebble/pull/3139
	return db.NewBatch()
}
