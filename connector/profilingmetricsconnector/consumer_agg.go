package profilingmetricsconnector

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/elastic/opentelemetry-collector-components/connector/profilingmetricsconnector/internal/identity"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type aggConsumer struct {
	nextConsumer consumer.Metrics

	flushInterval time.Duration
	logger        *zap.Logger

	stateLock    sync.Mutex
	md           pmetric.Metrics
	rmLookup     map[identity.Resource]pmetric.ResourceMetrics
	smLookup     map[identity.Scope]pmetric.ScopeMetrics
	mLookup      map[identity.Metric]pmetric.Metric
	numberLookup map[identity.Stream]pmetric.NumberDataPoint
}

func newAggConsumer(nextConsumer consumer.Metrics, flushInterval time.Duration, logger *zap.Logger) *aggConsumer {
	return &aggConsumer{
		nextConsumer:  nextConsumer,
		flushInterval: flushInterval,
		md:            pmetric.NewMetrics(),
		stateLock:     sync.Mutex{},
		logger:        logger,
		rmLookup:      make(map[identity.Resource]pmetric.ResourceMetrics),
		smLookup:      make(map[identity.Scope]pmetric.ScopeMetrics),
		mLookup:       make(map[identity.Metric]pmetric.Metric),
		numberLookup:  make(map[identity.Stream]pmetric.NumberDataPoint),
	}
}

func (c *aggConsumer) Start(ctx context.Context) error {
	exportTicker := time.NewTicker(c.flushInterval)
	go func() {
		defer exportTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-exportTicker.C:
				c.stateLock.Lock()
				if c.md.ResourceMetrics().Len() > 0 {
					err := c.nextConsumer.ConsumeMetrics(ctx, c.md)
					if err != nil {
						c.logger.Error("Metrics export failed", zap.Error(err))
					}
					c.md = pmetric.NewMetrics()
					// Clear all the lookup references
					clear(c.rmLookup)
					clear(c.smLookup)
					clear(c.mLookup)
					clear(c.numberLookup)
				}
				c.stateLock.Unlock()
			}
		}
	}()

	return nil
}

// Capabilities returns the capabilities of the component
func (c *aggConsumer) Capabilities() consumer.Capabilities {
	return c.nextConsumer.Capabilities()
}

func (c *aggConsumer) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	var errs error
	c.stateLock.Lock()
	defer c.stateLock.Unlock()

	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			sm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					mClone, metricID := c.getOrCloneMetric(rm, sm, m)
					aggregateDataPoints(m.Gauge().DataPoints(), mClone.Gauge().DataPoints(), metricID, c.numberLookup)
					return true
				case pmetric.MetricTypeSum:
					sum := m.Sum()
					if !sum.IsMonotonic() {
						return false
					}

					if sum.AggregationTemporality() == pmetric.AggregationTemporalityUnspecified {
						return false
					}

					mClone, metricID := c.getOrCloneMetric(rm, sm, m)
					cloneSum := mClone.Sum()

					aggregateDataPoints(sum.DataPoints(), cloneSum.DataPoints(), metricID, c.numberLookup)
					return true
				default:
					errs = errors.Join(fmt.Errorf("invalid MetricType %d", m.Type()))
					return false
				}
			})
			return sm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})
	return errs
}

func (c *aggConsumer) getOrCloneMetric(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, m pmetric.Metric) (pmetric.Metric, identity.Metric) {
	// Find the ResourceMetrics
	resID := identity.OfResource(rm.Resource())
	rmClone, ok := c.rmLookup[resID]
	if !ok {
		// We need to clone it *without* the ScopeMetricsSlice data
		rmClone = c.md.ResourceMetrics().AppendEmpty()
		rm.Resource().CopyTo(rmClone.Resource())
		rmClone.SetSchemaUrl(rm.SchemaUrl())
		c.rmLookup[resID] = rmClone
	}

	// Find the ScopeMetrics
	scopeID := identity.OfScope(resID, sm.Scope())
	smClone, ok := c.smLookup[scopeID]
	if !ok {
		// We need to clone it *without* the MetricSlice data
		smClone = rmClone.ScopeMetrics().AppendEmpty()
		sm.Scope().CopyTo(smClone.Scope())
		smClone.SetSchemaUrl(sm.SchemaUrl())
		c.smLookup[scopeID] = smClone
	}

	// Find the Metric
	metricID := identity.OfMetric(scopeID, m)
	mClone, ok := c.mLookup[metricID]
	if !ok {
		// We need to clone it *without* the datapoint data
		mClone = smClone.Metrics().AppendEmpty()
		mClone.SetName(m.Name())
		mClone.SetDescription(m.Description())
		mClone.SetUnit(m.Unit())

		switch m.Type() {
		case pmetric.MetricTypeGauge:
			mClone.SetEmptyGauge()
		case pmetric.MetricTypeSum:
			src := m.Sum()

			dest := mClone.SetEmptySum()
			dest.SetAggregationTemporality(src.AggregationTemporality())
			dest.SetIsMonotonic(src.IsMonotonic())
		default:
		}

		c.mLookup[metricID] = mClone
	}

	return mClone, metricID
}

func aggregateDataPoints(dataPoints, mCloneDataPoints pmetric.NumberDataPointSlice, metricID identity.Metric, dpLookup map[identity.Stream]pmetric.NumberDataPoint) {
	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)

		streamID := identity.OfStream(metricID, dp)
		existingDP, ok := dpLookup[streamID]
		if !ok {
			dpClone := mCloneDataPoints.AppendEmpty()
			dp.CopyTo(dpClone)
			dpLookup[streamID] = dpClone
			continue
		}

		// Check if the datapoint is newer
		if dp.Timestamp() >= existingDP.Timestamp() {
			switch existingDP.ValueType() {
			case pmetric.NumberDataPointValueTypeInt:
				dp.SetIntValue(dp.IntValue() + existingDP.IntValue())
			case pmetric.NumberDataPointValueTypeDouble:
				dp.SetDoubleValue(dp.DoubleValue() + existingDP.DoubleValue())
			}

			dp.CopyTo(existingDP)
			continue
		}
	}
}
