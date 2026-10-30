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

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/identity"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/merger/limits"
)

// unmarshalV1 reads a value written in the v1 format: the limit trackers,
// encoded positionally, followed by the pmetric protobuf. Entries are added
// without checking the limits, which the stored data already respected.
func (v *Value) unmarshalV1(data []byte) error {
	v.initIndex()
	if len(data) == 0 {
		return nil
	}
	if len(data) < 4 {
		// For non-nil value, tracker must be marshaled
		return errors.New("failed to unmarshal value, invalid length")
	}
	trackersLen := int(binary.BigEndian.Uint32(data[:4]))
	data = data[4:]
	trackers := limits.NewTrackers(
		uint64(v.resourceLimitCfg.MaxCardinality),
		uint64(v.scopeLimitCfg.MaxCardinality),
		uint64(v.metricLimitCfg.MaxCardinality),
		uint64(v.datapointLimitCfg.MaxCardinality),
	)
	if trackersLen > 0 {
		if len(data) < trackersLen {
			return errors.New("failed to unmarshal value, invalid length")
		}
		if err := trackers.Unmarshal(data[:trackersLen]); err != nil {
			return fmt.Errorf("failed to unmarshal limits: %w", err)
		}
		data = data[trackersLen:]
	}
	var unmarshaler pmetric.ProtoUnmarshaler
	md, err := unmarshaler.UnmarshalMetrics(data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal data: %w", err)
	}

	v.resourceTracker = trackers.GetResourceTracker()
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		res := rm.Resource()
		scopeTracker := trackers.GetScopeTracker(i)
		re := &resourceEntry{
			id:           identity.OfResource(res),
			schemaURL:    rm.SchemaUrl(),
			attrs:        encodeResource(res),
			scopeTracker: v.newScopeTracker(),
		}
		if scopeTracker != nil {
			re.scopeTracker = scopeTracker.Tracker
		}
		v.resources = append(v.resources, re)
		v.resourceIndex[re.id] = re

		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			scope := sm.Scope()
			se := &scopeEntry{
				id:            identity.OfScope(re.id, scope),
				schemaURL:     sm.SchemaUrl(),
				attrs:         encodeAttrs(scope.Attributes(), scope.DroppedAttributesCount()),
				metricTracker: v.newMetricTracker(),
			}
			var metricTracker *limits.MetricTracker
			if scopeTracker != nil {
				metricTracker = scopeTracker.GetMetricTracker(j)
			}
			if metricTracker != nil {
				se.metricTracker = metricTracker.Tracker
			}
			re.scopes = append(re.scopes, se)
			v.scopeIndex[se.id] = se

			ms := sm.Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				me := &metricEntry{
					id:          identity.OfMetric(se.id, m),
					description: m.Description(),
					dpTracker:   v.newDatapointTracker(),
				}
				if metricTracker != nil {
					if t := metricTracker.GetDatapointTracker(k); t != nil {
						me.dpTracker = t
					}
				}
				se.metrics = append(se.metrics, me)
				v.metricIndex[me.id] = me

				switch m.Type() {
				case pmetric.MetricTypeSum:
					addV1Streams(v, me, m.Sum().DataPoints())
				case pmetric.MetricTypeSummary:
					addV1Streams(v, me, m.Summary().DataPoints())
				case pmetric.MetricTypeHistogram:
					addV1Streams(v, me, m.Histogram().DataPoints())
				case pmetric.MetricTypeExponentialHistogram:
					addV1Streams(v, me, m.ExponentialHistogram().DataPoints())
				}
			}
		}
	}
	return nil
}

func addV1Streams[DPS dataPointSlice[DP], DP dataPoint[DP]](v *Value, me *metricEntry, dps DPS) {
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		id := identity.OfStream(me.id, dp)
		st := &streamEntry{
			attrsHash: id.AttrsHash(),
			attrs:     encodeAttrs(dp.Attributes(), 0),
			dp:        cloneDP(dp),
		}
		v.insertStream(me, id, st)
	}
}

// appendBinaryV1 preserves the positional trackers without materializing
// overflow metrics: overflow must be finalized exactly once, at export.
func (v *Value) appendBinaryV1(b []byte) ([]byte, error) {
	b = append(b, versionV1)
	if v.streamCount == 0 {
		return b, nil
	}
	md, _, err := v.finalize(false)
	if err != nil {
		return nil, err
	}
	trackers := limits.NewTrackers(uint64(v.resourceLimitCfg.MaxCardinality), uint64(v.scopeLimitCfg.MaxCardinality), uint64(v.metricLimitCfg.MaxCardinality), uint64(v.datapointLimitCfg.MaxCardinality))
	*trackers.GetResourceTracker() = *v.resourceTracker
	for _, re := range v.resources {
		rt := trackers.NewScopeTracker()
		rt.Tracker = re.scopeTracker
		for _, se := range re.scopes {
			st := rt.NewMetricTracker()
			st.Tracker = se.metricTracker
			for _, me := range se.metrics {
				*st.NewDatapointTracker() = *me.dpTracker
			}
		}
	}
	offset := len(b)
	b = append(b, 0, 0, 0, 0)
	b, err = trackers.AppendBinary(b)
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint32(b[offset:offset+4], uint32(len(b)-offset-4))
	raw, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
	if err != nil {
		return nil, err
	}
	return append(b, raw...), nil
}
