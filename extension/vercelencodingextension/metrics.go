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

package vercelencodingextension // import "github.com/elastic/opentelemetry-collector-components/extension/vercelencodingextension"

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"

	"github.com/elastic/opentelemetry-collector-components/extension/vercelencodingextension/internal/metadata"
)

const vercelISO8601LocalLayout = "2006-01-02T15:04:05.999999999"

// metricsBatch accumulates decoded metric records into grouped resource, scope,
// and metric structures. Vercel drains send one schema per request, but a single
// batch can carry records from different projects, deployments, or environments,
// so grouping keeps records with differing resource attributes under distinct
// resources. Lookups are keyed by attribute identity through maps, so a batch
// with many distinct resources stays linear in the record count.
type metricsBatch struct {
	metrics   pmetric.Metrics
	resources map[speedInsightsRK]*resourceGroup
}

type resourceGroup struct {
	resourceMetrics pmetric.ResourceMetrics
	scopes          map[speedInsightsSK]*scopeGroup
}

type scopeGroup struct {
	scopeMetrics pmetric.ScopeMetrics
	metrics      map[string]pmetric.Metric
}

// newMetricsDecoder builds the metric payload and a per-record decode function.
func newMetricsDecoder() (pmetric.Metrics, func(recordSource) error) {
	batch := newMetricsBatch()
	return batch.metrics, func(src recordSource) error {
		return appendMetrics(src, batch)
	}
}

// appendMetrics decodes one record from src and appends its data points to
// batch. Speed insights is the only schema routed to the metrics signal, so
// routing already guarantees the record shape.
func appendMetrics(src recordSource, batch *metricsBatch) error {
	var record speedInsightsSchema
	if err := src.decode(&record); err != nil {
		return err
	}
	return appendSpeedInsightsDataPoint(batch, &record)
}

func newMetricsBatch() *metricsBatch {
	return &metricsBatch{
		metrics:   pmetric.NewMetrics(),
		resources: make(map[speedInsightsRK]*resourceGroup),
	}
}

// scope returns the scope group for the given resource and scope identity,
// creating and populating the resource and scope attributes on first use.
func (b *metricsBatch) scope(rk speedInsightsRK, sk speedInsightsSK) *scopeGroup {
	rg, ok := b.resources[rk]
	if !ok {
		resourceMetrics := b.metrics.ResourceMetrics().AppendEmpty()
		rk.putAttributes(resourceMetrics.Resource().Attributes())
		rg = &resourceGroup{
			resourceMetrics: resourceMetrics,
			scopes:          make(map[speedInsightsSK]*scopeGroup),
		}
		b.resources[rk] = rg
	}

	sg, ok := rg.scopes[sk]
	if !ok {
		scopeMetrics := rg.resourceMetrics.ScopeMetrics().AppendEmpty()
		sk.putScope(scopeMetrics.Scope())
		sg = &scopeGroup{
			scopeMetrics: scopeMetrics,
			metrics:      make(map[string]pmetric.Metric),
		}
		rg.scopes[sk] = sg
	}
	return sg
}

// gauge returns the gauge metric with the given name, creating it on first use.
func (g *scopeGroup) gauge(name string) pmetric.Metric {
	metric, ok := g.metrics[name]
	if !ok {
		metric = g.scopeMetrics.Metrics().AppendEmpty()
		metric.SetName(name)
		metric.SetEmptyGauge()
		g.metrics[name] = metric
	}
	return metric
}

// speedInsightsRK identifies the Vercel resource a speed insights record
// belongs to.
type speedInsightsRK struct {
	projectID    string
	ownerID      string
	environment  string
	url          string
	deploymentID string
}

// putAttributes writes the resource-scoped attributes documented in README.md.
func (k speedInsightsRK) putAttributes(attrs pcommon.Map) {
	putStr(attrs, attrVercelProjectID, k.projectID)
	putStr(attrs, attrVercelOwnerID, k.ownerID)
	putStr(attrs, string(semconv.DeploymentEnvironmentNameKey), k.environment)
	putStr(attrs, attrVercelURL, k.url)
	putStr(attrs, string(semconv.DeploymentIDKey), k.deploymentID)
}

// speedInsightsSK identifies the instrumentation scope a speed insights record
// belongs to.
type speedInsightsSK struct {
	format        format
	scriptVersion string
	sdkVersion    string
	sdkName       string
}

// putScope sets the scope name and its scope-scoped attributes.
func (k speedInsightsSK) putScope(scope pcommon.InstrumentationScope) {
	scope.SetName(metadata.ScopeName)
	attrs := scope.Attributes()
	attrs.PutStr(formatIdentificationTag, string(k.format))
	putStr(attrs, attrVercelSpeedInsightsScriptVersion, k.scriptVersion)
	putStr(attrs, attrVercelSpeedInsightsSDKVersion, k.sdkVersion)
	putStr(attrs, attrVercelSpeedInsightsSDKName, k.sdkName)
}

type speedInsightsSchema struct {
	Schema               string  `json:"schema"`
	Timestamp            string  `json:"timestamp"`
	ProjectID            string  `json:"projectId"`
	OwnerID              string  `json:"ownerId"`
	DeviceID             int64   `json:"deviceId"`
	MetricType           string  `json:"metricType"`
	Value                float64 `json:"value"`
	Origin               string  `json:"origin"`
	Path                 string  `json:"path"`
	Route                string  `json:"route"`
	Country              string  `json:"country"`
	Region               string  `json:"region"`
	City                 string  `json:"city"`
	OSName               string  `json:"osName"`
	OSVersion            string  `json:"osVersion"`
	ClientName           string  `json:"clientName"`
	ClientType           string  `json:"clientType"`
	ClientVersion        string  `json:"clientVersion"`
	DeviceType           string  `json:"deviceType"`
	DeviceBrand          string  `json:"deviceBrand"`
	ConnectionSpeed      string  `json:"connectionSpeed"`
	BrowserEngine        string  `json:"browserEngine"`
	BrowserEngineVersion string  `json:"browserEngineVersion"`
	ScriptVersion        string  `json:"scriptVersion"`
	SDKVersion           string  `json:"sdkVersion"`
	SDKName              string  `json:"sdkName"`
	VercelEnvironment    string  `json:"vercelEnvironment"`
	VercelURL            string  `json:"vercelUrl"`
	DeploymentID         string  `json:"deploymentId"`
	Attribution          string  `json:"attribution"`
}

// appendSpeedInsightsDataPoint appends the decoded record's gauge data point to
// the batch, mapping fields to resource, scope, and data point attributes as
// documented in README.md. Decoding is done by the caller so records can be read
// straight from the stream without a second parse.
func appendSpeedInsightsDataPoint(batch *metricsBatch, record *speedInsightsSchema) error {
	timestamp, err := parseVercelTimestamp(record.Timestamp)
	if err != nil {
		return fmt.Errorf("failed to parse speed insights timestamp %q: %w", record.Timestamp, err)
	}

	scope := batch.scope(
		speedInsightsRK{
			projectID:    record.ProjectID,
			ownerID:      record.OwnerID,
			environment:  record.VercelEnvironment,
			url:          record.VercelURL,
			deploymentID: record.DeploymentID,
		},
		speedInsightsSK{
			format:        formatSpeedInsights,
			scriptVersion: record.ScriptVersion,
			sdkVersion:    record.SDKVersion,
			sdkName:       record.SDKName,
		},
	)

	dataPoint := scope.gauge(record.MetricType).Gauge().DataPoints().AppendEmpty()
	dataPoint.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	dataPoint.SetDoubleValue(record.Value)

	pointAttrs := dataPoint.Attributes()
	if record.DeviceID != 0 {
		pointAttrs.PutStr(string(semconv.DeviceIDKey), strconv.FormatInt(record.DeviceID, 10))
	}
	putURLAttrs(pointAttrs, record.Origin, record.Path, "")
	putStr(pointAttrs, string(semconv.HTTPRouteKey), record.Route)
	putStr(pointAttrs, string(semconv.GeoCountryISOCodeKey), record.Country)
	putStr(pointAttrs, string(semconv.GeoRegionISOCodeKey), record.Region)
	putStr(pointAttrs, string(semconv.GeoLocalityNameKey), record.City)
	putStr(pointAttrs, string(semconv.UserAgentOSNameKey), record.OSName)
	putStr(pointAttrs, string(semconv.UserAgentOSVersionKey), record.OSVersion)
	putStr(pointAttrs, string(semconv.UserAgentNameKey), record.ClientName)
	putStr(pointAttrs, attrVercelClientType, record.ClientType)
	putStr(pointAttrs, string(semconv.UserAgentVersionKey), record.ClientVersion)
	putStr(pointAttrs, attrVercelDeviceType, record.DeviceType)
	putStr(pointAttrs, string(semconv.DeviceManufacturerKey), record.DeviceBrand)
	putStr(pointAttrs, attrVercelConnectionSpeed, record.ConnectionSpeed)
	putStr(pointAttrs, attrVercelBrowserEngineName, record.BrowserEngine)
	putStr(pointAttrs, attrVercelBrowserEngineVersion, record.BrowserEngineVersion)
	putStr(pointAttrs, attrVercelSpeedInsightsAttribution, record.Attribution)

	return nil
}

// parseVercelTimestamp parses ISO timestamps from Vercel Speed Insights drains.
// It normalizes space separators to 'T', handles RFC 3339 offsets, and forces
// zoneless local date-times to time.UTC.
func parseVercelTimestamp(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, errors.New("empty timestamp")
	}

	// Normalize space separator to 'T' (e.g. "2026-07-29 13:45:30" -> "2026-07-29T13:45:30")
	normalized := value
	if len(normalized) > 10 && normalized[10] == ' ' {
		normalized = normalized[:10] + "T" + normalized[11:]
	}

	// Try parsing full RFC 3339 / RFC 3339 Nano (handles 'Z' or '+02:00' offsets)
	if t, err := time.Parse(time.RFC3339Nano, normalized); err == nil {
		return t.UTC(), nil // Explicitly ensure result is UTC location
	}

	// Fall back to zoneless ISO 8601 date-time, forcing UTC location
	t, err := time.ParseInLocation(vercelISO8601LocalLayout, normalized, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf(
			"invalid Vercel ISO timestamp %q (expected RFC3339 or zoneless ISO 8601): %w",
			value, err,
		)
	}

	return t, nil
}
