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

package elasticapmreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmreceiver"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/elastic/opentelemetry-lib/agentcfg"
	"go.opentelemetry.io/collector/component"
)

// http header keys
const (
	AccessControlExposeHeaders = "Access-Control-Expose-Headers"
	APIKey                     = "ApiKey"
	CacheControl               = "Cache-Control"
	ContentType                = "Content-Type"
	Etag                       = "Etag"
	IfNoneMatch                = "If-None-Match"
)

const (
	msgMethodUnsupported = "method not supported"
)

func (r *elasticAPMReceiver) newElasticAPMConfigsHandler(ctx context.Context, host component.Host) http.HandlerFunc {
	mapBodyError := func(err string) map[string]string {
		return map[string]string{"error": err}
	}

	// write a json response and log any encoding error
	encodeJsonLogError := func(w http.ResponseWriter, data any) {
		w.Header().Set(ContentType, "application/json")
		err := json.NewEncoder(w).Encode(data)
		if err != nil {
			r.settings.Logger.Error(fmt.Sprintf("error encoding json response: %s", err.Error()))
		}
	}

	// servers where the Kibana/ES connection is not enabled (server responds with 403)
	// https://github.com/elastic/apm/blob/main/specs/agents/configuration.md#dealing-with-errors
	if r.fetcherFactory == nil {
		r.settings.Logger.Warn("no agent configuration fetcher available")

		return func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			encodeJsonLogError(w, mapBodyError("remote configuration fetcher not enabled"))
		}
	}

	fetcher, err := r.fetcherFactory(ctx, host)
	if err != nil {
		r.settings.Logger.Error(fmt.Sprintf("could not start agent configuration fetcher: %s", err.Error()))

		// servers where the Kibana/ES connection is enabled, but unavailable (server responds with 503)
		return func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			encodeJsonLogError(w, mapBodyError(err.Error()))
		}
	}

	return func(w http.ResponseWriter, req *http.Request) {
		query, queryErr := buildQuery(req)
		if queryErr != nil {
			w.WriteHeader(http.StatusBadRequest)
			encodeJsonLogError(w, mapBodyError(queryErr.Error()))
			return
		}

		result, err := fetcher.Fetch(req.Context(), query)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			encodeJsonLogError(w, mapBodyError(err.Error()))
			return
		}

		// configuration successfully fetched
		w.Header().Set(CacheControl, fmt.Sprintf("max-age=%v, must-revalidate", r.cfg.CacheDuration.Seconds()))
		w.Header().Set(Etag, fmt.Sprintf("%q", result.Source.Etag))
		w.Header().Set(AccessControlExposeHeaders, Etag)

		if result.Source.Etag == query.Etag {
			// c.Result.SetDefault(request.IDResponseValidNotModified)
			w.WriteHeader(http.StatusNotModified)
		} else {
			// c.Result.SetWithBody(request.IDResponseValidOK, result.Source.Settings)
			w.WriteHeader(http.StatusOK)
			encodeJsonLogError(w, result.Source.Settings)
		}
	}
}

func buildQuery(r *http.Request) (agentcfg.Query, error) {
	var query agentcfg.Query
	switch r.Method {
	case http.MethodPost:
		if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
			return query, err
		}
	case http.MethodGet:
		params := r.URL.Query()
		query = agentcfg.Query{
			Service: agentcfg.Service{
				Name:        params.Get(agentcfg.ServiceName),
				Environment: params.Get(agentcfg.ServiceEnv),
			},
		}
	default:
		if err := fmt.Errorf("%s: %s", msgMethodUnsupported, r.Method); err != nil {
			return query, err
		}
	}
	if query.Service.Name == "" {
		return query, errors.New(agentcfg.ServiceName + " is required")
	}

	if query.Etag == "" {
		query.Etag = ifNoneMatch(r)
	}

	return query, nil
}

func ifNoneMatch(r *http.Request) string {
	if h := r.Header.Get("If-None-Match"); h != "" {
		return strings.Replace(h, "\"", "", -1)
	}
	return r.URL.Query().Get(agentcfg.Etag)
}
