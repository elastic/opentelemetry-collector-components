package elasticapmreceiver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/elastic/opentelemetry-collector-components/internal/agentcfg"
	"go.opentelemetry.io/collector/component"
)

// http header keys
const (
	Accept                     = "Accept"
	AccessControlAllowHeaders  = "Access-Control-Allow-Headers"
	AccessControlAllowMethods  = "Access-Control-Allow-Methods"
	AccessControlAllowOrigin   = "Access-Control-Allow-Origin"
	AccessControlExposeHeaders = "Access-Control-Expose-Headers"
	AccessControlMaxAge        = "Access-Control-Max-Age"
	Authorization              = "Authorization"
	APIKey                     = "ApiKey"
	Bearer                     = "Bearer"
	CacheControl               = "Cache-Control"
	Connection                 = "Connection"
	ContentEncoding            = "Content-Encoding"
	ContentLength              = "Content-Length"
	ContentType                = "Content-Type"
	Etag                       = "Etag"
	IfNoneMatch                = "If-None-Match"
	Origin                     = "Origin"
	UserAgent                  = "User-Agent"
	Vary                       = "Vary"
	XContentTypeOptions        = "X-Content-Type-Options"
)

const (
	msgMethodUnsupported = "method not supported"
	notModifed           = "not modified"
)

func (r *elasticAPMReceiver) newElasticAPMConfigsHandler(ctx context.Context, host component.Host) http.HandlerFunc {
	jsonBodyError := func(err error) map[string]string {
		return map[string]string{"error": err.Error()}
	}

	fetcher, err := r.getFetcher(ctx, host)
	if err != nil {
		r.settings.Logger.Error(fmt.Sprintf("could not start elasticsearch agent configuration client: %s", err.Error()))

		return func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set(ContentType, "application/json")

			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(jsonBodyError(err))
			return
		}
	}

	return func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set(ContentType, "application/json")

		query, queryErr := buildQuery(req)
		if queryErr != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(jsonBodyError(queryErr))
			return
		}

		result, err := fetcher.Fetch(req.Context(), query)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(jsonBodyError(err))
			return
		}

		// configuration successfully fetched
		w.Header().Set(CacheControl, fmt.Sprintf("max-age=%v, must-revalidate", r.cfg.Elasticsearch.cacheDuration.Seconds()))
		w.Header().Set(Etag, fmt.Sprintf("\"%s\"", result.Source.Etag))
		w.Header().Set(AccessControlExposeHeaders, Etag)

		if result.Source.Etag == ifNoneMatch(req) {
			// c.Result.SetDefault(request.IDResponseValidNotModified)
			w.WriteHeader(http.StatusNotModified)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "not modified"})
		} else {
			// c.Result.SetWithBody(request.IDResponseValidOK, result.Source.Settings)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(&result)
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

	query.Etag = ifNoneMatch(r)
	return query, nil
}

func ifNoneMatch(r *http.Request) string {
	if h := r.Header.Get("If-None-Match"); h != "" {
		return strings.Replace(h, "\"", "", -1)
	}
	return r.URL.Query().Get(agentcfg.Etag)
}
