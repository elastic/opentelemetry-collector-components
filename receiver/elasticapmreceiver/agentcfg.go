package elasticapmreceiver

import (
	"encoding/json"
	"net/http"

	"github.com/elastic/apm-data/input/elasticapm"
	"github.com/elastic/apm-data/model/modelpb"
)

func (r *elasticAPMReceiver) newElasticAPMConfigsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		statusCode := http.StatusAccepted

		var elasticapmResult elasticapm.Result
		baseEvent := &modelpb.APMEvent{}
		baseEvent.Event = &modelpb.Event{}
		streamErr := elasticapmProcessor.HandleStream(
			r.Context(),
			baseEvent,
			r.Body,
			batchSize,
			batchProcessor,
			&elasticapmResult,
		)
		_ = streamErr
		// TODO record metrics about errors?

		var result struct {
			Accepted int         `json:"accepted"`
			Errors   []jsonError `json:"errors,omitempty"`
		}
		result.Accepted = elasticapmResult.Accepted
		// TODO process elasticapmResult.Errors, add to result
		// TODO process streamErr, conditionally add to result
		// TODO process r.Context().Err(), conditionally add to result

		w.WriteHeader(statusCode)
		_ = json.NewEncoder(w).Encode(&result)
	}
}
