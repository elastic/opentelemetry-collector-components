package attr

// These constants hold attribute names that are defined by the Elastic APM data model and do not match
// any SemConv attribute. These fields are not used by the UI, and store information related to a specific span type
const (
	SpanDBLink                  = "span.db.link"
	SpanDBRowsAffected          = "span.db.rows_affected"
	SpanDBUserName              = "span.db.user_name"
	HTTPRequestBody             = "http.request.body"
	HTTPRequestID               = "http.request.id"
	HTTPRequestReferrer         = "http.request.referrer"
	HTTPResponseDecodedBodySize = "http.response.decoded_body_size"
	HTTPResponseEncodedBodySize = "http.response.encoded_body_size"
	HTTPResponseTransferSize    = "http.response.transfer_size"
	SpanMessageBody             = "span.message.body"
)
