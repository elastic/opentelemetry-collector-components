package routing

import "go.opentelemetry.io/collector/pdata/pcommon"

func EncodeDataStream(resource pcommon.Resource, dataStreamType string) {
	attributes := resource.Attributes()

	attributes.PutStr("data_stream.type", dataStreamType)
	attributes.PutStr("data_stream.dataset", "apm")
	attributes.PutStr("data_stream.namespace", "default") //TODO: make this configurable
}
