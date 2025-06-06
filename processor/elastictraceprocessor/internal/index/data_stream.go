package index

import "go.opentelemetry.io/collector/pdata/pcommon"

func EncodeDataStream(resource pcommon.Resource, dataStreamType string) {
	attributes := resource.Attributes()
	serviceName, _ := attributes.Get("service.name")

	attributes.PutStr("data_stream.type", dataStreamType)
	attributes.PutStr("data_stream.dataset", "apm."+serviceName.AsString())
	attributes.PutStr("data_stream.dataset", "apm")
	attributes.PutStr("data_stream.namespace", "default") // Use a default or configurable namespace
}
