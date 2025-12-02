package routing_test

import (
	"testing"

	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/routing"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestDataStremaEncoderDefault(t *testing.T) {
	resource := pcommon.NewResource()
	routing.EncodeDataStream(resource, "logs", false)

	attributes := resource.Attributes()

	dataStreamType, ok := attributes.Get("data_stream.type")
	assert.True(t, ok)
	assert.Equal(t, "logs", dataStreamType.Str())

	dataStreamDataset, ok := attributes.Get("data_stream.dataset")
	assert.True(t, ok)
	assert.Equal(t, "apm", dataStreamDataset.Str())

	dataStreamNamespace, ok := attributes.Get("data_stream.namespace")
	assert.True(t, ok)
	assert.Equal(t, "default", dataStreamNamespace.Str())
}

func TestDataStreamEncoderWithServiceName(t *testing.T) {
	resource := pcommon.NewResource()
	attributes := resource.Attributes()
	attributes.PutStr("service.name", "my-service")

	routing.EncodeDataStream(resource, "metrics", true)

	dataStreamType, ok := attributes.Get("data_stream.type")
	assert.True(t, ok)
	assert.Equal(t, "metrics", dataStreamType.Str())

	dataStreamDataset, ok := attributes.Get("data_stream.dataset")
	assert.True(t, ok)
	assert.Equal(t, "apm.app.my_service", dataStreamDataset.Str())

	dataStreamNamespace, ok := attributes.Get("data_stream.namespace")
	assert.True(t, ok)
	assert.Equal(t, "default", dataStreamNamespace.Str())
}
