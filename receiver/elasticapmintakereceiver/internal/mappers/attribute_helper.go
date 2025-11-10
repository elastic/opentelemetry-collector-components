package mappers

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// putNonEmptyStr puts a string attribute in the given map
// only if the provided value is not empty.
func putNonEmptyStr(attributes pcommon.Map, key, value string) {
	if value != "" {
		attributes.PutStr(key, value)
	}
}
