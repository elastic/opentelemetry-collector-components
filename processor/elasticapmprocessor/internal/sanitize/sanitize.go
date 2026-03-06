package sanitize

import (
	"regexp"
	"strings"
)

const (
	MaxDataStreamBytes = 100
)

var (
	serviceNameInvalidRegexp = regexp.MustCompile("[^a-zA-Z0-9_]")
)

// Truncate returns s truncated at n runes, and the number of runes in the resulting string (<= n).
func Truncate(s string, length uint) string {
	var j uint
	for i := range s {
		if j == length {
			return s[:i]
		}
		j++
	}
	return s
}

func replaceReservedLabelKeyRune(r rune) rune {
	switch r {
	case '.', '*', '"':
		return '_'
	}
	return r
}

// HandleAttributeKey sanitizes an attribute key, replacing the reserved characters
// '.', '*' and '"' with '_'. This matches the apm-server behavior.
// This matches the logic in the apm-data library here:
// https://github.com/elastic/apm-data/blob/e3e170b/model/modeljson/labels.go.
func HandleAttributeKey(k string) string {
	if strings.ContainsAny(k, ".*\"") {
		return strings.Map(replaceReservedLabelKeyRune, k)
	}
	return k
}

// HandleLabelAttributeKey sanitizes the suffix portion of a label attribute,
// preserving the "labels." or "numeric_labels." prefix.
func HandleLabelAttributeKey(attr string) string {
	if strings.HasPrefix(attr, "labels.") {
		return "labels." + HandleAttributeKey(strings.TrimPrefix(attr, "labels."))
	}
	if strings.HasPrefix(attr, "numeric_labels.") {
		return "numeric_labels." + HandleAttributeKey(strings.TrimPrefix(attr, "numeric_labels."))
	}
	return attr
}

// IsLabelAttribute returns true if the resource attribute is already a prefixed label.
// The elasticapmintake receiver moves labels and numeric_labels into attributes and
// already prefixes those with "labels." and "numeric_labels." respectively and also does de-dotting.
// So for those, we don't want to double prefix - we just leave them as is.
func IsLabelAttribute(attr string) bool {
	return strings.HasPrefix(attr, "labels.") || strings.HasPrefix(attr, "numeric_labels.")
}

// The following is adapted from apm-data
// https://github.com/elastic/apm-data/blob/46a81347bdbb81a7a308e8d2f58f39c0b1137a77/model/modelprocessor/datastream.go#L186C1-L209C2

// NormalizeServiceName translates serviceName into a string suitable
// for inclusion in a data stream name.
//
// Concretely, this function will lowercase the string and replace any
// reserved characters with "_".
func NormalizeServiceName(s string) string {
	s = strings.ToLower(s)
	s = CleanServiceName(s)
	return s
}

// CleanServiceName sanitizes a service name by truncating it to a defined length and replacing invalid characters with "_".
// see https://github.com/elastic/apm-data/blob/34677210900a68d6204cdb79da4ce0d1ee685d9a/input/otlp/metadata.go#L491
func CleanServiceName(name string) string {
	return serviceNameInvalidRegexp.ReplaceAllString(Truncate(name, MaxDataStreamBytes), "_")
}
