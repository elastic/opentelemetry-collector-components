package ecs

import (
	"context"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// SetHostIP sets the `host.ip` attribute using the client address.
// Does not override any existing `host.ip` values.
// Safely handles empty client addresses and empty attribute maps.
func SetHostIP(ctx context.Context, attributesMap pcommon.Map) {
	cl := client.FromContext(ctx)
	if cl.Addr == nil {
		return
	}

	ip := cl.Addr.String()
	if ip == "" {
		return
	}

	// Only set host.ip when it does not exist or is empty
	if value, ok := attributesMap.Get("host.ip"); !ok || value.Str() == "" {
		attributesMap.PutStr("host.ip", ip)
	}
}
