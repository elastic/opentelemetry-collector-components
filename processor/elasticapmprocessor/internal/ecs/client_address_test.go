package ecs

import (
	"context"
	"net"
	"testing"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestSetHostIP(t *testing.T) {
	var (
		ctxWithAddr = client.NewContext(context.Background(), client.Info{
			Addr: &net.IPAddr{
				IP: net.IPv4(1, 2, 3, 4),
			},
			Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {"ecs"}}),
		})
		ctxWithEmptyAddr = client.NewContext(context.Background(), client.Info{
			Addr:     &net.IPAddr{IP: nil},
			Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {"ecs"}}),
		})
	)

	tests := []struct {
		name          string
		ctx           context.Context
		attributesMap pcommon.Map
		wantIP        string
	}{
		{
			name:          "empty attributes map with valid client address",
			ctx:           ctxWithAddr,
			attributesMap: pcommon.NewMap(),
			wantIP:        "1.2.3.4",
		},
		{
			name:          "empty client context",
			ctx:           context.Background(),
			attributesMap: pcommon.NewMap(),
			wantIP:        "",
		},
		{
			name:          "client with nil address",
			ctx:           ctxWithEmptyAddr,
			attributesMap: pcommon.NewMap(),
			wantIP:        "",
		},
		{
			name: "attributes map with existing host.ip",
			ctx:  ctxWithAddr,
			attributesMap: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("host.ip", "10.0.0.1")
				m.PutStr("service.name", "test-service")
				return m
			}(),
			wantIP: "10.0.0.1",
		},
		{
			name: "attributes map with empty host.ip string",
			ctx:  ctxWithAddr,
			attributesMap: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("host.ip", "")
				m.PutStr("service.name", "test-service")
				return m
			}(),
			wantIP: "1.2.3.4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetHostIP(tt.ctx, tt.attributesMap)

			value, _ := tt.attributesMap.Get("host.ip")
			if value.Str() != tt.wantIP {
				t.Errorf("SetHostIP() host.ip = %q, want %q", value.Str(), tt.wantIP)
			}
		})
	}
}
