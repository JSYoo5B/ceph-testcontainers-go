package harness

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/namedregistry"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicekind"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

func TestPickSharedGatewayPrefersDefaultThenFirstByName(t *testing.T) {
	if pickSharedGateway(nil) != nil {
		t.Fatal("no gateways must give nil")
	}
	a, b, d := &rgw.Gateway{GatewayName: "a"}, &rgw.Gateway{GatewayName: "b"}, &rgw.Gateway{GatewayName: "default"}
	if got := pickSharedGateway([]*rgw.Gateway{a, b}); got != a {
		t.Fatalf("without default got %v, want the first by name", got)
	}
	if got := pickSharedGateway([]*rgw.Gateway{a, b, d}); got != d {
		t.Fatalf("with default got %v", got)
	}
}

func TestTenantGatewayRegistryUsesTheGivenStateOnce(t *testing.T) {
	store := map[string]any{}
	ext := func(kind string, create func() any) any {
		if v, ok := store[kind]; ok {
			return v
		}
		store[kind] = create()
		return store[kind]
	}
	r := namedregistry.From[*rgw.Gateway](ext, servicekind.RGW)
	if r == nil || r.ByName == nil || namedregistry.From[*rgw.Gateway](ext, servicekind.RGW) != r {
		t.Fatal("the registry is not created once per state")
	}
	if _, ok := store[servicekind.RGW]; !ok {
		t.Fatalf("registry stored under %v, want %q", store, servicekind.RGW)
	}
}
