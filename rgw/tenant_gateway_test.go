package rgw

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicekind"
)

func TestPickSharedGatewayPrefersDefaultThenFirstByName(t *testing.T) {
	if pickSharedGateway(nil) != nil {
		t.Fatal("no gateways must give nil")
	}
	a, b, d := &Gateway{GatewayName: "a"}, &Gateway{GatewayName: "b"}, &Gateway{GatewayName: "default"}
	if got := pickSharedGateway([]*Gateway{a, b}); got != a {
		t.Fatalf("without default got %v, want the first by name", got)
	}
	if got := pickSharedGateway([]*Gateway{a, b, d}); got != d {
		t.Fatalf("with default got %v", got)
	}
}

func TestGatewaysFromUsesTheGivenStateOnce(t *testing.T) {
	store := map[string]any{}
	ext := func(kind string, create func() any) any {
		if v, ok := store[kind]; ok {
			return v
		}
		store[kind] = create()
		return store[kind]
	}
	r := gatewaysFrom(ext)
	if r == nil || r.byName == nil || gatewaysFrom(ext) != r {
		t.Fatal("the registry is not created once per state")
	}
	if _, ok := store[servicekind.RGW]; !ok {
		t.Fatalf("registry stored under %v, want %q", store, servicekind.RGW)
	}
}
