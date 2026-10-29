package rgw

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/namedregistry"
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

func TestTenantGatewayRegistryUsesTheGivenStateOnce(t *testing.T) {
	store := map[string]any{}
	ext := func(kind string, create func() any) any {
		if v, ok := store[kind]; ok {
			return v
		}
		store[kind] = create()
		return store[kind]
	}
	r := namedregistry.From[*Gateway](ext, servicekind.RGW)
	if r == nil || r.ByName == nil || namedregistry.From[*Gateway](ext, servicekind.RGW) != r {
		t.Fatal("the registry is not created once per state")
	}
	if _, ok := store[servicekind.RGW]; !ok {
		t.Fatalf("registry stored under %v, want %q", store, servicekind.RGW)
	}
}

// A registry that a tenant created first still reports the owner's secrets and
// config targets once the owner takes it for writing.
func TestGatewayRegistryCreatedByAReaderIsDescribedByTheOwner(t *testing.T) {
	store := map[string]any{}
	ext := func(kind string, create func() any) any {
		if v, ok := store[kind]; ok {
			return v
		}
		store[kind] = create()
		return store[kind]
	}
	reader := namedregistry.From[*Gateway](ext, servicekind.RGW)
	reader.ByName["a"] = &Gateway{GatewayName: "a", AccessKey: "ak", SecretKey: "sk"}
	if got := reader.Secrets(); len(got) != 0 {
		t.Fatalf("an undescribed registry reported secrets: %v", got)
	}
	owner := describeGateways(namedregistry.From[*Gateway](ext, servicekind.RGW))
	if owner != reader {
		t.Fatal("reader and owner hold different registries")
	}
	if got := owner.Secrets(); len(got) != 2 || got[0] != "ak" || got[1] != "sk" {
		t.Fatalf("secrets = %v", got)
	}
	owner.ByName["nil"] = nil
	if got := owner.Secrets(); len(got) != 2 {
		t.Fatalf("a nil gateway contributed secrets: %v", got)
	}
}
