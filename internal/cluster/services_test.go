package cluster

import (
	"context"
	"slices"
	"strings"
	"testing"
)

func TestRBDPoolsValidateAndJoinInitialPools(t *testing.T) {
	for name, config := range map[string]PoolConfig{
		"other application": {Name: "volumes", Application: "rgw"},
		"erasure coded":     {Name: "volumes", ErasureCode: &ErasureCodeConfig{K: 2, M: 1}},
	} {
		if err := WithRBDPools(config)(&options{}); err == nil {
			t.Fatalf("%s RBD pool accepted", name)
		}
	}
	settings := options{osds: 2, pools: []PoolConfig{{Name: "data"}}}
	if err := WithRBDPools(PoolConfig{Name: "first"})(&settings); err != nil {
		t.Fatal(err)
	}
	// Repeated options replace the list, as for the other initial services.
	if err := WithRBDPools(PoolConfig{Name: "volumes"}, PoolConfig{Name: "images", Application: "rbd"})(&settings); err != nil {
		t.Fatal(err)
	}
	if err := prepareInitialComposition(&settings); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, pool := range settings.pools {
		names = append(names, pool.Name+"/"+pool.Application)
	}
	if !slices.Equal(names, []string{"data/", "volumes/rbd", "images/rbd"}) || !slices.Equal(settings.rbdPoolNames, []string{"volumes", "images"}) {
		t.Fatalf("RBD pools did not join the initial pools: %v %v", names, settings.rbdPoolNames)
	}
	duplicate := options{osds: 2, pools: []PoolConfig{{Name: "volumes"}}}
	if err := WithRBDPools(PoolConfig{Name: "volumes"})(&duplicate); err != nil {
		t.Fatal(err)
	}
	if err := prepareInitialComposition(&duplicate); err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("an RBD pool reused an initial pool name: %v", err)
	}
	cold := options{osds: 0, noInitialOSDs: true}
	if err := WithRBDPools(PoolConfig{Name: "volumes"})(&cold); err != nil {
		t.Fatal(err)
	}
	if err := prepareInitialComposition(&cold); err == nil {
		t.Fatal("an RBD pool was accepted without initial OSDs")
	}
}

func TestServiceDefaultsApplyOnlyWithoutExplicitConfiguration(t *testing.T) {
	settings := options{}
	for _, option := range []Option{DefaultCephFS(), DefaultRGW(), DefaultRBDPool()} {
		if err := option(&settings); err != nil {
			t.Fatal(err)
		}
	}
	if len(settings.filesystems) != 1 || settings.filesystems[0].Name != "" || len(settings.gateways) != 1 || len(settings.rbdPools) != 1 || settings.rbdPools[0].Name != "rbd" {
		t.Fatalf("defaults were not selected: %+v", settings)
	}
	explicit := options{filesystems: []CephFSConfig{{Name: "app"}}, gateways: []RGWConfig{{Name: "s3"}}, rbdPools: []PoolConfig{{Name: "volumes"}}}
	for _, option := range []Option{DefaultCephFS(), DefaultRGW(), DefaultRBDPool()} {
		if err := option(&explicit); err != nil {
			t.Fatal(err)
		}
	}
	if len(explicit.filesystems) != 1 || explicit.filesystems[0].Name != "app" || explicit.gateways[0].Name != "s3" || explicit.rbdPools[0].Name != "volumes" {
		t.Fatalf("defaults replaced explicit services: %+v", explicit)
	}
}

func TestServiceEntryPointsRejectMissingCluster(t *testing.T) {
	ctx := context.Background()
	if _, err := StartCephFS(ctx, nil, CephFSConfig{}); err == nil {
		t.Fatal("nil cluster started a filesystem")
	}
	if _, err := StartRGW(ctx, nil, RGWConfig{}); err == nil {
		t.Fatal("nil cluster started a gateway")
	}
	if err := RemoveRGW(ctx, nil, "a"); err == nil {
		t.Fatal("nil cluster removed a gateway")
	}
	if _, err := GatewaysContext(ctx, nil); err == nil {
		t.Fatal("nil cluster listed gateways")
	}
	if err := InitRBDPool(ctx, nil, "rbd"); err == nil {
		t.Fatal("nil cluster initialized a pool")
	}
	if _, err := CreateRBDNamespace(ctx, nil, "rbd", "ns"); err == nil {
		t.Fatal("nil cluster created a namespace")
	}
	if _, err := ListRBDNamespaces(ctx, nil, "rbd"); err == nil {
		t.Fatal("nil cluster listed namespaces")
	}
	if err := RemoveRBDNamespace(ctx, nil, nil); err == nil {
		t.Fatal("nil cluster removed a namespace")
	}
	if Filesystems(nil) != nil || Gateways(nil) != nil {
		t.Fatal("nil cluster reported services")
	}
}
