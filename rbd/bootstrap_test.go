package rbd

import (
	"slices"
	"strings"
	"testing"

	"github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/internal/servicehook"
)

func TestPoolsValidateAndJoinInitialPools(t *testing.T) {
	for name, config := range map[string]ceph.PoolConfig{
		"other application": {Name: "volumes", Application: "rgw"},
		"erasure coded":     {Name: "volumes", ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1}},
	} {
		if _, err := servicehook.TestApply(WithPools(config)); err == nil {
			t.Fatalf("%s RBD pool accepted", name)
		}
	}
	// Repeated options replace the list, as for the other initial services.
	pools, err := servicehook.TestPrepare(2, false, []ceph.PoolConfig{{Name: "data"}},
		WithPools(ceph.PoolConfig{Name: "first"}), WithPools(ceph.PoolConfig{Name: "volumes"}, ceph.PoolConfig{Name: "images", Application: "rbd"}))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, pool := range pools {
		names = append(names, pool.Name+"/"+pool.Application)
	}
	if !slices.Equal(names, []string{"data/", "volumes/rbd", "images/rbd"}) {
		t.Fatalf("RBD pools did not join the initial pools: %v", names)
	}
	if _, err := servicehook.TestPrepare(2, false, []ceph.PoolConfig{{Name: "volumes"}}, WithPools(ceph.PoolConfig{Name: "volumes"})); err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("an RBD pool reused an initial pool name: %v", err)
	}
	if _, err := servicehook.TestPrepare(0, true, nil, WithPools(ceph.PoolConfig{Name: "volumes"})); err == nil {
		t.Fatal("an RBD pool was accepted without initial OSDs")
	}
}

func TestDefaultPoolAppliesOnlyWithoutExplicitPools(t *testing.T) {
	for _, tc := range []struct {
		opts []ceph.Option
		want string
	}{
		{[]ceph.Option{defaultPool()}, "rbd"},
		{[]ceph.Option{WithPools(ceph.PoolConfig{Name: "volumes"}), defaultPool()}, "volumes"},
	} {
		services, err := servicehook.TestApply(tc.opts...)
		if err != nil || len(services) != 1 {
			t.Fatalf("bootstraps: %v %v", services, err)
		}
		if pools := services[0].InitialPools(); len(pools) != 1 || pools[0].Name != tc.want {
			t.Fatalf("pools = %+v, want %s", pools, tc.want)
		}
	}
}
