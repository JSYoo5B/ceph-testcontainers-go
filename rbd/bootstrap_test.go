package rbd

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestPoolsValidateAndJoinInitialPools(t *testing.T) {
	for name, config := range map[string]ceph.PoolConfig{
		"other application": {Name: "volumes", Application: "rgw"},
		"erasure coded":     {Name: "volumes", ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1}},
	} {
		if _, err := testhelper.TestApply(WithPools(config)); err == nil {
			t.Fatalf("%s RBD pool accepted", name)
		}
	}
	// Repeated options replace the list, as for the other initial services.
	pools, _, err := testhelper.TestPrepare(2, false, []ceph.PoolConfig{{Name: "data"}},
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
	if _, _, err := testhelper.TestPrepare(2, false, []ceph.PoolConfig{{Name: "volumes"}}, WithPools(ceph.PoolConfig{Name: "volumes"})); err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("an RBD pool reused an initial pool name: %v", err)
	}
	if _, _, err := testhelper.TestPrepare(0, true, nil, WithPools(ceph.PoolConfig{Name: "volumes"})); err == nil {
		t.Fatal("an RBD pool was accepted without initial OSDs")
	}
}

func TestDefaultPoolAppliesOnlyWithoutExplicitPools(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []ceph.Option
		want []string
	}{
		{"default", []ceph.Option{defaultPool()}, []string{"rbd"}},
		{"empty", []ceph.Option{WithPools(), defaultPool()}, nil},
		{"named", []ceph.Option{WithPools(ceph.PoolConfig{Name: "volumes"}), defaultPool()}, []string{"volumes"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pools, services, err := testhelper.TestPrepare(2, false, nil, tc.opts...)
			if err != nil || len(services) != 1 {
				t.Fatalf("bootstraps: %v %v", services, err)
			}
			for _, selected := range [][]ceph.PoolConfig{pools, services[0].InitialPools()} {
				var names []string
				for _, pool := range selected {
					if pool.Application != "rbd" {
						t.Fatalf("pool %s has application %q", pool.Name, pool.Application)
					}
					names = append(names, pool.Name)
				}
				if !slices.Equal(names, tc.want) {
					t.Fatalf("selected pools = %v, want %v", names, tc.want)
				}
			}
		})
	}
}
