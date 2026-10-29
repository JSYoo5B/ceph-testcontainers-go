//go:build all || integration

package integration_test

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	cephharness "github.com/JSYoo5B/ceph-testcontainers-go/ceph/harness"
)

// observeCluster returns the Observer of cluster. A nil cluster yields a nil
// Observer, whose queries return an error instead of a result.
func observeCluster(cluster *ceph.Container) *ceph.Observer {
	observer, _ := ceph.Observe(cluster)
	return observer
}

// observeTenant returns the Observer of tenant. A nil tenant yields a nil
// Observer, whose queries return an error instead of a result.
func observeTenant(tenant *cephharness.Tenant) *ceph.Observer {
	observer, _ := cephharness.ObserveTenant(tenant)
	return observer
}

// observedPoolStates lists the pools of cluster and reads the native state of
// each one through its Pool.
func observedPoolStates(ctx context.Context, cluster *ceph.Container) ([]ceph.PoolState, error) {
	pools, err := observeCluster(cluster).Pools(ctx)
	if err != nil {
		return nil, err
	}
	states := make([]ceph.PoolState, 0, len(pools))
	for _, pool := range pools {
		state, err := pool.Status(ctx)
		if err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	return states, nil
}
