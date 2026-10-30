//go:build all || integration

package harness

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// ObserveCluster returns the Observer of cluster. A nil cluster yields a nil
// Observer, whose queries return an error instead of a result.
func ObserveCluster(cluster *ceph.Container) *ceph.Observer {
	observer, _ := ceph.Observe(cluster)
	return observer
}

// ObservedPoolStates lists the pools of cluster and reads the native state of
// each one through its Pool.
func ObservedPoolStates(ctx context.Context, cluster *ceph.Container) ([]ceph.PoolState, error) {
	pools, err := ObserveCluster(cluster).Pools(ctx)
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
