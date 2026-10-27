//go:build all || integration

package integration_test

import "github.com/JSYoo5B/ceph-testcontainers-go/ceph"

// observeCluster returns the Observer of cluster. A nil cluster yields a nil
// Observer, whose queries return an error instead of a result.
func observeCluster(cluster *ceph.Container) *ceph.Observer {
	observer, _ := ceph.Observe(cluster)
	return observer
}

// observeTenant returns the Observer of tenant. A nil tenant yields a nil
// Observer, whose queries return an error instead of a result.
func observeTenant(tenant *ceph.Tenant) *ceph.Observer {
	observer, _ := ceph.ObserveTenant(tenant)
	return observer
}
