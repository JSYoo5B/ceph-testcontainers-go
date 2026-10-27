//go:build all || integration

package integration_test

import "github.com/JSYoo5B/ceph-testcontainers-go/ceph"

// staticFacts returns the static facts of a cluster that the scenario holds. A
// nil cluster is a defect of the scenario, so it panics instead of continuing
// with a zero snapshot.
func staticFacts(c *ceph.Container) ceph.ClusterFacts {
	facts, err := ceph.FactsOf(c)
	if err != nil {
		panic("static facts of the scenario cluster: " + err.Error())
	}
	return facts
}
