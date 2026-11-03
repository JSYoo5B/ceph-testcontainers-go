package ceph_test

import (
	"context"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The baseline is buffered before WatchHealth returns, so the OSD stops only
// after the watch has a known starting state. The test then waits for the
// fault's own check code to appear and checks the SUT separately. Messages
// are not printed: they can contain application data. This example is
// compiled, not run.
func ExampleObserver_WatchHealth() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(3)))
	if cluster != nil {
		defer func() { _ = cluster.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	observer, err := ceph.Observe(cluster)
	if err != nil {
		panic(err)
	}
	osds, err := cluster.OSDs(ctx)
	if err != nil {
		panic(err)
	}
	watch, err := observer.WatchHealth(ctx, ceph.WatchConfig{Interval: 2 * time.Second})
	if err != nil {
		panic(err)
	}
	defer func() { _ = watch.Stop() }() // before the cluster's Terminate
	if baseline := <-watch.Events(); len(baseline.Health.Checks) != 0 {
		panic("the cluster is not healthy before the fault")
	}
	grace := 10 * time.Second
	if err := osds[0].Stop(ctx, &grace); err != nil {
		panic(err)
	}
	// ... the SUT runs ...
	for event := range watch.Events() {
		if event.Kind == ceph.WatchGap {
			continue
		}
		if check, ok := event.Health.Checks["OSD_DOWN"]; ok && !check.Muted {
			fmt.Printf("OSD_DOWN %s with count %d (event %d)\n", check.Severity, check.Count, event.Sequence)
			return
		}
	}
	panic(watch.Stop()) // the watch ended before OSD_DOWN was reported
}
