package ceph_test

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The baseline is buffered before WatchMonitors returns, so the MON stops
// only after the watch has a known quorum. The event that shows the stopped
// MON out of the quorum but still in the monmap advances the test. This
// example is compiled, not run.
func ExampleObserver_WatchMonitors() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3)))
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
	monitors, err := cluster.Monitors(ctx)
	if err != nil {
		panic(err)
	}
	watch, err := observer.WatchMonitors(ctx, ceph.WatchConfig{})
	if err != nil {
		panic(err)
	}
	defer func() { _ = watch.Stop() }() // before the cluster's Terminate
	baseline := <-watch.Events()
	stopped := monitors[len(monitors)-1]
	grace := 10 * time.Second
	if err := stopped.Stop(ctx, &grace); err != nil {
		panic(err)
	}
	// ... the SUT runs with the remaining majority ...
	for event := range watch.Events() {
		if event.Kind == ceph.WatchGap {
			continue // an election in progress, not an empty quorum
		}
		member := slices.ContainsFunc(event.Members, func(m ceph.DaemonAddress) bool { return m.Name == stopped.DaemonName })
		if member && !slices.Contains(event.Quorum, stopped.DaemonName) {
			fmt.Printf("mon.%s left quorum %v at election epoch %d (baseline %d)\n", stopped.DaemonName, event.Quorum, event.ElectionEpoch, baseline.ElectionEpoch)
			return
		}
	}
	panic(watch.Stop()) // the watch ended before the MON left the quorum
}
