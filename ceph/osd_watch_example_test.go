package ceph_test

import (
	"context"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The baseline is buffered before WatchOSDs returns, so the fault starts
// only after the watch has a known starting state. One event then advances
// the test while the SUT keeps running. This example is compiled, not run.
func ExampleObserver_WatchOSDs() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage)
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
	watch, err := observer.WatchOSDs(ctx, ceph.OSDWatchConfig{Interval: time.Second})
	if err != nil {
		panic(err)
	}
	defer func() { _ = watch.Stop() }() // before the cluster's Terminate
	baseline := <-watch.Events()
	grace := 10 * time.Second
	if err := osds[0].Stop(ctx, &grace); err != nil {
		panic(err)
	}
	// ... the SUT runs ...
	for event := range watch.Events() {
		if event.Kind == ceph.OSDWatchGap {
			continue
		}
		for _, s := range event.States {
			if s.ID == osds[0].ID && !s.Up {
				fmt.Printf("osd.%d reported down at epoch %d (baseline %d)\n", s.ID, event.Epoch, baseline.Epoch)
				return
			}
		}
	}
	panic(watch.Stop()) // the watch ended before the OSD was reported down
}
