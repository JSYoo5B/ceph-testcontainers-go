package ceph_test

import (
	"context"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The baseline names the active manager before it is stopped. The event
// that names another available active GID advances the test, for example to
// a check that the SUT reconnects. This example is compiled, not run.
func ExampleObserver_WatchManagers() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithManagerCount(2)))
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
	managers, err := cluster.Managers(ctx)
	if err != nil {
		panic(err)
	}
	watch, err := observer.WatchManagers(ctx, ceph.WatchConfig{})
	if err != nil {
		panic(err)
	}
	defer func() { _ = watch.Stop() }() // before the cluster's Terminate
	baseline := <-watch.Events()
	for _, manager := range managers {
		if manager.DaemonName == baseline.Status.ActiveName {
			grace := 10 * time.Second
			if err := manager.Stop(ctx, &grace); err != nil {
				panic(err)
			}
		}
	}
	for event := range watch.Events() {
		if event.Kind == ceph.WatchGap {
			continue
		}
		if event.Status.Available && event.Status.ActiveGID != baseline.Status.ActiveGID {
			fmt.Printf("mgr.%s (GID %d) took over from mgr.%s at epoch %d\n", event.Status.ActiveName, event.Status.ActiveGID, baseline.Status.ActiveName, event.Epoch)
			// ... check that the SUT reconnects ...
			return
		}
	}
	panic(watch.Stop()) // the watch ended before another manager took over
}
