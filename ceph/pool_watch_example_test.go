package ceph_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The baseline is buffered before WatchPGs returns, so the OSD stops only
// after the watch has a known starting state. The first degraded report then
// advances the test while the SUT keeps writing; the SUT's own reads check
// the data. This example is compiled, not run.
func ExamplePool_WatchPGs() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(3)))
	if cluster != nil {
		defer func() { _ = cluster.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "app", Application: "rados", Replicas: 2, MinSize: 1, PGNum: 8})
	if err != nil {
		panic(err)
	}
	osds, err := cluster.OSDs(ctx)
	if err != nil {
		panic(err)
	}
	watch, err := pool.WatchPGs(ctx, ceph.WatchConfig{Interval: 2 * time.Second})
	if err != nil {
		panic(err)
	}
	defer func() { _ = watch.Stop() }() // before the cluster's Terminate
	baseline := <-watch.Events()
	grace := 10 * time.Second
	if err := osds[0].Stop(ctx, &grace); err != nil {
		panic(err)
	}
	// ... the SUT writes and reads through the pool ...
	for event := range watch.Events() {
		if event.Kind == ceph.WatchGap {
			continue
		}
		for _, pg := range event.Snapshot.PGs {
			if strings.Contains(pg.State, "degraded") {
				fmt.Printf("pg %s reported %s at OSDMap epoch %d (baseline %d)\n",
					pg.PGID, pg.State, event.Snapshot.OSDMapEpochAfter, baseline.Snapshot.OSDMapEpochAfter)
				return
			}
		}
	}
	panic(watch.Stop()) // the watch ended before a degraded report
}
