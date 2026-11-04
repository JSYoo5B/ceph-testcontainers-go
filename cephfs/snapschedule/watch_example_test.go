package snapschedule_test

import (
	"context"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/snapschedule"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// Each event is a change of the module's aggregate progress, not one
// scheduled run: here the test waits until the module has taken three
// snapshots and pruned one, whatever the number of events that took. This
// example is compiled, not run.
func ExampleLease_Watch() {
	ctx := context.Background()
	cluster, err := cephfs.Run(ctx, ceph.DefaultImage)
	if cluster != nil {
		defer func() { _ = cluster.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	if _, err := override.SetMGRModule(ctx, cluster.Container, "snap_schedule", true); err != nil {
		panic(err)
	}
	if _, err := override.SetConfig(ctx, cluster.Container, ceph.ConfigSetting{Section: "mgr", Name: "mgr/snap_schedule/allow_m_granularity", Value: "true"}); err != nil {
		panic(err)
	}
	// ... wait until the restarted MGR answers again ...
	fs := cephfs.Filesystems(cluster.Container)[0]
	lease, err := snapschedule.Add(ctx, fs, snapschedule.Config{Path: "/", Repeat: "1m", Retention: map[string]int{"n": 2}})
	if lease != nil {
		defer func() { _ = lease.Restore(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	watch, err := lease.Watch(ctx, ceph.WatchConfig{Interval: 10 * time.Second})
	if err != nil {
		panic(err)
	}
	defer func() { _ = watch.Stop() }() // before Restore
	for event := range watch.Events() {
		if event.Kind == ceph.WatchGap {
			continue
		}
		if !event.Status.Active {
			fmt.Println("the module deactivated the schedule")
			break
		}
		if event.Status.Created >= 3 && event.Status.Pruned >= 1 {
			fmt.Printf("created %d, pruned %d, last at %s\n", event.Status.Created, event.Status.Pruned, event.Status.Last)
			break
		}
	}
}
