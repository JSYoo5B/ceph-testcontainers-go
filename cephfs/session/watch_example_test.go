package session_test

import (
	"context"
	"fmt"
	"slices"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/session"
)

// The watch selects one client by its native ID and reports its session
// until the MDS that held it no longer lists it. Gaps, such as samples taken
// while a standby takes over, never remove it. The blocklist is read on its
// own: the removal does not say why the session went away. This example is
// compiled, not run.
func ExampleWatch() {
	ctx := context.Background()
	cluster, err := cephfs.Run(ctx, ceph.DefaultImage)
	if cluster != nil {
		defer func() { _ = cluster.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	fs := cephfs.Filesystems(cluster.Container)[0]
	// ... the SUT mounts fs; find its session with session.List ...
	var client session.Session
	w, err := session.Watch(ctx, fs, ceph.WatchConfig{}, client.ID)
	if err != nil {
		panic(err)
	}
	defer func() { _ = w.Stop() }() // before the cluster's Terminate
	<-w.Events()                    // the baseline
	// ... freeze the SUT with fault.Pause ...
	for event := range w.Events() {
		if event.Kind == ceph.WatchGap || len(event.Removed) == 0 {
			continue
		}
		observer, err := ceph.Observe(cluster.Container)
		if err != nil {
			panic(err)
		}
		entries, err := observer.BlocklistEntries(ctx)
		if err != nil {
			panic(err)
		}
		blocklisted := slices.ContainsFunc(entries, func(e ceph.BlocklistEntry) bool { return e.Address == event.Removed[0].Address })
		fmt.Println("session removed; address blocklisted:", blocklisted)
		return
	}
}
