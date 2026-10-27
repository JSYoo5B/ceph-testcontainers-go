package override_test

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

func ExampleSetConfig() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage)
	if cluster != nil {
		defer func() { _ = cluster.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	change, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{
		Section: "osd", Name: "osd_max_backfills", Value: "1",
	})
	// The handle is never nil. A failed apply can still have persisted, so
	// the caller always schedules restoration; after a refusal Restore does
	// nothing.
	defer func() {
		if err := change.Restore(context.Background()); err != nil {
			panic(err)
		}
	}()
	if err != nil {
		panic(err)
	}
	// Exercise the native client, then Restore removes an originally absent
	// entry instead of pinning the daemon's inherited/default value.
}
