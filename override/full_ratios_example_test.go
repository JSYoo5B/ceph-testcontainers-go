package override_test

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

func ExampleSetFullRatios() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.RunOptions{})
	if cluster != nil {
		defer func() { _ = cluster.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	change, err := override.SetFullRatios(ctx, cluster, ceph.FullRatios{
		NearFull: .01, BackfillFull: .02, Full: .03,
	})
	// The handle is never nil. The caller schedules restoration; the override
	// itself has no expiry, and after a refusal Restore does nothing.
	defer func() { _ = change.Restore(context.Background()) }()
	if err != nil {
		panic(err)
	}
	// The OSDMap policy is confirmed; observe HealthDetails for actual OSD reports.
}
