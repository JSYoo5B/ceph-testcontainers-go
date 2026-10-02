package ceph_test

import (
	"context"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go"
	"github.com/testcontainers/testcontainers-go"
)

func ExampleRun() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithOSDCount(2))
	if cluster != nil {
		defer testcontainers.TerminateContainer(cluster)
	}
	if err != nil {
		panic(err)
	}
	osd, err := cluster.AddOSD(ctx)
	if err != nil {
		panic(err)
	}
	if err := cluster.RemoveOSD(ctx, osd.ID); err != nil {
		panic(err)
	}
}
