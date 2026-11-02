package multisite_test

import (
	"context"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
)

func ExampleTimingProfile() {
	ctx := context.Background()
	source, err := ceph.Run(ctx, ceph.DefaultImage)
	if source != nil {
		defer func() { _ = source.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	destination, err := ceph.Run(ctx, ceph.DefaultImage)
	if destination != nil {
		defer func() { _ = destination.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	link, err := multisite.Run(ctx, ceph.DefaultImage, multisite.WithConfig(multisite.Config{
		Source: source, Destination: destination,
		Timing: multisite.TimingProfile{IncrementalSync: true},
	}))
	if link != nil {
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := link.Terminate(cleanup); err != nil {
				panic(err)
			}
		}()
	}
	if err != nil {
		panic(err)
	}
	// Prepare metadata readiness barriers before object writes; the native
	// gateways keep their ordinary sync leases and notifications.
}
