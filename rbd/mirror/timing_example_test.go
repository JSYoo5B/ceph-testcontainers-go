package mirror_test

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
)

func ExampleTimingProfile() {
	ctx := context.Background()
	source, err := rbd.Run(ctx, ceph.DefaultImage)
	if source != nil {
		defer func() { _ = source.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	destination, err := rbd.Run(ctx, ceph.DefaultImage)
	if destination != nil {
		defer func() { _ = destination.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	link, err := rbdmirror.Run(ctx, ceph.DefaultImage, rbdmirror.WithConfig(rbdmirror.Config{
		Source: source.Container, Destination: destination.Container, Pool: "rbd", Mode: rbdmirror.ModeJournal,
		Timing: rbdmirror.TimingProfile{JournalPolling: true},
	}))
	if link != nil {
		defer func() { _ = link.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	// Enable the intended journal images and exercise the application. The
	// one-second polling condition belongs to this mirror's daemon lifetime.
}
