package multicluster

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// InterruptOwnedPeerLink disconnects ctr from network and records the restore
// in owned, so cleanup reconnects it even if the caller does not.
func InterruptOwnedPeerLink(ctx context.Context, owned *Resources, ctr testcontainers.Container, network string) (*ceph.NetworkInterruption, error) {
	link, err := ceph.InterruptNetwork(ctx, ctr, network)
	if link != nil {
		owned.AddCleanup("restore interrupted peer network", link.Restore)
	}
	return link, err
}
