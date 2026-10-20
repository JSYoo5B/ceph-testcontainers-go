package multicluster

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/testcontainers/testcontainers-go"
)

// InterruptOwnedPeerLink disconnects ctr from network and records the restore
// in owned, so cleanup reconnects it even if the caller does not.
func InterruptOwnedPeerLink(ctx context.Context, owned *Resources, ctr testcontainers.Container, network string) (*fault.NetworkInterruption, error) {
	link, err := fault.InterruptLink(ctx, ctr, network)
	if link != nil {
		owned.AddCleanup("restore interrupted peer network", link.Restore)
	}
	return link, err
}
