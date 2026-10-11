package rbd

import (
	"context"
	"errors"
	"fmt"

	"github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/internal/multicluster"
)

// InterruptPeerLink disconnects one RBD receiver daemon from the source
// cluster's public bridge, retaining its destination/pool-election endpoint.
// The daemon stays running; peer and image policies remain unchanged. Restore
// the returned handle to resume reception. Host mode has no bridge endpoint.
func (m *Mirror) InterruptPeerLink(ctx context.Context, daemonName string) (*ceph.NetworkInterruption, error) {
	if m == nil {
		return nil, errors.New("RBD mirror is unavailable")
	}
	if err := multicluster.LockObservation(ctx, &m.mu); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	if m.closed || m.config.Source == nil || m.config.Source.UsesHostNetwork() {
		return nil, errors.New("peer interruption requires a live bridge RBD mirror")
	}
	for _, daemon := range m.daemons {
		if daemon.DaemonName == daemonName && daemon.Container != nil {
			return multicluster.InterruptOwnedPeerLink(ctx, &m.owned, daemon, m.config.Source.NetworkName())
		}
	}
	return nil, fmt.Errorf("RBD mirror daemon %s is not owned", daemonName)
}
