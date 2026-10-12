package cephfs

import (
	"context"
	"errors"
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
)

// InterruptPeerLink disconnects one CephFS daemon from the destination's public
// bridge while retaining the source filesystem and MGR registration. It does
// not stop/reassign the daemon or alter directory/peer policies. Restore the
// returned endpoint to allow native snapshot synchronization to resume.
func (m *Mirror) InterruptPeerLink(ctx context.Context, daemonName string) (*ceph.NetworkInterruption, error) {
	if m == nil {
		return nil, errors.New("CephFS mirror is unavailable")
	}
	if err := multicluster.LockObservation(ctx, &m.mu); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	if m.closed || m.destination == nil || m.destination.UsesHostNetwork() {
		return nil, errors.New("peer interruption requires a live bridge CephFS mirror")
	}
	for _, daemon := range m.daemons {
		if daemon.DaemonName == daemonName && daemon.Container != nil {
			return multicluster.InterruptOwnedPeerLink(ctx, &m.owned, daemon, m.destination.NetworkName())
		}
	}
	return nil, invalid(fmt.Errorf("CephFS mirror daemon %s is not owned", daemonName))
}
