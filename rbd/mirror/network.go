package mirror

import (
	"context"
	"errors"
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// InterruptPeerLink disconnects one RBD receiver daemon from the source
// cluster's public bridge, retaining its destination/pool-election endpoint.
// The daemon stays running; peer and image policies remain unchanged. Restore
// the returned handle to resume reception. Host mode has no bridge endpoint.
func (m *Mirror) InterruptPeerLink(ctx context.Context, daemonName string) (*fault.NetworkInterruption, error) {
	if m == nil {
		return nil, errors.New("RBD mirror is unavailable")
	}
	if err := multicluster.LockObservation(ctx, &m.mu); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	if m.closed || m.config.Source == nil {
		return nil, errors.New("peer interruption requires a live bridge RBD mirror")
	}
	facts, err := ceph.FactsOf(m.config.Source)
	if err != nil {
		return nil, err
	}
	if facts.UsesHostNetwork {
		return nil, errors.New("peer interruption requires a live bridge RBD mirror")
	}
	for _, daemon := range m.daemons {
		if daemon.DaemonName == daemonName && daemon.Container != nil {
			return multicluster.InterruptOwnedPeerLink(ctx, &m.owned, daemon, facts.NetworkName)
		}
	}
	return nil, servicehook.Invalid(fmt.Errorf("RBD mirror daemon %s is not owned", daemonName))
}
