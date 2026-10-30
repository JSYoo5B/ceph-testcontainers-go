package mirror

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// InterruptPeerLink disconnects this RBD receiver daemon from the source
// cluster's public bridge, retaining its destination/pool-election endpoint.
// The daemon stays running; peer and image policies remain unchanged. Restore
// the returned handle to resume reception. Host mode has no bridge endpoint.
func (d *Daemon) InterruptPeerLink(ctx context.Context) (*fault.NetworkInterruption, error) {
	if d == nil || d.owner == nil {
		return nil, servicehook.Invalid(errors.New("RBD mirror daemon is not owned by a fixture"))
	}
	m := d.owner
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
	if !slices.Contains(m.daemons, d) || d.Container == nil {
		return nil, servicehook.Invalid(fmt.Errorf("RBD mirror daemon %s is not owned", d.DaemonName))
	}
	return multicluster.InterruptOwnedPeerLink(ctx, &m.owned, d, facts.NetworkName)
}
