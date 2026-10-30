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

// InterruptPeerLink disconnects this CephFS daemon from the destination's public
// bridge while retaining the source filesystem and MGR registration. It does
// not stop/reassign the daemon or alter directory/peer policies. Restore the
// returned endpoint to allow native snapshot synchronization to resume.
//
// Keep a non-nil handle returned with an error: a transport error cannot prove
// that Docker did not disconnect the endpoint. The fixture also restores it on
// Terminate. Host networking has no bridge endpoint and is refused.
//
//purpose:condition
func (daemon *Daemon) InterruptPeerLink(ctx context.Context) (*fault.NetworkInterruption, error) {
	if daemon == nil || daemon.owner == nil {
		return nil, servicehook.Invalid(errors.New("CephFS mirror daemon is not owned by a mirror"))
	}
	m := daemon.owner
	if err := multicluster.LockObservation(ctx, &m.mu); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	if m.closed || m.destination == nil {
		return nil, errors.New("peer interruption requires a live bridge CephFS mirror")
	}
	facts, err := ceph.FactsOf(m.destination)
	if err != nil {
		return nil, err
	}
	if facts.UsesHostNetwork {
		return nil, errors.New("peer interruption requires a live bridge CephFS mirror")
	}
	if !slices.Contains(m.daemons, daemon) || daemon.Container == nil {
		return nil, servicehook.Invalid(fmt.Errorf("CephFS mirror daemon %s is not owned", daemon.DaemonName))
	}
	return multicluster.InterruptOwnedPeerLink(ctx, &m.owned, daemon, facts.NetworkName)
}
