package multicluster

import (
	"context"
	"errors"
	"fmt"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func interruptOwnedPeerLink(ctx context.Context, owned *resources, ctr testcontainers.Container, network string) (*ceph.NetworkInterruption, error) {
	link, err := ceph.InterruptNetwork(ctx, ctr, network)
	if link != nil {
		owned.addCleanup("restore interrupted peer network", link.Restore)
	}
	return link, err
}

// InterruptPeerLink disconnects one RBD receiver daemon from the source
// cluster's public bridge, retaining its destination/pool-election endpoint.
// The daemon stays running; peer and image policies remain unchanged. Restore
// the returned handle to resume reception. Host mode has no bridge endpoint.
func (m *RBDMirror) InterruptPeerLink(ctx context.Context, daemonName string) (*ceph.NetworkInterruption, error) {
	if m == nil {
		return nil, errors.New("RBD mirror is unavailable")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.config.Source == nil || m.config.Source.UsesHostNetwork() {
		return nil, errors.New("peer interruption requires a live bridge RBD mirror")
	}
	for _, daemon := range m.daemons {
		if daemon.DaemonName == daemonName && daemon.Container != nil {
			return interruptOwnedPeerLink(ctx, &m.owned, daemon, m.config.Source.NetworkName())
		}
	}
	return nil, fmt.Errorf("RBD mirror daemon %s is not owned", daemonName)
}

// InterruptPeerLink disconnects one CephFS daemon from the destination's public
// bridge while retaining the source filesystem and MGR registration. It does
// not stop/reassign the daemon or alter directory/peer policies. Restore the
// returned endpoint to allow native snapshot synchronization to resume.
func (m *CephFSMirror) InterruptPeerLink(ctx context.Context, daemonName string) (*ceph.NetworkInterruption, error) {
	if m == nil {
		return nil, errors.New("CephFS mirror is unavailable")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.destination == nil || m.destination.UsesHostNetwork() {
		return nil, errors.New("peer interruption requires a live bridge CephFS mirror")
	}
	for _, daemon := range m.daemons {
		if daemon.DaemonName == daemonName && daemon.Container != nil {
			return interruptOwnedPeerLink(ctx, &m.owned, daemon, m.destination.NetworkName())
		}
	}
	return nil, fmt.Errorf("CephFS mirror daemon %s is not owned", daemonName)
}

// InterruptZoneLink disconnects a gateway from the private multisite HTTP
// bridge. Its local Ceph/S3 endpoint stays attached, allowing local operations
// while cross-zone synchronization is interrupted. Restore reconnects the
// original peer IP/aliases; realm/period membership is unchanged.
func (f *RGWMultisite) InterruptZoneLink(ctx context.Context, zoneName string) (*ceph.NetworkInterruption, error) {
	if f == nil {
		return nil, errors.New("RGW multisite is unavailable")
	}
	f.topologyMu.Lock()
	defer f.topologyMu.Unlock()
	if f.closed || f.httpNetwork == nil {
		return nil, errors.New("zone link interruption requires a live bridge RGW topology")
	}
	for _, zone := range f.zoneStates() {
		if zone.Name == zoneName && zone.Gateway != nil {
			return interruptOwnedPeerLink(ctx, &f.owned, zone.Gateway, f.httpNetwork.Name)
		}
	}
	return nil, fmt.Errorf("RGW zone %s is not owned", zoneName)
}
