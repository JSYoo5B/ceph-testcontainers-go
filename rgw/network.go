package rgw

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"strconv"

	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
)

// InterruptZoneLink disconnects a gateway from the private multisite HTTP
// bridge. Its local Ceph/S3 endpoint stays attached, allowing local operations
// while cross-zone synchronization is interrupted. Restore reconnects the
// original peer IP/aliases; realm/period membership is unchanged.
func (f *Multisite) InterruptZoneLink(ctx context.Context, zoneName string) (*fault.NetworkInterruption, error) {
	if f == nil {
		return nil, errors.New("RGW multisite is unavailable")
	}
	if err := multicluster.LockObservation(ctx, &f.topologyMu); err != nil {
		return nil, err
	}
	defer f.topologyMu.Unlock()
	if f.closed || f.httpNetwork == nil {
		return nil, errors.New("zone link interruption requires a live bridge RGW topology")
	}
	for _, zone := range f.zoneStates() {
		if zone.Name == zoneName && zone.Gateway != nil {
			return multicluster.InterruptOwnedPeerLink(ctx, &f.owned, zone.Gateway, f.httpNetwork.Name)
		}
	}
	return nil, invalid(fmt.Errorf("RGW zone %s is not owned", zoneName))
}

// ApplyZoneLinkProfile applies netem shaping, below RGW, to packets that the
// gateway of zoneName sends to the gateway of peerZone on the private
// multisite bridge: to the peer gateway's bridge address and the port of its
// peer endpoint. Those carry the requests that zoneName makes of peerZone,
// its sync reads and its notifications. Connections that peerZone opens to
// zoneName answer from zoneName's own port and stay unselected, so peerZone
// keeps syncing from zoneName; the zones' local endpoints and other links
// are untouched. It is fault.ApplyLinkProfile with the opt-in netem helper:
// delay names the helper and shaping fields, and leaves Plane,
// Peer, PeerPort and Peers empty, which the fixture fills in. The fixture
// restores the lease when it terminates; restore it earlier to end the fault.
func (f *Multisite) ApplyZoneLinkProfile(ctx context.Context, zoneName, peerZone string, delay fault.NetworkProfile) (*fault.NetworkProfileLease, error) {
	if f == nil {
		return nil, errors.New("RGW multisite is unavailable")
	}
	if delay.Plane != "" || delay.Peer.IsValid() || delay.PeerPort != 0 || delay.Peers != nil {
		return nil, invalid(errors.New("a zone link delay selects its own network and peer; leave Plane, Peer, PeerPort and Peers empty"))
	}
	if zoneName == peerZone {
		return nil, invalid(fmt.Errorf("RGW zone %s has no link to itself", zoneName))
	}
	if err := multicluster.LockObservation(ctx, &f.topologyMu); err != nil {
		return nil, err
	}
	defer f.topologyMu.Unlock()
	if f.closed || f.httpNetwork == nil {
		return nil, errors.New("a zone link delay requires a live bridge RGW topology")
	}
	var zone, peer *rgwZoneState
	for _, state := range f.zoneStates() {
		switch {
		case state.Gateway == nil:
		case state.Name == zoneName:
			zone = state
		case state.Name == peerZone:
			peer = state
		}
	}
	if zone == nil || peer == nil {
		return nil, invalid(fmt.Errorf("RGW zones %s and %s are not both owned with a gateway", zoneName, peerZone))
	}
	endpoint, err := url.Parse(peer.PeerEndpoint)
	if err != nil || endpoint.Port() == "" {
		return nil, fmt.Errorf("RGW zone %s has no peer endpoint with a port: %q", peerZone, peer.PeerEndpoint)
	}
	port, err := strconv.ParseUint(endpoint.Port(), 10, 16)
	if err != nil {
		return nil, fmt.Errorf("RGW zone %s peer endpoint %q: %w", peerZone, peer.PeerEndpoint, err)
	}
	inspection, err := peer.Gateway.Inspect(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect the gateway of RGW zone %s: %w", peerZone, err)
	}
	var address netip.Addr
	if inspection.NetworkSettings != nil {
		if bridge := inspection.NetworkSettings.Networks[f.httpNetwork.Name]; bridge != nil {
			address = bridge.IPAddress
		}
	}
	if !address.IsValid() {
		return nil, fmt.Errorf("the gateway of RGW zone %s has no address on the multisite bridge %s", peerZone, f.httpNetwork.Name)
	}
	delay.Peers = []fault.NetworkPeer{{Addr: address, Port: uint16(port)}}
	lease, err := fault.ApplyLinkProfile(ctx, zone.Gateway, f.httpNetwork.Name, delay)
	if lease != nil {
		f.owned.AddCleanup("restore RGW zone link delay", lease.Restore)
	}
	return lease, err
}
