package rgw

import (
	"context"
	"errors"
	"fmt"

	"github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/internal/multicluster"
)

// InterruptZoneLink disconnects a gateway from the private multisite HTTP
// bridge. Its local Ceph/S3 endpoint stays attached, allowing local operations
// while cross-zone synchronization is interrupted. Restore reconnects the
// original peer IP/aliases; realm/period membership is unchanged.
func (f *Multisite) InterruptZoneLink(ctx context.Context, zoneName string) (*ceph.NetworkInterruption, error) {
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
	return nil, fmt.Errorf("RGW zone %s is not owned", zoneName)
}
