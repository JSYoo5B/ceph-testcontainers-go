package ceph

import (
	"context"
	"errors"
)

// ClientRevocationHandle manages removal and restoration of a client identity
// that this cluster created. It retains ownership and possibly uncertain apply
// state; a non-nil handle does not guarantee a successful removal. Restore puts
// back the same key and capabilities. It holds the original credentials
// privately and never prints them. Copies share its state.
type ClientRevocationHandle struct {
	owner *Container
	state *clientRevocationState
}

// clientRevocationState is the state of one revocation; the cluster's mutex
// guards it.
type clientRevocationState struct {
	client *ClientConfig
	// original is the auth entry that Restore puts back.
	original clientRecord
	// restored means that the identity holds original again, and deleted that
	// ClientConfig.Delete removed it for good.
	restored, deleted bool
}

// Name returns the revoked entity, for example client.reader.
//
//purpose:observation
func (r *ClientRevocationHandle) Name() string { return r.state.client.name }

// String deliberately excludes credentials.
func (r ClientRevocationHandle) String() string {
	if r.state == nil {
		return "temporary revocation of a Cephx client"
	}
	return "temporary revocation of Cephx client " + r.state.client.name
}

// GoString excludes credentials from formatted Go representations as well.
func (r ClientRevocationHandle) GoString() string { return r.String() }

// Restore puts back the identity's original key and capabilities and makes
// its descriptor usable again. It succeeds again once restored, as long as
// the identity still holds them. It refuses an identity that was created
// again with another key or other capabilities, and one that ClientConfig.Delete
// removed for good. The restoration is fenced like the removal: when its
// outcome stays uncertain, the next Restore settles first.
//
//purpose:condition,lifetime
func (r *ClientRevocationHandle) Restore(ctx context.Context) error {
	if r == nil || r.owner == nil || r.state == nil {
		return errors.New("client revocation is unavailable")
	}
	c, state := r.owner, r.state
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if c.closed {
		return errClusterTerminated
	}
	return c.authLedger().restoreClient(ctx, state)
}
