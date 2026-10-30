package ceph

import (
	"context"
	"errors"
	"fmt"
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
	if err := c.authLedger().settleClient(ctx, state.client.name); err != nil {
		return err
	}
	// Settling may have confirmed a permanent removal.
	if state.deleted {
		return fmt.Errorf("cephx client %s was deleted for good", state.client.name)
	}
	current, err := authStore{c.conn()}.readRecord(ctx, state.client.name)
	if err != nil {
		return err
	}
	finish := func() {
		state.restored = true
		c.ledger.authRecords.dropRevocationIf(state.client.name, state)
	}
	switch {
	case current.equal(state.original):
		// The removal never applied, or the identity is restored already.
		finish()
		return nil
	case state.restored || current.exists:
		return invalid(fmt.Errorf("cephx identity %s now has another key or other capabilities; refusing to overwrite it", state.client.name))
	}
	// auth add with the original keyring creates exactly the original entry,
	// which auth import refuses without capabilities, and fails rather than
	// changing an entry that another call created meanwhile.
	_, err = c.authLedger().changeClient(ctx, state.client.name, map[string]any{"prefix": "auth add", "entity": state.client.name}, state.original.keyring, current, state.original, finish)
	return err
}
