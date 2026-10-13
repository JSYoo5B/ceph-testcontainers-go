package ceph

import (
	"context"
	"errors"
	"fmt"
)

// ClientRevocation is a temporary removal of a client identity that this
// cluster created; Restore puts back the same key and capabilities. It holds
// the original credentials privately and never prints them. Copies of a
// revocation share its state.
type ClientRevocation struct {
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
	// DeleteClient removed it for good.
	restored, deleted bool
}

// Name returns the revoked entity, for example client.reader.
func (r *ClientRevocation) Name() string { return r.state.client.name }

// String deliberately excludes credentials.
func (r ClientRevocation) String() string {
	if r.state == nil {
		return "temporary revocation of a Cephx client"
	}
	return "temporary revocation of Cephx client " + r.state.client.name
}

// GoString excludes credentials from formatted Go representations as well.
func (r ClientRevocation) GoString() string { return r.String() }

// TemporaryClientRevocation removes a client identity that this cluster
// created from the MONs until Restore puts back exactly its key and
// capabilities, so that a test can observe its rejection and then the
// recovery of the same credentials. The identity must still have the key of
// client; one changed outside the module is refused. While it is revoked,
// the descriptor works only for ConnectionConfig, whose credentials stay
// valid for after Restore; WithClientIdentity, ClientCapabilities,
// UpdateClientCaps and ReplaceClientKey fail, CreateClient refuses its name,
// and DeleteClient makes the removal permanent.
//
// A fresh connection with the client's key fails at once. A client that
// authenticated before keeps its sessions until it renews its tickets, within
// auth_mon_ticket_ttl (AuthConfig.MonTicketTTL), when the MONs reject the
// renewal; it keeps the MGR and OSD sessions it already holds. Tentacle also
// drops its MON session, and a librados client then does not authenticate
// again on its own, while Squid keeps an open MON session answering. A new
// connection with the same credentials works once Restore returns.
//
// The removal and the restoration are fenced like the import of
// ReplaceClientKey. A failed call settles the MONs and reports what they
// hold. When that stays uncertain, TemporaryClientRevocation returns the
// revocation with the error, and Restore settles first and then restores.
// The cluster needs no Restore before Terminate.
func (c *Container) TemporaryClientRevocation(ctx context.Context, client *ClientConfig) (*ClientRevocation, error) {
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	current, err := c.ownedClientRecord(ctx, client, "revocation")
	if err != nil {
		return nil, err
	}
	state := &clientRevocationState{client: client, original: current}
	revocation := &ClientRevocation{owner: c, state: state}
	if c.clientRevocations == nil {
		c.clientRevocations = make(map[string]*clientRevocationState)
	}
	c.clientRevocations[client.name] = state
	_, err = c.changeClient(ctx, client.name, map[string]any{"prefix": "auth rm", "entity": client.name}, nil, current, clientRecord{}, func() {})
	switch {
	case err == nil:
		return revocation, nil
	case c.authFences[client.name] != nil:
		return revocation, err
	}
	delete(c.clientRevocations, client.name)
	return nil, err
}

// Restore puts back the identity's original key and capabilities and makes
// its descriptor usable again. It succeeds again once restored, as long as
// the identity still holds them. It refuses an identity that was created
// again with another key or other capabilities, and one that DeleteClient
// removed for good. The restoration is fenced like the removal: when its
// outcome stays uncertain, the next Restore settles first.
func (r *ClientRevocation) Restore(ctx context.Context) error {
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
	if err := c.settleClient(ctx, state.client.name); err != nil {
		return err
	}
	// Settling may have confirmed a permanent removal.
	if state.deleted {
		return fmt.Errorf("cephx client %s was deleted for good", state.client.name)
	}
	current, err := c.readClientRecord(ctx, state.client.name)
	if err != nil {
		return err
	}
	finish := func() {
		state.restored = true
		if c.clientRevocations[state.client.name] == state {
			delete(c.clientRevocations, state.client.name)
		}
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
	_, err = c.changeClient(ctx, state.client.name, map[string]any{"prefix": "auth add", "entity": state.client.name}, state.original.keyring, current, state.original, finish)
	return err
}
