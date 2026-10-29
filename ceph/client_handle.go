package ceph

import "context"

// cluster returns the cluster that created the identity.
func (client *ClientConfig) cluster(op string) (*Container, error) {
	if client == nil || client.owner == nil {
		return nil, invalidRequest("%s: the client descriptor does not come from a cluster; obtain it from Container.CreateClient", op)
	}

	return client.owner, nil
}

// Capabilities returns the current caps of this confirmed, owned identity.
// It verifies the native key without returning auth database secrets.
func (client *ClientConfig) Capabilities(ctx context.Context) (ClientCaps, error) {
	c, err := client.cluster("ClientConfig.Capabilities")
	if err != nil {
		return ClientCaps{}, err
	}
	return c.clientCapabilities(ctx, client)
}

// UpdateCaps replaces all capabilities while retaining the identity/key.
// Omitted services lose their previous capabilities. External auth edits must
// not race this call. Already issued session tickets may retain previous caps;
// verify permission changes using a fresh client connection. The change is
// fenced like the import of ReplaceKey: a failed call reports what the
// MONs hold once settled, and an uncertain one is resolved by the next
// operation on the identity.
func (client *ClientConfig) UpdateCaps(ctx context.Context, caps ClientCaps) error {
	c, err := client.cluster("ClientConfig.UpdateCaps")
	if err != nil {
		return err
	}
	return c.updateClientCaps(ctx, client, caps)
}

// ReplaceKey gives this client identity, which the cluster created, a fresh
// Cephx key and keeps its name and its exact capabilities, in one auth
// import, so that the MONs never lack the identity. The identity needs a
// capability, as auth import refuses an entry without any. The new key has
// the type of the old one unless WithClientKeyType selects another. It
// returns a new descriptor for the new key, and the old descriptor goes
// stale: its ConnectionConfig still returns the old credentials, for tests
// that check their rejection, while every other operation on it fails.
// Containers that were attached with the old descriptor keep the old
// keyring.
//
// A fresh connection with the old key fails at once. A client that
// authenticated before keeps its session and its tickets until it renews them
// within auth_mon_ticket_ttl (AuthConfig.MonTicketTTL), when the MONs reject
// the old key: Tentacle then drops the client's MON session, while Squid
// keeps an open one answering. The new key works for fresh MON and MGR
// clients at once.
//
// The identity must still have the key of client; one changed outside the
// module is refused. External auth changes must not race this call. The
// import is fenced like the MON commands of RotateKey: when it fails,
// the call settles the MONs and reads whether it applied. When that stays
// uncertain, the call returns a descriptor of the attempted key that stays
// unusable, with the error, and a later call with client, or any other
// operation on the identity, settles first: ReplaceKey then returns a
// usable descriptor of that key once the import turns out to have applied,
// or replaces the key anew once it turns out not to have. A returned
// descriptor never changes afterwards.
func (client *ClientConfig) ReplaceKey(ctx context.Context, opts ...ClientOption) (*ClientConfig, error) {
	c, err := client.cluster("ClientConfig.ReplaceKey")
	if err != nil {
		return nil, err
	}
	return c.replaceClientKey(ctx, client, opts...)
}

// Revoke removes this client identity, which the cluster
// created, from the MONs until Restore puts back exactly its key and
// capabilities, so that a test can observe its rejection and then the
// recovery of the same credentials. The identity must still have the key of
// client; one changed outside the module is refused. While it is revoked,
// the descriptor works only for ConnectionConfig, whose credentials stay
// valid for after Restore; WithClientIdentity, Capabilities,
// UpdateCaps and ReplaceKey fail, CreateClient refuses its name,
// and Delete makes the removal permanent.
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
// ReplaceKey. A failed call settles the MONs and reports what they
// hold. When that stays uncertain, Revoke returns the
// revocation with the error, and Restore settles first and then restores.
// The cluster needs no Restore before Terminate.
func (client *ClientConfig) Revoke(ctx context.Context) (*ClientRevocationHandle, error) {
	c, err := client.cluster("ClientConfig.Revoke")
	if err != nil {
		return nil, err
	}
	return c.revokeClient(ctx, client)
}

// Delete revokes this identity, which the cluster created, without deleting data.
// It checks for an externally replaced key before deleting; external auth edits
// must not race this operation. A repeat call after success is a no-op; existing
// sessions may retain issued tickets. The removal is fenced like the import of
// ReplaceKey: a failed call reports what the MONs hold once settled, and
// an uncertain one is resolved by the next operation on the identity.
func (client *ClientConfig) Delete(ctx context.Context) error {
	c, err := client.cluster("ClientConfig.Delete")
	if err != nil {
		return err
	}
	return c.deleteClient(ctx, client)
}
