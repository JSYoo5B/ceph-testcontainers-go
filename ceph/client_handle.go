package ceph

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

// cluster returns the cluster that created the identity.
func (client *ClientConfig) cluster(op string) (*Container, error) {
	if client == nil || client.owner == nil {
		return nil, invalidRequest("%s: the client descriptor does not come from a cluster; obtain it from Container.CreateClient", op)
	}

	return client.owner, nil
}

// WithIdentity attaches a container with only this client's keyring. It
// rejects a descriptor whose creation is incomplete, that is revoked or that
// ReplaceKey made stale. Caller owns the attached container and must terminate
// it before the cluster. The options customize the container's ceph.conf.
//
// The container gets /etc/ceph/ceph.conf, with the current MON addresses and a
// [client.<id>] section that points to the keyring, and the keyring at
// KeyringPath. The client must select its identity, for example rados --id
// reader, CEPH_ARGS="--id reader", librados rados_create(&cluster, "reader")
// or go-ceph rados.NewConnWithUser("reader"). It also refuses an identity that
// Revoke removed until its Restore, or that has an unresolved change.
//
//purpose:handoff
func (client *ClientConfig) WithIdentity(opts ...ClientContainerOptions) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		c, err := client.cluster("ClientConfig.WithIdentity")
		if err != nil {
			return err
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.closed {
			return errClusterTerminated
		}
		if !client.ready || client.revoked || len(client.keyring) == 0 {
			return errors.New("cephx client must be an active identity")
		}
		if err := c.authLedger().checkCurrentClient(client); err != nil {
			return err
		}
		if c.settings.hostNetwork {
			if err := hostContainerCustomizer(c.settings.publicAddress).Customize(req); err != nil {
				return err
			}
		} else if err := network.WithNetworkName(nil, c.staticFacts().NetworkName)(req); err != nil {
			return err
		}
		config, err := clientContainerConfig(clientConfigBytes(c.bootstrap.rawConfig(), client), opts)
		if err != nil {
			return err
		}
		return testcontainers.WithFiles(
			textFile("/etc/ceph/ceph.conf", config, 0o644),
			textFile(client.KeyringPath(), bytes.Clone(client.keyring), 0o600),
		)(req)
	}
}

// Capabilities returns the current caps of this confirmed, owned identity.
// It verifies the native key without returning auth database secrets.
//
//purpose:observation
func (client *ClientConfig) Capabilities(ctx context.Context) (ClientCaps, error) {
	c, err := client.cluster("ClientConfig.Capabilities")
	if err != nil {
		return ClientCaps{}, err
	}
	if err := c.lockTopology(ctx); err != nil {
		return ClientCaps{}, err
	}
	defer c.mu.Unlock()
	current, err := c.ownedClientRecord(ctx, client, "capability access")
	if err != nil {
		return ClientCaps{}, err
	}
	return ClientCaps{Mon: current.caps["mon"], OSD: current.caps["osd"], MGR: current.caps["mgr"], MDS: current.caps["mds"]}, nil
}

// UpdateCaps replaces all capabilities while retaining the identity/key.
// Omitted services lose their previous capabilities. External auth edits must
// not race this call. Already issued session tickets may retain previous caps;
// verify permission changes using a fresh client connection. The change is
// fenced like the import of ReplaceKey: a failed call reports what the
// MONs hold once settled, and an uncertain one is resolved by the next
// operation on the identity.
//
//purpose:construction,condition
func (client *ClientConfig) UpdateCaps(ctx context.Context, caps ClientCaps) error {
	c, err := client.cluster("ClientConfig.UpdateCaps")
	if err != nil {
		return err
	}
	args, err := clientCapArgs(caps)
	if err != nil {
		return err
	}
	if len(args) == 0 {
		return errors.New("at least one service capability is required; use ClientConfig.Delete for revocation")
	}
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	current, err := c.ownedClientRecord(ctx, client, "capability changes")
	if err != nil {
		return err
	}
	target := current
	target.caps = map[string]string{}
	for i := 0; i < len(args); i += 2 {
		target.caps[args[i]] = args[i+1]
	}
	_, err = c.authLedger().changeClient(ctx, client.name, map[string]any{"prefix": "auth caps", "entity": client.name, "caps": args}, nil, current, target, func() {})
	return err
}

// ReplaceKey gives this client identity, which the cluster created, a fresh
// Cephx key and keeps its name and its exact capabilities, in one auth
// import, so that the MONs never lack the identity. The identity needs a
// capability, as auth import refuses an entry without any. The new key has
// the type of the old one unless ClientOptions.WithKeyType selects another. It
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
//
//purpose:condition
func (client *ClientConfig) ReplaceKey(ctx context.Context, opts ...ClientOptions) (*ClientConfig, error) {
	c, err := client.cluster("ClientConfig.ReplaceKey")
	if err != nil {
		return nil, err
	}
	keyType, err := resolveClientOptions("", opts)
	if err != nil {
		return nil, err
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if c.closed {
		return nil, errClusterTerminated
	}
	if client == nil || client.owner != c || !client.created || !client.ready || client.revoked {
		return nil, errors.New("cephx client must be an active identity created by this cluster")
	}
	if err := c.authLedger().settleClient(ctx, client.name); err != nil {
		return nil, err
	}
	if next := client.replacedBy; next != nil {
		// This or an earlier call's import has given the identity the new
		// key, which must still be the identity's.
		if _, err := c.ownedClientRecord(ctx, next, "key replacement"); err != nil {
			return nil, fmt.Errorf("the key that replaced the key of %s is no longer current: %w", client.name, err)
		}
		return next, nil
	}
	current, err := c.ownedClientRecord(ctx, client, "key replacement")
	if err != nil {
		return nil, err
	}
	if len(current.caps) == 0 {
		// auth import refuses an entry without capabilities.
		return nil, fmt.Errorf("cephx identity %s has no capabilities, which a key replacement needs", client.name)
	}
	if keyType == "" {
		if keyType, err = keyCipher(current.key); err != nil {
			return nil, fmt.Errorf("cephx identity %s: %w", client.name, err)
		}
	}
	generate, err := c.generateKeyArgs(keyType)
	if err != nil {
		return nil, err
	}
	secret, err := c.conn().rotationCommand(ctx, generate...)
	key := strings.TrimSpace(string(secret))
	if err != nil || key == "" {
		return nil, clientOperationError(ctx, "generate fresh Cephx key: command failed", err)
	}
	return c.authLedger().replaceClientKey(ctx, client, current, key)
}

// Revoke removes this client identity, which the cluster
// created, from the MONs until Restore puts back exactly its key and
// capabilities, so that a test can observe its rejection and then the
// recovery of the same credentials. The identity must still have the key of
// client; one changed outside the module is refused. While it is revoked,
// the descriptor works only for ConnectionConfig, whose credentials stay
// valid for after Restore; ClientConfig.WithIdentity, Capabilities,
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
//
//purpose:condition
func (client *ClientConfig) Revoke(ctx context.Context) (*ClientRevocationHandle, error) {
	c, err := client.cluster("ClientConfig.Revoke")
	if err != nil {
		return nil, err
	}
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
	state, err := c.authLedger().revokeClient(ctx, client, current)
	if state == nil {
		return nil, err
	}
	return &ClientRevocationHandle{owner: c, state: state}, err
}

// Delete revokes this identity, which the cluster created, without deleting data.
// It checks for an externally replaced key before deleting; external auth edits
// must not race this operation. A repeat call after success is a no-op; existing
// sessions may retain issued tickets. The removal is fenced like the import of
// ReplaceKey: a failed call reports what the MONs hold once settled, and
// an uncertain one is resolved by the next operation on the identity.
//
//purpose:condition,lifetime
func (client *ClientConfig) Delete(ctx context.Context) error {
	c, err := client.cluster("ClientConfig.Delete")
	if err != nil {
		return err
	}
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if c.closed {
		return errClusterTerminated
	}
	if client == nil || client.owner != c || !client.created {
		return errors.New("cephx client was not created successfully by this cluster")
	}
	if client.revoked {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	return c.authLedger().deleteClient(ctx, client)
}
