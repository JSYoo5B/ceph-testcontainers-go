package ceph

import (
	"context"
	"errors"
)

// ClientCapabilities returns the current caps of a confirmed, owned identity.
// It verifies the native key without returning auth database secrets.
func (c *Container) ClientCapabilities(ctx context.Context, client *ClientConfig) (ClientCaps, error) {
	if gateErr := c.sharedGate("ClientCapabilities"); gateErr != nil {
		return ClientCaps{}, gateErr
	}
	if err := c.lockTopology(ctx); err != nil {
		return ClientCaps{}, err
	}
	defer c.mu.Unlock()
	return c.ownedClientCapabilities(ctx, client)
}

// UpdateClientCaps replaces all capabilities while retaining the identity/key.
// Omitted services lose their previous capabilities. External auth edits must
// not race this call. Already issued session tickets may retain previous caps;
// verify permission changes using a fresh client connection. The change is
// fenced like the import of ReplaceClientKey: a failed call reports what the
// MONs hold once settled, and an uncertain one is resolved by the next
// operation on the identity.
func (c *Container) UpdateClientCaps(ctx context.Context, client *ClientConfig, caps ClientCaps) error {
	if gateErr := c.sharedGate("UpdateClientCaps"); gateErr != nil {
		return gateErr
	}
	args, err := clientCapArgs(caps)
	if err != nil {
		return err
	}
	if len(args) == 0 {
		return errors.New("at least one service capability is required; use DeleteClient for revocation")
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
	_, err = c.changeClient(ctx, client.name, map[string]any{"prefix": "auth caps", "entity": client.name, "caps": args}, nil, current, target, func() {})
	return err
}

func (c *Container) ownedClientCapabilities(ctx context.Context, client *ClientConfig) (ClientCaps, error) {
	current, err := c.ownedClientRecord(ctx, client, "capability access")
	if err != nil {
		return ClientCaps{}, err
	}
	return ClientCaps{Mon: current.caps["mon"], OSD: current.caps["osd"], MGR: current.caps["mgr"], MDS: current.caps["mds"]}, nil
}
