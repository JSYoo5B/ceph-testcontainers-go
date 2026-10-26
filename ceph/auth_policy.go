package ceph

import (
	"context"
	"errors"
)

// clientCapabilities implements ClientConfig.Capabilities, which documents the behavior.
func (c *Container) clientCapabilities(ctx context.Context, client *ClientConfig) (ClientCaps, error) {
	if err := c.lockTopology(ctx); err != nil {
		return ClientCaps{}, err
	}
	defer c.mu.Unlock()
	return c.ownedClientCapabilities(ctx, client)
}

// updateClientCaps implements ClientConfig.UpdateCaps, which documents the behavior.
func (c *Container) updateClientCaps(ctx context.Context, client *ClientConfig, caps ClientCaps) error {
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
