package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// ClientCapabilities returns the current caps of a confirmed, owned identity.
// It verifies the native key without returning auth database secrets.
func (c *Container) ClientCapabilities(ctx context.Context, client *ClientConfig) (ClientCaps, error) {
	if err := c.lockTopology(ctx); err != nil {
		return ClientCaps{}, err
	}
	defer c.mu.Unlock()
	return c.ownedClientCapabilities(ctx, client)
}

// UpdateClientCaps replaces all capabilities while retaining the identity/key.
// Omitted services lose their previous capabilities. External auth edits must
// not race this call. Already issued session tickets may retain previous caps;
// verify permission changes using a fresh client connection. On an uncertain
// command error inspect ClientCapabilities before retrying.
func (c *Container) UpdateClientCaps(ctx context.Context, client *ClientConfig, caps ClientCaps) error {
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
	if _, err := c.ownedClientCapabilities(ctx, client); err != nil {
		return err
	}
	_, err = c.clientAuthCommand(ctx, "update capabilities of "+client.name, append([]string{"auth", "caps", client.name}, args...)...)
	return err
}

func (c *Container) ownedClientCapabilities(ctx context.Context, client *ClientConfig) (ClientCaps, error) {
	if c.closed {
		return ClientCaps{}, errors.New("ceph cluster is terminated")
	}
	if client == nil || client.owner != c || !client.created || !client.ready || client.revoked {
		return ClientCaps{}, errors.New("Cephx client must be an active identity created by this cluster")
	}
	data, err := c.clientAuthCommand(ctx, "inspect capabilities of "+client.name, "auth", "get", client.name, "--format", "json")
	if err != nil {
		return ClientCaps{}, err
	}
	var entries []struct {
		Entity string            `json:"entity"`
		Key    string            `json:"key"`
		Caps   map[string]string `json:"caps"`
	}
	if err := json.Unmarshal(data, &entries); err != nil || len(entries) != 1 || entries[0].Entity != client.name || entries[0].Key == "" || entries[0].Caps == nil {
		return ClientCaps{}, errors.New("decode owned Cephx capabilities")
	}
	entry := entries[0]
	if entry.Key != clientKey(client.keyring, client.name) {
		return ClientCaps{}, fmt.Errorf("Cephx identity %s has a different key; refusing capability access", client.name)
	}
	return ClientCaps{Mon: entry.Caps["mon"], OSD: entry.Caps["osd"], MGR: entry.Caps["mgr"], MDS: entry.Caps["mds"]}, nil
}
