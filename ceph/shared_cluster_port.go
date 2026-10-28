package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"syscall"

	"github.com/testcontainers/testcontainers-go"
)

// This file holds the *Container side of the shared-cluster backend port:
// containerBackend (shared_backend.go) reaches the cluster only through these
// purpose-named methods and the exported CreatePool, WaitForQuorum and
// WaitForPGClean, and names no Container field. TestContainerBackendReach
// pins the remaining reach.

// runSharedScript runs request in the control container.
func (c *Container) runSharedScript(ctx context.Context, request sharedRequest) (sharedResult, error) {
	var result sharedResult
	control, err := c.controlContainerContext(ctx)
	if err != nil {
		return result, err
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return result, err
	}
	data, err := command(ctx, control, "python3", "-c", sharedScript, string(encoded))
	if err != nil {
		return result, fmt.Errorf("read the shared cluster's state: %w", err)
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return result, errors.New("decode the shared cluster's state")
	}
	return result, nil
}

// sharedDaemons returns the cluster's daemon containers by name, read under
// the topology mutex. The callers inspect them after the lock is released, so
// a slow Docker call never holds a lock.
func (c *Container) sharedDaemons(ctx context.Context) (map[string]testcontainers.Container, error) {
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	daemons := map[string]testcontainers.Container{}
	if c.Container != nil && !c.monitorTerminated {
		daemons["mon"] = c.Container
	}
	for name, mon := range c.monitors {
		if mon.Container != nil {
			daemons["mon."+name] = mon.Container
		}
	}
	for name, mgr := range c.managers {
		if mgr.Container != nil && !mgr.terminated {
			daemons["mgr."+name] = mgr.Container
		}
	}
	for id, osd := range c.osdHandles.each() {
		if osd.Container != nil && !osd.purged {
			daemons[fmt.Sprintf("osd.%d", id)] = osd.Container
		}
	}
	for name, ctr := range c.services {
		daemons[name] = ctr
	}
	c.mu.Unlock()
	if control := c.controlPlane.current(); control != nil {
		daemons["control"] = control
	}
	return daemons, nil
}

// setSharingMode sets the gate of the cluster.
func (c *Container) setSharingMode(mode sharingMode) { c.sharing.Store(int32(mode)) }

// createSharedClient creates a tenant's client in one script run: a fresh key,
// auth add with it, which the MONs refuse for an existing entity, and auth
// get. CreateClient takes several Ceph CLI processes and a keyring copy,
// about a second under the cluster mutex.
func (c *Container) createSharedClient(ctx context.Context, entity string, caps ClientCaps) (*ClientConfig, error) {
	generate, err := c.generateKeyArgs(c.settings.auth.KeyType)
	if err != nil {
		return nil, err
	}
	capArgs, err := clientCapArgs(caps)
	if err != nil {
		return nil, err
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return nil, errClusterTerminated
	}
	if len(c.config) == 0 {
		return nil, errors.New("ceph cluster bootstrap is incomplete")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := c.settleClient(ctx, entity); err != nil {
		return nil, err
	}
	client := &ClientConfig{owner: c, name: entity}
	client.config = clientConfigBytes(c.config, client)
	result, err := c.runSharedScript(ctx, sharedRequest{CreateAuth: &sharedCreateAuth{Entity: entity, Caps: capArgs, Generate: generate}})
	if err != nil {
		// The script's output can hold the key.
		return client, clientOperationError(ctx, "create "+entity+": command failed", err)
	}
	created := result.Created
	if created == nil || created.Key == "" {
		return client, fmt.Errorf("create %s: no key was generated", entity)
	}
	c.authRecords.addSecret(created.Key)
	if created.Ret != 0 {
		return client, fmt.Errorf("create %s: the MON answered %d", entity, created.Ret)
	}
	client.created = true
	c.authRecords.confirmKey(entity, created.Key)
	keyring := []byte(created.Keyring)
	if clientKey(keyring, entity) != created.Key {
		return client, fmt.Errorf("cephx keyring for %s does not contain the created key", entity)
	}
	client.keyring = keyring
	client.ready = true
	return client, nil
}

// deleteSharedClient removes a tenant's client with one auth rm. ClientConfig.Delete
// settles the MONs and reads the entry back, which takes seconds and holds
// the cluster mutex; here the release's verification, which must not find
// the client, confirms the removal instead.
func (c *Container) deleteSharedClient(ctx context.Context, client *ClientConfig) error {
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
	if err := c.settleClient(ctx, client.name); err != nil {
		return err
	}
	result, err := c.runSharedScript(ctx, sharedRequest{RemoveAuth: []string{client.name}})
	if err != nil {
		return err
	}
	// auth rm of an absent entity answers 0 or ENOENT.
	if ret, ok := result.Removed[client.name]; !ok || (ret != 0 && ret != -int(syscall.ENOENT)) {
		return fmt.Errorf("remove %s: the MON answered %d", client.name, ret)
	}
	client.revoked = true
	c.authRecords.confirmKey(client.name, "")
	return nil
}
