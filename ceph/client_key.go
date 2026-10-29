package ceph

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
)

// clientRecord is the auth entry of a client identity as the MONs hold it.
type clientRecord struct {
	exists bool
	key    string
	caps   map[string]string
	// keyring is the entry in the keyring form that auth get prints and auth
	// import takes back. It holds the key.
	keyring []byte
}

func (r clientRecord) equal(other clientRecord) bool {
	if r.exists != other.exists {
		return false
	}
	return !r.exists || (r.key == other.key && maps.Equal(r.caps, other.caps))
}

// clientTransition is a change of a client's auth entry from one record to
// another that has not been confirmed. applied runs once the MONs show to.
type clientTransition struct {
	from, to clientRecord
	applied  func()
}

// readClientRecord reads the auth entry of entity, which must not change
// meanwhile. The caller holds c.mu.
func (c *Container) readClientRecord(ctx context.Context, entity string) (clientRecord, error) {
	result, err := c.rotationCommandResult(ctx, 1<<20, 64<<10, "ceph", "--connect-timeout", "5", "auth", "get", entity, "--format", "json")
	if err != nil {
		return clientRecord{}, clientOperationError(ctx, "read the auth entry of "+entity, err)
	}
	if result.ExitCode == 2 && bytes.Contains(result.Stderr, []byte("ENOENT")) {
		return clientRecord{}, nil
	}
	var entries []struct {
		Entity string            `json:"entity"`
		Key    string            `json:"key"`
		Caps   map[string]string `json:"caps"`
	}
	if result.ExitCode != 0 || json.Unmarshal(result.Stdout, &entries) != nil || len(entries) != 1 || entries[0].Entity != entity || entries[0].Key == "" {
		return clientRecord{}, fmt.Errorf("read the auth entry of %s: unexpected result (exit %d)", entity, result.ExitCode)
	}
	keyring, err := c.rotationCommand(ctx, "ceph", "--connect-timeout", "5", "auth", "get", entity)
	if err != nil {
		return clientRecord{}, clientOperationError(ctx, "read the keyring of "+entity, err)
	}
	if clientKey(keyring, entity) != entries[0].Key {
		return clientRecord{}, fmt.Errorf("the auth entry of %s changed while it was read", entity)
	}
	caps := entries[0].Caps
	if caps == nil {
		caps = map[string]string{}
	}
	return clientRecord{exists: true, key: entries[0].Key, caps: caps, keyring: keyring}, nil
}

// withKey returns the keyring of record with key in place of its key.
func (r clientRecord) withKey(entity, key string) ([]byte, error) {
	var lines []string
	section, replaced := "", 0
	for _, line := range strings.Split(string(r.keyring), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") {
			section = trimmed
		}
		if name, _, ok := strings.Cut(trimmed, "="); ok && section == "["+entity+"]" && strings.TrimSpace(name) == "key" {
			line = "\tkey = " + key
			replaced++
		}
		lines = append(lines, line)
	}
	if replaced != 1 {
		return nil, fmt.Errorf("the keyring of %s has %d keys", entity, replaced)
	}
	return []byte(strings.Join(lines, "\n")), nil
}

// keyCipher returns the type of a Cephx key, which its encoding starts with.
func keyCipher(key string) (CephxCipher, error) {
	secret, err := base64.StdEncoding.DecodeString(key)
	if err != nil || len(secret) < 2 {
		return "", errors.New("undecodable Cephx key")
	}
	switch binary.LittleEndian.Uint16(secret) {
	case 1:
		return CipherAES, nil
	case 2:
		return CipherAES256K, nil
	}
	return "", fmt.Errorf("unknown Cephx key type %d", binary.LittleEndian.Uint16(secret))
}

// settleClient makes sure that nothing an earlier call left for the client
// entity can still change its auth entry, and resolves what it changed. The
// caller holds c.mu.
func (c *Container) settleClient(ctx context.Context, entity string) error {
	c.removeStrayAuthInputs(ctx)
	fence := c.authRecords.fence(entity)
	if fence == nil {
		return nil
	}
	if err := c.stopEarlierAuthCommands(ctx, entity); err != nil {
		return err
	}
	if len(fence.clients) > 0 {
		if err := c.settleMonitors(ctx, fence.clients); err != nil {
			return fmt.Errorf("an earlier call left a MON command for %s that may still apply, and the MONs cannot be settled yet: %w", entity, err)
		}
		_, _ = c.rotationCommand(ctx, append([]string{"rm", "-f"}, fence.clients...)...)
		fence.clients = nil
	}
	if transition := fence.transition; transition != nil {
		record, err := c.readClientRecord(ctx, entity)
		if err != nil {
			return err
		}
		fence.transition = nil
		switch {
		case record.equal(transition.to):
			transition.applied()
		case !record.equal(transition.from):
			c.authRecords.dropFence(entity)
			return fmt.Errorf("the auth entry of %s changed outside this module while an earlier change was uncertain; inspect ceph auth get %s", entity, entity)
		}
	}
	c.authRecords.dropFence(entity)
	return nil
}

// changeClient sends one auth command that changes the auth entry of entity
// from one record to another, settles it and reads the outcome. It returns
// true once the MONs show to. An outcome that stays uncertain is recorded in
// the entity's fence with applied, which runs once a later call finds to;
// the error then says so. The caller holds c.mu.
func (c *Container) changeClient(ctx context.Context, entity string, command map[string]any, input []byte, from, to clientRecord, applied func()) (bool, error) {
	prefix, _ := command["prefix"].(string)
	operation := prefix + " " + entity
	call := &authCall{c: c, entity: entity}
	_, sendErr := call.send(ctx, command, input)
	cause := errors.New(operation + " answered")
	if sendErr != nil {
		// Native errors of auth commands may hold keys.
		cause = clientOperationError(ctx, operation+" failed", sendErr)
	}
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), keyRotationCleanupTimeout)
	defer cancel()
	err := call.settle(settleCtx)
	var record clientRecord
	if err == nil {
		record, err = c.readClientRecord(settleCtx, entity)
	}
	if err != nil {
		c.authRecords.fenceFor(entity).transition = &clientTransition{from: from, to: to, applied: applied}
		return false, errors.Join(cause, fmt.Errorf("whether it applied is not known yet; the next operation on %s settles that first: %w", entity, err))
	}
	switch {
	case record.equal(to):
		applied()
		return true, nil
	case record.equal(from) && sendErr != nil:
		return false, fmt.Errorf("%w; the MONs show that it did not apply, and it no longer can", cause)
	case record.equal(from):
		return false, fmt.Errorf("%s: the MONs answered without applying it", operation)
	}
	return false, fmt.Errorf("%s: the auth entry changed in an unexpected way; inspect ceph auth get %s", operation, entity)
}

// replaceClientKey implements ClientConfig.ReplaceKey, which documents the behavior.
func (c *Container) replaceClientKey(ctx context.Context, client *ClientConfig, opts ...ClientOption) (*ClientConfig, error) {
	settings := clientOptions{}
	for _, opt := range opts {
		if err := opt(&settings); err != nil {
			return nil, err
		}
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
	if err := c.settleClient(ctx, client.name); err != nil {
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
	keyType := settings.keyType
	if keyType == "" {
		if keyType, err = keyCipher(current.key); err != nil {
			return nil, fmt.Errorf("cephx identity %s: %w", client.name, err)
		}
	}
	generate, err := c.generateKeyArgs(keyType)
	if err != nil {
		return nil, err
	}
	secret, err := c.rotationCommand(ctx, generate...)
	key := strings.TrimSpace(string(secret))
	if err != nil || key == "" {
		return nil, clientOperationError(ctx, "generate fresh Cephx key: command failed", err)
	}
	c.authRecords.addSecret(current.key)
	c.authRecords.addSecret(key)
	keyring, err := current.withKey(client.name, key)
	if err != nil {
		return nil, err
	}
	target := current
	target.key, target.keyring = key, keyring
	_, err = c.changeClient(ctx, client.name, map[string]any{"prefix": "auth import"}, keyring, current, target, func() {
		// A descriptor is complete when it is returned, because its
		// ConnectionConfig reads it without the cluster's lock.
		client.replacedBy = &ClientConfig{owner: c, name: client.name, config: bytes.Clone(client.config),
			keyring: bytes.Clone(keyring), created: true, ready: true, tenant: client.tenant}
		c.authRecords.confirmKey(client.name, key)
	})
	switch {
	case err == nil:
		return client.replacedBy, nil
	case c.authRecords.fence(client.name) != nil:
		// The attempted key, which ConnectionConfig does not return, and
		// which no operation accepts.
		return &ClientConfig{owner: c, name: client.name, config: bytes.Clone(client.config),
			keyring: []byte("[" + client.name + "]\nkey = " + key + "\n"), tenant: client.tenant}, err
	}
	return nil, err
}

// ownedClientRecord settles the identity of an active descriptor of this
// cluster and returns its auth entry, which must have the descriptor's key.
// operation names the caller in errors. The caller holds c.mu.
func (c *Container) ownedClientRecord(ctx context.Context, client *ClientConfig, operation string) (clientRecord, error) {
	if c.closed {
		return clientRecord{}, errClusterTerminated
	}
	if client == nil || client.owner != c || !client.created || !client.ready || client.revoked {
		return clientRecord{}, errors.New("cephx client must be an active identity created by this cluster")
	}
	if err := c.settleClient(ctx, client.name); err != nil {
		return clientRecord{}, err
	}
	if err := c.checkCurrentClient(client); err != nil {
		return clientRecord{}, err
	}
	current, err := c.readClientRecord(ctx, client.name)
	if err != nil {
		return clientRecord{}, err
	}
	if !current.exists || current.key != clientKey(client.keyring, client.name) {
		return clientRecord{}, invalid(fmt.Errorf("cephx identity %s has a different key; refusing %s", client.name, operation))
	}
	return current, nil
}

// checkCurrentClient refuses an active descriptor whose key is no longer
// the identity's, or whose identity has a change that is not resolved yet.
// The caller holds c.mu and has settled the identity when it could.
func (c *Container) checkCurrentClient(client *ClientConfig) error {
	if client.replacedBy != nil {
		return invalid(fmt.Errorf("cephx client %s has a new key; ClientConfig.ReplaceKey with this descriptor returns the current one", client.name))
	}
	if c.authRecords.fence(client.name) != nil {
		return invalid(fmt.Errorf("an earlier change of cephx client %s is not resolved yet; a call that can run commands, such as ClientConfig.Capabilities, resolves it", client.name))
	}
	if c.authRecords.revocation(client.name) != nil {
		return invalid(fmt.Errorf("cephx client %s is revoked temporarily; Restore its revocation first", client.name))
	}
	if key, ok := c.authRecords.confirmedKey(client.name); !ok || key != clientKey(client.keyring, client.name) {
		return invalid(fmt.Errorf("cephx client %s is stale: the identity has another key or was deleted", client.name))
	}
	return nil
}
