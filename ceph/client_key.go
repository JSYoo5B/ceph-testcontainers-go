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

// readRecord reads the auth entry of entity, which must not change
// meanwhile. The caller holds c.mu.
func (as authStore) readRecord(ctx context.Context, entity string) (clientRecord, error) {
	result, err := as.conn.rotationResult(ctx, 1<<20, 64<<10, "ceph", "--connect-timeout", "5", "auth", "get", entity, "--format", "json")
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
	keyring, err := as.conn.rotationCommand(ctx, "ceph", "--connect-timeout", "5", "auth", "get", entity)
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
func (l authLedger) settleClient(ctx context.Context, entity string) error {
	l.removeStrayAuthInputs(ctx)
	fence := l.authRecords.fence(entity)
	if fence == nil {
		return nil
	}
	if err := l.stopEarlierAuthCommands(ctx, entity); err != nil {
		return err
	}
	if len(fence.clients) > 0 {
		if err := l.monitorSettling().settle(ctx, fence.clients); err != nil {
			return fmt.Errorf("an earlier call left a MON command for %s that may still apply, and the MONs cannot be settled yet: %w", entity, err)
		}
		_, _ = l.conn.rotationCommand(ctx, append([]string{"rm", "-f"}, fence.clients...)...)
		fence.clients = nil
	}
	if transition := fence.transition; transition != nil {
		record, err := authStore{l.conn}.readRecord(ctx, entity)
		if err != nil {
			return err
		}
		fence.transition = nil
		switch {
		case record.equal(transition.to):
			transition.applied()
		case !record.equal(transition.from):
			l.authRecords.dropFence(entity)
			return fmt.Errorf("the auth entry of %s changed outside this module while an earlier change was uncertain; inspect ceph auth get %s", entity, entity)
		}
	}
	l.authRecords.dropFence(entity)
	return nil
}

// changeClient sends one auth command that changes the auth entry of entity
// from one record to another, settles it and reads the outcome. It returns
// true once the MONs show to. An outcome that stays uncertain is recorded in
// the entity's fence with applied, which runs once a later call finds to;
// the error then says so. The caller holds c.mu.
func (l authLedger) changeClient(ctx context.Context, entity string, command map[string]any, input []byte, from, to clientRecord, applied func()) (bool, error) {
	prefix, _ := command["prefix"].(string)
	operation := prefix + " " + entity
	call := &authCall{l: l, entity: entity}
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
		record, err = authStore{l.conn}.readRecord(settleCtx, entity)
	}
	if err != nil {
		l.authRecords.fenceFor(entity).transition = &clientTransition{from: from, to: to, applied: applied}
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

// ownedClientRecord admits an active descriptor of this cluster and returns
// its identity's settled auth entry; see authLedger.currentClientRecord.
// operation names the caller in errors. The caller holds c.mu.
func (c *Container) ownedClientRecord(ctx context.Context, client *ClientConfig, operation string) (clientRecord, error) {
	if c.closed {
		return clientRecord{}, errClusterTerminated
	}
	if client == nil || client.owner != c || !client.created || !client.ready || client.revoked {
		return clientRecord{}, errors.New("cephx client must be an active identity created by this cluster")
	}
	return c.authLedger().currentClientRecord(ctx, client, operation)
}

// currentClientRecord settles the identity of an active descriptor and
// returns its auth entry, which must have the descriptor's key. operation
// names the caller in errors. The caller holds c.mu.
func (l authLedger) currentClientRecord(ctx context.Context, client *ClientConfig, operation string) (clientRecord, error) {
	if err := l.settleClient(ctx, client.name); err != nil {
		return clientRecord{}, err
	}
	if err := l.checkCurrentClient(client); err != nil {
		return clientRecord{}, err
	}
	current, err := authStore{l.conn}.readRecord(ctx, client.name)
	if err != nil {
		return clientRecord{}, err
	}
	if !current.exists || current.key != clientKey(client.keyring, client.name) {
		return clientRecord{}, invalid(fmt.Errorf("cephx identity %s has a different key; refusing %s", client.name, operation))
	}
	return current, nil
}

// replaceClientKey imports key in place of the key of client's identity,
// whose settled auth entry is current, keeping its capabilities. It returns
// the new descriptor once the MONs hold key. When the outcome stays
// uncertain it returns an unusable descriptor of the attempted key with the
// error, and the next call settles first. The caller holds c.mu.
func (l authLedger) replaceClientKey(ctx context.Context, client *ClientConfig, current clientRecord, key string) (*ClientConfig, error) {
	l.authRecords.addSecret(current.key)
	l.authRecords.addSecret(key)
	keyring, err := current.withKey(client.name, key)
	if err != nil {
		return nil, err
	}
	target := current
	target.key, target.keyring = key, keyring
	_, err = l.changeClient(ctx, client.name, map[string]any{"prefix": "auth import"}, keyring, current, target, func() {
		// A descriptor is complete when it is returned, because its
		// ConnectionConfig reads it without the cluster's lock.
		client.replacedBy = &ClientConfig{owner: client.owner, name: client.name, config: bytes.Clone(client.config),
			keyring: bytes.Clone(keyring), created: true, ready: true}
		l.authRecords.confirmKey(client.name, key)
	})
	switch {
	case err == nil:
		return client.replacedBy, nil
	case l.authRecords.fence(client.name) != nil:
		// The attempted key, which ConnectionConfig does not return, and
		// which no operation accepts.
		return &ClientConfig{owner: client.owner, name: client.name, config: bytes.Clone(client.config),
			keyring: []byte("[" + client.name + "]\nkey = " + key + "\n")}, err
	}
	return nil, err
}

// revokeClient removes client's identity, whose settled auth entry is
// current, until restoreClient puts it back. It returns the revocation when
// the removal applied, and also, with the error, when its outcome stays
// uncertain; otherwise the identity is not revoked. The caller holds c.mu.
func (l authLedger) revokeClient(ctx context.Context, client *ClientConfig, current clientRecord) (*clientRevocationState, error) {
	state := &clientRevocationState{client: client, original: current}
	l.authRecords.setRevocation(client.name, state)
	_, err := l.changeClient(ctx, client.name, map[string]any{"prefix": "auth rm", "entity": client.name}, nil, current, clientRecord{}, func() {})
	switch {
	case err == nil:
		return state, nil
	case l.authRecords.fence(client.name) != nil:
		return state, err
	}
	l.authRecords.dropRevocation(client.name)
	return nil, err
}

// restoreClient puts back the original auth entry of a revocation. It
// settles first, succeeds when the identity already holds the original, and
// refuses an identity deleted for good or created again otherwise. The caller
// holds c.mu.
func (l authLedger) restoreClient(ctx context.Context, state *clientRevocationState) error {
	if err := l.settleClient(ctx, state.client.name); err != nil {
		return err
	}
	// Settling may have confirmed a permanent removal.
	if state.deleted {
		return fmt.Errorf("cephx client %s was deleted for good", state.client.name)
	}
	current, err := authStore{l.conn}.readRecord(ctx, state.client.name)
	if err != nil {
		return err
	}
	finish := func() {
		state.restored = true
		l.authRecords.dropRevocationIf(state.client.name, state)
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
	_, err = l.changeClient(ctx, state.client.name, map[string]any{"prefix": "auth add", "entity": state.client.name}, state.original.keyring, current, state.original, finish)
	return err
}

// deleteClient removes client's identity for good, also when it is revoked
// temporarily with the descriptor's key or its creation failed after auth
// add applied. It refuses an identity with another key, or one that was
// created again after its revocation. An earlier removal that turns out to
// have applied makes it a no-op. The caller holds c.mu and has admitted a
// created, not revoked descriptor.
func (l authLedger) deleteClient(ctx context.Context, client *ClientConfig) error {
	if err := l.settleClient(ctx, client.name); err != nil {
		return err
	}
	if _, ok := l.authRecords.confirmedKey(client.name); client.revoked || !ok {
		// An earlier removal, maybe through a copy of the descriptor,
		// turned out to have applied.
		client.revoked = true
		return nil
	}
	// The revocation belongs to the identity, whichever descriptor of the
	// revoked key ClientConfig.Delete gets.
	if revocation := l.authRecords.revocation(client.name); revocation != nil && client.replacedBy == nil && clientKey(client.keyring, client.name) == revocation.original.key {
		// A temporary revocation becomes permanent.
		deleted := func() {
			revocation.deleted = true
			l.authRecords.dropRevocation(client.name)
			l.clientDeleted(client)
		}
		current, err := authStore{l.conn}.readRecord(ctx, client.name)
		switch {
		case err != nil:
			return err
		case !current.exists:
			deleted()
			return nil
		case !current.equal(revocation.original):
			return invalid(fmt.Errorf("cephx identity %s was created again with another key or other capabilities; refusing revocation", client.name))
		}
		_, err = l.changeClient(ctx, client.name, map[string]any{"prefix": "auth rm", "entity": client.name}, nil, current, clientRecord{}, deleted)
		return err
	}
	// A creation that failed after auth add applied can be revoked as well.
	if err := l.checkCurrentClient(client); err != nil {
		return err
	}
	current, err := authStore{l.conn}.readRecord(ctx, client.name)
	if err != nil {
		return err
	}
	if !current.exists || current.key != clientKey(client.keyring, client.name) {
		return invalid(fmt.Errorf("cephx identity %s has a different key; refusing revocation", client.name))
	}
	_, err = l.changeClient(ctx, client.name, map[string]any{"prefix": "auth rm", "entity": client.name}, nil, current, clientRecord{}, func() {
		l.clientDeleted(client)
	})
	return err
}

// clientDeleted records that client's identity is gone: the descriptor is
// revoked and the identity has no confirmed key. The caller holds c.mu.
func (l authLedger) clientDeleted(client *ClientConfig) {
	client.revoked = true
	l.authRecords.confirmKey(client.name, "")
}

// checkCurrentClient refuses an active descriptor whose key is no longer
// the identity's, or whose identity has a change that is not resolved yet.
// The caller holds c.mu and has settled the identity when it could.
func (l authLedger) checkCurrentClient(client *ClientConfig) error {
	if client.replacedBy != nil {
		return invalid(fmt.Errorf("cephx client %s has a new key; ClientConfig.ReplaceKey with this descriptor returns the current one", client.name))
	}
	if l.authRecords.fence(client.name) != nil {
		return invalid(fmt.Errorf("an earlier change of cephx client %s is not resolved yet; a call that can run commands, such as ClientConfig.Capabilities, resolves it", client.name))
	}
	if l.authRecords.revocation(client.name) != nil {
		return invalid(fmt.Errorf("cephx client %s is revoked temporarily; Restore its revocation first", client.name))
	}
	if key, ok := l.authRecords.confirmedKey(client.name); !ok || key != clientKey(client.keyring, client.name) {
		return invalid(fmt.Errorf("cephx client %s is stale: the identity has another key or was deleted", client.name))
	}
	return nil
}
