package ceph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
)

// keyRotationCleanupTimeout bounds the rollback of a rotation that failed
// before the daemon received the new key, independently of the caller's
// context, which may be the reason for the failure.
const keyRotationCleanupTimeout = 30 * time.Second

// RotateDaemonKey replaces the Cephx key of a running MGR or OSD that the
// cluster owns, as cephadm's daemon key rotation does, without a restart:
//
//  1. auth get-or-create-pending creates the new key on the MONs, which then
//     accept both keys.
//  2. The daemon's keyring files in its container get the new key, so that a
//     later restart authenticates with it.
//  3. ceph tell <daemon> rotate-key hands the key to the running daemon.
//  4. auth commit-pending makes it the entity's only key.
//
// daemon is "mgr.<name>" or "osd.<id>". MONs share the mon. key and are
// refused.
//
// A failure before step 3 rolls back: the keyring files get the old key
// again, and the pending key is cleared only after every file is read back
// with the old key. When that cannot be confirmed, the pending key stays and
// the error says so. A failed step 3 does not prove whether the daemon
// received the key, and a failed step 4 leaves a daemon that holds it; in
// both cases both keys stay valid and the keyring files hold the new key.
// Calling RotateDaemonKey again finishes such a rotation with the same pending
// key; a resumed call never clears it.
func (c *Container) RotateDaemonKey(ctx context.Context, daemon string) error {
	kind, id, err := parseAdminSocketDaemon(daemon)
	if err != nil {
		return err
	}
	if kind == "mon" {
		return errors.New("MONs share the mon. key; RotateDaemonKey rotates MGR and OSD keys")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("ceph cluster is terminated")
	}
	if !c.ownsDaemon(kind, id) {
		return fmt.Errorf("daemon %s is not owned by this cluster", daemon)
	}
	ctr, keyrings := c.daemonKeyrings(kind, id)
	state, err := ctr.State(ctx)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", daemon, err)
	}
	if !state.Running || state.Paused {
		return fmt.Errorf("daemon %s must be running to receive a new key", daemon)
	}

	before, err := c.authKeys(ctx, "get", daemon)
	if err != nil {
		return err
	}
	// A pending key from an earlier call may already be in the daemon or its
	// keyring files, so a resumed rotation must not take it away.
	resumed := before.PendingKey != ""
	keys, err := c.authKeys(ctx, "get-or-create-pending", daemon)
	if err != nil {
		return err
	}
	if keys.PendingKey == "" || keys.Key != before.Key || (resumed && keys.PendingKey != before.PendingKey) {
		return fmt.Errorf("the keys of %s changed during the rotation; inspect ceph auth get %s", daemon, daemon)
	}
	current, next := keys.Key, keys.PendingKey
	abandon := func(cause error) error {
		if resumed {
			return fmt.Errorf("%w; the pending key of %s from an earlier rotation stays valid, call RotateDaemonKey again", cause, daemon)
		}
		return errors.Join(cause, c.abandonPendingKey(ctx, ctr, daemon, current, keyrings))
	}
	if err := writeDaemonKeyrings(ctx, ctr, daemon, next, keyrings); err != nil {
		return abandon(err)
	}
	control := c.cliContainer()
	keyPath := "/tmp/tc-rotate-" + uuid.NewString() + ".key"
	if err := control.CopyToContainer(ctx, []byte(next), keyPath, 0o600); err != nil {
		return abandon(fmt.Errorf("copy the new key of %s: %w", daemon, err))
	}
	_, tellErr := c.Ceph(ctx, "tell", daemon, "rotate-key", "-i", keyPath)
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), keyRotationCleanupTimeout)
	defer cleanupCancel()
	_, removeErr := command(cleanupCtx, control, "rm", "-f", keyPath)
	if tellErr != nil {
		return errors.Join(fmt.Errorf("hand the new key to %s: %w; it is unknown whether the daemon holds it, so the MONs keep accepting both keys and its keyring files hold the new key. Call RotateDaemonKey again to finish with the same key, or restart %s and then run ceph auth commit-pending", daemon, tellErr, daemon), removeErr)
	}
	if _, err := c.Ceph(ctx, "auth", "commit-pending", daemon); err != nil {
		return errors.Join(fmt.Errorf("commit the new key of %s, which the daemon holds: %w; the MONs keep accepting both keys. Call RotateDaemonKey again or run ceph auth commit-pending %s", daemon, err, daemon), removeErr)
	}
	return removeErr
}

// authKeys runs ceph auth <verb> daemon and returns its key and pending key.
func (c *Container) authKeys(ctx context.Context, verb, daemon string) (struct{ Key, PendingKey string }, error) {
	var result struct{ Key, PendingKey string }
	out, err := c.Ceph(ctx, "auth", verb, daemon, "--format", "json")
	if err != nil {
		return result, fmt.Errorf("auth %s %s: %w", verb, daemon, err)
	}
	var entries []struct {
		Key        string `json:"key"`
		PendingKey string `json:"pending_key"`
	}
	if err := json.Unmarshal(out, &entries); err != nil || len(entries) != 1 || entries[0].Key == "" {
		return result, fmt.Errorf("auth %s %s returned an unexpected form", verb, daemon)
	}
	result.Key, result.PendingKey = entries[0].Key, entries[0].PendingKey
	return result, nil
}

// abandonPendingKey undoes a rotation that never reached the daemon. It uses
// its own bounded context, restores the old key into every keyring file and
// clears the pending key only once every file reads back with the old key;
// otherwise the pending key stays, so that whatever the files hold is still
// accepted.
func (c *Container) abandonPendingKey(ctx context.Context, ctr testcontainers.Container, daemon, current string, paths []string) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), keyRotationCleanupTimeout)
	defer cancel()
	if err := restoreDaemonKeyrings(cleanupCtx, ctr, daemon, current, paths); err != nil {
		return fmt.Errorf("the keyring files of %s may hold the new key, so its pending key stays valid: %w", daemon, err)
	}
	if _, err := c.Ceph(cleanupCtx, "auth", "clear-pending", daemon); err != nil {
		return fmt.Errorf("the keyring files of %s hold the old key again, but its pending key stays: %w", daemon, err)
	}
	return nil
}

func daemonKeyring(daemon, key string) []byte {
	return []byte("[" + daemon + "]\n\tkey = " + strings.TrimSpace(key) + "\n")
}

func writeDaemonKeyrings(ctx context.Context, ctr testcontainers.Container, daemon, key string, paths []string) error {
	for _, path := range paths {
		if err := ctr.CopyToContainer(ctx, daemonKeyring(daemon, key), path, 0o600); err != nil {
			return fmt.Errorf("write %s keyring %s: %w", daemon, path, err)
		}
	}
	return nil
}

// restoreDaemonKeyrings writes key into every file, also after a failed
// write, and then reads each file back.
func restoreDaemonKeyrings(ctx context.Context, ctr testcontainers.Container, daemon, key string, paths []string) error {
	want := daemonKeyring(daemon, key)
	var errs []error
	for _, path := range paths {
		if err := ctr.CopyToContainer(ctx, want, path, 0o600); err != nil {
			errs = append(errs, fmt.Errorf("restore %s: %w", path, err))
		}
	}
	for _, path := range paths {
		reader, err := ctr.CopyFileFromContainer(ctx, path)
		if err != nil {
			errs = append(errs, fmt.Errorf("read back %s: %w", path, err))
			continue
		}
		got, err := io.ReadAll(io.LimitReader(reader, 1<<16))
		_ = reader.Close()
		if err != nil || !bytes.Equal(got, want) {
			errs = append(errs, fmt.Errorf("%s does not hold the old key", path))
		}
	}
	return errors.Join(errs...)
}

// daemonKeyrings returns the container of an owned daemon and the keyring
// files from which it authenticates when it starts. The caller holds c.mu.
func (c *Container) daemonKeyrings(kind, id string) (testcontainers.Container, []string) {
	if kind == "mgr" {
		return c.managers[id].Container, []string{"/etc/ceph/mgr.keyring", "/var/lib/ceph/mgr/ceph-" + id + "/keyring"}
	}
	n, _ := strconv.Atoi(id)
	return c.osds[n].Container, []string{"/etc/ceph/osd.keyring", "/var/lib/ceph/osd/ceph-" + id + "/keyring"}
}
