package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
)

// RotateDaemonKey replaces the Cephx key of a running MGR or OSD that the
// cluster owns, as cephadm's daemon key rotation does, without a restart:
//
//  1. auth get-or-create-pending creates the new key on the MONs.
//  2. The daemon's keyring files in its container get the new key, so that a
//     later restart authenticates with it.
//  3. ceph tell <daemon> rotate-key hands the key to the running daemon.
//  4. auth commit-pending makes it the entity's only key.
//
// daemon is "mgr.<name>" or "osd.<id>". MONs share the mon. key and are
// refused. A failure before step 3 restores the keyring files and clears the
// pending key. A failure in step 3 or 4 leaves the daemon holding the pending
// key, which the MONs accept; inspect auth get and retry the commit or
// terminate the cluster.
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

	out, err := c.Ceph(ctx, "auth", "get-or-create-pending", daemon, "--format", "json")
	if err != nil {
		return fmt.Errorf("create pending key of %s: %w", daemon, err)
	}
	var pending []struct {
		Key        string `json:"key"`
		PendingKey string `json:"pending_key"`
	}
	if err := json.Unmarshal(out, &pending); err != nil || len(pending) != 1 || pending[0].Key == "" || pending[0].PendingKey == "" {
		return errors.Join(fmt.Errorf("pending key of %s has an unexpected form", daemon), c.clearPendingKey(ctx, daemon))
	}
	current, next := pending[0].Key, pending[0].PendingKey
	if err := writeDaemonKeyrings(ctx, ctr, daemon, next, keyrings); err != nil {
		return errors.Join(err, writeDaemonKeyrings(ctx, ctr, daemon, current, keyrings), c.clearPendingKey(ctx, daemon))
	}
	control := c.cliContainer()
	keyPath := "/tmp/tc-rotate-" + uuid.NewString() + ".key"
	if err := control.CopyToContainer(ctx, []byte(next), keyPath, 0o600); err != nil {
		return errors.Join(fmt.Errorf("copy the pending key of %s: %w", daemon, err),
			writeDaemonKeyrings(ctx, ctr, daemon, current, keyrings), c.clearPendingKey(ctx, daemon))
	}
	_, tellErr := c.Ceph(ctx, "tell", daemon, "rotate-key", "-i", keyPath)
	_, removeErr := command(ctx, control, "rm", "-f", keyPath)
	if tellErr != nil {
		return errors.Join(fmt.Errorf("hand the pending key to %s: %w", daemon, tellErr), removeErr)
	}
	if _, err := c.Ceph(ctx, "auth", "commit-pending", daemon); err != nil {
		return errors.Join(fmt.Errorf("commit the pending key of %s: %w", daemon, err), removeErr)
	}
	return removeErr
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

func writeDaemonKeyrings(ctx context.Context, ctr testcontainers.Container, daemon, key string, paths []string) error {
	keyring := []byte("[" + daemon + "]\n\tkey = " + strings.TrimSpace(key) + "\n")
	for _, path := range paths {
		if err := ctr.CopyToContainer(ctx, keyring, path, 0o600); err != nil {
			return fmt.Errorf("write %s keyring %s: %w", daemon, path, err)
		}
	}
	return nil
}

func (c *Container) clearPendingKey(ctx context.Context, daemon string) error {
	if _, err := c.Ceph(ctx, "auth", "clear-pending", daemon); err != nil {
		return fmt.Errorf("clear the pending key of %s: %w", daemon, err)
	}
	return nil
}
