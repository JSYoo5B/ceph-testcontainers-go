package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
)

// MonitorBootstrapAddresses returns the current majority monmap's versioned
// address vectors, suitable for the Ceph mon_host setting. The live cluster
// FSID must match this fixture's original bootstrap configuration. This is a
// bounded native observation, not a network attachment or membership mutation.
func (c *Container) MonitorBootstrapAddresses(ctx context.Context) (string, error) {
	if c == nil {
		return "", errors.New("ceph cluster is unavailable")
	}
	ctx, cancel := c.monitorClientRefreshContext(ctx)
	defer cancel()
	if err := c.lockTopology(ctx); err != nil {
		return "", err
	}
	defer c.mu.Unlock()
	addresses, _, err := c.currentMonitorBootstrapAddresses(ctx)
	return addresses, err
}

// RefreshClientMonitorConfig updates only the explicit client's global mon_host
// entry in /etc/ceph/ceph.conf. The client's global FSID must match this fixture
// and its live quorum. Docker archive copies also work with stopped containers;
// this method never starts, stops, terminates or adopts the client's lifetime.
// Other settings and keyring files are retained. It does not update mirror peer
// attributes, network attachments, or private-section mon_host overrides.
//
// Successful writes remain on partial failure and may be retried with a fresh
// context. External configuration writers must not race this operation: Docker
// has no file compare-and-swap. Callers own clients and their cleanup.
func (c *Container) RefreshClientMonitorConfig(ctx context.Context, client testcontainers.Container) error {
	if c == nil || client == nil || client.GetContainerID() == "" {
		return errors.New("ceph cluster and explicit client container are required")
	}
	ctx, cancel := c.monitorClientRefreshContext(ctx)
	defer cancel()
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	addresses, fsid, err := c.currentMonitorBootstrapAddresses(ctx)
	if err != nil {
		return err
	}
	if err := copyClientMonitorBootstrapConfig(ctx, client, addresses, fsid); err != nil {
		return fmt.Errorf("refresh explicit client monitor configuration: %w", err)
	}
	// The fixture mutex excludes its own topology mutations. Reject native
	// external replacement or membership drift during the archive operation.
	after, afterFSID, err := c.currentMonitorBootstrapAddresses(ctx)
	if err != nil {
		return err
	}
	if afterFSID != fsid || after != addresses {
		return errors.New("ceph quorum changed during client monitor configuration refresh; retry with a fresh context")
	}
	return ctx.Err()
}

func (c *Container) monitorClientRefreshContext(ctx context.Context) (context.Context, context.CancelFunc) {
	limit := time.Minute
	if c.settings.startupTimeout > 0 && c.settings.startupTimeout < limit {
		limit = c.settings.startupTimeout
	}
	return context.WithTimeout(ctx, limit)
}

// Called under c.mu. Never adopts a native FSID or another container's file as
// the original identity when the fixture has no confirmed bootstrap template.
func (c *Container) currentMonitorBootstrapAddresses(ctx context.Context) (string, string, error) {
	if c.closed {
		return "", "", errors.New("ceph cluster is terminated")
	}
	c.configMu.RLock()
	template := bytes.Clone(c.config)
	confirmed := len(c.keyring) != 0
	c.configMu.RUnlock()
	fsid, err := monitorConfigClusterIdentity(template)
	if err != nil || !confirmed {
		return "", "", errors.New("ceph original bootstrap identity is unavailable")
	}
	status, err := c.QuorumStatus(ctx)
	if err != nil {
		return "", "", fmt.Errorf("read current monitor bootstrap: %w", err)
	}
	if status.MonMap.FSID != fsid {
		return "", "", errors.New("ceph native monmap FSID differs from the original cluster")
	}
	addresses, err := monitorBootstrapAddresses(status)
	if err != nil {
		return "", "", err
	}
	return addresses, fsid, ctx.Err()
}

func copyClientMonitorBootstrapConfig(ctx context.Context, client testcontainers.Container, addresses, expectedFSID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	reader, err := client.CopyFileFromContainer(ctx, "/etc/ceph/ceph.conf")
	if err != nil {
		return err
	}
	config, readErr := io.ReadAll(io.LimitReader(reader, monitorConfigMaxBytes+1))
	if err := errors.Join(readErr, reader.Close()); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	fsid, err := monitorConfigClusterIdentity(config)
	if err != nil {
		return err
	}
	if fsid != expectedFSID {
		return errors.New("client configuration belongs to another Ceph cluster")
	}
	updated, err := replaceGlobalMonitorHost(config, addresses)
	if err != nil {
		return err
	}
	if bytes.Equal(config, updated) {
		return nil
	}
	return client.CopyToContainer(ctx, updated, "/etc/ceph/ceph.conf", 0o644)
}

func monitorConfigClusterIdentity(config []byte) (string, error) {
	// Reuse the strict section/include/quote/size validation of the updater.
	if _, err := replaceGlobalMonitorHost(config, "validate-only"); err != nil {
		return "", err
	}
	global, count := false, 0
	var fsid string
	for _, line := range strings.Split(string(config), "\n") {
		comment, _ := monitorConfigComment(line)
		if comment >= 0 {
			line = line[:comment]
		}
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "[") {
			global = strings.TrimSpace(line[1:len(line)-1]) == "global"
			continue
		}
		key, value, hasValue := strings.Cut(line, "=")
		if !hasValue || strings.TrimSpace(key) != "fsid" {
			continue
		}
		count++
		if !global || count != 1 {
			return "", errors.New("client configuration has an ambiguous cluster FSID")
		}
		value = strings.TrimSpace(value)
		if len(value) >= 2 && (value[0] == '\'' || value[0] == '"') && value[len(value)-1] == value[0] {
			value = value[1 : len(value)-1]
		}
		parsed, err := uuid.Parse(value)
		if err != nil || parsed.String() != value || parsed == uuid.Nil {
			return "", errors.New("client configuration has an invalid cluster FSID")
		}
		fsid = value
	}
	if count != 1 {
		return "", errors.New("client configuration needs exactly one explicit global FSID")
	}
	return fsid, nil
}
