package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// removeCephFS deletes one owned filesystem, its MDS daemons and Cephx keys,
// and the pools its configuration created. Pools attached later with
// AddDataPool are detached by the native removal but kept. A retry after an
// error continues with whatever remains.
func (c *Container) removeCephFS(ctx context.Context, name string) error {
	if err := lockTopologyMutex(ctx, &c.cephfsSetupMu); err != nil {
		return err
	}
	defer c.cephfsSetupMu.Unlock()
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return err
	}
	fs := c.filesystems[name]
	if fs == nil {
		return fmt.Errorf("cephfs filesystem %q is not owned by this cluster", name)
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	present, err := c.cephFSExists(ctx, name)
	if err != nil {
		return err
	}
	if present {
		// fs fail stops every rank so fs rm accepts the filesystem; daemons
		// that are already gone are simply recorded as failed.
		if _, err := c.Ceph(ctx, "fs", "fail", name); err != nil {
			return fmt.Errorf("fail cephfs %q: %w", name, err)
		}
		if _, err := c.Ceph(ctx, "fs", "rm", name, "--yes-i-really-mean-it"); err != nil {
			return fmt.Errorf("remove cephfs %q: %w", name, err)
		}
	}
	for _, daemon := range slices.Clone(fs.mdss) {
		if daemon.Container != nil {
			if err := daemon.Terminate(ctx); err != nil && !onlyMissingHostResource(err) {
				return fmt.Errorf("terminate mds.%s of removed cephfs %q: %w", daemon.ID, name, err)
			}
			service := "mds." + daemon.ID
			if owned := c.services[service]; owned != nil && owned.GetContainerID() == daemon.GetContainerID() {
				delete(c.services, service)
			}
		}
		if _, err := c.Ceph(ctx, "auth", "del", "mds."+daemon.ID); err != nil && !strings.Contains(err.Error(), "ENOENT") && !strings.Contains(err.Error(), "does not exist") {
			return fmt.Errorf("remove key of mds.%s: %w", daemon.ID, err)
		}
		fs.mdss = slices.DeleteFunc(fs.mdss, func(owned *MDSContainer) bool { return owned == daemon })
	}
	fs.Container = nil
	for _, pool := range cephFSConfiguredPools(fs.config) {
		if err := c.removePool(ctx, pool.Name, true); err != nil && !errors.Is(err, errPoolMissing) {
			return fmt.Errorf("remove pool %q of removed cephfs %q: %w", pool.Name, name, err)
		}
	}
	delete(c.filesystems, name)
	return nil
}

func (c *Container) cephFSExists(ctx context.Context, name string) (bool, error) {
	data, err := c.Ceph(ctx, "fs", "ls", "--format", "json")
	if err != nil {
		return false, err
	}
	var filesystems []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(data, &filesystems); err != nil {
		return false, fmt.Errorf("decode CephFS filesystems: %w", err)
	}
	return slices.ContainsFunc(filesystems, func(fs struct {
		Name string `json:"name"`
	}) bool {
		return fs.Name == name
	}), nil
}
