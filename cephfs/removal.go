package cephfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	iofs "io/fs"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// removeFilesystem deletes one owned filesystem, its MDS daemons and Cephx keys,
// and the pools its configuration created. Pools attached later with
// AddDataPool are detached by the native removal but kept. A retry after an
// error continues with whatever remains.
func removeFilesystem(ctx context.Context, c *ceph.Container, handle *Filesystem) error {
	name := handle.FilesystemName
	release, err := lockSetupThenTopology(ctx, c)
	if err != nil {
		return err
	}
	defer release()
	defer servicehook.Unlock(c)
	if err := servicehook.PoolPolicyReady(c); err != nil {
		return err
	}
	fs := filesystems(c).byName[name]
	if fs == nil || fs != handle {
		return servicehook.Invalid(fmt.Errorf("cephfs filesystem %q is not owned by this cluster, or this descriptor is stale", name))
	}
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(c))
	defer cancel()
	present, err := filesystemExists(ctx, c, name)
	if err != nil {
		return err
	}
	if present {
		// Gracefully drain only this filesystem. Older monitors can refuse
		// fs fail because an unrelated filesystem's MDS has a warning.
		if err := drainFilesystemForRemoval(ctx, c, fs); err != nil {
			return fmt.Errorf("drain cephfs %q: %w", name, err)
		}
		if _, err := c.Ceph(ctx, "fs", "rm", name, "--yes-i-really-mean-it"); err != nil {
			return fmt.Errorf("remove cephfs %q: %w", name, err)
		}
	}
	for _, daemon := range fs.roster.snapshot() {
		if daemon.Container != nil {
			if err := daemon.Terminate(ctx); err != nil && !servicehook.OnlyMissingHostResource(err) {
				return fmt.Errorf("terminate mds.%s of removed cephfs %q: %w", daemon.ID, name, err)
			}
			service := "mds." + daemon.ID
			if owned := servicehook.Service(c, service); owned != nil && owned.GetContainerID() == daemon.GetContainerID() {
				servicehook.DeleteService(c, service)
			}
		}
		if _, err := c.Ceph(ctx, "auth", "del", "mds."+daemon.ID); err != nil && !errors.Is(err, iofs.ErrNotExist) && !strings.Contains(err.Error(), "does not exist") {
			return fmt.Errorf("remove key of mds.%s: %w", daemon.ID, err)
		}
		fs.roster.remove(daemon)
	}
	fs.Container = nil
	for _, pool := range cephFSConfiguredPools(fs.config) {
		if err := servicehook.RemovePool(ctx, c, pool.Name, true); err != nil && !servicehook.PoolMissing(err) {
			return fmt.Errorf("remove pool %q of removed cephfs %q: %w", pool.Name, name, err)
		}
	}
	delete(filesystems(c).byName, name)
	return nil
}

func filesystemExists(ctx context.Context, c *ceph.Container, name string) (bool, error) {
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
