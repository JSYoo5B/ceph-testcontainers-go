package ceph

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// CephFSContainer is the metadata server for the cluster's test filesystem.
// The cluster owns its lifetime. Clients connect through WithClient and can use
// libcephfs inside their container without a kernel or FUSE mount on the host.
type CephFSContainer struct {
	testcontainers.Container
	FilesystemName string
	MetadataPool   string
	DataPool       string
}

// StartCephFS creates a test filesystem and starts one metadata server.
// It waits until rank zero is active. The two pools use the cluster's disposable
// test replication settings; this does not configure production CephFS HA.
// A non-nil result returned with an error is owned by the cluster for cleanup.
func (c *Container) StartCephFS(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*CephFSContainer, error) {
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	fs := &CephFSContainer{FilesystemName: "tc-cephfs", MetadataPool: "tc-cephfs-metadata", DataPool: "tc-cephfs-data"}
	for _, pool := range []string{fs.MetadataPool, fs.DataPool} {
		for _, args := range [][]string{
			{"osd", "pool", "create", pool, "8"},
			{"osd", "pool", "set", pool, "pg_autoscale_mode", "off"},
		} {
			if _, err := c.Ceph(ctx, args...); err != nil {
				return nil, fmt.Errorf("create cephfs pool: %w", err)
			}
		}
	}
	if _, err := c.Ceph(ctx, "fs", "new", fs.FilesystemName, fs.MetadataPool, fs.DataPool); err != nil {
		return nil, fmt.Errorf("create cephfs: %w", err)
	}
	keyring, err := c.Ceph(ctx, "auth", "get-or-create", "mds.a", "mon", "allow profile mds", "mgr", "allow profile mds", "osd", "allow rw tag cephfs *=*", "mds", "allow")
	if err != nil {
		return nil, fmt.Errorf("create mds credentials: %w", err)
	}
	moduleOpts := []testcontainers.ContainerCustomizer{
		testcontainers.WithEntrypoint("/bin/sh", "/tc/mds.sh"), testcontainers.WithCmd(),
		testcontainers.WithFiles(scriptFile("mds"), textFile("/etc/ceph/mds.keyring", keyring, 0o600)),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"test", "-S", "/var/run/ceph/ceph-mds.a.asok"}).WithStartupTimeout(c.settings.startupTimeout)),
	}
	moduleOpts = append(moduleOpts, opts...)
	ctr, err := c.startService(ctx, "cephfs", moduleOpts...)
	if ctr != nil {
		fs.Container = ctr
	}
	if err != nil {
		return fs, fmt.Errorf("run cephfs mds: %w", err)
	}
	if err := c.poll(ctx, func() (bool, error) {
		data, err := c.Ceph(ctx, "fs", "get", fs.FilesystemName, "--format", "json")
		if err != nil {
			return false, err
		}
		var status struct {
			MDSMap struct {
				Info map[string]struct {
					Name  string `json:"name"`
					Rank  int    `json:"rank"`
					State string `json:"state"`
				} `json:"info"`
			} `json:"mdsmap"`
		}
		if err := json.Unmarshal(data, &status); err != nil {
			return false, fmt.Errorf("decode cephfs map: %w", err)
		}
		for _, mds := range status.MDSMap.Info {
			if mds.Name == "a" && mds.Rank == 0 && mds.State == "up:active" {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		return fs, fmt.Errorf("wait for active cephfs mds: %w", err)
	}
	return fs, nil
}
