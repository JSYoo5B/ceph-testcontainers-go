package cephfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// removalShutdownState uses the explicit up map, not a count of up:active
// daemons: stopping and reconnecting ranks must also prevent removal.
type removalShutdownState struct {
	id     int64
	maxMDS int
	up     map[string]uint64
}

func parseRemovalShutdownState(data []byte, name string) (removalShutdownState, error) {
	var wire struct {
		ID     *int64 `json:"id"`
		MDSMap *struct {
			Name   string             `json:"fs_name"`
			MaxMDS *int               `json:"max_mds"`
			Up     *map[string]uint64 `json:"up"`
		} `json:"mdsmap"`
	}
	if err := json.Unmarshal(data, &wire); err != nil || wire.ID == nil || *wire.ID < 0 || wire.MDSMap == nil ||
		wire.MDSMap.Name != name || wire.MDSMap.MaxMDS == nil || *wire.MDSMap.MaxMDS < 0 || wire.MDSMap.Up == nil {
		return removalShutdownState{}, errors.New("incomplete or invalid filesystem shutdown map")
	}
	for rank, gid := range *wire.MDSMap.Up {
		suffix, found := strings.CutPrefix(rank, "mds_")
		n, err := strconv.Atoi(suffix)
		if !found || err != nil || n < 0 || strconv.Itoa(n) != suffix || gid == 0 {
			return removalShutdownState{}, errors.New("invalid filesystem shutdown rank identity")
		}
	}
	return removalShutdownState{id: *wire.ID, maxMDS: *wire.MDSMap.MaxMDS, up: *wire.MDSMap.Up}, nil
}

// drainFilesystemForRemoval runs under the caller's setup/topology locks, so it
// deliberately does not call the public MDSStatus method (which takes them).
// A failed or cancelled drain leaves ownership intact. It neither forces an
// unhealthy daemon to fail nor changes the filesystem back to up on error.
func drainFilesystemForRemoval(ctx context.Context, c *ceph.Container, fs *Filesystem) error {
	read := func() (removalShutdownState, error) {
		data, err := c.Ceph(ctx, "fs", "get", fs.FilesystemName, "--format", "json")
		if err != nil {
			return removalShutdownState{}, err
		}
		return parseRemovalShutdownState(data, fs.FilesystemName)
	}
	before, err := read()
	if err != nil {
		return fmt.Errorf("read target before shutdown: %w", err)
	}
	if fs.nativeIdentity != nil && before.id != fs.nativeIdentity.id {
		return errors.New("filesystem identity changed before shutdown")
	}
	if _, err := c.Ceph(ctx, "fs", "set", fs.FilesystemName, "down", "true"); err != nil {
		return fmt.Errorf("request graceful shutdown: %w", err)
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("wait for all target ranks to stop: %w", err)
		}
		current, err := read()
		if err != nil {
			return fmt.Errorf("read target during shutdown: %w", err)
		}
		if current.id != before.id {
			return errors.New("filesystem identity changed during shutdown")
		}
		if current.maxMDS != 0 {
			return errors.New("filesystem is no longer marked down")
		}
		if len(current.up) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for all target ranks to stop: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
