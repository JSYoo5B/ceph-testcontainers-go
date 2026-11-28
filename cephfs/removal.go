package cephfs

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/strictjson"
)

// removeFilesystem deletes one owned filesystem, its MDS daemons and Cephx keys,
// and the pools its configuration created. Pools attached later with
// AddDataPool are detached by the native removal but kept. A retry after an
// error continues with whatever remains.
func removeFilesystem(ctx context.Context, c *ceph.Container, handle *Filesystem) error {
	name := handle.base().Name()
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
	survivors := make(map[int64][]string)
	for other, survivor := range filesystems(c).byName {
		if native := survivor.base().Native(); other != name && native != nil {
			survivors[native.ID] = survivor.fleet().ownedNames()
		}
	}
	if fs.fsNewAttempted && present {
		// Gracefully drain only this filesystem. Older monitors can refuse
		// fs fail because an unrelated filesystem's MDS has a warning.
		if err := drainFilesystemForRemoval(ctx, c, fs); err != nil {
			return fmt.Errorf("drain cephfs %q: %w", name, err)
		}
		if _, err := c.Ceph(ctx, "fs", "rm", name, "--yes-i-really-mean-it"); err != nil {
			return fmt.Errorf("remove cephfs %q: %w", name, err)
		}
	}
	fs.fsNewAttempted = false
	// Checked even when the filesystem is already absent: a retry after a
	// failed wait, or after an fs rm whose reply was lost, must still hold
	// the survivors' affinity before the removal is reported as done.
	if err := waitSurvivorAffinity(ctx, c, survivors); err != nil {
		return fmt.Errorf("remove cephfs %q: %w", name, err)
	}
	if err := fs.fleet().removeAll(ctx); err != nil {
		return err
	}
	for len(fs.createdPools) > 0 {
		pool := fs.createdPools[0]
		if err := servicehook.RemovePool(ctx, c, pool); err != nil && !servicehook.PoolMissing(err) {
			return fmt.Errorf("remove pool %q of removed cephfs %q: %w", pool.Name, name, err)
		}
		fs.createdPools = fs.createdPools[1:]
	}
	delete(filesystems(c).byName, name)
	return nil
}

// waitSurvivorAffinity returns once every owned MDS of the other owned
// filesystems that the FSMap lists again carries the FSMap ID of its own
// filesystem as join_fscid. Some monitors clear the join_fscid of every
// daemon of every remaining filesystem when one filesystem is removed; the
// next beacon of each daemon, which names its mds_join_fs, sets it back.
// Until then a standby or standby-replay daemon of a survivor is not bound
// to it, so MDSStatus and readiness would not count it. A daemon the FSMap
// does not list (stopped by the test) is not waited for. survivors maps the
// FSMap ID of each other owned filesystem to its owned daemon names.
func waitSurvivorAffinity(ctx context.Context, c *ceph.Container, survivors map[int64][]string) error {
	if len(survivors) == 0 {
		return nil
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		data, err := c.Ceph(ctx, "fs", "dump", "--format", "json")
		if err != nil {
			return fmt.Errorf("read FSMap after removal: %w", err)
		}
		lost, err := survivorAffinityLost(data, survivors)
		if err != nil {
			return err
		}
		if len(lost) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("MDS affinity of surviving filesystems was not restored (%v): %w", lost, ctx.Err())
		case <-ticker.C:
		}
	}
}

// survivorAffinityLost decodes one FSMap and returns, sorted, the owned
// survivor daemons it lists whose join_fscid is not the FSMap ID of their own
// filesystem. survivors maps a filesystem's FSMap ID to its owned daemon names.
func survivorAffinityLost(data []byte, survivors map[int64][]string) ([]string, error) {
	fsmap, err := fsbase.DecodeFSMap(data)
	if err != nil {
		return nil, fmt.Errorf("decode FSMap after removal: %w", err)
	}
	want := make(map[string]int64)
	for id, names := range survivors {
		for _, name := range names {
			want[name] = id
		}
	}
	var lost []string
	check := func(info fsbase.MDSInfo) {
		if id, ok := want[info.Name]; ok && info.JoinFSCID != id {
			lost = append(lost, fmt.Sprintf("%s join_fscid=%d want %d", info.Name, info.JoinFSCID, id))
		}
	}
	for _, filesystem := range fsmap.Filesystems {
		for _, info := range filesystem.MDSMap.Info {
			check(info)
		}
	}
	for _, info := range fsmap.Standbys {
		check(info)
	}
	slices.Sort(lost)
	return lost, nil
}

func filesystemExists(ctx context.Context, c *ceph.Container, name string) (bool, error) {
	data, err := c.Ceph(ctx, "fs", "ls", "--format", "json")
	if err != nil {
		return false, err
	}
	var filesystems []struct {
		Name string `json:"name"`
	}
	if err := strictjson.Decode(data, &filesystems, strictjson.Options{RejectNull: true}); err != nil {
		return false, fmt.Errorf("decode CephFS filesystems: %w", err)
	}
	// Establish presence or absence only after validating the entire list.
	names := make(map[string]bool, len(filesystems))
	for i, fs := range filesystems {
		if strings.TrimSpace(fs.Name) == "" || names[fs.Name] {
			return false, fmt.Errorf("decode CephFS filesystems: missing, blank or repeated name at entry %d", i)
		}
		names[fs.Name] = true
	}
	return names[name], nil
}
