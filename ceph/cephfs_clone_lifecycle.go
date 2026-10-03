package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"slices"
	"time"

	"github.com/google/uuid"
)

type cephFSCloneIncarnation struct {
	Path      string `json:"path"`
	Inode     uint64 `json:"inode"`
	BirthTime string `json:"birth_time"`
}

type cephFSCloneMetadata struct {
	cephFSCloneIncarnation
	Version     int `json:"version"`
	Type, State string
	Source      map[string]string `json:"source"`
}

// Tentacle's volumes v2 metadata persists the clone's UUID path before it is
// queued; CLI info/getpath intentionally refuse incomplete clones. Read this
// native metadata without changing files, through libcephfs inside the control
// image. This private schema is version checked and unknown versions fail
// conservatively. No native libraries or cgo are linked by the Go module.
// Sources: ceph v20.2.4 volumes/fs/operations/versions/{subvolume_v2,
// metadata_manager}.py and volumes/fs/async_cloner.py.
func (fs *CephFSContainer) readCloneIncarnation(ctx context.Context, identity *cephFSCloneIdentity) (*cephFSCloneIncarnation, error) {
	base := path.Join(path.Dir(path.Dir(path.Dir(identity.source.subvolume.path))), cephFSNativeGroup(identity.group), identity.name)
	data, err := command(ctx, fs.cluster.cliContainer(), "python3", "-c", cephFSCloneIdentityScript, fs.config.Name, base)
	if err != nil {
		return nil, fmt.Errorf("read native clone incarnation: %w", err)
	}
	var metadata cephFSCloneMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("decode native clone incarnation: %w", err)
	}
	if metadata.Version != 2 || metadata.Type != "clone" || metadata.Inode == 0 || metadata.BirthTime == "" || path.Dir(metadata.Path) != base {
		return nil, errors.New("unsupported or incomplete native clone incarnation")
	}
	if _, err := uuid.Parse(path.Base(metadata.Path)); err != nil {
		return nil, errors.New("native clone incarnation path is not a UUID")
	}
	if metadata.State != "pending" && metadata.State != "in-progress" && metadata.State != "complete" && metadata.State != "failed" && metadata.State != "canceled" {
		return nil, errors.New("unsupported native clone incarnation state")
	}
	source := identity.source.subvolume
	if len(metadata.Source) != 0 && (metadata.Source["volume"] != fs.config.Name || metadata.Source["group"] != source.group || metadata.Source["subvolume"] != source.name || metadata.Source["snapshot"] != identity.source.name) {
		return nil, errors.New("native clone metadata source does not match this request")
	}
	if metadata.State != "complete" && len(metadata.Source) == 0 {
		return nil, errors.New("incomplete clone has no native source metadata")
	}
	return &metadata.cephFSCloneIncarnation, nil
}

func cephFSNativeGroup(group string) string {
	if group == "" {
		return "_nogroup"
	}
	return group
}

func (fs *CephFSContainer) checkCloneIncarnation(ctx context.Context, identity *cephFSCloneIdentity) error {
	if identity.incarnation == nil {
		return errors.New("clone has no submission-confirmed native incarnation; inspect it through Ceph")
	}
	current, err := fs.readCloneIncarnation(ctx, identity)
	if err != nil {
		return err
	}
	if *current != *identity.incarnation {
		return errors.New("native clone UUID, inode or birth time was replaced; refusing mutation")
	}
	return nil
}

func (fs *CephFSContainer) cloneSourceDetached(ctx context.Context, fsID int64, identity *cephFSCloneIdentity) (bool, error) {
	info, err := fs.checkSnapshotIdentity(ctx, fsID, identity.source)
	if err != nil {
		return false, err
	}
	if info.OrphanClonesCount != 0 {
		return false, errors.New("source snapshot has unresolved orphan clones")
	}
	for _, pending := range info.PendingClones {
		if pending.Name == identity.name && pending.GroupName == identity.group {
			return false, nil
		}
	}
	if info.HasPendingClones && len(info.PendingClones) == 0 {
		return false, errors.New("source snapshot pending clone references are unavailable")
	}
	return true, nil
}

// CancelSubvolumeClone requests native cancellation of an owned pending or
// in-progress clone, then waits for canceled state and release of this clone's
// source snapshot protection. Its partial target is retained. A completed or
// failed clone is never treated as successfully canceled. Copied handles share
// identity; a retry after a lost reply first reconciles native canceled state.
// The original source snapshot must still exist. Native external clone/source
// edits must not race this operation; pending UUIDs are read from version-checked
// native volumes metadata captured immediately after submission.
func (fs *CephFSContainer) CancelSubvolumeClone(ctx context.Context, clone *CephFSSubvolumeClone) error {
	if err := fs.validateCloneHandle(clone); err != nil {
		return err
	}
	fs.cluster.mu.Lock()
	timeout := fs.cluster.settings.startupTimeout
	fs.cluster.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	request := true
	for {
		done, err := fs.cancelSubvolumeCloneStep(ctx, clone.identity, request)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		request = false
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (fs *CephFSContainer) cancelSubvolumeCloneStep(ctx context.Context, identity *cephFSCloneIdentity, request bool) (bool, error) {
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return false, err
	}
	defer done()
	if err := fs.checkCloneIncarnation(ctx, identity); err != nil {
		return false, err
	}
	status, err := fs.subvolumeCloneStatus(ctx, fsID, identity)
	if err != nil {
		return false, err
	}
	if status.State == "canceled" {
		return fs.cloneSourceDetached(ctx, fsID, identity)
	}
	if status.State != "pending" && status.State != "in-progress" {
		return false, fmt.Errorf("clone is %s; cannot cancel", status.State)
	}
	if _, err := fs.checkSnapshotIdentity(ctx, fsID, identity.source); err != nil {
		return false, err
	}
	if request {
		args := appendSubvolumeGroup([]string{"fs", "clone", "cancel", fs.config.Name, identity.name}, identity.group)
		if _, err := fs.cluster.Ceph(ctx, args...); err != nil {
			return false, err
		}
		status, err := fs.subvolumeCloneStatus(ctx, fsID, identity)
		if err != nil {
			return false, err
		}
		if status.State == "canceled" {
			return fs.cloneSourceDetached(ctx, fsID, identity)
		}
	}
	return false, nil
}

// RemovePartialSubvolumeClone explicitly removes an owned failed or canceled
// target with native --force. It refuses pending/in-progress/completed clones,
// replaced target incarnations and unresolved source protection. The original
// snapshot must remain present and unchanged. No source data or snapshots are
// removed. Native removal moves the target into asynchronous trash; success
// confirms logical absence, not physical purge completion. A failed command
// retains shared retry state so an unknown successful removal can be reconciled
// without deleting a subsequently recreated same-named target.
func (fs *CephFSContainer) RemovePartialSubvolumeClone(ctx context.Context, clone *CephFSSubvolumeClone) error {
	if clone == nil || clone.identity == nil || clone.identity.filesystem != fs || !clone.identity.submitted {
		return errors.New("partial clone must have been submitted by this filesystem")
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	identity := clone.identity
	if identity.removed {
		return nil
	}
	names, err := fs.subvolumeNames(ctx, identity.group)
	if err != nil {
		return err
	}
	if !slices.Contains(names, identity.name) {
		if identity.removalAttempted {
			identity.removed = true
			return nil
		}
		return errors.New("partial clone disappeared outside this fixture")
	}
	if err := fs.checkCloneIncarnation(ctx, identity); err != nil {
		return err
	}
	status, err := fs.subvolumeCloneStatus(ctx, fsID, identity)
	if err != nil {
		return err
	}
	if status.State != "failed" && status.State != "canceled" {
		return fmt.Errorf("clone is %s; only failed or canceled targets support partial cleanup", status.State)
	}
	detached, err := fs.cloneSourceDetached(ctx, fsID, identity)
	if err != nil {
		return err
	}
	if !detached {
		return errors.New("clone source snapshot is still protected; await cancellation/worker detachment before cleanup")
	}
	identity.removalAttempted = true
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "rm", fs.config.Name, identity.name}, identity.group)
	if _, err := fs.cluster.Ceph(ctx, append(args, "--force")...); err != nil {
		return err
	}
	names, err = fs.subvolumeNames(ctx, identity.group)
	if err != nil {
		return err
	}
	if slices.Contains(names, identity.name) {
		return errors.New("partial clone removal was not confirmed")
	}
	identity.removed = true
	return nil
}

const cephFSCloneIdentityScript = `import cephfs, configparser, json, os, stat, sys, threading
filesystem, base = sys.argv[1:]
deadline = threading.Timer(30, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '15')
fs.conf_set('rados_osd_op_timeout', '10')
try:
    fs.mount(filesystem_name=filesystem.encode())
    fd = fs.open(base + '/.meta', os.O_RDONLY)
    try:
        content, offset = [], 0
        while True:
            part = fs.read(fd, offset, 4096)
            if not part: break
            content.append(part)
            offset += len(part)
            if offset > 1048576: raise ValueError('clone metadata too large')
    finally:
        fs.close(fd)
    metadata = configparser.ConfigParser()
    metadata.read_string(b''.join(content).decode('utf-8'))
    target = metadata['GLOBAL']['path']
    if os.path.dirname(target) != base: raise ValueError('clone path outside expected target')
    status = fs.statx(target, cephfs.CEPH_STATX_BTIME | cephfs.CEPH_STATX_INO | cephfs.CEPH_STATX_MODE, cephfs.AT_SYMLINK_NOFOLLOW)
    if not stat.S_ISDIR(status['mode']): raise ValueError('clone path is not a directory')
    print(json.dumps({'version':int(metadata['GLOBAL']['version']), 'type':metadata['GLOBAL']['type'],
        'state':metadata['GLOBAL']['state'], 'path':target, 'inode':status['ino'],
        'birth_time':status['btime'].isoformat(),
        'source':dict(metadata['source']) if metadata.has_section('source') else {}}))
finally:
    fs.shutdown()
    deadline.cancel()
`
