package subvolume

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/volbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/google/uuid"
)

type cloneIncarnation struct {
	Path      string `json:"path"`
	Inode     uint64 `json:"inode"`
	BirthTime string `json:"birth_time"`
}

type cloneMetadata struct {
	cloneIncarnation
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
func readCloneIncarnation(ctx context.Context, v fsbase.VolumeCommands, identity *cloneRecord) (*cloneIncarnation, error) {
	base := path.Join(path.Dir(path.Dir(path.Dir(identity.source.subvolume.Path))), nativeGroup(identity.group), identity.name)
	control, err := v.Cluster().ClientContainer(ctx)
	if err != nil {
		return nil, fmt.Errorf("select CephFS clone CLI container: %w", err)
	}
	data, err := servicehook.Command(ctx, control, "python3", "-c", cloneIdentityScript, v.Name(), base)
	if err != nil {
		return nil, fmt.Errorf("read native clone incarnation: %w", err)
	}
	var metadata cloneMetadata
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
	if len(metadata.Source) != 0 && (metadata.Source["volume"] != v.Name() || metadata.Source["group"] != source.Group || metadata.Source["subvolume"] != source.Name || metadata.Source["snapshot"] != identity.source.name) {
		return nil, errors.New("native clone metadata source does not match this request")
	}
	if metadata.State != "complete" && len(metadata.Source) == 0 {
		return nil, errors.New("incomplete clone has no native source metadata")
	}
	return &metadata.cloneIncarnation, nil
}

func nativeGroup(group string) string {
	if group == "" {
		return "_nogroup"
	}
	return group
}

func checkCloneIncarnation(ctx context.Context, fs *fsbase.Core, identity *cloneRecord) error {
	if identity.incarnation == nil {
		return errors.New("clone has no submission-confirmed native incarnation; inspect it through Ceph")
	}
	current, err := readCloneIncarnation(ctx, fs.Volumes(), identity)
	if err != nil {
		return err
	}
	if *current != *identity.incarnation {
		return errors.New("native clone UUID, inode or birth time was replaced; refusing mutation")
	}
	return nil
}

func cloneSourceDetached(ctx context.Context, fs *fsbase.Core, fsID int64, identity *cloneRecord) (bool, error) {
	info, err := checkSnapshotIdentity(ctx, fs, fsID, identity.source)
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

// Cancel requests native cancellation of an owned pending or
// in-progress clone, then waits for canceled state and release of this clone's
// source snapshot protection. Its partial target is retained. A completed or
// failed clone is never treated as successfully canceled. Copied handles share
// identity; a retry after a lost reply first reconciles native canceled state.
// The original source snapshot must still exist. Native external clone/source
// edits must not race this operation; pending UUIDs are read from version-checked
// native volumes metadata captured immediately after submission.
//
//purpose:condition
func (clone *Clone) Cancel(ctx context.Context) error {
	fs, err := clone.filesystem()
	if err != nil {
		return err
	}
	if err := validateCloneHandle(fs, clone); err != nil {
		return err
	}
	timeout := servicehook.StartupTimeout(fs.Cluster())
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	request := true
	for {
		finished, err := clone.cancelStep(ctx, fs, request)
		if err != nil {
			return err
		}
		if finished {
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

// cancelStep is one poll of Cancel. It holds the subvolume operation gate only
// for its own step, sends the native cancel on the first poll alone and reports
// whether the clone is canceled with its source detached.
func (clone *Clone) cancelStep(ctx context.Context, fs *fsbase.Core, request bool) (bool, error) {
	ctx, fsID, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return false, err
	}
	defer done()
	identity := clone.identity
	if err := checkCloneIncarnation(ctx, fs, identity); err != nil {
		return false, err
	}
	status, err := cloneStatus(ctx, fs, fsID, identity)
	if err != nil {
		return false, err
	}
	if status.State == "canceled" {
		return cloneSourceDetached(ctx, fs, fsID, identity)
	}
	if status.State != "pending" && status.State != "in-progress" {
		return false, fmt.Errorf("clone is %s; cannot cancel", status.State)
	}
	if _, err := checkSnapshotIdentity(ctx, fs, fsID, identity.source); err != nil {
		return false, err
	}
	if !request {
		return false, nil
	}
	args := fsbase.AppendGroup([]string{"fs", "clone", "cancel", fs.Name(), identity.name}, identity.group)
	if _, err := fs.Cluster().Ceph(ctx, args...); err != nil {
		return false, err
	}
	status, err = cloneStatus(ctx, fs, fsID, identity)
	if err != nil {
		return false, err
	}
	if status.State == "canceled" {
		return cloneSourceDetached(ctx, fs, fsID, identity)
	}
	return false, nil
}

// RemovePartial explicitly removes an owned failed or canceled
// target with native --force. It refuses pending/in-progress/completed clones,
// replaced target incarnations and unresolved source protection. The original
// snapshot must remain present and unchanged. No source data or snapshots are
// removed. Native removal moves the target into asynchronous trash; success
// confirms logical absence, not physical purge completion. A failed command
// retains shared retry state so an unknown successful removal can be reconciled
// without deleting a subsequently recreated same-named target.
//
//purpose:lifetime
func (clone *Clone) RemovePartial(ctx context.Context) error {
	fs, err := clone.filesystem()
	if err != nil {
		return err
	}
	if clone == nil || clone.identity == nil || clone.identity.filesystem != fs || !clone.identity.submitted {
		return errors.New("partial clone must have been submitted by this filesystem")
	}
	ctx, fsID, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return err
	}
	defer done()
	identity := clone.identity
	if identity.removed {
		return nil
	}
	names, err := fs.Volumes().SubvolumeNames(ctx, identity.group)
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
	if err := checkCloneIncarnation(ctx, fs, identity); err != nil {
		return err
	}
	status, err := cloneStatus(ctx, fs, fsID, identity)
	if err != nil {
		return err
	}
	if status.State != "failed" && status.State != "canceled" {
		return fmt.Errorf("clone is %s; only failed or canceled targets support partial cleanup", status.State)
	}
	detached, err := cloneSourceDetached(ctx, fs, fsID, identity)
	if err != nil {
		return err
	}
	if !detached {
		return errors.New("clone source snapshot is still protected; await cancellation/worker detachment before cleanup")
	}
	identity.removalAttempted = true
	args := fsbase.AppendGroup([]string{"fs", "subvolume", "rm", fs.Name(), identity.name}, identity.group)
	if _, err := fs.Cluster().Ceph(ctx, append(args, "--force")...); err != nil {
		return err
	}
	names, err = fs.Volumes().SubvolumeNames(ctx, identity.group)
	if err != nil {
		return err
	}
	if slices.Contains(names, identity.name) {
		return errors.New("partial clone removal was not confirmed")
	}
	identity.removed = true
	return nil
}

const cloneIdentityScript = `import cephfs, configparser, json, os, stat, sys, threading
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
