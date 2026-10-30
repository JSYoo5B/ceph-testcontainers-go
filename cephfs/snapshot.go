package cephfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// SubvolumeSnapshot is an owned snapshot of a confirmed subvolume. Its
// public fields describe creation; mutations use the captured private identity.
// Snapshot creation does not quiesce concurrent application writers.
type SubvolumeSnapshot struct {
	Name, SubvolumeName, GroupName, FilesystemName, Path string
	identity                                             *cephFSSnapshotIdentity
}

// SnapshotPendingClone identifies a native clone still using a snapshot.
// An empty GroupName selects the default group.
type SnapshotPendingClone struct {
	Name      string `json:"name"`
	GroupName string `json:"target_group"`
}

// SubvolumeSnapshotInfo contains current native snapshot metadata. Path
// identifies the frozen data directory, including Ceph's incarnation UUID.
type SubvolumeSnapshotInfo struct {
	Name, SubvolumeName, GroupName, Path, CreatedAt, DataPool string
	HasPendingClones                                          bool
	PendingClones                                             []SnapshotPendingClone
	OrphanClonesCount                                         int
}

// CloneConfig names a new clone in an existing group. DataPool optionally
// overrides the snapshot's data pool. Ceph copies the snapshot's quota and
// layout; with a DataPool override it clears the inherited RADOS namespace.
// Quota and namespace changes are deliberately not part of the clone request.
type CloneConfig struct {
	Name, GroupName, DataPool string
}

// SubvolumeClone is an asynchronous, owned clone request. SubvolumeClone.Wait
// returns a usable subvolume only after native cloning completes. A non-nil
// descriptor on error records the attempted request but cannot be adopted or
// canceled automatically. Cluster cleanup removes incomplete data too.
type SubvolumeClone struct {
	Name, GroupName, FilesystemName string
	identity                        *cephFSCloneIdentity
}

// SubvolumeCloneStatus reports native pending, in-progress, complete,
// failed or canceled state. FailureErrno is zero when no native failure was
// recorded. Source fields are normally absent after successful completion.
type SubvolumeCloneStatus struct {
	State, SourceFilesystem, SourceSubvolume, SourceGroup, SourceSnapshot string
	FailureErrno                                                          int
	FailureMessage                                                        string
	Progress                                                              map[string]string
}

type cephFSSnapshotIdentity struct {
	subvolume                        *cephFSVolumeIdentity
	name, path, createdAt, dataPool  string
	ready, removed, removalAttempted bool
}

type cephFSCloneIdentity struct {
	filesystem                *Filesystem
	filesystemID              int64
	source                    *cephFSSnapshotIdentity
	name, group, dataPool     string
	submitted                 bool
	subvolume                 *Subvolume
	incarnation               *cephFSCloneIncarnation
	removed, removalAttempted bool
}

func (fs *Filesystem) checkSnapshotSubvolume(ctx context.Context, fsID int64, identity *cephFSVolumeIdentity) error {
	if err := fs.validateVolumeHandle(identity); err != nil {
		return err
	}
	if identity.removed {
		return errors.New("snapshot source subvolume has been removed")
	}
	info, err := fs.id.volumes().subvolumeInfo(ctx, identity.name, identity.group)
	if err != nil {
		return err
	}
	return fs.checkVolumeIdentity(fsID, identity, info.Path, info.CreatedAt)
}

func (v volumeCommands) snapshotNames(ctx context.Context, identity *cephFSVolumeIdentity) ([]string, error) {
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "snapshot", "ls", v.name, identity.name}, identity.group)
	data, err := v.cluster.Ceph(ctx, append(args, "--format", "json")...)
	if err != nil {
		return nil, err
	}
	return decodeCephFSVolumeNames(data)
}

// Snapshots lists native snapshots of an owned subvolume, including
// snapshots made by an external client. Only snapshots returned by this fixture's
// Subvolume.CreateSnapshot can be removed or cloned through an owned descriptor.
//
//purpose:observation
func (subvolume *Subvolume) Snapshots(ctx context.Context) ([]string, error) {
	fs, err := subvolume.filesystem()
	if err != nil {
		return nil, err
	}
	if subvolume == nil {
		return nil, errors.New("subvolume is unavailable")
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := fs.checkSnapshotSubvolume(ctx, fsID, subvolume.identity); err != nil {
		return nil, err
	}
	return fs.id.volumes().snapshotNames(ctx, subvolume.identity)
}

// CreateSnapshot creates a fresh snapshot without modifying an existing
// same-named snapshot. Application writers must be synchronized by the caller
// before taking a consistency-sensitive checkpoint. External CLI mutations must
// not race the native preflight and creation.
//
//purpose:construction
func (subvolume *Subvolume) CreateSnapshot(ctx context.Context, name string) (*SubvolumeSnapshot, error) {
	fs, err := subvolume.filesystem()
	if err != nil {
		return nil, err
	}
	if err := validateCephFSVolumeName(name, false); err != nil {
		return nil, err
	}
	if subvolume == nil {
		return nil, errors.New("subvolume is unavailable")
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := fs.checkSnapshotSubvolume(ctx, fsID, subvolume.identity); err != nil {
		return nil, err
	}
	names, err := fs.id.volumes().snapshotNames(ctx, subvolume.identity)
	if err != nil {
		return nil, err
	}
	if slices.Contains(names, name) {
		return nil, servicehook.Invalid(fmt.Errorf("subvolume snapshot %q already exists", name))
	}
	identity := &cephFSSnapshotIdentity{subvolume: subvolume.identity, name: name}
	snapshot := &SubvolumeSnapshot{Name: name, SubvolumeName: subvolume.identity.name, GroupName: subvolume.identity.group, FilesystemName: fs.id.name, identity: identity}
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "snapshot", "create", fs.id.name, subvolume.identity.name, name}, subvolume.identity.group)
	if _, err := fs.id.cluster.Ceph(ctx, args...); err != nil {
		return snapshot, err
	}
	info, err := fs.id.volumes().subvolumeSnapshotInfo(ctx, identity.subvolume.name, identity.subvolume.group, identity.name)
	if err != nil {
		return snapshot, err
	}
	snapshot.Path = info.Path
	identity.path, identity.createdAt, identity.dataPool, identity.ready = info.Path, info.CreatedAt, info.DataPool, true
	return snapshot, nil
}

// SubvolumeSnapshotInfo queries native metadata for any existing snapshot.
// Reading an external snapshot does not grant mutation ownership.
//
//purpose:observation
func (fs *Filesystem) SubvolumeSnapshotInfo(ctx context.Context, subvolumeName, groupName, snapshotName string) (*SubvolumeSnapshotInfo, error) {
	if err := validateCephFSVolumeName(subvolumeName, false); err != nil {
		return nil, err
	}
	if err := validateCephFSVolumeName(groupName, true); err != nil {
		return nil, err
	}
	if err := validateCephFSVolumeName(snapshotName, false); err != nil {
		return nil, err
	}
	ctx, _, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return fs.id.volumes().subvolumeSnapshotInfo(ctx, subvolumeName, groupName, snapshotName)
}

func decodeCephFSSnapshotInfo(data []byte) (*SubvolumeSnapshotInfo, error) {
	var raw struct {
		CreatedAt         string                 `json:"created_at"`
		DataPool          string                 `json:"data_pool"`
		HasPendingClones  string                 `json:"has_pending_clones"`
		PendingClones     []SnapshotPendingClone `json:"pending_clones"`
		OrphanClonesCount int                    `json:"orphan_clones_count"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("decode subvolume snapshot info: %w", err)
	}
	if raw.CreatedAt == "" || raw.DataPool == "" || (raw.HasPendingClones != "yes" && raw.HasPendingClones != "no") || raw.OrphanClonesCount < 0 {
		return nil, errors.New("decode subvolume snapshot info: missing or invalid creation time, pool or clone state")
	}
	if (raw.HasPendingClones == "yes") != (len(raw.PendingClones) > 0) {
		return nil, errors.New("decode subvolume snapshot info: contradictory pending clone state")
	}
	for i, clone := range raw.PendingClones {
		if validateCephFSVolumeName(clone.Name, false) != nil || validateCephFSVolumeName(clone.GroupName, true) != nil {
			return nil, errors.New("decode subvolume snapshot info: invalid pending clone name")
		}
		if slices.Contains(raw.PendingClones[:i], clone) {
			return nil, errors.New("decode subvolume snapshot info: duplicate pending clone")
		}
	}
	return &SubvolumeSnapshotInfo{CreatedAt: raw.CreatedAt, DataPool: raw.DataPool, HasPendingClones: raw.HasPendingClones == "yes", PendingClones: raw.PendingClones, OrphanClonesCount: raw.OrphanClonesCount}, nil
}

func (v volumeCommands) subvolumeSnapshotInfo(ctx context.Context, subvolumeName, groupName, snapshotName string) (*SubvolumeSnapshotInfo, error) {
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "snapshot", "info", v.name, subvolumeName, snapshotName}, groupName)
	data, err := v.cluster.Ceph(ctx, append(args, "--format", "json")...)
	if err != nil {
		return nil, err
	}
	info, err := decodeCephFSSnapshotInfo(data)
	if err != nil {
		return nil, err
	}
	args = appendSubvolumeGroup([]string{"fs", "subvolume", "snapshot", "getpath", v.name, subvolumeName, snapshotName}, groupName)
	data, err = v.cluster.Ceph(ctx, args...)
	if err != nil {
		return nil, err
	}
	info.Path = strings.TrimSpace(string(data))
	if !path.IsAbs(info.Path) || path.Clean(info.Path) != info.Path || info.Path == "/" {
		return nil, errors.New("decode snapshot path: expected an absolute frozen data path")
	}
	info.Name, info.SubvolumeName, info.GroupName = snapshotName, subvolumeName, groupName
	return info, nil
}

func (fs *Filesystem) checkSnapshotIdentity(ctx context.Context, fsID int64, identity *cephFSSnapshotIdentity) (*SubvolumeSnapshotInfo, error) {
	if identity.removed {
		return nil, errors.New("subvolume snapshot has been removed")
	}
	if err := fs.checkSnapshotSubvolume(ctx, fsID, identity.subvolume); err != nil {
		return nil, err
	}
	info, err := fs.id.volumes().subvolumeSnapshotInfo(ctx, identity.subvolume.name, identity.subvolume.group, identity.name)
	if err != nil {
		return nil, err
	}
	if info.Path != identity.path || info.CreatedAt != identity.createdAt || info.DataPool != identity.dataPool {
		return nil, errors.New("CephFS snapshot was replaced outside this fixture; refusing mutation")
	}
	return info, nil
}

// Remove removes only this owned snapshot without --force.
// Pending clones protect their source snapshot both in this preflight and in
// native Ceph. Copies share removal state, and retry reconciles an uncertain
// successful deletion. Source data and completed clones are preserved.
//
//purpose:lifetime
func (snapshot *SubvolumeSnapshot) Remove(ctx context.Context) error {
	fs, err := snapshot.filesystem()
	if err != nil {
		return err
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	identity := snapshot.identity
	if identity.removed {
		return nil
	}
	if err := fs.checkSnapshotSubvolume(ctx, fsID, identity.subvolume); err != nil {
		return err
	}
	if identity.removalAttempted {
		names, err := fs.id.volumes().snapshotNames(ctx, identity.subvolume)
		if err != nil {
			return err
		}
		if !slices.Contains(names, identity.name) {
			identity.removed = true
			return nil
		}
	}
	info, err := fs.checkSnapshotIdentity(ctx, fsID, identity)
	if err != nil {
		return err
	}
	if info.HasPendingClones || info.OrphanClonesCount > 0 {
		return servicehook.Invalid(errors.New("subvolume snapshot has pending or orphan clones; refusing removal"))
	}
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "snapshot", "rm", fs.id.name, identity.subvolume.name, identity.name}, identity.subvolume.group)
	identity.removalAttempted = true
	if _, err := fs.id.cluster.Ceph(ctx, args...); err != nil {
		return err
	}
	identity.removed = true
	return nil
}

// Clone submits an asynchronous copy to a fresh subvolume.
// Duplicate targets are rejected; the source snapshot is retained. Immediately
// after submission it captures the target UUID, inode and birth time from
// version-checked native volumes metadata inside the control container. External
// edits must not race submission. A failed command or identity read returns a
// non-nil descriptor for inspection, but cannot be adopted or mutated through
// this API. Failed/canceled targets are retained until explicit
// SubvolumeClone.RemovePartial or raw Ceph cleanup.
//
//purpose:construction
func (snapshot *SubvolumeSnapshot) Clone(ctx context.Context, config CloneConfig) (*SubvolumeClone, error) {
	fs, err := snapshot.filesystem()
	if err != nil {
		return nil, err
	}
	if err := fs.validateSubvolumeConfig(config.Name, config.GroupName, config.DataPool, 0); err != nil {
		return nil, err
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	selectedPool := config.DataPool
	if selectedPool == "" {
		selectedPool = snapshot.identity.dataPool
	}
	if err := fs.checkSubvolumeDataPool(ctx, selectedPool); err != nil {
		return nil, err
	}
	if _, err := fs.checkSnapshotIdentity(ctx, fsID, snapshot.identity); err != nil {
		return nil, err
	}
	names, err := fs.id.volumes().subvolumeNames(ctx, config.GroupName)
	if err != nil {
		return nil, err
	}
	if slices.Contains(names, config.Name) {
		return nil, servicehook.Invalid(fmt.Errorf("clone target subvolume %q already exists", config.Name))
	}
	identity := &cephFSCloneIdentity{filesystem: fs, filesystemID: fsID, source: snapshot.identity, name: config.Name, group: config.GroupName, dataPool: config.DataPool}
	clone := &SubvolumeClone{Name: config.Name, GroupName: config.GroupName, FilesystemName: fs.id.name, identity: identity}
	source := snapshot.identity.subvolume
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "snapshot", "clone", fs.id.name, source.name, snapshot.identity.name, config.Name}, source.group)
	if config.GroupName != "" {
		args = append(args, "--target_group_name", config.GroupName)
	}
	if config.DataPool != "" {
		args = append(args, "--pool_layout", config.DataPool)
	}
	if _, err := fs.id.cluster.Ceph(ctx, args...); err != nil {
		return clone, err
	}
	identity.submitted = true
	identity.incarnation, err = fs.id.volumes().readCloneIncarnation(ctx, identity)
	if err != nil {
		return clone, err
	}
	return clone, nil
}

func decodeCephFSCloneStatus(data []byte) (*SubvolumeCloneStatus, error) {
	var raw struct {
		Status *struct {
			State  string `json:"state"`
			Source *struct {
				Volume    string `json:"volume"`
				Subvolume string `json:"subvolume"`
				Group     string `json:"group"`
				Snapshot  string `json:"snapshot"`
			} `json:"source"`
			Failure *struct {
				Errno        json.RawMessage `json:"errno"`
				ErrorMessage string          `json:"error_msg"`
				Errstr       string          `json:"errstr"`
			} `json:"failure"`
			Progress map[string]string `json:"progress_report"`
		} `json:"status"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("decode CephFS clone status: %w", err)
	}
	if raw.Status == nil || !slices.Contains([]string{"pending", "in-progress", "complete", "failed", "canceled"}, raw.Status.State) {
		return nil, errors.New("decode CephFS clone status: missing or unknown state")
	}
	status := &SubvolumeCloneStatus{State: raw.Status.State, Progress: raw.Status.Progress}
	if source := raw.Status.Source; source != nil {
		if source.Volume == "" || source.Subvolume == "" || source.Snapshot == "" {
			return nil, errors.New("decode CephFS clone status: incomplete source")
		}
		status.SourceFilesystem, status.SourceSubvolume, status.SourceGroup, status.SourceSnapshot = source.Volume, source.Subvolume, source.Group, source.Snapshot
	} else if status.State != "complete" {
		return nil, errors.New("decode CephFS clone status: missing source for incomplete clone")
	}
	if failure := raw.Status.Failure; failure != nil {
		value := strings.Trim(string(failure.Errno), `"`)
		errno, err := strconv.Atoi(value)
		if err != nil || errno <= 0 {
			return nil, errors.New("decode CephFS clone failure: invalid errno")
		}
		status.FailureErrno, status.FailureMessage = errno, failure.ErrorMessage
		if status.FailureMessage == "" {
			status.FailureMessage = failure.Errstr
		}
	}
	return status, nil
}

func (fs *Filesystem) validateCloneHandle(clone *SubvolumeClone) error {
	if fs == nil || fs.id.cluster == nil || clone == nil || clone.identity == nil || clone.identity.filesystem != fs || !clone.identity.submitted || clone.identity.removed {
		return servicehook.Invalid(errors.New("clone must be a successfully submitted resource created by this filesystem"))
	}
	if clone.identity.incarnation == nil {
		return servicehook.Invalid(errors.New("clone has no submission-confirmed native incarnation; inspect it through Ceph"))
	}
	return nil
}

func (fs *Filesystem) subvolumeCloneStatus(ctx context.Context, fsID int64, identity *cephFSCloneIdentity) (*SubvolumeCloneStatus, error) {
	if identity.filesystemID != fsID {
		return nil, errors.New("CephFS filesystem was replaced outside this fixture; refusing clone adoption")
	}
	args := appendSubvolumeGroup([]string{"fs", "clone", "status", fs.id.name, identity.name}, identity.group)
	data, err := fs.id.cluster.Ceph(ctx, append(args, "--format", "json")...)
	if err != nil {
		return nil, err
	}
	status, err := decodeCephFSCloneStatus(data)
	if err != nil {
		return nil, err
	}
	source := identity.source.subvolume
	if status.SourceFilesystem != "" && (status.SourceFilesystem != fs.id.name || status.SourceSubvolume != source.name || status.SourceGroup != source.group || status.SourceSnapshot != identity.source.name) {
		return nil, errors.New("native clone source was replaced outside this fixture")
	}
	if identity.subvolume != nil {
		if identity.subvolume.identity.removed {
			return nil, errors.New("completed clone subvolume has been removed")
		}
		info, err := fs.id.volumes().subvolumeInfo(ctx, identity.name, identity.group)
		if err != nil {
			return nil, err
		}
		if err := fs.checkVolumeIdentity(fsID, identity.subvolume.identity, info.Path, info.CreatedAt); err != nil {
			return nil, err
		}
	}
	return status, nil
}

// Status inspects this owned request. Native status is read-only;
// failed and canceled targets are preserved. Every query verifies the captured
// native UUID, inode and birth time too.
//
//purpose:observation
func (clone *SubvolumeClone) Status(ctx context.Context) (*SubvolumeCloneStatus, error) {
	fs, err := clone.filesystem()
	if err != nil {
		return nil, err
	}
	if err := fs.validateCloneHandle(clone); err != nil {
		return nil, err
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := fs.checkCloneIncarnation(ctx, clone.identity); err != nil {
		return nil, err
	}
	return fs.subvolumeCloneStatus(ctx, fsID, clone.identity)
}

// Wait waits within the cluster startup timeout or the caller's
// earlier deadline. A timeout leaves cloning running and can be retried. Native
// failed/canceled states return promptly without removing partial data. A
// successful result supports existing Subvolume.Resize/Subvolume.Remove methods.
//
//purpose:observation
func (clone *SubvolumeClone) Wait(ctx context.Context) (*Subvolume, error) {
	fs, err := clone.filesystem()
	if err != nil {
		return nil, err
	}
	if err := fs.validateCloneHandle(clone); err != nil {
		return nil, err
	}
	timeout := servicehook.StartupTimeout(fs.id.cluster)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		subvolume, complete, err := clone.waitStep(ctx, fs)
		if err != nil {
			return nil, err
		}
		if complete {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return subvolume, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// waitStep is one poll of Wait. It holds the subvolume operation gate only for
// its own step and reports the completed subvolume once the native clone is
// complete.
func (clone *SubvolumeClone) waitStep(ctx context.Context, fs *Filesystem) (*Subvolume, bool, error) {
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, false, err
	}
	defer done()
	identity := clone.identity
	if err := fs.checkCloneIncarnation(ctx, identity); err != nil {
		return nil, false, err
	}
	status, err := fs.subvolumeCloneStatus(ctx, fsID, identity)
	if err != nil {
		return nil, false, err
	}
	if status.State == "failed" || status.State == "canceled" {
		return nil, false, fmt.Errorf("CephFS clone %q is %s (errno %d): %s", identity.name, status.State, status.FailureErrno, status.FailureMessage)
	}
	if status.State != "complete" {
		return nil, false, nil
	}
	if identity.subvolume == nil {
		if err := clone.adoptCompleted(ctx, fs, fsID); err != nil {
			return nil, false, err
		}
	}
	// Return a separate public descriptor while retaining shared private lifecycle.
	result := *identity.subvolume
	return &result, true, nil
}

// adoptCompleted records the subvolume of a complete clone once its native
// type, state, pool and path match the submission.
func (clone *SubvolumeClone) adoptCompleted(ctx context.Context, fs *Filesystem, fsID int64) error {
	identity := clone.identity
	info, err := fs.id.volumes().subvolumeInfo(ctx, identity.name, identity.group)
	if err != nil {
		return err
	}
	if info.Type != "clone" || info.State != "complete" || (identity.dataPool != "" && info.DataPool != identity.dataPool) {
		return errors.New("completed subvolume clone does not match requested native type, state or pool")
	}
	if identity.incarnation != nil && info.Path != identity.incarnation.Path {
		return errors.New("completed clone incarnation differs from its submission identity")
	}
	identity.subvolume = &Subvolume{Name: identity.name, GroupName: identity.group, FilesystemName: fs.id.name, Path: info.Path,
		identity: &cephFSVolumeIdentity{filesystem: fs, filesystemID: fsID, name: identity.name, group: identity.group, path: info.Path, createdAt: info.CreatedAt, ready: true}}
	return nil
}
