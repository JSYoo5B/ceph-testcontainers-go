package cluster

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
)

// CephFSSubvolumeSnapshot is an owned snapshot of a confirmed subvolume. Its
// public fields describe creation; mutations use the captured private identity.
// Snapshot creation does not quiesce concurrent application writers.
type CephFSSubvolumeSnapshot struct {
	Name, SubvolumeName, GroupName, FilesystemName, Path string
	identity                                             *cephFSSnapshotIdentity
}

// CephFSSnapshotPendingClone identifies a native clone still using a snapshot.
// An empty GroupName selects the default group.
type CephFSSnapshotPendingClone struct {
	Name      string `json:"name"`
	GroupName string `json:"target_group"`
}

// CephFSSubvolumeSnapshotInfo contains current native snapshot metadata. Path
// identifies the frozen data directory, including Ceph's incarnation UUID.
type CephFSSubvolumeSnapshotInfo struct {
	Name, SubvolumeName, GroupName, Path, CreatedAt, DataPool string
	HasPendingClones                                          bool
	PendingClones                                             []CephFSSnapshotPendingClone
	OrphanClonesCount                                         int
}

// CephFSCloneConfig names a new clone in an existing group. DataPool optionally
// overrides the snapshot's data pool. Ceph copies the snapshot's quota and
// layout; with a DataPool override it clears the inherited RADOS namespace.
// Quota and namespace changes are deliberately not part of the clone request.
type CephFSCloneConfig struct {
	Name, GroupName, DataPool string
}

// CephFSSubvolumeClone is an asynchronous, owned clone request. WaitForSubvolumeClone
// returns a usable subvolume only after native cloning completes. A non-nil
// descriptor on error records the attempted request but cannot be adopted or
// canceled automatically. Cluster cleanup removes incomplete data too.
type CephFSSubvolumeClone struct {
	Name, GroupName, FilesystemName string
	identity                        *cephFSCloneIdentity
}

// CephFSSubvolumeCloneStatus reports native pending, in-progress, complete,
// failed or canceled state. FailureErrno is zero when no native failure was
// recorded. Source fields are normally absent after successful completion.
type CephFSSubvolumeCloneStatus struct {
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
	filesystem                *CephFSContainer
	filesystemID              int64
	source                    *cephFSSnapshotIdentity
	name, group, dataPool     string
	submitted                 bool
	subvolume                 *CephFSSubvolume
	incarnation               *cephFSCloneIncarnation
	removed, removalAttempted bool
}

func (fs *CephFSContainer) checkSnapshotSubvolume(ctx context.Context, fsID int64, identity *cephFSVolumeIdentity) error {
	if err := fs.validateVolumeHandle(identity); err != nil {
		return err
	}
	if identity.removed {
		return errors.New("snapshot source subvolume has been removed")
	}
	info, err := fs.subvolumeInfo(ctx, identity.name, identity.group)
	if err != nil {
		return err
	}
	return fs.checkVolumeIdentity(fsID, identity, info.Path, info.CreatedAt)
}

func (fs *CephFSContainer) snapshotNames(ctx context.Context, identity *cephFSVolumeIdentity) ([]string, error) {
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "snapshot", "ls", fs.config.Name, identity.name}, identity.group)
	data, err := fs.cluster.Ceph(ctx, append(args, "--format", "json")...)
	if err != nil {
		return nil, err
	}
	return decodeCephFSVolumeNames(data)
}

// SubvolumeSnapshots lists native snapshots of an owned subvolume, including
// snapshots made by an external client. Only snapshots returned by this fixture's
// CreateSubvolumeSnapshot can be removed or cloned through an owned descriptor.
func (fs *CephFSContainer) SubvolumeSnapshots(ctx context.Context, subvolume *CephFSSubvolume) ([]string, error) {
	if subvolume == nil {
		return nil, errors.New("subvolume is unavailable")
	}
	if err := fs.validateVolumeHandle(subvolume.identity); err != nil {
		return nil, err
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := fs.checkSnapshotSubvolume(ctx, fsID, subvolume.identity); err != nil {
		return nil, err
	}
	return fs.snapshotNames(ctx, subvolume.identity)
}

// CreateSubvolumeSnapshot creates a fresh snapshot without modifying an existing
// same-named snapshot. Application writers must be synchronized by the caller
// before taking a consistency-sensitive checkpoint. External CLI mutations must
// not race the native preflight and creation.
func (fs *CephFSContainer) CreateSubvolumeSnapshot(ctx context.Context, subvolume *CephFSSubvolume, name string) (*CephFSSubvolumeSnapshot, error) {
	if err := validateCephFSVolumeName(name, false); err != nil {
		return nil, err
	}
	if subvolume == nil {
		return nil, errors.New("subvolume is unavailable")
	}
	if err := fs.validateVolumeHandle(subvolume.identity); err != nil {
		return nil, err
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := fs.checkSnapshotSubvolume(ctx, fsID, subvolume.identity); err != nil {
		return nil, err
	}
	names, err := fs.snapshotNames(ctx, subvolume.identity)
	if err != nil {
		return nil, err
	}
	if slices.Contains(names, name) {
		return nil, fmt.Errorf("subvolume snapshot %q already exists", name)
	}
	identity := &cephFSSnapshotIdentity{subvolume: subvolume.identity, name: name}
	snapshot := &CephFSSubvolumeSnapshot{Name: name, SubvolumeName: subvolume.identity.name, GroupName: subvolume.identity.group, FilesystemName: fs.config.Name, identity: identity}
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "snapshot", "create", fs.config.Name, subvolume.identity.name, name}, subvolume.identity.group)
	if _, err := fs.cluster.Ceph(ctx, args...); err != nil {
		return snapshot, err
	}
	info, err := fs.subvolumeSnapshotInfo(ctx, identity.subvolume.name, identity.subvolume.group, identity.name)
	if err != nil {
		return snapshot, err
	}
	snapshot.Path = info.Path
	identity.path, identity.createdAt, identity.dataPool, identity.ready = info.Path, info.CreatedAt, info.DataPool, true
	return snapshot, nil
}

// SubvolumeSnapshotInfo queries native metadata for any existing snapshot.
// Reading an external snapshot does not grant mutation ownership.
func (fs *CephFSContainer) SubvolumeSnapshotInfo(ctx context.Context, subvolumeName, groupName, snapshotName string) (*CephFSSubvolumeSnapshotInfo, error) {
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
	return fs.subvolumeSnapshotInfo(ctx, subvolumeName, groupName, snapshotName)
}

func decodeCephFSSnapshotInfo(data []byte) (*CephFSSubvolumeSnapshotInfo, error) {
	var raw struct {
		CreatedAt         string                       `json:"created_at"`
		DataPool          string                       `json:"data_pool"`
		HasPendingClones  string                       `json:"has_pending_clones"`
		PendingClones     []CephFSSnapshotPendingClone `json:"pending_clones"`
		OrphanClonesCount int                          `json:"orphan_clones_count"`
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
	return &CephFSSubvolumeSnapshotInfo{CreatedAt: raw.CreatedAt, DataPool: raw.DataPool, HasPendingClones: raw.HasPendingClones == "yes", PendingClones: raw.PendingClones, OrphanClonesCount: raw.OrphanClonesCount}, nil
}

func (fs *CephFSContainer) subvolumeSnapshotInfo(ctx context.Context, subvolumeName, groupName, snapshotName string) (*CephFSSubvolumeSnapshotInfo, error) {
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "snapshot", "info", fs.config.Name, subvolumeName, snapshotName}, groupName)
	data, err := fs.cluster.Ceph(ctx, append(args, "--format", "json")...)
	if err != nil {
		return nil, err
	}
	info, err := decodeCephFSSnapshotInfo(data)
	if err != nil {
		return nil, err
	}
	args = appendSubvolumeGroup([]string{"fs", "subvolume", "snapshot", "getpath", fs.config.Name, subvolumeName, snapshotName}, groupName)
	data, err = fs.cluster.Ceph(ctx, args...)
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

func (fs *CephFSContainer) validateSnapshotHandle(snapshot *CephFSSubvolumeSnapshot) error {
	if snapshot == nil || snapshot.identity == nil || !snapshot.identity.ready {
		return errors.New("snapshot must be a confirmed resource created by this filesystem")
	}
	return fs.validateVolumeHandle(snapshot.identity.subvolume)
}

func (fs *CephFSContainer) checkSnapshotIdentity(ctx context.Context, fsID int64, identity *cephFSSnapshotIdentity) (*CephFSSubvolumeSnapshotInfo, error) {
	if identity.removed {
		return nil, errors.New("subvolume snapshot has been removed")
	}
	if err := fs.checkSnapshotSubvolume(ctx, fsID, identity.subvolume); err != nil {
		return nil, err
	}
	info, err := fs.subvolumeSnapshotInfo(ctx, identity.subvolume.name, identity.subvolume.group, identity.name)
	if err != nil {
		return nil, err
	}
	if info.Path != identity.path || info.CreatedAt != identity.createdAt || info.DataPool != identity.dataPool {
		return nil, errors.New("CephFS snapshot was replaced outside this fixture; refusing mutation")
	}
	return info, nil
}

// RemoveSubvolumeSnapshot removes only this owned snapshot without --force.
// Pending clones protect their source snapshot both in this preflight and in
// native Ceph. Copies share removal state, and retry reconciles an uncertain
// successful deletion. Source data and completed clones are preserved.
func (fs *CephFSContainer) RemoveSubvolumeSnapshot(ctx context.Context, snapshot *CephFSSubvolumeSnapshot) error {
	if err := fs.validateSnapshotHandle(snapshot); err != nil {
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
		names, err := fs.snapshotNames(ctx, identity.subvolume)
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
		return errors.New("subvolume snapshot has pending or orphan clones; refusing removal")
	}
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "snapshot", "rm", fs.config.Name, identity.subvolume.name, identity.name}, identity.subvolume.group)
	identity.removalAttempted = true
	if _, err := fs.cluster.Ceph(ctx, args...); err != nil {
		return err
	}
	identity.removed = true
	return nil
}

// CloneSubvolumeSnapshot submits an asynchronous copy to a fresh subvolume.
// Duplicate targets are rejected; the source snapshot is retained. Immediately
// after submission it captures the target UUID, inode and birth time from
// version-checked native volumes metadata inside the control container. External
// edits must not race submission. A failed command or identity read returns a
// non-nil descriptor for inspection, but cannot be adopted or mutated through
// this API. Failed/canceled targets are retained until explicit
// RemovePartialSubvolumeClone or raw Ceph cleanup.
func (fs *CephFSContainer) CloneSubvolumeSnapshot(ctx context.Context, snapshot *CephFSSubvolumeSnapshot, config CephFSCloneConfig) (*CephFSSubvolumeClone, error) {
	if err := fs.validateSubvolumeConfig(config.Name, config.GroupName, config.DataPool, 0); err != nil {
		return nil, err
	}
	if err := fs.validateSnapshotHandle(snapshot); err != nil {
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
	names, err := fs.subvolumeNames(ctx, config.GroupName)
	if err != nil {
		return nil, err
	}
	if slices.Contains(names, config.Name) {
		return nil, fmt.Errorf("clone target subvolume %q already exists", config.Name)
	}
	identity := &cephFSCloneIdentity{filesystem: fs, filesystemID: fsID, source: snapshot.identity, name: config.Name, group: config.GroupName, dataPool: config.DataPool}
	clone := &CephFSSubvolumeClone{Name: config.Name, GroupName: config.GroupName, FilesystemName: fs.config.Name, identity: identity}
	source := snapshot.identity.subvolume
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "snapshot", "clone", fs.config.Name, source.name, snapshot.identity.name, config.Name}, source.group)
	if config.GroupName != "" {
		args = append(args, "--target_group_name", config.GroupName)
	}
	if config.DataPool != "" {
		args = append(args, "--pool_layout", config.DataPool)
	}
	if _, err := fs.cluster.Ceph(ctx, args...); err != nil {
		return clone, err
	}
	identity.submitted = true
	identity.incarnation, err = fs.readCloneIncarnation(ctx, identity)
	if err != nil {
		return clone, err
	}
	return clone, nil
}

func decodeCephFSCloneStatus(data []byte) (*CephFSSubvolumeCloneStatus, error) {
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
	status := &CephFSSubvolumeCloneStatus{State: raw.Status.State, Progress: raw.Status.Progress}
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

func (fs *CephFSContainer) validateCloneHandle(clone *CephFSSubvolumeClone) error {
	if fs == nil || fs.cluster == nil || clone == nil || clone.identity == nil || clone.identity.filesystem != fs || !clone.identity.submitted || clone.identity.removed {
		return errors.New("clone must be a successfully submitted resource created by this filesystem")
	}
	if clone.identity.incarnation == nil {
		return errors.New("clone has no submission-confirmed native incarnation; inspect it through Ceph")
	}
	return nil
}

func (fs *CephFSContainer) subvolumeCloneStatus(ctx context.Context, fsID int64, identity *cephFSCloneIdentity) (*CephFSSubvolumeCloneStatus, error) {
	if identity.filesystemID != fsID {
		return nil, errors.New("CephFS filesystem was replaced outside this fixture; refusing clone adoption")
	}
	args := appendSubvolumeGroup([]string{"fs", "clone", "status", fs.config.Name, identity.name}, identity.group)
	data, err := fs.cluster.Ceph(ctx, append(args, "--format", "json")...)
	if err != nil {
		return nil, err
	}
	status, err := decodeCephFSCloneStatus(data)
	if err != nil {
		return nil, err
	}
	source := identity.source.subvolume
	if status.SourceFilesystem != "" && (status.SourceFilesystem != fs.config.Name || status.SourceSubvolume != source.name || status.SourceGroup != source.group || status.SourceSnapshot != identity.source.name) {
		return nil, errors.New("native clone source was replaced outside this fixture")
	}
	if identity.subvolume != nil {
		if identity.subvolume.identity.removed {
			return nil, errors.New("completed clone subvolume has been removed")
		}
		info, err := fs.subvolumeInfo(ctx, identity.name, identity.group)
		if err != nil {
			return nil, err
		}
		if err := fs.checkVolumeIdentity(fsID, identity.subvolume.identity, info.Path, info.CreatedAt); err != nil {
			return nil, err
		}
	}
	return status, nil
}

// SubvolumeCloneStatus inspects this owned request. Native status is read-only;
// failed and canceled targets are preserved. Every query verifies the captured
// native UUID, inode and birth time too.
func (fs *CephFSContainer) SubvolumeCloneStatus(ctx context.Context, clone *CephFSSubvolumeClone) (*CephFSSubvolumeCloneStatus, error) {
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

// WaitForSubvolumeClone waits within the cluster startup timeout or the caller's
// earlier deadline. A timeout leaves cloning running and can be retried. Native
// failed/canceled states return promptly without removing partial data. A
// successful result supports existing ResizeSubvolume/RemoveSubvolume methods.
func (fs *CephFSContainer) WaitForSubvolumeClone(ctx context.Context, clone *CephFSSubvolumeClone) (*CephFSSubvolume, error) {
	if err := fs.validateCloneHandle(clone); err != nil {
		return nil, err
	}
	timeout := fs.cluster.settings.startupTimeout
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		subvolume, complete, err := fs.advanceSubvolumeClone(ctx, clone.identity)
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

func (fs *CephFSContainer) advanceSubvolumeClone(ctx context.Context, identity *cephFSCloneIdentity) (*CephFSSubvolume, bool, error) {
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, false, err
	}
	defer done()
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
		info, err := fs.subvolumeInfo(ctx, identity.name, identity.group)
		if err != nil {
			return nil, false, err
		}
		if info.Type != "clone" || info.State != "complete" || (identity.dataPool != "" && info.DataPool != identity.dataPool) {
			return nil, false, errors.New("completed subvolume clone does not match requested native type, state or pool")
		}
		if identity.incarnation != nil && info.Path != identity.incarnation.Path {
			return nil, false, errors.New("completed clone incarnation differs from its submission identity")
		}
		identity.subvolume = &CephFSSubvolume{Name: identity.name, GroupName: identity.group, FilesystemName: fs.config.Name, Path: info.Path,
			identity: &cephFSVolumeIdentity{filesystem: fs, filesystemID: fsID, name: identity.name, group: identity.group, path: info.Path, createdAt: info.CreatedAt, ready: true}}
	}
	// Return a separate public descriptor while retaining shared private lifecycle.
	result := *identity.subvolume
	return &result, true, nil
}
