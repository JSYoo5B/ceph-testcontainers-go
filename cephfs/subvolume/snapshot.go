package subvolume

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

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/volbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// Snapshot is an owned snapshot of a confirmed subvolume. Its public fields
// describe creation; mutations use the captured private identity. Snapshot
// creation does not quiesce concurrent application writers.
type Snapshot struct {
	Name, SubvolumeName, GroupName, FilesystemName, Path string
	identity                                             *snapshotRecord
}

// SnapshotPendingClone identifies a native clone still using a snapshot.
// An empty GroupName selects the default group.
type SnapshotPendingClone struct {
	Name      string `json:"name"`
	GroupName string `json:"target_group"`
}

// SnapshotMetadata contains current native snapshot metadata. Path
// identifies the frozen data directory, including Ceph's incarnation UUID.
type SnapshotMetadata struct {
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

// Clone is an asynchronous, owned clone request. Clone.Wait returns a usable
// subvolume only after native cloning completes. A non-nil descriptor on error
// records the attempted request but cannot be adopted or canceled
// automatically. Cluster cleanup removes incomplete data too.
type Clone struct {
	Name, GroupName, FilesystemName string
	identity                        *cloneRecord
}

// CloneStatus reports native pending, in-progress, complete,
// failed or canceled state. FailureErrno is zero when no native failure was
// recorded. Source fields are normally absent after successful completion.
type CloneStatus struct {
	State, SourceFilesystem, SourceSubvolume, SourceGroup, SourceSnapshot string
	FailureErrno                                                          int
	FailureMessage                                                        string
	Progress                                                              map[string]string
}

type snapshotRecord struct {
	subvolume                        *volumeRecord
	name, path, createdAt, dataPool  string
	ready, removed, removalAttempted bool
}

type cloneRecord struct {
	filesystem                *fsbase.Core
	filesystemID              int64
	source                    *snapshotRecord
	name, group, dataPool     string
	submitted                 bool
	subvolume                 *Subvolume
	incarnation               *cloneIncarnation
	removed, removalAttempted bool
}

func checkSnapshotSubvolume(ctx context.Context, fs *fsbase.Core, fsID int64, identity *volumeRecord) error {
	if err := validateVolumeHandle(fs, identity); err != nil {
		return err
	}
	if identity.Removed {
		return errors.New("snapshot source subvolume has been removed")
	}
	info, err := fs.Volumes().SubvolumeInfo(ctx, identity.Name, identity.Group)
	if err != nil {
		return err
	}
	return identity.CheckIdentity(fsID, info.Path, info.CreatedAt)
}

func snapshotNames(ctx context.Context, v fsbase.VolumeCommands, identity *volumeRecord) ([]string, error) {
	args := fsbase.AppendGroup([]string{"fs", "subvolume", "snapshot", "ls", v.Name(), identity.Name}, identity.Group)
	data, err := v.Cluster().Ceph(ctx, append(args, "--format", "json")...)
	if err != nil {
		return nil, err
	}
	return fsbase.DecodeVolumeNames(data)
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
	ctx, fsID, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := checkSnapshotSubvolume(ctx, fs, fsID, subvolume.identity); err != nil {
		return nil, err
	}
	return snapshotNames(ctx, fs.Volumes(), subvolume.identity)
}

// CreateSnapshot creates a fresh snapshot without modifying an existing
// same-named snapshot. Application writers must be synchronized by the caller
// before taking a consistency-sensitive checkpoint. External CLI mutations must
// not race the native preflight and creation.
//
//purpose:construction
func (subvolume *Subvolume) CreateSnapshot(ctx context.Context, name string) (*Snapshot, error) {
	fs, err := subvolume.filesystem()
	if err != nil {
		return nil, err
	}
	if err := validateName(name, false); err != nil {
		return nil, err
	}
	if subvolume == nil {
		return nil, errors.New("subvolume is unavailable")
	}
	ctx, fsID, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := checkSnapshotSubvolume(ctx, fs, fsID, subvolume.identity); err != nil {
		return nil, err
	}
	names, err := snapshotNames(ctx, fs.Volumes(), subvolume.identity)
	if err != nil {
		return nil, err
	}
	if slices.Contains(names, name) {
		return nil, servicehook.Invalid(fmt.Errorf("subvolume snapshot %q already exists", name))
	}
	identity := &snapshotRecord{subvolume: subvolume.identity, name: name}
	snapshot := &Snapshot{Name: name, SubvolumeName: subvolume.identity.Name, GroupName: subvolume.identity.Group, FilesystemName: fs.Name(), identity: identity}
	args := fsbase.AppendGroup([]string{"fs", "subvolume", "snapshot", "create", fs.Name(), subvolume.identity.Name, name}, subvolume.identity.Group)
	if _, err := fs.Cluster().Ceph(ctx, args...); err != nil {
		return snapshot, err
	}
	info, err := snapshotInfo(ctx, fs.Volumes(), identity.subvolume.Name, identity.subvolume.Group, identity.name)
	if err != nil {
		return snapshot, err
	}
	snapshot.Path = info.Path
	identity.path, identity.createdAt, identity.dataPool, identity.ready = info.Path, info.CreatedAt, info.DataPool, true
	return snapshot, nil
}

// SnapshotInfo queries native metadata for any existing snapshot of a
// subvolume in fs. Reading an external snapshot does not grant mutation
// ownership.
//
//purpose:observation
func SnapshotInfo(ctx context.Context, fs *cephfs.Filesystem, subvolumeName, groupName, snapshotName string) (*SnapshotMetadata, error) {
	return lookupSnapshot(ctx, coreOf(fs), subvolumeName, groupName, snapshotName)
}

func lookupSnapshot(ctx context.Context, fs *fsbase.Core, subvolumeName, groupName, snapshotName string) (*SnapshotMetadata, error) {
	if err := validateName(subvolumeName, false); err != nil {
		return nil, err
	}
	if err := validateName(groupName, true); err != nil {
		return nil, err
	}
	if err := validateName(snapshotName, false); err != nil {
		return nil, err
	}
	ctx, _, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return nil, err
	}
	defer done()
	return snapshotInfo(ctx, fs.Volumes(), subvolumeName, groupName, snapshotName)
}

func decodeSnapshotInfo(data []byte) (*SnapshotMetadata, error) {
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
		if validateName(clone.Name, false) != nil || validateName(clone.GroupName, true) != nil {
			return nil, errors.New("decode subvolume snapshot info: invalid pending clone name")
		}
		if slices.Contains(raw.PendingClones[:i], clone) {
			return nil, errors.New("decode subvolume snapshot info: duplicate pending clone")
		}
	}
	return &SnapshotMetadata{CreatedAt: raw.CreatedAt, DataPool: raw.DataPool, HasPendingClones: raw.HasPendingClones == "yes", PendingClones: raw.PendingClones, OrphanClonesCount: raw.OrphanClonesCount}, nil
}

func snapshotInfo(ctx context.Context, v fsbase.VolumeCommands, subvolumeName, groupName, snapshotName string) (*SnapshotMetadata, error) {
	args := fsbase.AppendGroup([]string{"fs", "subvolume", "snapshot", "info", v.Name(), subvolumeName, snapshotName}, groupName)
	data, err := v.Cluster().Ceph(ctx, append(args, "--format", "json")...)
	if err != nil {
		return nil, err
	}
	info, err := decodeSnapshotInfo(data)
	if err != nil {
		return nil, err
	}
	args = fsbase.AppendGroup([]string{"fs", "subvolume", "snapshot", "getpath", v.Name(), subvolumeName, snapshotName}, groupName)
	data, err = v.Cluster().Ceph(ctx, args...)
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

func checkSnapshotIdentity(ctx context.Context, fs *fsbase.Core, fsID int64, identity *snapshotRecord) (*SnapshotMetadata, error) {
	if identity.removed {
		return nil, errors.New("subvolume snapshot has been removed")
	}
	if err := checkSnapshotSubvolume(ctx, fs, fsID, identity.subvolume); err != nil {
		return nil, err
	}
	info, err := snapshotInfo(ctx, fs.Volumes(), identity.subvolume.Name, identity.subvolume.Group, identity.name)
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
func (snapshot *Snapshot) Remove(ctx context.Context) error {
	fs, err := snapshot.filesystem()
	if err != nil {
		return err
	}
	ctx, fsID, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return err
	}
	defer done()
	identity := snapshot.identity
	if identity.removed {
		return nil
	}
	if err := checkSnapshotSubvolume(ctx, fs, fsID, identity.subvolume); err != nil {
		return err
	}
	if identity.removalAttempted {
		names, err := snapshotNames(ctx, fs.Volumes(), identity.subvolume)
		if err != nil {
			return err
		}
		if !slices.Contains(names, identity.name) {
			identity.removed = true
			return nil
		}
	}
	info, err := checkSnapshotIdentity(ctx, fs, fsID, identity)
	if err != nil {
		return err
	}
	if info.HasPendingClones || info.OrphanClonesCount > 0 {
		return servicehook.Invalid(errors.New("subvolume snapshot has pending or orphan clones; refusing removal"))
	}
	args := fsbase.AppendGroup([]string{"fs", "subvolume", "snapshot", "rm", fs.Name(), identity.subvolume.Name, identity.name}, identity.subvolume.Group)
	identity.removalAttempted = true
	if _, err := fs.Cluster().Ceph(ctx, args...); err != nil {
		return err
	}
	identity.removed = true
	return nil
}

// Clone submits an asynchronous copy to a fresh subvolume.
// Duplicate targets are rejected; the source snapshot is retained. Immediately
// after submission it captures the target UUID, inode and birth time from
// version-checked native volumes metadata inside the client container. External
// edits must not race submission. A failed command or identity read returns a
// non-nil descriptor for inspection, but cannot be adopted or mutated through
// this API. Failed/canceled targets are retained until explicit
// Clone.RemovePartial or raw Ceph cleanup.
//
//purpose:construction
func (snapshot *Snapshot) Clone(ctx context.Context, config CloneConfig) (*Clone, error) {
	fs, err := snapshot.filesystem()
	if err != nil {
		return nil, err
	}
	if err := validateConfig(fs, config.Name, config.GroupName, config.DataPool, 0); err != nil {
		return nil, err
	}
	ctx, fsID, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return nil, err
	}
	defer done()
	selectedPool := config.DataPool
	if selectedPool == "" {
		selectedPool = snapshot.identity.dataPool
	}
	if err := fs.CheckSubvolumeDataPool(ctx, selectedPool); err != nil {
		return nil, err
	}
	if _, err := checkSnapshotIdentity(ctx, fs, fsID, snapshot.identity); err != nil {
		return nil, err
	}
	names, err := fs.Volumes().SubvolumeNames(ctx, config.GroupName)
	if err != nil {
		return nil, err
	}
	if slices.Contains(names, config.Name) {
		return nil, servicehook.Invalid(fmt.Errorf("clone target subvolume %q already exists", config.Name))
	}
	identity := &cloneRecord{filesystem: fs, filesystemID: fsID, source: snapshot.identity, name: config.Name, group: config.GroupName, dataPool: config.DataPool}
	clone := &Clone{Name: config.Name, GroupName: config.GroupName, FilesystemName: fs.Name(), identity: identity}
	source := snapshot.identity.subvolume
	args := fsbase.AppendGroup([]string{"fs", "subvolume", "snapshot", "clone", fs.Name(), source.Name, snapshot.identity.name, config.Name}, source.Group)
	if config.GroupName != "" {
		args = append(args, "--target_group_name", config.GroupName)
	}
	if config.DataPool != "" {
		args = append(args, "--pool_layout", config.DataPool)
	}
	if _, err := fs.Cluster().Ceph(ctx, args...); err != nil {
		return clone, err
	}
	identity.submitted = true
	identity.incarnation, err = readCloneIncarnation(ctx, fs.Volumes(), identity)
	if err != nil {
		return clone, err
	}
	return clone, nil
}

func decodeCloneStatus(data []byte) (*CloneStatus, error) {
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
	status := &CloneStatus{State: raw.Status.State, Progress: raw.Status.Progress}
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

func validateCloneHandle(fs *fsbase.Core, clone *Clone) error {
	if fs == nil || fs.Cluster() == nil || clone == nil || clone.identity == nil || clone.identity.filesystem != fs || !clone.identity.submitted || clone.identity.removed {
		return servicehook.Invalid(errors.New("clone must be a successfully submitted resource created by this filesystem"))
	}
	if clone.identity.incarnation == nil {
		return servicehook.Invalid(errors.New("clone has no submission-confirmed native incarnation; inspect it through Ceph"))
	}
	return nil
}

func cloneStatus(ctx context.Context, fs *fsbase.Core, fsID int64, identity *cloneRecord) (*CloneStatus, error) {
	if identity.filesystemID != fsID {
		return nil, errors.New("CephFS filesystem was replaced outside this fixture; refusing clone adoption")
	}
	args := fsbase.AppendGroup([]string{"fs", "clone", "status", fs.Name(), identity.name}, identity.group)
	data, err := fs.Cluster().Ceph(ctx, append(args, "--format", "json")...)
	if err != nil {
		return nil, err
	}
	status, err := decodeCloneStatus(data)
	if err != nil {
		return nil, err
	}
	source := identity.source.subvolume
	if status.SourceFilesystem != "" && (status.SourceFilesystem != fs.Name() || status.SourceSubvolume != source.Name || status.SourceGroup != source.Group || status.SourceSnapshot != identity.source.name) {
		return nil, errors.New("native clone source was replaced outside this fixture")
	}
	if identity.subvolume != nil {
		if identity.subvolume.identity.Removed {
			return nil, errors.New("completed clone subvolume has been removed")
		}
		info, err := fs.Volumes().SubvolumeInfo(ctx, identity.name, identity.group)
		if err != nil {
			return nil, err
		}
		if err := identity.subvolume.identity.CheckIdentity(fsID, info.Path, info.CreatedAt); err != nil {
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
func (clone *Clone) Status(ctx context.Context) (*CloneStatus, error) {
	fs, err := clone.filesystem()
	if err != nil {
		return nil, err
	}
	if err := validateCloneHandle(fs, clone); err != nil {
		return nil, err
	}
	ctx, fsID, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := checkCloneIncarnation(ctx, fs, clone.identity); err != nil {
		return nil, err
	}
	return cloneStatus(ctx, fs, fsID, clone.identity)
}

// Wait waits within the cluster startup timeout or the caller's
// earlier deadline. A timeout leaves cloning running and can be retried. Native
// failed/canceled states return promptly without removing partial data. A
// successful result supports existing Subvolume.Resize/Subvolume.Remove methods.
//
//purpose:observation
func (clone *Clone) Wait(ctx context.Context) (*Subvolume, error) {
	fs, err := clone.filesystem()
	if err != nil {
		return nil, err
	}
	if err := validateCloneHandle(fs, clone); err != nil {
		return nil, err
	}
	timeout := servicehook.StartupTimeout(fs.Cluster())
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
func (clone *Clone) waitStep(ctx context.Context, fs *fsbase.Core) (*Subvolume, bool, error) {
	ctx, fsID, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return nil, false, err
	}
	defer done()
	identity := clone.identity
	if err := checkCloneIncarnation(ctx, fs, identity); err != nil {
		return nil, false, err
	}
	status, err := cloneStatus(ctx, fs, fsID, identity)
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
func (clone *Clone) adoptCompleted(ctx context.Context, fs *fsbase.Core, fsID int64) error {
	identity := clone.identity
	info, err := fs.Volumes().SubvolumeInfo(ctx, identity.name, identity.group)
	if err != nil {
		return err
	}
	if info.Type != "clone" || info.State != "complete" || (identity.dataPool != "" && info.DataPool != identity.dataPool) {
		return errors.New("completed subvolume clone does not match requested native type, state or pool")
	}
	if identity.incarnation != nil && info.Path != identity.incarnation.Path {
		return errors.New("completed clone incarnation differs from its submission identity")
	}
	identity.subvolume = newSubvolume(&volumeRecord{Volume: volbase.Volume{Core: fs, FilesystemID: fsID, Name: identity.name, Group: identity.group, Path: info.Path, CreatedAt: info.CreatedAt, Ready: true}})
	return nil
}
