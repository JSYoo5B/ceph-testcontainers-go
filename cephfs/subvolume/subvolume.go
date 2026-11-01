package subvolume

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/volbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// GroupConfig creates a named group inside an existing filesystem. SizeBytes
// is its aggregate directory quota; zero leaves it unlimited. DataPool
// optionally selects one of the filesystem's existing data pools. These helpers
// do not create pools, filesystems, MDS daemons or orchestrator services.
type GroupConfig struct {
	Name      string
	SizeBytes int64
	DataPool  string
}

// Config creates a directory tree managed by the MGR volumes module. An empty
// GroupName selects Ceph's default group. Zero SizeBytes leaves it unlimited.
// DataPool selects an existing filesystem data pool; empty inherits the
// group's layout. NamespaceIsolated gives file data a separate RADOS
// namespace; client access still requires appropriate Cephx capabilities.
type Config struct {
	Name              string
	GroupName         string
	SizeBytes         int64
	DataPool          string
	NamespaceIsolated bool
}

// Group is an owned group returned by CreateGroup. Public fields are
// creation-time descriptions; mutations use the private identity captured
// after successful creation. Get current quota through GroupInfo. Removing a
// group requires it to be empty.
type Group struct {
	Name           string
	FilesystemName string
	Path           string
	identity       *volumeRecord
}

// Subvolume is an owned subvolume returned by Create. Its public fields
// describe creation. Info queries current native state. Remove deletes this
// directory tree and its contents, preserving its filesystem and pools; native
// trash purging is asynchronous.
type Subvolume struct {
	Name           string
	GroupName      string
	FilesystemName string
	Path           string
	// volHandle binds the subvolume to its shared volume identity, which
	// cephfs/quiesce reads through volbase.Of.
	volHandle
	identity *volumeRecord
}

// volHandle is the volume handle a Subvolume embeds. The alias keeps the
// embedded field unexported, so nothing is promoted into the public API, and
// *Subvolume satisfies volbase.Owner.
type volHandle = volbase.Handle

// GroupMetadata reports native group metadata. QuotaBytes is zero for an
// unlimited quota. BytesUsed is Ceph's directory size accounting, which can lag
// recent writes. CreatedAt is the directory birth time used by the native
// volumes module.
type GroupMetadata struct {
	Name       string
	Path       string
	DataPool   string
	QuotaBytes int64
	BytesUsed  int64
	CreatedAt  string
}

// Metadata reports a native subvolume, including its real mount path and
// RADOS namespace. Read-only queries also describe resources created outside
// these helpers, but those resources cannot be resized or removed through an
// owned handle. QuotaBytes is zero for unlimited.
type Metadata struct {
	Name          string
	GroupName     string
	Path          string
	DataPool      string
	PoolNamespace string
	QuotaBytes    int64
	BytesUsed     int64
	CreatedAt     string
	State         string
	Type          string
}

// volumeRecord is the identity of one subvolume or group that every copy of
// its handle shares: the volume identity that other packages read through
// volbase, the removal retry state and the authorizations of a subvolume.
// Identity fields are private so editing exported descriptors cannot redirect
// a mutation to another filesystem, group or subvolume. The setup gate
// serializes all reads and mutations of this state with the native create
// preflight.
type volumeRecord struct {
	volbase.Volume
	removalAttempted bool
	authorizations   map[*authorizationRecord]struct{}
}

// newSubvolume returns the handle of record, bound to its volume identity.
func newSubvolume(record *volumeRecord) *Subvolume {
	return &Subvolume{Name: record.Name, GroupName: record.Group, FilesystemName: record.Core.Name(), Path: record.Path,
		volHandle: volbase.NewHandle(&record.Volume), identity: record}
}

// coreOf returns the foundation core of fs; nil for a nil fs.
func coreOf(fs *cephfs.Filesystem) *fsbase.Core {
	if fs == nil {
		return nil
	}
	return fsbase.Of(fs)
}

var volumeName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func validateName(name string, optional bool) error {
	if (name == "" && optional) || volumeName.MatchString(name) {
		return nil
	}
	return servicehook.Invalid(errors.New("subvolume and group names must start with a letter or digit and use at most 128 letters, digits, dots, underscores or dashes"))
}

func validateConfig(fs *fsbase.Core, name, group, dataPool string, size int64) error {
	if err := validateName(name, false); err != nil {
		return err
	}
	if err := validateName(group, true); err != nil {
		return err
	}
	if size < 0 {
		return servicehook.Invalid(errors.New("subvolume quota must not be negative; zero means unlimited"))
	}
	if fs == nil || fs.Cluster() == nil {
		return servicehook.Invalid(errors.New("CephFS filesystem is unavailable"))
	}
	if dataPool == "" {
		return nil
	}
	if err := fsbase.ValidateExistingPool(dataPool); err != nil {
		return err
	}
	// Native membership is checked after acquiring the setup gate. Initial
	// config names cannot describe dynamically attached or detached pools.
	return nil
}

func appendLayout(args []string, size int64, pool string) []string {
	if size > 0 {
		args = append(args, "--size", strconv.FormatInt(size, 10))
	}
	if pool != "" {
		args = append(args, "--pool_layout", pool)
	}
	return args
}

// CreateGroup creates a fresh group in fs. Native Ceph's create is idempotent
// and can change an existing group's quota/layout; this helper rejects
// duplicates before issuing create. Do not race external subvolume/group
// commands with these operations. A non-nil handle on error records an
// attempted creation, but only a fully confirmed identity can be mutated.
// Cluster termination cleans up the entire disposable filesystem even after
// partial creation.
//
//purpose:construction
func CreateGroup(ctx context.Context, fs *cephfs.Filesystem, config GroupConfig) (*Group, error) {
	return createGroup(ctx, coreOf(fs), config)
}

func createGroup(ctx context.Context, fs *fsbase.Core, config GroupConfig) (*Group, error) {
	if err := validateConfig(fs, config.Name, "", config.DataPool, config.SizeBytes); err != nil {
		return nil, err
	}
	ctx, fsID, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := fs.CheckSubvolumeDataPool(ctx, config.DataPool); err != nil {
		return nil, err
	}
	names, err := fs.Volumes().SubvolumeGroupNames(ctx)
	if err != nil {
		return nil, err
	}
	if slices.Contains(names, config.Name) {
		return nil, servicehook.Invalid(fmt.Errorf("subvolume group %q already exists", config.Name))
	}
	group := &Group{Name: config.Name, FilesystemName: fs.Name(),
		identity: &volumeRecord{Volume: volbase.Volume{Core: fs, FilesystemID: fsID, Name: config.Name}}}
	args := appendLayout([]string{"fs", "subvolumegroup", "create", fs.Name(), config.Name}, config.SizeBytes, config.DataPool)
	if _, err := fs.Cluster().Ceph(ctx, args...); err != nil {
		return group, fmt.Errorf("create subvolume group %q: %w", config.Name, err)
	}
	info, err := fs.Volumes().SubvolumeGroupInfo(ctx, config.Name)
	if err != nil {
		return group, err
	}
	if (config.DataPool != "" && info.DataPool != config.DataPool) || info.QuotaBytes != config.SizeBytes {
		return group, errors.New("created subvolume group does not match requested layout or quota")
	}
	group.Path = info.Path
	group.identity.Path, group.identity.CreatedAt, group.identity.Ready = info.Path, info.CreatedAt, true
	return group, nil
}

// Create creates a fresh subvolume in the default or an existing named group
// of fs. It rejects duplicates and unknown filesystem data pools before
// create. A non-nil result on failure identifies an attempted resource; it
// never removes partial data. Only successfully confirmed handles support
// Resize/Remove.
//
//purpose:construction
func Create(ctx context.Context, fs *cephfs.Filesystem, config Config) (*Subvolume, error) {
	return create(ctx, coreOf(fs), config)
}

func create(ctx context.Context, fs *fsbase.Core, config Config) (*Subvolume, error) {
	if err := validateConfig(fs, config.Name, config.GroupName, config.DataPool, config.SizeBytes); err != nil {
		return nil, err
	}
	ctx, fsID, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := fs.CheckSubvolumeCreationPool(ctx, config.DataPool, config.GroupName); err != nil {
		return nil, err
	}
	names, err := fs.Volumes().SubvolumeNames(ctx, config.GroupName)
	if err != nil {
		return nil, err
	}
	if slices.Contains(names, config.Name) {
		return nil, servicehook.Invalid(fmt.Errorf("subvolume %q in group %q already exists", config.Name, config.GroupName))
	}
	subvolume := newSubvolume(&volumeRecord{Volume: volbase.Volume{Core: fs, FilesystemID: fsID, Name: config.Name, Group: config.GroupName}})
	args := fsbase.AppendGroup([]string{"fs", "subvolume", "create", fs.Name(), config.Name}, config.GroupName)
	args = appendLayout(args, config.SizeBytes, config.DataPool)
	if config.NamespaceIsolated {
		args = append(args, "--namespace-isolated")
	}
	if _, err := fs.Cluster().Ceph(ctx, args...); err != nil {
		return subvolume, fmt.Errorf("create subvolume %q: %w", config.Name, err)
	}
	info, err := fs.Volumes().SubvolumeInfo(ctx, config.Name, config.GroupName)
	if err != nil {
		return subvolume, err
	}
	if info.State != "complete" || info.Type != "subvolume" || info.QuotaBytes != config.SizeBytes ||
		(config.DataPool != "" && info.DataPool != config.DataPool) || (config.NamespaceIsolated && info.PoolNamespace == "") {
		return subvolume, errors.New("created subvolume does not match requested state, layout, quota or namespace")
	}
	subvolume.Path = info.Path
	subvolume.identity.Path, subvolume.identity.CreatedAt, subvolume.identity.Ready = info.Path, info.CreatedAt, true
	return subvolume, nil
}

// GroupNames lists the native named groups of fs, sorted by name. It omits
// Ceph's internal default group and includes groups created outside these
// helpers.
//
//purpose:observation
func GroupNames(ctx context.Context, fs *cephfs.Filesystem) ([]string, error) {
	return groupNames(ctx, coreOf(fs))
}

func groupNames(ctx context.Context, fs *fsbase.Core) ([]string, error) {
	ctx, _, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return nil, err
	}
	defer done()
	return fs.Volumes().SubvolumeGroupNames(ctx)
}

// Names lists the native subvolume names of groupName in fs, sorted by name.
// Empty selects the default group. Native snapshot-retained entries can appear
// in this listing.
//
//purpose:observation
func Names(ctx context.Context, fs *cephfs.Filesystem, groupName string) ([]string, error) {
	return names(ctx, coreOf(fs), groupName)
}

func names(ctx context.Context, fs *fsbase.Core, groupName string) ([]string, error) {
	if err := validateName(groupName, true); err != nil {
		return nil, err
	}
	ctx, _, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return nil, err
	}
	defer done()
	return fs.Volumes().SubvolumeNames(ctx, groupName)
}

// GroupInfo queries the current native quota/layout and path of the group
// name in fs.
//
//purpose:observation
func GroupInfo(ctx context.Context, fs *cephfs.Filesystem, name string) (*GroupMetadata, error) {
	return groupInfo(ctx, coreOf(fs), name)
}

func groupInfo(ctx context.Context, fs *fsbase.Core, name string) (*GroupMetadata, error) {
	if err := validateName(name, false); err != nil {
		return nil, err
	}
	ctx, _, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return nil, err
	}
	defer done()
	info, err := fs.Volumes().SubvolumeGroupInfo(ctx, name)
	return (*GroupMetadata)(info), err
}

// Info queries the current metadata of a subvolume in fs. name identifies the
// subvolume and an empty groupName selects the default group. A
// snapshot-retained entry lacks a usable Path and is never accepted as an
// active owned resource.
//
//purpose:observation
func Info(ctx context.Context, fs *cephfs.Filesystem, name, groupName string) (*Metadata, error) {
	return info(ctx, coreOf(fs), name, groupName)
}

func info(ctx context.Context, fs *fsbase.Core, name, groupName string) (*Metadata, error) {
	if err := validateName(name, false); err != nil {
		return nil, err
	}
	if err := validateName(groupName, true); err != nil {
		return nil, err
	}
	ctx, _, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return nil, err
	}
	defer done()
	info, err := fs.Volumes().SubvolumeInfo(ctx, name, groupName)
	return (*Metadata)(info), err
}

func validateVolumeHandle(fs *fsbase.Core, identity *volumeRecord) error {
	if fs == nil || identity == nil || identity.Core != fs || !identity.Ready {
		return servicehook.Invalid(errors.New("subvolume or group must be a confirmed resource created by this filesystem"))
	}
	return nil
}

func resizeQuota(size int64) (string, error) {
	if size < 0 {
		return "", errors.New("subvolume quota must not be negative; zero means unlimited")
	}
	if size == 0 {
		return "inf", nil
	}
	return strconv.FormatInt(size, 10), nil
}

// Resize changes only an owned group's quota. Zero removes its limit.
// --no_shrink refuses a positive limit below native current usage; it never
// truncates files. Directory accounting and quota enforcement are native
// CephFS behavior and can lag concurrent writes.
//
//purpose:construction,condition
func (group *Group) Resize(ctx context.Context, sizeBytes int64) error {
	fs, err := group.filesystem()
	if err != nil {
		return err
	}
	quota, err := resizeQuota(sizeBytes)
	if err != nil {
		return err
	}
	if group == nil {
		return errors.New("subvolume group is unavailable")
	}
	ctx, fsID, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return err
	}
	defer done()
	if group.identity.Removed {
		return errors.New("subvolume group has been removed")
	}
	info, err := fs.Volumes().SubvolumeGroupInfo(ctx, group.identity.Name)
	if err != nil {
		return err
	}
	if err := group.identity.CheckIdentity(fsID, info.Path, info.CreatedAt); err != nil {
		return err
	}
	_, err = fs.Cluster().Ceph(ctx, "fs", "subvolumegroup", "resize", fs.Name(), group.identity.Name, quota, "--no_shrink")
	return err
}

// Resize changes only an owned subvolume's quota, with the same zero and
// --no_shrink behavior as Group.Resize.
//
//purpose:construction,condition
func (subvolume *Subvolume) Resize(ctx context.Context, sizeBytes int64) error {
	fs, err := subvolume.filesystem()
	if err != nil {
		return err
	}
	quota, err := resizeQuota(sizeBytes)
	if err != nil {
		return err
	}
	if subvolume == nil {
		return errors.New("subvolume is unavailable")
	}
	ctx, fsID, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return err
	}
	defer done()
	if subvolume.identity.Removed {
		return errors.New("subvolume has been removed")
	}
	info, err := fs.Volumes().SubvolumeInfo(ctx, subvolume.identity.Name, subvolume.identity.Group)
	if err != nil {
		return err
	}
	if err := subvolume.identity.CheckIdentity(fsID, info.Path, info.CreatedAt); err != nil {
		return err
	}
	args := fsbase.AppendGroup([]string{"fs", "subvolume", "resize", fs.Name(), subvolume.identity.Name, quota}, subvolume.identity.Group)
	_, err = fs.Cluster().Ceph(ctx, append(args, "--no_shrink")...)
	return err
}

// Remove removes an owned empty group without --force. A native refusal (for
// example a nonempty group) preserves its handle for retry. A repeat call after
// confirmed success is a no-op. External edits must not race removal.
//
//purpose:lifetime
func (group *Group) Remove(ctx context.Context) error {
	fs, err := group.filesystem()
	if err != nil {
		return err
	}
	if group == nil {
		return errors.New("subvolume group is unavailable")
	}
	ctx, fsID, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return err
	}
	defer done()
	if group.identity.Removed {
		return nil
	}
	if group.identity.FilesystemID != fsID {
		return servicehook.Invalid(errors.New("CephFS filesystem was replaced outside this fixture; refusing removal"))
	}
	if group.identity.removalAttempted {
		names, err := fs.Volumes().SubvolumeGroupNames(ctx)
		if err != nil {
			return err
		}
		if !slices.Contains(names, group.identity.Name) {
			group.identity.Removed = true
			return nil
		}
	}
	info, err := fs.Volumes().SubvolumeGroupInfo(ctx, group.identity.Name)
	if err != nil {
		return err
	}
	if err := group.identity.CheckIdentity(fsID, info.Path, info.CreatedAt); err != nil {
		return err
	}
	group.identity.removalAttempted = true
	if _, err := fs.Cluster().Ceph(ctx, "fs", "subvolumegroup", "rm", fs.Name(), group.identity.Name); err != nil {
		return err
	}
	group.identity.Removed = true
	return nil
}

// Remove removes this owned directory tree and its contents without --force or
// --retain-snapshots. Native Ceph refuses subvolumes with snapshots; use the
// snapshot helpers to remove owned snapshots first. Trash purging is async.
// Repeat removal after confirmed success is a no-op; external edits must not
// race this operation. Its filesystem, groups and pools remain available.
//
//purpose:lifetime
func (subvolume *Subvolume) Remove(ctx context.Context) error {
	fs, err := subvolume.filesystem()
	if err != nil {
		return err
	}
	if subvolume == nil {
		return errors.New("subvolume is unavailable")
	}
	ctx, fsID, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return err
	}
	defer done()
	if subvolume.identity.Removed {
		return nil
	}
	for authorization := range subvolume.identity.authorizations {
		if !authorization.deauthorized {
			return errors.New("subvolume has an owned or partial authorization; deauthorize it before removing data")
		}
	}
	if subvolume.identity.FilesystemID != fsID {
		return servicehook.Invalid(errors.New("CephFS filesystem was replaced outside this fixture; refusing removal"))
	}
	if subvolume.identity.removalAttempted {
		names, err := fs.Volumes().SubvolumeNames(ctx, subvolume.identity.Group)
		if err != nil {
			return err
		}
		if !slices.Contains(names, subvolume.identity.Name) {
			subvolume.identity.Removed = true
			return nil
		}
	}
	info, err := fs.Volumes().SubvolumeInfo(ctx, subvolume.identity.Name, subvolume.identity.Group)
	if err != nil {
		return err
	}
	if err := subvolume.identity.CheckIdentity(fsID, info.Path, info.CreatedAt); err != nil {
		return err
	}
	args := fsbase.AppendGroup([]string{"fs", "subvolume", "rm", fs.Name(), subvolume.identity.Name}, subvolume.identity.Group)
	subvolume.identity.removalAttempted = true
	if _, err := fs.Cluster().Ceph(ctx, args...); err != nil {
		return err
	}
	subvolume.identity.Removed = true
	return nil
}
