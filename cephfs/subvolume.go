package cephfs

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// SubvolumeGroupConfig creates a named group inside an existing filesystem.
// SizeBytes is its aggregate directory quota; zero leaves it unlimited. DataPool
// optionally selects one of the filesystem's existing data pools. These helpers
// do not create pools, filesystems, MDS daemons or orchestrator services.
type SubvolumeGroupConfig struct {
	Name      string
	SizeBytes int64
	DataPool  string
}

// SubvolumeConfig creates a directory tree managed by the MGR volumes
// module. An empty GroupName selects Ceph's default group. Zero SizeBytes leaves
// it unlimited. DataPool selects an existing filesystem data pool; empty inherits
// the group's layout. NamespaceIsolated gives file data a separate RADOS
// namespace; client access still requires appropriate Cephx capabilities.
type SubvolumeConfig struct {
	Name              string
	GroupName         string
	SizeBytes         int64
	DataPool          string
	NamespaceIsolated bool
}

// SubvolumeGroup is an owned group returned by CreateSubvolumeGroup.
// Public fields are creation-time descriptions; mutations use the private
// identity captured after successful creation. Get current quota through
// SubvolumeGroupInfo. Removing a group requires it to be empty.
type SubvolumeGroup struct {
	Name           string
	FilesystemName string
	Path           string
	identity       *cephFSVolumeIdentity
}

// Subvolume is an owned subvolume returned by CreateSubvolume. Its public
// fields describe creation. SubvolumeInfo queries current native state. Remove
// deletes this directory tree and its contents, preserving its filesystem and
// pools; native trash purging is asynchronous.
type Subvolume struct {
	Name           string
	GroupName      string
	FilesystemName string
	Path           string
	identity       *cephFSVolumeIdentity
}

// SubvolumeGroupInfo reports native group metadata. QuotaBytes is zero for
// an unlimited quota. BytesUsed is Ceph's directory size accounting, which can
// lag recent writes. CreatedAt is the directory birth time used by the native
// volumes module.
type SubvolumeGroupInfo struct {
	Name       string
	Path       string
	DataPool   string
	QuotaBytes int64
	BytesUsed  int64
	CreatedAt  string
}

// SubvolumeInfo reports a native subvolume, including its real mount path
// and RADOS namespace. Read-only queries also describe resources created outside
// these helpers, but those resources cannot be resized or removed through an
// owned handle. QuotaBytes is zero for unlimited.
type SubvolumeInfo struct {
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

// Identity fields are private so editing exported descriptors cannot redirect a
// mutation to another filesystem, group or subvolume. the setup gate serializes
// all reads and mutations of this state with the native create preflight.
type cephFSVolumeIdentity struct {
	filesystem                   *Filesystem
	filesystemID                 int64
	name, group, path, createdAt string
	ready, removed               bool
	removalAttempted             bool
	authorizations               map[*cephFSAuthorizationIdentity]struct{}
}

var cephFSVolumeName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func validateCephFSVolumeName(name string, optional bool) error {
	if (name == "" && optional) || cephFSVolumeName.MatchString(name) {
		return nil
	}
	return servicehook.Invalid(errors.New("subvolume and group names must start with a letter or digit and use at most 128 letters, digits, dots, underscores or dashes"))
}

func (fs *Filesystem) validateSubvolumeConfig(name, group, dataPool string, size int64) error {
	if err := validateCephFSVolumeName(name, false); err != nil {
		return err
	}
	if err := validateCephFSVolumeName(group, true); err != nil {
		return err
	}
	if size < 0 {
		return servicehook.Invalid(errors.New("subvolume quota must not be negative; zero means unlimited"))
	}
	if fs == nil || fs.base().Cluster() == nil {
		return servicehook.Invalid(errors.New("CephFS filesystem is unavailable"))
	}
	if dataPool == "" {
		return nil
	}
	if err := validateExistingPool(dataPool); err != nil {
		return err
	}
	// Native membership is checked after acquiring the setup gate. Initial
	// config names cannot describe dynamically attached or detached pools.
	return nil
}

// beginSubvolumeOperation holds the existing filesystem setup lock until done.
// It enables only MGR volumes, then waits for that CLI to recognize the existing
// filesystem; using fs volume create would invoke an orchestrator and create
// resources outside this fixture's ownership.
func (fs *Filesystem) beginSubvolumeOperation(ctx context.Context) (context.Context, int64, func(), error) {
	ctx, done, err := fs.base().Admit(ctx, fsbase.AdmissionSpec{
		RecheckCanceled: true,
		Refusal:         "CephFS filesystem must be an active filesystem created by this cluster",
		Check: func(ctx context.Context) (bool, error) {
			c := fs.base().Cluster()
			if !fs.base().Registered() || !fs.base().DescriptorIntact() || fs.base().Native() == nil {
				return false, nil
			}
			control, err := c.ControlContainer(ctx)
			if err != nil {
				return false, err
			}
			return control != nil, nil
		},
	})
	if err != nil {
		return ctx, 0, func() {}, err
	}
	state, err := fs.base().ReadNativePools(ctx)
	if err != nil {
		done()
		return ctx, 0, func() {}, err
	}
	if err := fs.base().Volumes().EnsureVolumesModule(ctx); err != nil {
		done()
		return ctx, 0, func() {}, err
	}
	return ctx, state.ID, done, nil
}

func appendSubvolumeLayout(args []string, size int64, pool string) []string {
	if size > 0 {
		args = append(args, "--size", strconv.FormatInt(size, 10))
	}
	if pool != "" {
		args = append(args, "--pool_layout", pool)
	}
	return args
}

// CreateSubvolumeGroup creates a fresh group. Native Ceph's create is idempotent
// and can change an existing group's quota/layout; this helper rejects duplicates
// before issuing create. Do not race external subvolume/group commands with these
// operations. A non-nil handle on error records an attempted creation, but only a
// fully confirmed identity can be mutated. Cluster termination cleans up the
// entire disposable filesystem even after partial creation.
//
//purpose:construction
func (fs *Filesystem) CreateSubvolumeGroup(ctx context.Context, config SubvolumeGroupConfig) (*SubvolumeGroup, error) {
	if err := fs.validateSubvolumeConfig(config.Name, "", config.DataPool, config.SizeBytes); err != nil {
		return nil, err
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := fs.base().CheckSubvolumeDataPool(ctx, config.DataPool); err != nil {
		return nil, err
	}
	names, err := fs.base().Volumes().SubvolumeGroupNames(ctx)
	if err != nil {
		return nil, err
	}
	if slices.Contains(names, config.Name) {
		return nil, servicehook.Invalid(fmt.Errorf("subvolume group %q already exists", config.Name))
	}
	group := &SubvolumeGroup{Name: config.Name, FilesystemName: fs.base().Name(),
		identity: &cephFSVolumeIdentity{filesystem: fs, filesystemID: fsID, name: config.Name}}
	args := appendSubvolumeLayout([]string{"fs", "subvolumegroup", "create", fs.base().Name(), config.Name}, config.SizeBytes, config.DataPool)
	if _, err := fs.base().Cluster().Ceph(ctx, args...); err != nil {
		return group, fmt.Errorf("create subvolume group %q: %w", config.Name, err)
	}
	info, err := fs.base().Volumes().SubvolumeGroupInfo(ctx, config.Name)
	if err != nil {
		return group, err
	}
	if (config.DataPool != "" && info.DataPool != config.DataPool) || info.QuotaBytes != config.SizeBytes {
		return group, errors.New("created subvolume group does not match requested layout or quota")
	}
	group.Path = info.Path
	group.identity.path, group.identity.createdAt, group.identity.ready = info.Path, info.CreatedAt, true
	return group, nil
}

// CreateSubvolume creates a fresh subvolume in the default or an existing named
// group. It rejects duplicates and unknown filesystem data pools before create.
// A non-nil result on failure identifies an attempted resource; it never removes
// partial data. Only successfully confirmed handles support Resize/Remove.
//
//purpose:construction
func (fs *Filesystem) CreateSubvolume(ctx context.Context, config SubvolumeConfig) (*Subvolume, error) {
	if err := fs.validateSubvolumeConfig(config.Name, config.GroupName, config.DataPool, config.SizeBytes); err != nil {
		return nil, err
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := fs.base().CheckSubvolumeCreationPool(ctx, config.DataPool, config.GroupName); err != nil {
		return nil, err
	}
	names, err := fs.base().Volumes().SubvolumeNames(ctx, config.GroupName)
	if err != nil {
		return nil, err
	}
	if slices.Contains(names, config.Name) {
		return nil, servicehook.Invalid(fmt.Errorf("subvolume %q in group %q already exists", config.Name, config.GroupName))
	}
	subvolume := &Subvolume{Name: config.Name, GroupName: config.GroupName, FilesystemName: fs.base().Name(),
		identity: &cephFSVolumeIdentity{filesystem: fs, filesystemID: fsID, name: config.Name, group: config.GroupName}}
	args := fsbase.AppendGroup([]string{"fs", "subvolume", "create", fs.base().Name(), config.Name}, config.GroupName)
	args = appendSubvolumeLayout(args, config.SizeBytes, config.DataPool)
	if config.NamespaceIsolated {
		args = append(args, "--namespace-isolated")
	}
	if _, err := fs.base().Cluster().Ceph(ctx, args...); err != nil {
		return subvolume, fmt.Errorf("create subvolume %q: %w", config.Name, err)
	}
	info, err := fs.base().Volumes().SubvolumeInfo(ctx, config.Name, config.GroupName)
	if err != nil {
		return subvolume, err
	}
	if info.State != "complete" || info.Type != "subvolume" || info.QuotaBytes != config.SizeBytes ||
		(config.DataPool != "" && info.DataPool != config.DataPool) || (config.NamespaceIsolated && info.PoolNamespace == "") {
		return subvolume, errors.New("created subvolume does not match requested state, layout, quota or namespace")
	}
	subvolume.Path = info.Path
	subvolume.identity.path, subvolume.identity.createdAt, subvolume.identity.ready = info.Path, info.CreatedAt, true
	return subvolume, nil
}

// SubvolumeGroups lists native named groups, sorted by name. It omits Ceph's
// internal default group and includes groups created outside these helpers.
//
//purpose:observation
func (fs *Filesystem) SubvolumeGroups(ctx context.Context) ([]string, error) {
	ctx, _, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return fs.base().Volumes().SubvolumeGroupNames(ctx)
}

// Subvolumes lists native names in groupName, sorted by name. Empty selects the
// default group. Native snapshot-retained entries can appear in this listing.
//
//purpose:observation
func (fs *Filesystem) Subvolumes(ctx context.Context, groupName string) ([]string, error) {
	if err := validateCephFSVolumeName(groupName, true); err != nil {
		return nil, err
	}
	ctx, _, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return fs.base().Volumes().SubvolumeNames(ctx, groupName)
}

// SubvolumeGroupInfo queries the current native group quota/layout and path.
//
//purpose:observation
func (fs *Filesystem) SubvolumeGroupInfo(ctx context.Context, name string) (*SubvolumeGroupInfo, error) {
	if err := validateCephFSVolumeName(name, false); err != nil {
		return nil, err
	}
	ctx, _, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	info, err := fs.base().Volumes().SubvolumeGroupInfo(ctx, name)
	return (*SubvolumeGroupInfo)(info), err
}

// SubvolumeInfo queries current metadata. name identifies the subvolume and an
// empty groupName selects the default group. A snapshot-retained entry lacks a
// usable Path and is never accepted as an active owned resource.
//
//purpose:observation
func (fs *Filesystem) SubvolumeInfo(ctx context.Context, name, groupName string) (*SubvolumeInfo, error) {
	if err := validateCephFSVolumeName(name, false); err != nil {
		return nil, err
	}
	if err := validateCephFSVolumeName(groupName, true); err != nil {
		return nil, err
	}
	ctx, _, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	info, err := fs.base().Volumes().SubvolumeInfo(ctx, name, groupName)
	return (*SubvolumeInfo)(info), err
}

func (fs *Filesystem) validateVolumeHandle(identity *cephFSVolumeIdentity) error {
	if fs == nil || identity == nil || identity.filesystem != fs || !identity.ready {
		return servicehook.Invalid(errors.New("subvolume or group must be a confirmed resource created by this filesystem"))
	}
	return nil
}

func (fs *Filesystem) checkVolumeIdentity(fsID int64, identity *cephFSVolumeIdentity, currentPath, createdAt string) error {
	if identity.filesystemID != fsID || identity.path != currentPath || identity.createdAt != createdAt {
		return errors.New("CephFS resource was replaced outside this fixture; refusing mutation")
	}
	return nil
}

func cephFSResizeQuota(size int64) (string, error) {
	if size < 0 {
		return "", errors.New("subvolume quota must not be negative; zero means unlimited")
	}
	if size == 0 {
		return "inf", nil
	}
	return strconv.FormatInt(size, 10), nil
}

// Resize changes only an owned group's quota. Zero removes its
// limit. --no_shrink refuses a positive limit below native current usage; it
// never truncates files. Directory accounting and quota enforcement are native
// CephFS behavior and can lag concurrent writes.
//
//purpose:construction,condition
func (group *SubvolumeGroup) Resize(ctx context.Context, sizeBytes int64) error {
	fs, err := group.filesystem()
	if err != nil {
		return err
	}
	quota, err := cephFSResizeQuota(sizeBytes)
	if err != nil {
		return err
	}
	if group == nil {
		return errors.New("subvolume group is unavailable")
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	if group.identity.removed {
		return errors.New("subvolume group has been removed")
	}
	info, err := fs.base().Volumes().SubvolumeGroupInfo(ctx, group.identity.name)
	if err != nil {
		return err
	}
	if err := fs.checkVolumeIdentity(fsID, group.identity, info.Path, info.CreatedAt); err != nil {
		return err
	}
	_, err = fs.base().Cluster().Ceph(ctx, "fs", "subvolumegroup", "resize", fs.base().Name(), group.identity.name, quota, "--no_shrink")
	return err
}

// Resize changes only an owned subvolume's quota, with the same zero
// and --no_shrink behavior as SubvolumeGroup.Resize.
//
//purpose:construction,condition
func (subvolume *Subvolume) Resize(ctx context.Context, sizeBytes int64) error {
	fs, err := subvolume.filesystem()
	if err != nil {
		return err
	}
	quota, err := cephFSResizeQuota(sizeBytes)
	if err != nil {
		return err
	}
	if subvolume == nil {
		return errors.New("subvolume is unavailable")
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	if subvolume.identity.removed {
		return errors.New("subvolume has been removed")
	}
	info, err := fs.base().Volumes().SubvolumeInfo(ctx, subvolume.identity.name, subvolume.identity.group)
	if err != nil {
		return err
	}
	if err := fs.checkVolumeIdentity(fsID, subvolume.identity, info.Path, info.CreatedAt); err != nil {
		return err
	}
	args := fsbase.AppendGroup([]string{"fs", "subvolume", "resize", fs.base().Name(), subvolume.identity.name, quota}, subvolume.identity.group)
	_, err = fs.base().Cluster().Ceph(ctx, append(args, "--no_shrink")...)
	return err
}

// Remove removes an owned empty group without --force. A native
// refusal (for example a nonempty group) preserves its handle for retry. A repeat
// call after confirmed success is a no-op. External edits must not race removal.
//
//purpose:lifetime
func (group *SubvolumeGroup) Remove(ctx context.Context) error {
	fs, err := group.filesystem()
	if err != nil {
		return err
	}
	if group == nil {
		return errors.New("subvolume group is unavailable")
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	if group.identity.removed {
		return nil
	}
	if group.identity.filesystemID != fsID {
		return servicehook.Invalid(errors.New("CephFS filesystem was replaced outside this fixture; refusing removal"))
	}
	if group.identity.removalAttempted {
		names, err := fs.base().Volumes().SubvolumeGroupNames(ctx)
		if err != nil {
			return err
		}
		if !slices.Contains(names, group.identity.name) {
			group.identity.removed = true
			return nil
		}
	}
	info, err := fs.base().Volumes().SubvolumeGroupInfo(ctx, group.identity.name)
	if err != nil {
		return err
	}
	if err := fs.checkVolumeIdentity(fsID, group.identity, info.Path, info.CreatedAt); err != nil {
		return err
	}
	group.identity.removalAttempted = true
	if _, err := fs.base().Cluster().Ceph(ctx, "fs", "subvolumegroup", "rm", fs.base().Name(), group.identity.name); err != nil {
		return err
	}
	group.identity.removed = true
	return nil
}

// Remove removes this owned directory tree and its contents without
// --force or --retain-snapshots. Native Ceph refuses subvolumes with snapshots;
// use the snapshot helpers to remove owned snapshots first. Trash purging is async.
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
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	if subvolume.identity.removed {
		return nil
	}
	for authorization := range subvolume.identity.authorizations {
		if !authorization.deauthorized {
			return errors.New("subvolume has an owned or partial authorization; deauthorize it before removing data")
		}
	}
	if subvolume.identity.filesystemID != fsID {
		return servicehook.Invalid(errors.New("CephFS filesystem was replaced outside this fixture; refusing removal"))
	}
	if subvolume.identity.removalAttempted {
		names, err := fs.base().Volumes().SubvolumeNames(ctx, subvolume.identity.group)
		if err != nil {
			return err
		}
		if !slices.Contains(names, subvolume.identity.name) {
			subvolume.identity.removed = true
			return nil
		}
	}
	info, err := fs.base().Volumes().SubvolumeInfo(ctx, subvolume.identity.name, subvolume.identity.group)
	if err != nil {
		return err
	}
	if err := fs.checkVolumeIdentity(fsID, subvolume.identity, info.Path, info.CreatedAt); err != nil {
		return err
	}
	args := fsbase.AppendGroup([]string{"fs", "subvolume", "rm", fs.base().Name(), subvolume.identity.name}, subvolume.identity.group)
	subvolume.identity.removalAttempted = true
	if _, err := fs.base().Cluster().Ceph(ctx, args...); err != nil {
		return err
	}
	subvolume.identity.removed = true
	return nil
}
