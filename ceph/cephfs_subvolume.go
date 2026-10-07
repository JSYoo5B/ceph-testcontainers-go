package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// CephFSSubvolumeGroupConfig creates a named group inside an existing filesystem.
// SizeBytes is its aggregate directory quota; zero leaves it unlimited. DataPool
// optionally selects one of the filesystem's existing data pools. These helpers
// do not create pools, filesystems, MDS daemons or orchestrator services.
type CephFSSubvolumeGroupConfig struct {
	Name      string
	SizeBytes int64
	DataPool  string
}

// CephFSSubvolumeConfig creates a directory tree managed by the MGR volumes
// module. An empty GroupName selects Ceph's default group. Zero SizeBytes leaves
// it unlimited. DataPool selects an existing filesystem data pool; empty inherits
// the group's layout. NamespaceIsolated gives file data a separate RADOS
// namespace; client access still requires appropriate Cephx capabilities.
type CephFSSubvolumeConfig struct {
	Name              string
	GroupName         string
	SizeBytes         int64
	DataPool          string
	NamespaceIsolated bool
}

// CephFSSubvolumeGroup is an owned group returned by CreateSubvolumeGroup.
// Public fields are creation-time descriptions; mutations use the private
// identity captured after successful creation. Get current quota through
// SubvolumeGroupInfo. Removing a group requires it to be empty.
type CephFSSubvolumeGroup struct {
	Name           string
	FilesystemName string
	Path           string
	identity       *cephFSVolumeIdentity
}

// CephFSSubvolume is an owned subvolume returned by CreateSubvolume. Its public
// fields describe creation. SubvolumeInfo queries current native state. Remove
// deletes this directory tree and its contents, preserving its filesystem and
// pools; native trash purging is asynchronous.
type CephFSSubvolume struct {
	Name           string
	GroupName      string
	FilesystemName string
	Path           string
	identity       *cephFSVolumeIdentity
}

// CephFSSubvolumeGroupInfo reports native group metadata. QuotaBytes is zero for
// an unlimited quota. BytesUsed is Ceph's directory size accounting, which can
// lag recent writes. CreatedAt is the directory birth time used by the native
// volumes module.
type CephFSSubvolumeGroupInfo struct {
	Name       string
	Path       string
	DataPool   string
	QuotaBytes int64
	BytesUsed  int64
	CreatedAt  string
}

// CephFSSubvolumeInfo reports a native subvolume, including its real mount path
// and RADOS namespace. Read-only queries also describe resources created outside
// these helpers, but those resources cannot be resized or removed through an
// owned handle. QuotaBytes is zero for unlimited.
type CephFSSubvolumeInfo struct {
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
// mutation to another filesystem, group or subvolume. cephfsSetupMu serializes
// all reads and mutations of this state with the native create preflight.
type cephFSVolumeIdentity struct {
	filesystem                   *CephFSContainer
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
	return errors.New("subvolume and group names must start with a letter or digit and use at most 128 letters, digits, dots, underscores or dashes")
}

func (fs *CephFSContainer) validateSubvolumeConfig(name, group, dataPool string, size int64) error {
	if err := validateCephFSVolumeName(name, false); err != nil {
		return err
	}
	if err := validateCephFSVolumeName(group, true); err != nil {
		return err
	}
	if size < 0 {
		return errors.New("subvolume quota must not be negative; zero means unlimited")
	}
	if fs == nil || fs.cluster == nil {
		return errors.New("CephFS filesystem is unavailable")
	}
	if dataPool == "" {
		return nil
	}
	if err := validateExistingPoolName(dataPool); err != nil {
		return err
	}
	// Native membership is checked after acquiring cephfsSetupMu. Initial
	// config names cannot describe dynamically attached or detached pools.
	return nil
}

// beginSubvolumeOperation holds the existing filesystem setup lock until done.
// It enables only MGR volumes, then waits for that CLI to recognize the existing
// filesystem; using fs volume create would invoke an orchestrator and create
// resources outside this fixture's ownership.
func (fs *CephFSContainer) beginSubvolumeOperation(ctx context.Context) (context.Context, int64, func(), error) {
	if err := ctx.Err(); err != nil {
		return ctx, 0, func() {}, err
	}
	if fs == nil || fs.cluster == nil {
		return ctx, 0, func() {}, errors.New("CephFS filesystem is unavailable")
	}
	c := fs.cluster
	if err := lockTopologyMutex(ctx, &c.cephfsSetupMu); err != nil {
		return ctx, 0, func() {}, err
	}
	if err := ctx.Err(); err != nil {
		c.cephfsSetupMu.Unlock()
		return ctx, 0, func() {}, err
	}
	if err := c.lockTopology(ctx); err != nil {
		c.cephfsSetupMu.Unlock()
		return ctx, 0, func() {}, err
	}
	valid := !c.closed && c.filesystems[fs.config.Name] == fs && fs.FilesystemName == fs.config.Name && fs.nativeIdentity != nil
	timeout := c.settings.startupTimeout
	if valid {
		control, err := c.ControlContainerContext(ctx)
		if err != nil {
			c.mu.Unlock()
			c.cephfsSetupMu.Unlock()
			return ctx, 0, func() {}, err
		}
		valid = control != nil
	}
	c.mu.Unlock()
	if !valid {
		c.cephfsSetupMu.Unlock()
		return ctx, 0, func() {}, errors.New("CephFS filesystem must be an active filesystem created by this cluster")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	done := func() { cancel(); c.cephfsSetupMu.Unlock() }
	state, err := fs.readNativePools(ctx)
	if err != nil {
		done()
		return ctx, 0, func() {}, err
	}
	if err := fs.ensureVolumesModule(ctx); err != nil {
		done()
		return ctx, 0, func() {}, err
	}
	return ctx, state.id, done, nil
}

func (fs *CephFSContainer) ensureVolumesModule(ctx context.Context) error {
	data, err := fs.cluster.Ceph(ctx, "mgr", "module", "ls", "--format", "json")
	if err != nil {
		return fmt.Errorf("inspect MGR volumes module: %w", err)
	}
	var modules struct {
		Enabled  []string        `json:"enabled_modules"`
		AlwaysOn json.RawMessage `json:"always_on_modules"`
	}
	if err := json.Unmarshal(data, &modules); err != nil {
		return fmt.Errorf("decode MGR modules: %w", err)
	}
	if modules.Enabled == nil && len(modules.AlwaysOn) == 0 {
		return errors.New("decode MGR modules: missing enabled/always-on module listing")
	}
	enabled := slices.Contains(modules.Enabled, "volumes")
	var always []string
	if json.Unmarshal(modules.AlwaysOn, &always) == nil {
		enabled = enabled || slices.Contains(always, "volumes")
	}
	if !enabled {
		if _, err := fs.cluster.Ceph(ctx, "mgr", "module", "enable", "volumes"); err != nil {
			return fmt.Errorf("enable MGR volumes module: %w", err)
		}
	}
	return fs.cluster.poll(ctx, func() (bool, error) {
		data, err := fs.cluster.Ceph(ctx, "fs", "volume", "ls", "--format", "json")
		if err != nil {
			return false, err
		}
		names, err := decodeCephFSVolumeNames(data)
		return slices.Contains(names, fs.config.Name), err
	})
}

func decodeCephFSVolumeNames(data []byte) ([]string, error) {
	var entries []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(data, &entries); err != nil || entries == nil {
		return nil, fmt.Errorf("decode CephFS resource listing: expected JSON array (error: %v)", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.Name == "" || slices.Contains(names, entry.Name) {
			return nil, errors.New("decode CephFS resource listing: empty or duplicate name")
		}
		names = append(names, entry.Name)
	}
	slices.Sort(names)
	return names, nil
}

func (fs *CephFSContainer) subvolumeNames(ctx context.Context, group string) ([]string, error) {
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "ls", fs.config.Name}, group)
	data, err := fs.cluster.Ceph(ctx, append(args, "--format", "json")...)
	if err != nil {
		return nil, err
	}
	return decodeCephFSVolumeNames(data)
}

func (fs *CephFSContainer) subvolumeGroupNames(ctx context.Context) ([]string, error) {
	data, err := fs.cluster.Ceph(ctx, "fs", "subvolumegroup", "ls", fs.config.Name, "--format", "json")
	if err != nil {
		return nil, err
	}
	return decodeCephFSVolumeNames(data)
}

func appendSubvolumeGroup(args []string, group string) []string {
	if group != "" {
		args = append(args, "--group_name", group)
	}
	return args
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
func (fs *CephFSContainer) CreateSubvolumeGroup(ctx context.Context, config CephFSSubvolumeGroupConfig) (*CephFSSubvolumeGroup, error) {
	if err := fs.validateSubvolumeConfig(config.Name, "", config.DataPool, config.SizeBytes); err != nil {
		return nil, err
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := fs.checkSubvolumeDataPool(ctx, config.DataPool); err != nil {
		return nil, err
	}
	names, err := fs.subvolumeGroupNames(ctx)
	if err != nil {
		return nil, err
	}
	if slices.Contains(names, config.Name) {
		return nil, fmt.Errorf("subvolume group %q already exists", config.Name)
	}
	group := &CephFSSubvolumeGroup{Name: config.Name, FilesystemName: fs.config.Name,
		identity: &cephFSVolumeIdentity{filesystem: fs, filesystemID: fsID, name: config.Name}}
	args := appendSubvolumeLayout([]string{"fs", "subvolumegroup", "create", fs.config.Name, config.Name}, config.SizeBytes, config.DataPool)
	if _, err := fs.cluster.Ceph(ctx, args...); err != nil {
		return group, fmt.Errorf("create subvolume group %q: %w", config.Name, err)
	}
	info, err := fs.subvolumeGroupInfo(ctx, config.Name)
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
func (fs *CephFSContainer) CreateSubvolume(ctx context.Context, config CephFSSubvolumeConfig) (*CephFSSubvolume, error) {
	if err := fs.validateSubvolumeConfig(config.Name, config.GroupName, config.DataPool, config.SizeBytes); err != nil {
		return nil, err
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := fs.checkSubvolumeCreationPool(ctx, config.DataPool, config.GroupName); err != nil {
		return nil, err
	}
	names, err := fs.subvolumeNames(ctx, config.GroupName)
	if err != nil {
		return nil, err
	}
	if slices.Contains(names, config.Name) {
		return nil, fmt.Errorf("subvolume %q in group %q already exists", config.Name, config.GroupName)
	}
	subvolume := &CephFSSubvolume{Name: config.Name, GroupName: config.GroupName, FilesystemName: fs.config.Name,
		identity: &cephFSVolumeIdentity{filesystem: fs, filesystemID: fsID, name: config.Name, group: config.GroupName}}
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "create", fs.config.Name, config.Name}, config.GroupName)
	args = appendSubvolumeLayout(args, config.SizeBytes, config.DataPool)
	if config.NamespaceIsolated {
		args = append(args, "--namespace-isolated")
	}
	if _, err := fs.cluster.Ceph(ctx, args...); err != nil {
		return subvolume, fmt.Errorf("create subvolume %q: %w", config.Name, err)
	}
	info, err := fs.subvolumeInfo(ctx, config.Name, config.GroupName)
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
func (fs *CephFSContainer) SubvolumeGroups(ctx context.Context) ([]string, error) {
	ctx, _, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return fs.subvolumeGroupNames(ctx)
}

// Subvolumes lists native names in groupName, sorted by name. Empty selects the
// default group. Native snapshot-retained entries can appear in this listing.
func (fs *CephFSContainer) Subvolumes(ctx context.Context, groupName string) ([]string, error) {
	if err := validateCephFSVolumeName(groupName, true); err != nil {
		return nil, err
	}
	ctx, _, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return fs.subvolumeNames(ctx, groupName)
}

// SubvolumeGroupInfo queries the current native group quota/layout and path.
func (fs *CephFSContainer) SubvolumeGroupInfo(ctx context.Context, name string) (*CephFSSubvolumeGroupInfo, error) {
	if err := validateCephFSVolumeName(name, false); err != nil {
		return nil, err
	}
	ctx, _, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return fs.subvolumeGroupInfo(ctx, name)
}

// SubvolumeInfo queries current metadata. name identifies the subvolume and an
// empty groupName selects the default group. A snapshot-retained entry lacks a
// usable Path and is never accepted as an active owned resource.
func (fs *CephFSContainer) SubvolumeInfo(ctx context.Context, name, groupName string) (*CephFSSubvolumeInfo, error) {
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
	return fs.subvolumeInfo(ctx, name, groupName)
}

type cephFSVolumeRawInfo struct {
	Path          string          `json:"path"`
	DataPool      string          `json:"data_pool"`
	PoolNamespace string          `json:"pool_namespace"`
	Quota         json.RawMessage `json:"bytes_quota"`
	BytesUsed     int64           `json:"bytes_used"`
	CreatedAt     string          `json:"created_at"`
	State         string          `json:"state"`
	Type          string          `json:"type"`
}

func decodeCephFSVolumeQuota(raw json.RawMessage) (int64, error) {
	if string(raw) == `"infinite"` {
		return 0, nil
	}
	var quota int64
	if err := json.Unmarshal(raw, &quota); err != nil || quota < 0 || string(raw) == "null" {
		return 0, errors.New("decode CephFS quota: expected a non-negative integer or infinite")
	}
	return quota, nil
}

func validateCephFSVolumeInfo(raw cephFSVolumeRawInfo) error {
	if raw.Path == "" || !path.IsAbs(raw.Path) || path.Clean(raw.Path) != raw.Path || raw.Path == "/" || raw.CreatedAt == "" || raw.DataPool == "" || raw.BytesUsed < 0 {
		return errors.New("decode CephFS volume metadata: missing or invalid path, birth time, pool or used bytes")
	}
	return nil
}

func (fs *CephFSContainer) subvolumeGroupInfo(ctx context.Context, name string) (*CephFSSubvolumeGroupInfo, error) {
	data, err := fs.cluster.Ceph(ctx, "fs", "subvolumegroup", "info", fs.config.Name, name, "--format", "json")
	if err != nil {
		return nil, err
	}
	var raw cephFSVolumeRawInfo
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("decode subvolume group info: %w", err)
	}
	data, err = fs.cluster.Ceph(ctx, "fs", "subvolumegroup", "getpath", fs.config.Name, name)
	if err != nil {
		return nil, err
	}
	raw.Path = strings.TrimSpace(string(data))
	if err := validateCephFSVolumeInfo(raw); err != nil {
		return nil, err
	}
	quota, err := decodeCephFSVolumeQuota(raw.Quota)
	if err != nil {
		return nil, err
	}
	return &CephFSSubvolumeGroupInfo{Name: name, Path: raw.Path, DataPool: raw.DataPool, QuotaBytes: quota, BytesUsed: raw.BytesUsed, CreatedAt: raw.CreatedAt}, nil
}

func (fs *CephFSContainer) subvolumeInfo(ctx context.Context, name, group string) (*CephFSSubvolumeInfo, error) {
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "info", fs.config.Name, name}, group)
	data, err := fs.cluster.Ceph(ctx, append(args, "--format", "json")...)
	if err != nil {
		return nil, err
	}
	var raw cephFSVolumeRawInfo
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("decode subvolume info: %w", err)
	}
	if raw.State == "snapshot-retained" {
		return &CephFSSubvolumeInfo{Name: name, GroupName: group, State: raw.State, Type: raw.Type}, nil
	}
	if err := validateCephFSVolumeInfo(raw); err != nil {
		return nil, err
	}
	quota, err := decodeCephFSVolumeQuota(raw.Quota)
	if err != nil {
		return nil, err
	}
	return &CephFSSubvolumeInfo{Name: name, GroupName: group, Path: raw.Path, DataPool: raw.DataPool, PoolNamespace: raw.PoolNamespace, QuotaBytes: quota, BytesUsed: raw.BytesUsed, CreatedAt: raw.CreatedAt, State: raw.State, Type: raw.Type}, nil
}

func (fs *CephFSContainer) validateVolumeHandle(identity *cephFSVolumeIdentity) error {
	if fs == nil || identity == nil || identity.filesystem != fs || !identity.ready {
		return errors.New("subvolume or group must be a confirmed resource created by this filesystem")
	}
	return nil
}

func (fs *CephFSContainer) checkVolumeIdentity(fsID int64, identity *cephFSVolumeIdentity, currentPath, createdAt string) error {
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

// ResizeSubvolumeGroup changes only an owned group's quota. Zero removes its
// limit. --no_shrink refuses a positive limit below native current usage; it
// never truncates files. Directory accounting and quota enforcement are native
// CephFS behavior and can lag concurrent writes.
func (fs *CephFSContainer) ResizeSubvolumeGroup(ctx context.Context, group *CephFSSubvolumeGroup, sizeBytes int64) error {
	quota, err := cephFSResizeQuota(sizeBytes)
	if err != nil {
		return err
	}
	if group == nil {
		return errors.New("subvolume group is unavailable")
	}
	if err := fs.validateVolumeHandle(group.identity); err != nil {
		return err
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	if group.identity.removed {
		return errors.New("subvolume group has been removed")
	}
	info, err := fs.subvolumeGroupInfo(ctx, group.identity.name)
	if err != nil {
		return err
	}
	if err := fs.checkVolumeIdentity(fsID, group.identity, info.Path, info.CreatedAt); err != nil {
		return err
	}
	_, err = fs.cluster.Ceph(ctx, "fs", "subvolumegroup", "resize", fs.config.Name, group.identity.name, quota, "--no_shrink")
	return err
}

// ResizeSubvolume changes only an owned subvolume's quota, with the same zero
// and --no_shrink behavior as ResizeSubvolumeGroup.
func (fs *CephFSContainer) ResizeSubvolume(ctx context.Context, subvolume *CephFSSubvolume, sizeBytes int64) error {
	quota, err := cephFSResizeQuota(sizeBytes)
	if err != nil {
		return err
	}
	if subvolume == nil {
		return errors.New("subvolume is unavailable")
	}
	if err := fs.validateVolumeHandle(subvolume.identity); err != nil {
		return err
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	if subvolume.identity.removed {
		return errors.New("subvolume has been removed")
	}
	info, err := fs.subvolumeInfo(ctx, subvolume.identity.name, subvolume.identity.group)
	if err != nil {
		return err
	}
	if err := fs.checkVolumeIdentity(fsID, subvolume.identity, info.Path, info.CreatedAt); err != nil {
		return err
	}
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "resize", fs.config.Name, subvolume.identity.name, quota}, subvolume.identity.group)
	_, err = fs.cluster.Ceph(ctx, append(args, "--no_shrink")...)
	return err
}

// RemoveSubvolumeGroup removes an owned empty group without --force. A native
// refusal (for example a nonempty group) preserves its handle for retry. A repeat
// call after confirmed success is a no-op. External edits must not race removal.
func (fs *CephFSContainer) RemoveSubvolumeGroup(ctx context.Context, group *CephFSSubvolumeGroup) error {
	if group == nil {
		return errors.New("subvolume group is unavailable")
	}
	if err := fs.validateVolumeHandle(group.identity); err != nil {
		return err
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
		return errors.New("CephFS filesystem was replaced outside this fixture; refusing removal")
	}
	if group.identity.removalAttempted {
		names, err := fs.subvolumeGroupNames(ctx)
		if err != nil {
			return err
		}
		if !slices.Contains(names, group.identity.name) {
			group.identity.removed = true
			return nil
		}
	}
	info, err := fs.subvolumeGroupInfo(ctx, group.identity.name)
	if err != nil {
		return err
	}
	if err := fs.checkVolumeIdentity(fsID, group.identity, info.Path, info.CreatedAt); err != nil {
		return err
	}
	group.identity.removalAttempted = true
	if _, err := fs.cluster.Ceph(ctx, "fs", "subvolumegroup", "rm", fs.config.Name, group.identity.name); err != nil {
		return err
	}
	group.identity.removed = true
	return nil
}

// RemoveSubvolume removes this owned directory tree and its contents without
// --force or --retain-snapshots. Native Ceph refuses subvolumes with snapshots;
// use the snapshot helpers to remove owned snapshots first. Trash purging is async.
// Repeat removal after confirmed success is a no-op; external edits must not
// race this operation. Its filesystem, groups and pools remain available.
func (fs *CephFSContainer) RemoveSubvolume(ctx context.Context, subvolume *CephFSSubvolume) error {
	if subvolume == nil {
		return errors.New("subvolume is unavailable")
	}
	if err := fs.validateVolumeHandle(subvolume.identity); err != nil {
		return err
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
		return errors.New("CephFS filesystem was replaced outside this fixture; refusing removal")
	}
	if subvolume.identity.removalAttempted {
		names, err := fs.subvolumeNames(ctx, subvolume.identity.group)
		if err != nil {
			return err
		}
		if !slices.Contains(names, subvolume.identity.name) {
			subvolume.identity.removed = true
			return nil
		}
	}
	info, err := fs.subvolumeInfo(ctx, subvolume.identity.name, subvolume.identity.group)
	if err != nil {
		return err
	}
	if err := fs.checkVolumeIdentity(fsID, subvolume.identity, info.Path, info.CreatedAt); err != nil {
		return err
	}
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "rm", fs.config.Name, subvolume.identity.name}, subvolume.identity.group)
	subvolume.identity.removalAttempted = true
	if _, err := fs.cluster.Ceph(ctx, args...); err != nil {
		return err
	}
	subvolume.identity.removed = true
	return nil
}
