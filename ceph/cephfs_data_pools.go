package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// CephFSDataPoolState reports a currently registered native filesystem data
// pool. ID distinguishes a pool recreated under the same name. Default marks
// the immutable first data pool, which also stores inode backtraces.
type CephFSDataPoolState struct {
	Name    string
	ID      int64
	Default bool
}

// CephFSDataPool identifies an additional pool attached by AddDataPool. Its
// private identity and state are shared by copied handles. Exported fields are
// descriptive; editing them does not redirect removal to another pool.
type CephFSDataPool struct {
	Name, FilesystemName string
	ID                   int64
	identity             *cephFSDataPoolIdentity
}

type cephFSNativeIdentity struct {
	id, metadataPool, defaultPool int64
	attachments                   map[string]*cephFSDataPoolIdentity
}

type cephFSDataPoolIdentity struct {
	filesystem                                 *CephFSContainer
	name                                       string
	id                                         int64
	confirmed, used, removed, removalAttempted bool
}

type cephFSNativePools struct {
	mapState         cephFSMap
	id, metadataPool int64
	dataPools        []int64
	pools            []PoolState
}

// captureNativePoolIdentity is called during filesystem creation, never on
// first use of a handle. Failed setup descriptors cannot adopt a later FS.
func (fs *CephFSContainer) captureNativePoolIdentity(ctx context.Context) error {
	state, err := fs.readNativePools(ctx)
	if err != nil {
		return err
	}
	if len(state.dataPools) == 0 {
		return errors.New("created filesystem has no default data pool")
	}
	metadata, err := state.poolByID(state.metadataPool)
	if err != nil {
		return err
	}
	data, err := state.poolByID(state.dataPools[0])
	if err != nil {
		return err
	}
	if metadata.Name != fs.config.MetadataPool.Name || data.Name != fs.config.DataPool.Name {
		return errors.New("created filesystem pool names do not match requested pools")
	}
	fs.cluster.mu.Lock()
	fs.nativeIdentity = &cephFSNativeIdentity{id: state.id, metadataPool: metadata.ID, defaultPool: data.ID,
		attachments: make(map[string]*cephFSDataPoolIdentity)}
	fs.cluster.mu.Unlock()
	return nil
}

func (fs *CephFSContainer) readNativePools(ctx context.Context) (*cephFSNativePools, error) {
	data, err := fs.cluster.Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		return nil, err
	}
	var fsmap cephFSMap
	if err := json.Unmarshal(data, &fsmap); err != nil || fsmap.Filesystems == nil {
		return nil, errors.New("decode filesystem pool map")
	}
	var result *cephFSNativePools
	for _, native := range fsmap.Filesystems {
		if native.MDSMap.Name != fs.config.Name {
			continue
		}
		if result != nil || native.ID < 0 || native.MDSMap.MetadataPool < 0 || len(native.MDSMap.DataPools) == 0 {
			return nil, errors.New("incomplete or duplicate native filesystem pool map")
		}
		result = &cephFSNativePools{mapState: fsmap, id: native.ID, metadataPool: native.MDSMap.MetadataPool,
			dataPools: slices.Clone(native.MDSMap.DataPools)}
	}
	if result == nil {
		return nil, errors.New("owned filesystem is absent from FSMap")
	}
	result.pools, err = fs.cluster.poolStates(ctx)
	if err != nil {
		return nil, err
	}
	if identity := fs.nativeIdentity; identity != nil {
		if result.id != identity.id || result.metadataPool != identity.metadataPool || result.dataPools[0] != identity.defaultPool {
			return nil, errors.New("filesystem or its original pools were replaced; refusing operation")
		}
		for _, original := range []struct {
			id   int64
			name string
		}{
			{identity.metadataPool, fs.config.MetadataPool.Name}, {identity.defaultPool, fs.config.DataPool.Name},
		} {
			pool, err := result.poolByID(original.id)
			if err != nil || pool.Name != original.name {
				return nil, errors.New("original filesystem pool was replaced or renamed")
			}
		}
	}
	return result, nil
}

func (state *cephFSNativePools) poolByID(id int64) (PoolState, error) {
	for _, pool := range state.pools {
		if pool.ID == id {
			return pool, nil
		}
	}
	return PoolState{}, fmt.Errorf("native pool ID %d is absent", id)
}

func (state *cephFSNativePools) poolByName(name string) (PoolState, error) {
	for _, pool := range state.pools {
		if pool.Name == name {
			return pool, nil
		}
	}
	return PoolState{}, fmt.Errorf("native pool %q is absent", name)
}

func (fs *CephFSContainer) beginDataPoolOperation(ctx context.Context) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return ctx, func() {}, err
	}
	if fs == nil || fs.cluster == nil {
		return ctx, func() {}, errors.New("CephFS filesystem is unavailable")
	}
	c := fs.cluster
	if err := lockTopologyMutex(ctx, &c.cephfsSetupMu); err != nil {
		return ctx, func() {}, err
	}
	if err := ctx.Err(); err != nil {
		c.cephfsSetupMu.Unlock()
		return ctx, func() {}, err
	}
	if err := c.lockTopology(ctx); err != nil {
		c.cephfsSetupMu.Unlock()
		return ctx, func() {}, err
	}
	valid := !c.closed && c.filesystems[fs.config.Name] == fs && fs.nativeIdentity != nil
	timeout := c.settings.startupTimeout
	if valid {
		control, err := c.ControlContainerContext(ctx)
		if err != nil {
			c.mu.Unlock()
			c.cephfsSetupMu.Unlock()
			return ctx, func() {}, err
		}
		valid = control != nil
	}
	c.mu.Unlock()
	if !valid {
		c.cephfsSetupMu.Unlock()
		return ctx, func() {}, errors.New("filesystem must have a confirmed creation identity in this active cluster")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	return ctx, func() { cancel(); c.cephfsSetupMu.Unlock() }, nil
}

// DataPools reads the authoritative FSMap registration and current pool IDs.
// AdditionalDataPools in the creation descriptor describes initial settings;
// this method also reports subsequent native attachments and removals.
func (fs *CephFSContainer) DataPools(ctx context.Context) ([]CephFSDataPoolState, error) {
	ctx, done, err := fs.beginDataPoolOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	state, err := fs.readNativePools(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]CephFSDataPoolState, 0, len(state.dataPools))
	for i, id := range state.dataPools {
		pool, err := state.poolByID(id)
		if err != nil {
			return nil, err
		}
		result = append(result, CephFSDataPoolState{Name: pool.Name, ID: id, Default: i == 0})
	}
	return result, nil
}

func (fs *CephFSContainer) poolApplications(ctx context.Context, name string) (map[string]map[string]string, error) {
	data, err := fs.cluster.Ceph(ctx, "osd", "pool", "application", "get", name, "--format", "json")
	if err != nil {
		return nil, err
	}
	var apps map[string]map[string]string
	if err := json.Unmarshal(data, &apps); err != nil || apps == nil {
		return nil, errors.New("decode pool application metadata")
	}
	return apps, nil
}

func cephFSPoolHandle(identity *cephFSDataPoolIdentity) *CephFSDataPool {
	return &CephFSDataPool{Name: identity.name, ID: identity.id, FilesystemName: identity.filesystem.config.Name, identity: identity}
}

// AddDataPool attaches an existing pool with application=cephfs and no prior
// filesystem role. Replicated pools and EC pools with allow_ec_overwrites are
// accepted. Metadata pools, cross-filesystem reuse and repeated attachments are
// refused. It creates or deletes no pools. A handle returned with an error
// records an attempted attachment; retrying the same name reconciles an unknown
// successful command using the original native IDs. External edits must not
// race these operations. Ceph's retained application tag after detachment must
// be inspected and explicitly cleared through Ceph before a new attachment.
func (fs *CephFSContainer) AddDataPool(ctx context.Context, name string) (*CephFSDataPool, error) {
	if err := validateExistingPoolName(name); err != nil {
		return nil, err
	}
	ctx, done, err := fs.beginDataPoolOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	state, err := fs.readNativePools(ctx)
	if err != nil {
		return nil, err
	}
	pool, err := state.poolByName(name)
	if err != nil {
		return nil, err
	}
	previous := fs.nativeIdentity.attachments[name]
	if previous != nil && !previous.removed {
		if pool.ID != previous.id {
			return nil, errors.New("previously attempted data pool was replaced")
		}
		if previous.confirmed {
			return nil, errors.New("data pool attachment was already confirmed")
		}
		if slices.Contains(state.dataPools, pool.ID) {
			if err := fs.confirmDataPoolAttachment(ctx, previous); err != nil {
				return cephFSPoolHandle(previous), err
			}
			return cephFSPoolHandle(previous), nil
		}
	}
	for _, native := range state.mapState.Filesystems {
		if native.MDSMap.MetadataPool == pool.ID || slices.Contains(native.MDSMap.DataPools, pool.ID) {
			return nil, errors.New("pool is already a filesystem metadata or data pool")
		}
	}
	if pool.Type != "replicated" && (pool.Type != "erasure" || !slices.Contains(strings.Split(pool.Flags, ","), "ec_overwrites")) {
		return nil, errors.New("CephFS EC data pools require allow_ec_overwrites")
	}
	apps, err := fs.poolApplications(ctx, name)
	if err != nil {
		return nil, err
	}
	metadata, ok := apps["cephfs"]
	if !ok || len(apps) != 1 || metadata["data"] != "" || metadata["metadata"] != "" {
		return nil, errors.New("pool must have only application=cephfs and no filesystem role; detached native tags require explicit inspection/cleanup")
	}
	identity := previous
	if identity == nil || identity.removed {
		identity = &cephFSDataPoolIdentity{filesystem: fs, name: name, id: pool.ID}
		fs.nativeIdentity.attachments[name] = identity
	}
	handle := cephFSPoolHandle(identity)
	if _, err := fs.cluster.Ceph(ctx, "fs", "add_data_pool", fs.config.Name, strconv.FormatInt(pool.ID, 10)); err != nil {
		return handle, fmt.Errorf("attach CephFS data pool: %w", err)
	}
	if err := fs.confirmDataPoolAttachment(ctx, identity); err != nil {
		return handle, err
	}
	return handle, nil
}

func (fs *CephFSContainer) confirmDataPoolAttachment(ctx context.Context, identity *cephFSDataPoolIdentity) error {
	state, err := fs.readNativePools(ctx)
	if err != nil {
		return err
	}
	pool, err := state.poolByName(identity.name)
	if err != nil || pool.ID != identity.id || !slices.Contains(state.dataPools, identity.id) {
		return errors.New("attempted data pool registration was not confirmed with its original identity")
	}
	apps, err := fs.poolApplications(ctx, identity.name)
	if err != nil {
		return err
	}
	if len(apps) != 1 || apps["cephfs"]["data"] != fs.config.Name || apps["cephfs"]["metadata"] != "" {
		return errors.New("attached pool's native application role was not confirmed")
	}
	identity.confirmed = true
	return nil
}

// checkSubvolumeDataPool runs while cephfsSetupMu is held. Selection is sticky even after a partial
// native create, so removal never assumes unknown partial resources are safe.
func (fs *CephFSContainer) checkSubvolumeDataPool(ctx context.Context, name string) error {
	if fs.nativeIdentity == nil {
		return errors.New("filesystem has no confirmed creation identity")
	}
	state, err := fs.readNativePools(ctx)
	if err != nil {
		return err
	}
	if name == "" {
		return nil
	}
	pool, err := state.poolByName(name)
	if err != nil {
		return err
	}
	if !slices.Contains(state.dataPools, pool.ID) {
		return errors.New("selected pool is not currently registered to this filesystem")
	}
	if identity := fs.nativeIdentity.attachments[name]; identity != nil && identity.id == pool.ID {
		identity.used = true
	}
	return nil
}

// A named group's layout can select an additional pool even when callers
// omit DataPool. Record that selection before the native create, including
// unknown-success failures, so later group removal cannot erase its history.
func (fs *CephFSContainer) checkSubvolumeCreationPool(ctx context.Context, name, group string) error {
	if name == "" && group != "" && fs.nativeIdentity != nil {
		info, err := fs.subvolumeGroupInfo(ctx, group)
		if err != nil {
			return err
		}
		name = info.DataPool
	}
	return fs.checkSubvolumeDataPool(ctx, name)
}

// RemoveUnusedDataPool removes only the FSMap registration of a fresh helper
// attachment. A pool selected even once by a managed group/subvolume/clone is
// refused, including partial creations. Native groups/subvolumes must have no
// references and every RADOS namespace must be empty; unknown clone or retained
// snapshot information is conservatively refused. No pool, application tag or
// data is deleted. Native rm_data_pool cannot detect arbitrary POSIX layout
// references: the caller MUST stop external clients and reset all such layouts
// before removal, and prevent external edits from racing it. This method does
// not establish safety for a live workload. Default data pools are immutable.
// A copied handle shares removal state; retry reconciles a lost command reply.
func (fs *CephFSContainer) RemoveUnusedDataPool(ctx context.Context, attachment *CephFSDataPool) error {
	if fs == nil || attachment == nil || attachment.identity == nil || attachment.identity.filesystem != fs {
		return errors.New("data pool attachment must have been created by this filesystem")
	}
	ctx, done, err := fs.beginDataPoolOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	identity := attachment.identity
	if identity.removed {
		return nil
	}
	if fs.nativeIdentity.attachments[identity.name] != identity || identity.id == fs.nativeIdentity.defaultPool || identity.used {
		return errors.New("only an unused additional pool attachment may be removed")
	}
	state, err := fs.readNativePools(ctx)
	if err != nil {
		return err
	}
	pool, err := state.poolByName(identity.name)
	if err != nil || pool.ID != identity.id {
		return errors.New("attached pool was replaced or renamed")
	}
	if !slices.Contains(state.dataPools, identity.id) {
		if identity.removalAttempted {
			identity.removed = true
			return nil
		}
		return errors.New("data pool was detached externally; refusing removal")
	}
	if !identity.confirmed {
		return errors.New("attachment must be confirmed before removal; retry AddDataPool first")
	}
	if err := fs.confirmDataPoolAttachment(ctx, identity); err != nil {
		return err
	}
	if err := fs.checkUnusedDataPool(ctx, identity.name); err != nil {
		return err
	}
	identity.removalAttempted = true
	if _, err := fs.cluster.Ceph(ctx, "fs", "rm_data_pool", fs.config.Name, strconv.FormatInt(identity.id, 10)); err != nil {
		return err
	}
	state, err = fs.readNativePools(ctx)
	if err != nil {
		return err
	}
	if slices.Contains(state.dataPools, identity.id) {
		return errors.New("data pool detachment was not confirmed")
	}
	identity.removed = true
	return nil
}

func (fs *CephFSContainer) checkUnusedDataPool(ctx context.Context, name string) error {
	if err := fs.ensureVolumesModule(ctx); err != nil {
		return err
	}
	groups, err := fs.subvolumeGroupNames(ctx)
	if err != nil {
		return err
	}
	for _, group := range groups {
		info, err := fs.subvolumeGroupInfo(ctx, group)
		if err != nil {
			return fmt.Errorf("cannot prove group pool is unused: %w", err)
		}
		if info.DataPool == name {
			return errors.New("native subvolume group still selects this pool")
		}
	}
	for _, group := range append([]string{""}, groups...) {
		names, err := fs.subvolumeNames(ctx, group)
		if err != nil {
			return err
		}
		for _, volume := range names {
			info, err := fs.subvolumeInfo(ctx, volume, group)
			if err != nil {
				return fmt.Errorf("cannot prove subvolume/clone pool is unused: %w", err)
			}
			if info.DataPool == "" {
				return errors.New("cannot prove retained snapshot or incomplete subvolume pool is unused")
			}
			if info.DataPool == name {
				return errors.New("native subvolume or clone still selects this pool")
			}
		}
	}
	control, err := fs.cluster.ControlContainerContext(ctx)
	if err != nil {
		return fmt.Errorf("select data pool CLI container: %w", err)
	}
	data, err := command(ctx, control, "rados", "-p", name, "--all", "ls")
	if err != nil {
		return fmt.Errorf("inspect all data pool namespaces: %w", err)
	}
	if strings.TrimSpace(string(data)) != "" {
		return errors.New("data pool still contains RADOS objects")
	}
	return nil
}
