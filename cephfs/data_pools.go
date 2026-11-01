package cephfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// DataPoolState reports a currently registered native filesystem data
// pool. ID distinguishes a pool recreated under the same name. Default marks
// the immutable first data pool, which also stores inode backtraces.
type DataPoolState struct {
	Name    string
	ID      int64
	Default bool
}

// DataPool identifies an additional pool attached by AddDataPool. Its
// private identity and state are shared by copied handles. Exported fields are
// descriptive; editing them does not redirect removal to another pool.
type DataPool struct {
	Name, FilesystemName string
	ID                   int64
	identity             dataPoolIdentity
}

// dataPoolIdentity is the private identity of a DataPool handle: the
// filesystem that recorded the attachment, and the attachment record that
// filesystem's native identity holds, which copied handles share.
type dataPoolIdentity struct {
	filesystem *Filesystem
	record     *fsbase.PoolAttachment
}

func (fs *Filesystem) beginDataPoolOperation(ctx context.Context) (context.Context, func(), error) {
	return fs.base().Admit(ctx, fsbase.AdmissionSpec{
		RecheckCanceled: true,
		Refusal:         "filesystem must have a confirmed creation identity in this active cluster",
		Check: func(ctx context.Context) (bool, error) {
			c := fs.base().Cluster()
			if !fs.base().Registered() || fs.base().Native() == nil {
				return false, nil
			}
			control, err := c.ControlContainer(ctx)
			if err != nil {
				return false, err
			}
			return control != nil, nil
		},
	})
}

// DataPools reads the authoritative FSMap registration and current pool IDs.
// AdditionalDataPools in the creation descriptor describes initial settings;
// this method also reports subsequent native attachments and removals.
//
//purpose:observation
func (fs *Filesystem) DataPools(ctx context.Context) ([]DataPoolState, error) {
	ctx, done, err := fs.beginDataPoolOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	state, err := fs.base().ReadNativePools(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]DataPoolState, 0, len(state.DataPools))
	for i, id := range state.DataPools {
		pool, err := state.PoolByID(id)
		if err != nil {
			return nil, err
		}
		result = append(result, DataPoolState{Name: pool.Name, ID: id, Default: i == 0})
	}
	return result, nil
}

func poolApplications(ctx context.Context, v fsbase.VolumeCommands, name string) (map[string]map[string]string, error) {
	data, err := v.Cluster().Ceph(ctx, "osd", "pool", "application", "get", name, "--format", "json")
	if err != nil {
		return nil, err
	}
	var apps map[string]map[string]string
	if err := json.Unmarshal(data, &apps); err != nil || apps == nil {
		return nil, errors.New("decode pool application metadata")
	}
	return apps, nil
}

// cephFSPoolHandle returns a handle of the attachment record that fs's native
// identity holds.
func cephFSPoolHandle(fs *Filesystem, identity *fsbase.PoolAttachment) *DataPool {
	return &DataPool{Name: identity.Name, ID: identity.ID, FilesystemName: fs.base().Name(), identity: dataPoolIdentity{filesystem: fs, record: identity}}
}

// AddDataPool attaches an existing pool with application=cephfs and no prior
// filesystem role. Replicated pools and EC pools with allow_ec_overwrites are
// accepted. Metadata pools, cross-filesystem reuse and repeated attachments are
// refused. It creates or deletes no pools. A handle returned with an error
// records an attempted attachment; retrying the same name reconciles an unknown
// successful command using the original native IDs. External edits must not
// race these operations. Ceph's retained application tag after detachment must
// be inspected and explicitly cleared through Ceph before a new attachment.
//
// Clear the retained tag with ceph osd pool application rm <name> cephfs data
// before attaching a detached pool again.
//
//purpose:construction
func (fs *Filesystem) AddDataPool(ctx context.Context, name string) (*DataPool, error) {
	if err := validateExistingPool(name); err != nil {
		return nil, err
	}
	ctx, done, err := fs.beginDataPoolOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	state, err := fs.base().ReadNativePools(ctx)
	if err != nil {
		return nil, err
	}
	pool, err := state.PoolByName(name)
	if err != nil {
		return nil, err
	}
	previous := fs.base().Native().Attachments[name]
	if previous != nil && !previous.Removed {
		if pool.ID != previous.ID {
			return nil, errors.New("previously attempted data pool was replaced")
		}
		if previous.Confirmed {
			return nil, errors.New("data pool attachment was already confirmed")
		}
		if slices.Contains(state.DataPools, pool.ID) {
			if err := fs.confirmDataPoolAttachment(ctx, previous); err != nil {
				return cephFSPoolHandle(fs, previous), err
			}
			return cephFSPoolHandle(fs, previous), nil
		}
	}
	for _, native := range state.Map.Filesystems {
		if native.MDSMap.MetadataPool == pool.ID || slices.Contains(native.MDSMap.DataPools, pool.ID) {
			return nil, errors.New("pool is already a filesystem metadata or data pool")
		}
	}
	if pool.Type != "replicated" && (pool.Type != "erasure" || !slices.Contains(strings.Split(pool.Flags, ","), "ec_overwrites")) {
		return nil, errors.New("CephFS EC data pools require allow_ec_overwrites")
	}
	apps, err := poolApplications(ctx, fs.base().Volumes(), name)
	if err != nil {
		return nil, err
	}
	metadata, ok := apps["cephfs"]
	if !ok || len(apps) != 1 || metadata["data"] != "" || metadata["metadata"] != "" {
		return nil, errors.New("pool must have only application=cephfs and no filesystem role; detached native tags require explicit inspection/cleanup")
	}
	identity := previous
	if identity == nil || identity.Removed {
		identity = &fsbase.PoolAttachment{Name: name, ID: pool.ID}
		fs.base().Native().Attachments[name] = identity
	}
	handle := cephFSPoolHandle(fs, identity)
	if _, err := fs.base().Cluster().Ceph(ctx, "fs", "add_data_pool", fs.base().Name(), strconv.FormatInt(pool.ID, 10)); err != nil {
		return handle, fmt.Errorf("attach CephFS data pool: %w", err)
	}
	if err := fs.confirmDataPoolAttachment(ctx, identity); err != nil {
		return handle, err
	}
	return handle, nil
}

func (fs *Filesystem) confirmDataPoolAttachment(ctx context.Context, identity *fsbase.PoolAttachment) error {
	state, err := fs.base().ReadNativePools(ctx)
	if err != nil {
		return err
	}
	pool, err := state.PoolByName(identity.Name)
	if err != nil || pool.ID != identity.ID || !slices.Contains(state.DataPools, identity.ID) {
		return errors.New("attempted data pool registration was not confirmed with its original identity")
	}
	apps, err := poolApplications(ctx, fs.base().Volumes(), identity.Name)
	if err != nil {
		return err
	}
	if len(apps) != 1 || apps["cephfs"]["data"] != fs.base().Name() || apps["cephfs"]["metadata"] != "" {
		return errors.New("attached pool's native application role was not confirmed")
	}
	identity.Confirmed = true
	return nil
}

// RemoveUnused removes only the FSMap registration of a fresh helper
// attachment. A pool selected even once by a managed group/subvolume/clone is
// refused, including partial creations. Native groups/subvolumes must have no
// references and every RADOS namespace must be empty; unknown clone or retained
// snapshot information is conservatively refused. No pool, application tag or
// data is deleted. Native rm_data_pool cannot detect arbitrary POSIX layout
// references: the caller MUST stop external clients and reset all such layouts
// before removal, and prevent external edits from racing it. This method does
// not establish safety for a live workload. Default data pools are immutable.
// A copied handle shares removal state; retry reconciles a lost command reply.
//
//purpose:lifetime
func (attachment *DataPool) RemoveUnused(ctx context.Context) error {
	fs, err := attachment.filesystem()
	if err != nil {
		return err
	}
	if fs == nil || attachment == nil || attachment.identity.record == nil || attachment.identity.filesystem != fs {
		return errors.New("data pool attachment must have been created by this filesystem")
	}
	ctx, done, err := fs.beginDataPoolOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	identity := attachment.identity.record
	if identity.Removed {
		return nil
	}
	if fs.base().Native().Attachments[identity.Name] != identity || identity.ID == fs.base().Native().DefaultPool || identity.Used {
		return errors.New("only an unused additional pool attachment may be removed")
	}
	state, err := fs.base().ReadNativePools(ctx)
	if err != nil {
		return err
	}
	pool, err := state.PoolByName(identity.Name)
	if err != nil || pool.ID != identity.ID {
		return errors.New("attached pool was replaced or renamed")
	}
	if !slices.Contains(state.DataPools, identity.ID) {
		if identity.RemovalAttempted {
			identity.Removed = true
			return nil
		}
		return servicehook.Invalid(errors.New("data pool was detached externally; refusing removal"))
	}
	if !identity.Confirmed {
		return errors.New("attachment must be confirmed before removal; retry AddDataPool first")
	}
	if err := fs.confirmDataPoolAttachment(ctx, identity); err != nil {
		return err
	}
	if err := fs.checkUnusedDataPool(ctx, identity.Name); err != nil {
		return err
	}
	identity.RemovalAttempted = true
	if _, err := fs.base().Cluster().Ceph(ctx, "fs", "rm_data_pool", fs.base().Name(), strconv.FormatInt(identity.ID, 10)); err != nil {
		return err
	}
	state, err = fs.base().ReadNativePools(ctx)
	if err != nil {
		return err
	}
	if slices.Contains(state.DataPools, identity.ID) {
		return errors.New("data pool detachment was not confirmed")
	}
	identity.Removed = true
	return nil
}

func (fs *Filesystem) checkUnusedDataPool(ctx context.Context, name string) error {
	if err := fs.base().Volumes().EnsureVolumesModule(ctx); err != nil {
		return err
	}
	groups, err := fs.base().Volumes().SubvolumeGroupNames(ctx)
	if err != nil {
		return err
	}
	for _, group := range groups {
		info, err := fs.base().Volumes().SubvolumeGroupInfo(ctx, group)
		if err != nil {
			return fmt.Errorf("cannot prove group pool is unused: %w", err)
		}
		if info.DataPool == name {
			return errors.New("native subvolume group still selects this pool")
		}
	}
	for _, group := range append([]string{""}, groups...) {
		names, err := fs.base().Volumes().SubvolumeNames(ctx, group)
		if err != nil {
			return err
		}
		for _, volume := range names {
			info, err := fs.base().Volumes().SubvolumeInfo(ctx, volume, group)
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
	control, err := fs.base().Cluster().ClientContainer(ctx)
	if err != nil {
		return fmt.Errorf("select data pool CLI container: %w", err)
	}
	data, err := servicehook.Command(ctx, control, "rados", "-p", name, "--all", "ls")
	if err != nil {
		return fmt.Errorf("inspect all data pool namespaces: %w", err)
	}
	if strings.TrimSpace(string(data)) != "" {
		return errors.New("data pool still contains RADOS objects")
	}
	return nil
}
