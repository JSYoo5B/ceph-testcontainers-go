package fsbase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// NativeIdentity is the native identity of a filesystem captured at creation:
// its FSMap ID, its metadata and default data pool IDs, and the additional
// pool attachments its descriptor recorded. Attachments is read and written
// with the setup gate held.
type NativeIdentity struct {
	ID, MetadataPool, DefaultPool int64
	Attachments                   map[string]*PoolAttachment
}

// PoolAttachment is the record of one additional data pool attachment. Every
// copy of a data pool handle shares it. Used is sticky: a subvolume, group or
// clone that selected the pool once, even in a partial creation, keeps it set.
type PoolAttachment struct {
	Name                                       string
	ID                                         int64
	Confirmed, Used, Removed, RemovalAttempted bool
}

// FSMap is the decoded native global MDS map (ceph fs dump).
type FSMap struct {
	FeatureFlags struct {
		EnableMultiple bool `json:"enable_multiple"`
	} `json:"feature_flags"`
	Standbys    []MDSInfo `json:"standbys"`
	Filesystems []struct {
		ID     int64 `json:"id"`
		MDSMap struct {
			Name         string             `json:"fs_name"`
			MaxMDS       int                `json:"max_mds"`
			Info         map[string]MDSInfo `json:"info"`
			MetadataPool int64              `json:"metadata_pool"`
			DataPools    []int64            `json:"data_pools"`
		} `json:"mdsmap"`
	} `json:"filesystems"`
}

// MDSInfo is one daemon entry of the FSMap. Owned is not a native field; the
// status reader sets it.
type MDSInfo struct {
	Name      string `json:"name"`
	Rank      int    `json:"rank"`
	GID       uint64 `json:"gid"`
	State     string `json:"state"`
	Owned     bool   `json:"owned"`
	JoinFSCID int64  `json:"join_fscid"`
}

// NativePools is one native pool identity read: the FSMap it came from, the
// filesystem's FSMap ID, metadata pool and data pools, and the cluster's pools.
type NativePools struct {
	Map              FSMap
	ID, MetadataPool int64
	DataPools        []int64
	Pools            []ceph.PoolState
}

// CaptureNativePools records the native pool identity during filesystem
// creation, never on first use of a handle, so a failed setup descriptor
// cannot adopt a later filesystem. The caller holds the setup gate; the write
// takes the cluster mutex.
func (c *Core) CaptureNativePools(ctx context.Context) error {
	state, err := c.ReadNativePools(ctx)
	if err != nil {
		return err
	}
	if len(state.DataPools) == 0 {
		return errors.New("created filesystem has no default data pool")
	}
	metadata, err := state.PoolByID(state.MetadataPool)
	if err != nil {
		return err
	}
	data, err := state.PoolByID(state.DataPools[0])
	if err != nil {
		return err
	}
	if metadata.Name != c.metadataPool || data.Name != c.dataPool {
		return errors.New("created filesystem pool names do not match requested pools")
	}
	servicehook.Lock(c.cluster)
	c.native = &NativeIdentity{ID: state.ID, MetadataPool: metadata.ID, DefaultPool: data.ID,
		Attachments: make(map[string]*PoolAttachment)}
	servicehook.Unlock(c.cluster)
	return nil
}

// ReadNativePools reads the filesystem's FSMap entry and the cluster's pools.
// Once the native identity is captured it refuses a filesystem or an original
// pool that was replaced or renamed.
func (c *Core) ReadNativePools(ctx context.Context) (*NativePools, error) {
	data, err := c.cluster.Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		return nil, err
	}
	var fsmap FSMap
	if err := json.Unmarshal(data, &fsmap); err != nil || fsmap.Filesystems == nil {
		return nil, errors.New("decode filesystem pool map")
	}
	var result *NativePools
	for _, native := range fsmap.Filesystems {
		if native.MDSMap.Name != c.name {
			continue
		}
		if result != nil || native.ID < 0 || native.MDSMap.MetadataPool < 0 || len(native.MDSMap.DataPools) == 0 {
			return nil, errors.New("incomplete or duplicate native filesystem pool map")
		}
		result = &NativePools{Map: fsmap, ID: native.ID, MetadataPool: native.MDSMap.MetadataPool,
			DataPools: slices.Clone(native.MDSMap.DataPools)}
	}
	if result == nil {
		return nil, errors.New("owned filesystem is absent from FSMap")
	}
	result.Pools, err = servicehook.PoolStates(ctx, c.cluster)
	if err != nil {
		return nil, err
	}
	if identity := c.native; identity != nil {
		if result.ID != identity.ID || result.MetadataPool != identity.MetadataPool || result.DataPools[0] != identity.DefaultPool {
			return nil, errors.New("filesystem or its original pools were replaced; refusing operation")
		}
		for _, original := range []struct {
			id   int64
			name string
		}{
			{identity.MetadataPool, c.metadataPool}, {identity.DefaultPool, c.dataPool},
		} {
			pool, err := result.PoolByID(original.id)
			if err != nil || pool.Name != original.name {
				return nil, errors.New("original filesystem pool was replaced or renamed")
			}
		}
	}
	return result, nil
}

// PoolByID returns the pool with the native ID id.
func (state *NativePools) PoolByID(id int64) (ceph.PoolState, error) {
	for _, pool := range state.Pools {
		if pool.ID == id {
			return pool, nil
		}
	}
	return ceph.PoolState{}, fmt.Errorf("native pool ID %d is absent", id)
}

// PoolByName returns the pool named name.
func (state *NativePools) PoolByName(name string) (ceph.PoolState, error) {
	for _, pool := range state.Pools {
		if pool.Name == name {
			return pool, nil
		}
	}
	return ceph.PoolState{}, fmt.Errorf("native pool %q is absent", name)
}

// CheckSubvolumeDataPool refuses a data pool name that is not currently
// registered to this filesystem and marks a recorded attachment used. It runs
// while the setup gate is held. Selection is sticky even after a partial
// native create, so removal never assumes unknown partial resources are safe.
func (c *Core) CheckSubvolumeDataPool(ctx context.Context, name string) error {
	if c.native == nil {
		return errors.New("filesystem has no confirmed creation identity")
	}
	state, err := c.ReadNativePools(ctx)
	if err != nil {
		return err
	}
	if name == "" {
		return nil
	}
	pool, err := state.PoolByName(name)
	if err != nil {
		return err
	}
	if !slices.Contains(state.DataPools, pool.ID) {
		return errors.New("selected pool is not currently registered to this filesystem")
	}
	if identity := c.native.Attachments[name]; identity != nil && identity.ID == pool.ID {
		identity.Used = true
	}
	return nil
}

// CheckSubvolumeCreationPool is CheckSubvolumeDataPool for a subvolume
// creation: a named group's layout can select an additional pool even when
// callers omit DataPool. Record that selection before the native create,
// including unknown-success failures, so later group removal cannot erase its
// history.
func (c *Core) CheckSubvolumeCreationPool(ctx context.Context, name, group string) error {
	if name == "" && group != "" && c.native != nil {
		info, err := c.Volumes().SubvolumeGroupInfo(ctx, group)
		if err != nil {
			return err
		}
		name = info.DataPool
	}
	return c.CheckSubvolumeDataPool(ctx, name)
}
