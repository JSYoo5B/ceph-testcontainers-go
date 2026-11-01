package cephfs

import (
	"context"
	"sync"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/testcontainers/testcontainers-go"
)

// fsHandle is the foundation handle a Filesystem embeds. The alias keeps the
// embedded field unexported, so nothing is promoted into the public API, and
// *Filesystem satisfies fsbase.Owner.
type fsHandle = fsbase.Handle

// mdsState is the MDS state of one filesystem: its owned daemons and, for a
// filesystem created without an initial MDS, the cold-start record. The MDS
// fleet (Filesystem.fleet) is the only code that reaches it; its lock contract
// is the roster's, documented on mdsRoster.
type mdsState struct {
	roster  mdsRoster
	coldMDS *cephFSColdMDS
}

// newFilesystem returns the unregistered descriptor of the filesystem that
// the validated config describes, bound to its own foundation core. The core
// receives the registry's setup gate, the registry's liveness predicate, the
// address of the exported FilesystemName, which only the descriptor-integrity
// refusals read, and the reader of the owned active ranks.
func newFilesystem(c *ceph.Container, config Config, opts []testcontainers.ContainerCustomizer) *Filesystem {
	fs := &Filesystem{FilesystemName: config.Name, MetadataPool: config.MetadataPool.Name, DataPool: config.DataPool.Name, config: config, mds: mdsState{roster: newMDSRoster(opts)}}
	for _, pool := range config.AdditionalDataPools {
		fs.AdditionalDataPools = append(fs.AdditionalDataPools, pool.Name)
	}
	fs.fsHandle = fsbase.NewHandle(fsbase.NewCore(fsbase.Params{
		Cluster: c, Name: config.Name, DescriptorName: &fs.FilesystemName,
		MetadataPool: config.MetadataPool.Name, DataPool: config.DataPool.Name,
		SetupMu: setupGate(c), Live: ownsRegistryEntry, ActiveRanks: fs.activeRanks,
	}))
	fsbase.Bind(fs)
	return fs
}

// activeRanks is the rank reader of fs's core: MDSStatus, reduced to the
// active ranks with the ownership of their daemons, which only the roster
// knows.
func (fs *Filesystem) activeRanks(ctx context.Context) (*fsbase.ActiveRanks, error) {
	status, err := fs.MDSStatus(ctx)
	if err != nil {
		return nil, err
	}
	ranks := &fsbase.ActiveRanks{FilesystemID: status.FilesystemID, MaxMDS: status.MaxMDS}
	for _, active := range status.Active {
		ranks.Active = append(ranks.Active, fsbase.ActiveRank{Rank: active.Rank, State: active.State, Owned: active.Owned})
	}
	return ranks, nil
}

// setupGate returns the address of the cluster registry's setupMu; nil for a
// nil cluster.
func setupGate(c *ceph.Container) *sync.Mutex {
	if c == nil {
		return nil
	}
	return &filesystems(c).setupMu
}

// ownsRegistryEntry is the liveness predicate of every core: the registry
// holds core's filesystem under its name. Comparing the cores is comparing the
// Filesystems: each Filesystem holds its own core. The caller holds the
// cluster topology mutex.
func ownsRegistryEntry(core *fsbase.Core) bool {
	entry := filesystems(core.Cluster()).byName[core.Name()]
	return entry != nil && fsbase.Of(entry) == core
}

// base returns the foundation core of fs; nil for a nil fs.
func (fs *Filesystem) base() *fsbase.Core {
	if fs == nil {
		return nil
	}
	return fsbase.Of(fs)
}
