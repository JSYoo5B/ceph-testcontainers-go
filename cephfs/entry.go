package cephfs

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
)

var errNilCluster = errors.New("ceph cluster is unavailable")

// Start creates one filesystem with dedicated pools and its
// ActiveMDS+StandbyMDS daemons on an existing cluster. A zero config selects
// the default filesystem tc-cephfs with one active MDS named a. Every pool and
// OSD placement is validated before mutations. Existing filesystems are
// rejected instead of modified. Creating a second filesystem enables Ceph's
// global enable_multiple flag; the existing filesystem and its configuration
// are preserved.
//
// Customizers apply to each MDS and must preserve its identity, config, keyring,
// entrypoint and networking. A non-nil result with an error identifies attempted
// resources; the cluster owns cleanup, including partial daemon startup.
func Start(ctx context.Context, c *ceph.Container, config Config, opts ...testcontainers.ContainerCustomizer) (*Filesystem, error) {
	if c == nil {
		return nil, errNilCluster
	}
	return startFilesystem(ctx, c, config, opts...)
}

// Filesystems returns the cluster's owned filesystem descriptors sorted by
// filesystem name, including a descriptor whose setup partially failed. The
// cluster owns their MDS containers.
func Filesystems(c *ceph.Container) []*Filesystem {
	if c == nil {
		return nil
	}
	servicehook.Lock(c)
	defer servicehook.Unlock(c)
	return filesystems(c).sorted()
}

// Remove deletes this owned filesystem: it gracefully stops its ranks and removes
// it in the FSMap, terminates its MDS daemons, deletes their Cephx keys and removes the
// metadata, default data and AdditionalDataPools that its configuration
// created, together with their CRUSH rules and EC profiles. Pools attached
// later with AddDataPool are detached but kept; remove them with
// Pool.Remove. File data in the removed pools is destroyed. Stop
// clients, CephFS mirrors and subvolume operations on the filesystem first;
// their handles fail afterwards. A retry after an error continues with
// whatever remains. If graceful shutdown cannot complete within the caller or
// startup timeout, removal returns an error without forcing the MDSs to fail.
// The filesystem may remain marked down and stays owned for a later retry.
//
// Remove acts on this descriptor. One that Remove already removed, or that is
// not the one the cluster owns for its name, returns an error.
func (fs *Filesystem) Remove(ctx context.Context) error {
	if fs == nil || fs.id.cluster == nil {
		return errNilCluster
	}
	return removeFilesystem(ctx, fs.id.cluster, fs)
}

// WithFilesystems creates named filesystems and their MDS topologies during
// Run. No arguments selects the default filesystem. Repeated options replace
// the list; Start can add further filesystems after Run returns.
func WithFilesystems(configs ...Config) ceph.Option {
	if len(configs) == 0 {
		configs = []Config{{}}
	}
	// Copy now, so that later changes to the caller's configs do not reach Run.
	captured := newCephFSBootstrap(configs).configs
	return servicehook.WithBootstrap(bootstrapKind, false, func() (servicehook.Bootstrap, error) { return newCephFSBootstrap(captured), nil })
}

// defaultFilesystem selects the default filesystem when no filesystem option
// was given.
func defaultFilesystem() ceph.Option {
	return servicehook.WithBootstrap(bootstrapKind, true, func() (servicehook.Bootstrap, error) { return newCephFSBootstrap([]Config{{}}), nil })
}

// filesystemRegistry is the cluster's per-cluster CephFS state.
type filesystemRegistry struct {
	// setupMu serializes filesystem creation, removal and MDS topology
	// changes. It is taken before the cluster mutex.
	setupMu sync.Mutex
	// byName holds the owned filesystems. The cluster mutex guards it.
	byName map[string]*Filesystem
}

// filesystems returns the cluster's filesystem registry, creating it on first
// use.
func filesystems(c *ceph.Container) *filesystemRegistry {
	return servicehook.Extension(c, bootstrapKind, func() any {
		return &filesystemRegistry{byName: make(map[string]*Filesystem)}
	}).(*filesystemRegistry)
}

func (r *filesystemRegistry) sorted() []*Filesystem {
	result := slices.Collect(maps.Values(r.byName))
	slices.SortFunc(result, func(a, b *Filesystem) int { return strings.Compare(a.id.name, b.id.name) })
	return result
}

// ConfigTargets lists MDS containers that read the cluster's ceph.conf.
func (r *filesystemRegistry) ConfigTargets() map[string]testcontainers.Container {
	targets := make(map[string]testcontainers.Container)
	for _, fs := range r.byName {
		if fs == nil {
			continue
		}
		fs.fleet().addConfigTargets(targets)
	}
	return targets
}

// RequiresMGRModule reports that owned filesystems need the volumes module.
func (r *filesystemRegistry) RequiresMGRModule(module string) bool {
	return module == "volumes" && len(r.byName) != 0
}

func resolveFilesystemDefaults(c *ceph.Container, config Config) Config {
	return resolveFilesystemPools(func(pool ceph.PoolConfig) ceph.PoolConfig { return servicehook.ResolvePool(c, pool) }, config)
}

func resolveFilesystemPools(resolve func(ceph.PoolConfig) ceph.PoolConfig, config Config) Config {
	config = cloneCephFSConfig(config)
	config.MetadataPool = resolve(config.MetadataPool)
	config.DataPool = resolve(config.DataPool)
	for i := range config.AdditionalDataPools {
		config.AdditionalDataPools[i] = resolve(config.AdditionalDataPools[i])
	}
	return config
}

func cloneCephFSConfig(config Config) Config {
	config.MetadataPool = clonePoolConfig(config.MetadataPool)
	config.DataPool = clonePoolConfig(config.DataPool)
	config.AdditionalDataPools = slices.Clone(config.AdditionalDataPools)
	for i := range config.AdditionalDataPools {
		config.AdditionalDataPools[i] = clonePoolConfig(config.AdditionalDataPools[i])
	}
	return config
}

func clonePoolConfig(config ceph.PoolConfig) ceph.PoolConfig {
	if config.ErasureCode != nil {
		ec := *config.ErasureCode
		config.ErasureCode = &ec
	}
	return config
}

func validateExistingPool(name string) error {
	if !naming.ExistingPool.MatchString(name) {
		return servicehook.Invalid(errors.New("invalid native pool name"))
	}
	return nil
}

func clientEntity(name string) (string, error) {
	id := strings.TrimPrefix(name, "client.")
	if !naming.ClientID.MatchString(id) {
		return "", errors.New("client name must be an ID or client.ID using letters, digits, dots, underscores or hyphens")
	}
	return "client." + id, nil
}
