package fsbase

import (
	"context"
	"errors"
	"sync"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// Core identifies one filesystem to every operation on it: the cluster, the
// authoritative name and the native pool identity. Each filesystem descriptor
// holds its own Core, so comparing two Core pointers compares filesystems.
//
// cluster and name are captured before the descriptor is published and are
// never written again, so they need no lock to read. name is the one
// authoritative identity: every registry key, ownership predicate and native
// command target uses it. The exported descriptor name is a descriptive copy;
// DescriptorIntact is the only read of it here.
type Core struct {
	cluster *ceph.Container
	name    string
	// descriptorName is the address of the exported descriptor name, which
	// callers may edit.
	descriptorName *string
	// metadataPool and dataPool are the configured names of the original pools.
	metadataPool, dataPool string
	// setupMu is the registry's setup gate; Core holds its address only.
	setupMu *sync.Mutex
	// live is the registry's liveness predicate.
	live func(*Core) bool
	// activeRanks is the filesystem's owned-MDS rank reader.
	activeRanks func(context.Context) (*ActiveRanks, error)
	// native is written once by CaptureNativePools; see the package lock
	// contract.
	native *NativeIdentity
}

// Params are the construction arguments of a Core. Package cephfs passes the
// registry state the foundation needs instead of the foundation keeping its
// own copy.
type Params struct {
	// Cluster is the cluster that owns the filesystem.
	Cluster *ceph.Container
	// Name is the validated, authoritative filesystem name.
	Name string
	// DescriptorName is the address of the descriptor's exported name, read
	// only by DescriptorIntact.
	DescriptorName *string
	// MetadataPool and DataPool are the configured names of the original
	// metadata and default data pools; the native pool reads refuse a
	// replaced or renamed original pool.
	MetadataPool, DataPool string
	// SetupMu is the cluster registry's setup gate. It may be nil only when
	// Cluster is nil.
	SetupMu *sync.Mutex
	// Live reports whether the registry still holds this Core under its name.
	// It runs with the topology mutex held and takes no lock.
	Live func(*Core) bool
	// ActiveRanks reads the filesystem's active ranks with the ownership of
	// their daemons, which only the MDS roster of package cephfs knows. It
	// takes the cluster mutex itself, so the caller must not hold it.
	ActiveRanks func(context.Context) (*ActiveRanks, error)
}

// ActiveRanks is the MDS topology of one filesystem as an export pin
// admission reads it: the native FSMap ID, max_mds and the active ranks.
type ActiveRanks struct {
	FilesystemID int64
	MaxMDS       int
	Active       []ActiveRank
}

// ActiveRank is one active rank: its native state and whether a daemon of
// this filesystem holds it.
type ActiveRank struct {
	Rank  int
	State string
	Owned bool
}

// NewCore returns the Core of one filesystem. It does not register it.
func NewCore(p Params) *Core {
	return &Core{cluster: p.Cluster, name: p.Name, descriptorName: p.DescriptorName,
		metadataPool: p.MetadataPool, dataPool: p.DataPool, setupMu: p.SetupMu, live: p.Live,
		activeRanks: p.ActiveRanks}
}

// Handle binds a descriptor to its Core. It has only unexported members, so
// embedding it promotes nothing into a public API.
type Handle struct{ c *Core }

// NewHandle returns the handle of c.
func NewHandle(c *Core) Handle { return Handle{c: c} }

func (h Handle) core() *Core { return h.c }

// Owner is a type that embeds a Handle. It is sealed: only types that embed
// Handle satisfy it.
type Owner interface{ core() *Core }

// Of returns the Core of o. o must not be a nil pointer.
func Of(o Owner) *Core { return o.core() }

// Cluster returns the owning cluster; nil for a nil Core.
func (c *Core) Cluster() *ceph.Container {
	if c == nil {
		return nil
	}
	return c.cluster
}

// Name returns the authoritative filesystem name; empty for a nil Core.
func (c *Core) Name() string {
	if c == nil {
		return ""
	}
	return c.name
}

// Registered reports whether c is still the live filesystem of its name on an
// open cluster. It is the ownership invariant that every admission predicate
// and every MDS ownership proof shares; each site adds its own conjuncts (the
// descriptor integrity, the native identity, the first-daemon container, the
// roster) and keeps its own refusal. The caller holds the cluster topology
// mutex.
func (c *Core) Registered() bool {
	if servicehook.Closed(c.cluster) {
		return false
	}
	return c.live(c)
}

// DescriptorIntact reports whether the exported descriptor name still equals
// the authoritative name. The descriptor-integrity refusals call it; no other
// operation reads the exported name.
func (c *Core) DescriptorIntact() bool {
	return *c.descriptorName == c.name
}

// Native returns the native pool identity captured at creation, or nil before
// it is captured. See the package lock contract.
func (c *Core) Native() *NativeIdentity { return c.native }

// ReplaceNative installs or clears the native identity without a creation, so
// that a test of a package that builds a Core can start from a captured or
// missing identity. Production code captures it with CaptureNativePools.
//
//testdouble:test-mutator
func (c *Core) ReplaceNative(native *NativeIdentity) { c.native = native }

// ActiveRanks reads the active ranks through the reader package cephfs
// injected. Its errors are passed through as is.
func (c *Core) ActiveRanks(ctx context.Context) (*ActiveRanks, error) {
	if c == nil || c.activeRanks == nil {
		return nil, errors.New("cephfs cluster is unavailable")
	}
	return c.activeRanks(ctx)
}

// Volumes returns the command transport of the filesystem's volumes module.
func (c *Core) Volumes() VolumeCommands {
	return VolumeCommands{cluster: c.cluster, name: c.name}
}
