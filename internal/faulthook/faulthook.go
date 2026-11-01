// Package faulthook gives fault operations outside package ceph the parts of
// a ceph.Container that are not public API: the cluster's fault registry, its mutex and the private helpers that fault
// operations use. Package ceph registers them in internal/hookreg when it is
// initialized.
//
// The registry is the only way to make a fault known to Terminate. Each kind
// has a phase that package ceph fixes: network delays are undone first with
// the cluster mutex released, interruptions second under it (a failure keeps
// the network), pauses third under it, and the blocklist, full ratios,
// configuration, OSD flag and MGR module overrides are only tracked for
// overlap and identity. Callers cannot add a
// phase, change the order or register another cleanup.
//
// Lock rules: Reserve, Settle, Release, Tracks, Closed, Owns, OwnedOSD,
// PoolPolicyReady and PoolState need the cluster mutex, held through
// WithTopology or WithTopologyNoContext. A function passed to them must not
// lock a handle's mutex that is itself held while the cluster mutex is
// taken (a network delay lease), must not call a public ceph.Container
// method that takes the cluster mutex, and returns before the hook unlocks.
package faulthook

import (
	"context"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
	"github.com/testcontainers/testcontainers-go"
)

// Kind names one family of faults that a cluster tracks.
type Kind string

const (
	// NetworkDelay holds a netem lease per network and container until its
	// Restore releases it; Terminate restores it first, unlocked.
	NetworkDelay Kind = "NetworkDelay"
	// NetworkInterruption holds a disconnected endpoint per network and
	// container; Terminate restores it under the mutex, and a failure keeps
	// its network.
	NetworkInterruption Kind = "NetworkInterruption"
	// ContainerPause holds a frozen container per ID; Terminate resumes it
	// under the mutex before native commands stop.
	ContainerPause Kind = "ContainerPause"
	// Blocklist tracks a blocklist override per client session.
	Blocklist Kind = "Blocklist"
	// FullRatios tracks the one full ratios override, with an empty key.
	FullRatios Kind = "FullRatios"
)

// Token identifies one reservation. Only the token that a key still maps to
// settles, releases or is tracked, so a stale handle cannot release its
// successor.
type Token struct{ entry any }

// Held is what Settle records for a reservation.
type Held struct {
	// Network is the network that an interruption needs back.
	Network string
	// Handle is the public handle that an overlap refusal may return.
	Handle any
	// Undo is what Terminate calls in the kind's phase.
	Undo func(context.Context) error
	// Complete reports a fault that no longer needs Undo. A kind that has it
	// returns the previous handle with an overlap refusal and replaces a
	// complete one; without it the key is held until Release.
	Complete func(context.Context) (bool, error)
}

type hooks interface {
	Reserve(ctx context.Context, c *ceph.Container, kind, key string) (any, any, error)
	Settle(c *ceph.Container, token any, network string, handle any, undo func(context.Context) error, complete func(context.Context) (bool, error)) (bool, error)
	Release(c *ceph.Container, token any) bool
	Tracks(c *ceph.Container, token any) bool
	WithTopology(ctx context.Context, c *ceph.Container, fn func() error) error
	WithTopologyNoContext(c *ceph.Container, fn func())
	Closed(c *ceph.Container) bool
	Owns(c *ceph.Container, id string) bool
	OwnedLabel(c *ceph.Container) testcontainers.CustomizeRequestOption
	Control(ctx context.Context, c *ceph.Container) (testcontainers.Container, error)
	CLI(ctx context.Context, c *ceph.Container, args ...string) ([]byte, error)
	ClusterIdentity(ctx context.Context, c *ceph.Container) (string, bool, error)
	PoolPolicyReady(c *ceph.Container) error
	PoolState(ctx context.Context, c *ceph.Container, name string) (ceph.PoolState, error)
	CheckPoolID(ctx context.Context, c *ceph.Container, expected ceph.PoolState) error
	OwnedOSD(c *ceph.Container, id int) bool
	BlueStore(c *ceph.Container) bool
	StartupTimeout(c *ceph.Container) time.Duration
	Terminated() (native, cluster error)
	ClassifyDocker(ctx context.Context, err error) error
	NativeFailure(program string, code int, stdout, stderr []byte) error
	OnlyMissingHostResource(err error) bool
	AddBlockDeviceOSD(ctx context.Context, c *ceph.Container, hostPath string, major, minor uint32, sizeBytes int64, lease string) (*ceph.OSDContainer, error)
	JudgeDeviceChain(ctx context.Context, c *ceph.Container, hostPath string, major, minor uint32, sizeBytes int64, lease string,
		evidence func(context.Context) (string, string, uint32, uint32, int64, [3]uint8, int, error)) (string, string)
}

func get() hooks {
	h, ok := hookreg.Fault.(hooks)
	if !ok {
		panic("faulthook: package ceph did not register its fault hooks")
	}
	return h
}

// Reserve reserves key for a fault of kind. The caller holds the mutex and
// has refused a closed cluster with its own error. A refusal of
// NetworkInterruption or ContainerPause may return the previous handle,
// which still needs its undo.
func Reserve(ctx context.Context, c *ceph.Container, kind Kind, key string) (Token, any, error) {
	token, previous, err := get().Reserve(ctx, c, string(kind), key)
	if err != nil {
		return Token{}, previous, err
	}
	return Token{entry: token}, nil, nil
}

// Settle records held for a reservation, or ends the reservation when
// held.Handle is nil. It reports whether Terminate has started meanwhile,
// in which case Terminate did not see the handle and the caller undoes it.
// The caller holds the mutex.
func Settle(c *ceph.Container, token Token, held Held) (bool, error) {
	return get().Settle(c, token.entry, held.Network, held.Handle, held.Undo, held.Complete)
}

// Release forgets the fault of token while its key still maps to it. The
// caller holds the mutex.
func Release(c *ceph.Container, token Token) bool { return get().Release(c, token.entry) }

// Tracks reports whether token is still the fault of its key. The caller
// holds the mutex.
func Tracks(c *ceph.Container, token Token) bool { return get().Tracks(c, token.entry) }

// WithTopology runs fn with the cluster mutex held, unless ctx ends first.
func WithTopology(ctx context.Context, c *ceph.Container, fn func() error) error {
	return get().WithTopology(ctx, c, fn)
}

// WithTopologyNoContext runs fn with the cluster mutex held, waiting for it
// without a context.
func WithTopologyNoContext(c *ceph.Container, fn func()) { get().WithTopologyNoContext(c, fn) }

// Closed reports a terminated cluster. The caller holds the mutex.
func Closed(c *ceph.Container) bool { return get().Closed(c) }

// Owns reports a container of the cluster's own daemons. The caller holds
// the mutex.
func Owns(c *ceph.Container, id string) bool { return get().Owns(c, id) }

// OwnedLabel labels a helper container as owned by the cluster.
func OwnedLabel(c *ceph.Container) testcontainers.CustomizeRequestOption {
	return get().OwnedLabel(c)
}

// Control returns the container that runs the control CLI.
func Control(ctx context.Context, c *ceph.Container) (testcontainers.Container, error) {
	return get().Control(ctx, c)
}

// CLI runs a Ceph CLI command with the admin identity.
func CLI(ctx context.Context, c *ceph.Container, args ...string) ([]byte, error) {
	return get().CLI(ctx, c, args...)
}

// ClusterIdentity returns the bootstrap FSID and whether the configuration
// and admin keyring were confirmed. The error is only ctx's.
func ClusterIdentity(ctx context.Context, c *ceph.Container) (string, bool, error) {
	return get().ClusterIdentity(ctx, c)
}

// PoolPolicyReady refuses a terminated cluster or one without a control
// container. The caller holds the mutex.
func PoolPolicyReady(c *ceph.Container) error { return get().PoolPolicyReady(c) }

// PoolState reads one pool. The caller holds the mutex.
func PoolState(ctx context.Context, c *ceph.Container, name string) (ceph.PoolState, error) {
	return get().PoolState(ctx, c, name)
}

// CheckPoolID refuses a pool that was replaced since expected was read.
func CheckPoolID(ctx context.Context, c *ceph.Container, expected ceph.PoolState) error {
	return get().CheckPoolID(ctx, c, expected)
}

// OwnedOSD reports an OSD that the cluster owns and has not purged. The
// caller holds the mutex.
func OwnedOSD(c *ceph.Container, id int) bool { return get().OwnedOSD(c, id) }

// BlueStore reports a cluster with BlueStore OSDs.
func BlueStore(c *ceph.Container) bool { return get().BlueStore(c) }

// StartupTimeout bounds one fixture operation.
func StartupTimeout(c *ceph.Container) time.Duration { return get().StartupTimeout(c) }

// Terminated returns the error of a fault that Terminate overtook and the
// error of an operation on a terminated cluster.
func Terminated() (native, cluster error) { return get().Terminated() }

// ClassifyDocker classifies a failed Docker call.
func ClassifyDocker(ctx context.Context, err error) error { return get().ClassifyDocker(ctx, err) }

// NativeFailure describes a native command that exited nonzero.
func NativeFailure(program string, code int, stdout, stderr []byte) error {
	return get().NativeFailure(program, code, stdout, stderr)
}

// OnlyMissingHostResource reports an error that only says a container or
// network is already gone.
func OnlyMissingHostResource(err error) bool { return get().OnlyMissingHostResource(err) }

// BlockDevice is the identity a lease owner verified for one device-mapper
// node: its /dev/mapper path on the engine host, its number and its size.
type BlockDevice struct {
	HostPath     string
	Major, Minor uint32
	SizeBytes    int64
}

// AddBlockDeviceOSD adds one OSD whose BlueStore main device is device, in the
// cluster's default placement, and waits for it to be up and in. The cluster
// must use the default OSD storage. The caller owns the file, loop and
// device-mapper chain behind device and keeps it until JudgeDeviceChain
// reports ChainClear; an error here never releases it. It is internal: no
// public option adds a device-backed OSD.
func AddBlockDeviceOSD(ctx context.Context, c *ceph.Container, device BlockDevice, lease string) (*ceph.OSDContainer, error) {
	return get().AddBlockDeviceOSD(ctx, c, device.HostPath, device.Major, device.Minor, device.SizeBytes, lease)
}

// Fact is a tri-state observation of the lease owner. The zero value is
// unknown and never counts as evidence.
type Fact uint8

const (
	FactUnknown Fact = iota
	FactNo
	FactYes
)

// ChainEvidence is what the lease owner observed about its chain: the lease
// and device it saw, whether the OSD daemon ended, whether every observer
// borrow of the device was closed, whether its opener scan was complete, and
// how many openers that scan found.
type ChainEvidence struct {
	Lease                                      string
	Device                                     BlockDevice
	DaemonStopped, BorrowsClosed, ScanComplete Fact
	Openers                                    int
}

// ChainClear is the one outcome of JudgeDeviceChain that raises no objection.
// Every other outcome retains the chain: retain-unknown, retain-claim,
// retain-identity or retain-busy.
const ChainClear = "clear"

// JudgeDeviceChain joins the cluster's claim table with the owner's evidence
// for device and lease. It is not authority to release anything; it only
// reports that neither receipt objects. It works after Terminate.
func JudgeDeviceChain(ctx context.Context, c *ceph.Container, device BlockDevice, lease string, evidence func(context.Context) (ChainEvidence, error)) (outcome, detail string) {
	var raw func(context.Context) (string, string, uint32, uint32, int64, [3]uint8, int, error)
	if evidence != nil {
		raw = func(ctx context.Context) (string, string, uint32, uint32, int64, [3]uint8, int, error) {
			ev, err := evidence(ctx)
			return ev.Lease, ev.Device.HostPath, ev.Device.Major, ev.Device.Minor, ev.Device.SizeBytes,
				[3]uint8{uint8(ev.DaemonStopped), uint8(ev.BorrowsClosed), uint8(ev.ScanComplete)}, ev.Openers, err
		}
	}
	return get().JudgeDeviceChain(ctx, c, device.HostPath, device.Major, device.Minor, device.SizeBytes, lease, raw)
}
