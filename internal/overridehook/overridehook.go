// Package overridehook gives the override package the parts of a
// ceph.Container that are not public API: shared-cluster admission, the
// cluster's registry of held overrides, its mutex and one typed operation per
// native command. Package ceph registers them in internal/hookreg when it is
// initialized.
//
// The seam is typed on purpose. It has no raw executor: there is no method
// that runs an arbitrary Ceph command, and none that returns the cluster
// mutex, the keyring, the configuration bytes or a control container.
//
// Lock rules: Reserve, Settle, Release, Tracks, Ready, Terminated and the
// native operations need the cluster mutex, held through WithTopology or
// WithTopologyNoContext. A function passed to them must not call a public
// ceph.Container method that takes the cluster mutex and returns before the
// hook unlocks.
package overridehook

import (
	"context"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

// Kind names one kind of override that a cluster tracks.
type Kind string

const (
	// Blocklist tracks one client blocklist entry per exact session. The
	// registry only records it for overlap and identity: Terminate does not
	// undo it, the entry ends with the cluster.
	Blocklist Kind = "Blocklist"
)

// Token identifies one reservation. Only the token that a key still maps to
// settles, releases or is tracked, so a stale handle cannot release its
// successor.
type Token struct{ entry any }

type hooks interface {
	TestHeld(c *ceph.Container, kind string) map[string]any
	Admit(c *ceph.Container, op string) error
	WithTopology(ctx context.Context, c *ceph.Container, fn func() error) error
	WithTopologyNoContext(c *ceph.Container, fn func())
	Ready(c *ceph.Container) error
	StartupTimeout(c *ceph.Container) time.Duration
	Terminated() error
	Invalid(err error) error
	Reserve(ctx context.Context, c *ceph.Container, kind, key string) (any, error)
	Settle(c *ceph.Container, token any, handle any) (bool, error)
	Release(c *ceph.Container, token any) bool
	Tracks(c *ceph.Container, token any) bool
	NormalizeBlocklistSession(address string) (string, error)
	BlocklistEntries(ctx context.Context, c *ceph.Container) ([]ceph.BlocklistEntry, error)
	BlocklistAdd(ctx context.Context, c *ceph.Container, address string, duration time.Duration) error
	BlocklistRemove(ctx context.Context, c *ceph.Container, address string) error
}

func get() hooks {
	h, ok := hookreg.Override.(hooks)
	if !ok {
		panic("overridehook: package ceph did not register its override hooks")
	}
	return h
}

// Admit refuses the override operation op ("override.AddBlocklist" and so
// on) while a SharedCluster shares c, with the policy of the method that it
// replaced. An operation without a policy is refused, even on a cluster of
// its own, with ceph.ErrInvalidRequest. A nil cluster is not refused here.
func Admit(c *ceph.Container, op string) error { return get().Admit(c, op) }

// WithTopology runs fn with the cluster mutex held, unless ctx ends first.
func WithTopology(ctx context.Context, c *ceph.Container, fn func() error) error {
	return get().WithTopology(ctx, c, fn)
}

// WithTopologyNoContext runs fn with the cluster mutex held, waiting for it
// without a context.
func WithTopologyNoContext(c *ceph.Container, fn func()) { get().WithTopologyNoContext(c, fn) }

// Ready refuses a terminated cluster or one without a control container. A
// terminated cluster returns the error that Terminated returns. The caller
// holds the mutex.
func Ready(c *ceph.Container) error { return get().Ready(c) }

// StartupTimeout bounds one fixture operation.
func StartupTimeout(c *ceph.Container) time.Duration { return get().StartupTimeout(c) }

// Terminated returns the error of an operation on a terminated cluster.
func Terminated() error { return get().Terminated() }

// Invalid classifies err as ceph.ErrInvalidRequest and keeps its text.
func Invalid(err error) error { return get().Invalid(err) }

// Reserve reserves key for an override of kind. The caller holds the mutex
// and has refused a closed cluster with Ready. A key that is reserved or held
// is refused with the kind's own text.
func Reserve(ctx context.Context, c *ceph.Container, kind Kind, key string) (Token, error) {
	entry, err := get().Reserve(ctx, c, string(kind), key)
	if err != nil {
		return Token{}, err
	}
	return Token{entry: entry}, nil
}

// Settle records handle for a reservation, or ends the reservation when
// handle is nil. It reports whether Terminate has started meanwhile. The
// caller holds the mutex.
func Settle(c *ceph.Container, token Token, handle any) (bool, error) {
	return get().Settle(c, token.entry, handle)
}

// Release forgets the override of token while its key still maps to it. The
// caller holds the mutex.
func Release(c *ceph.Container, token Token) bool { return get().Release(c, token.entry) }

// Tracks reports whether token is still the override of its key. The caller
// holds the mutex.
func Tracks(c *ceph.Container, token Token) bool { return get().Tracks(c, token.entry) }

// NormalizeBlocklistSession validates an exact client session address for
// fencing and returns its canonical form.
func NormalizeBlocklistSession(address string) (string, error) {
	return get().NormalizeBlocklistSession(address)
}

// BlocklistEntries reads the native blocklist. The caller holds the mutex.
func BlocklistEntries(ctx context.Context, c *ceph.Container) ([]ceph.BlocklistEntry, error) {
	return get().BlocklistEntries(ctx, c)
}

// BlocklistAdd adds one exact session to the native blocklist for duration.
// The caller holds the mutex.
func BlocklistAdd(ctx context.Context, c *ceph.Container, address string, duration time.Duration) error {
	return get().BlocklistAdd(ctx, c, address, duration)
}

// BlocklistRemove removes one exact session from the native blocklist. The
// caller holds the mutex.
func BlocklistRemove(ctx context.Context, c *ceph.Container, address string) error {
	return get().BlocklistRemove(ctx, c, address)
}

// TestHeld snapshots the handles that one kind holds, for unit tests. Call
// with the cluster mutex held.
func TestHeld(c *ceph.Container, kind Kind) map[string]any {
	return get().TestHeld(c, string(kind))
}
