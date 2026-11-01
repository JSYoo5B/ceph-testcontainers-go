package fsbase

import (
	"context"
	"errors"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// AdmissionSpec is what one begin*Operation flow keeps for itself when it
// shares the admission lifetime: whether it rechecks cancellation after the
// setup gate, its own ownership predicate and its own refusal text.
type AdmissionSpec struct {
	// RecheckCanceled makes Admit test ctx.Err() after setupMu is acquired.
	// Subvolume and data-pool admission do; session admission does not.
	RecheckCanceled bool
	// Refusal is returned when Check reports the handle invalid.
	Refusal string
	// Check is the operation's own predicate. It runs with setupMu and the
	// cluster mutex held, before the timeout context exists, and must not take
	// either lock. A returned error is passed through as is; valid == false is
	// reported as Refusal. It may capture values read under the locks.
	Check func(ctx context.Context) (valid bool, err error)
}

// Admit owns only the lifetime shared by the begin*Operation family: the
// canceled and nil-handle prefix, setupMu (context aware), the cluster
// topology mutex, the startup timeout, and the release. Locks are taken in the
// order of the package contract: setupMu, then LockTopology. The topology
// mutex is released before Admit returns; the caller holds setupMu, through
// the returned done, for the whole operation.
//
// Every operation keeps its own predicate in spec.Check. Admit has no notion
// of a valid filesystem: the flows differ on purpose: subvolume and session
// operations require DescriptorIntact, data-pool operations do not, and
// MDSStatus takes no admission. Merging them would change which edits of an
// exported field are refused.
//
// On any error the returned done is a no-op and no lock is held. On success the
// caller must call done once: it cancels the timeout and releases setupMu.
func (c *Core) Admit(ctx context.Context, spec AdmissionSpec) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return ctx, func() {}, err
	}
	if c == nil || c.cluster == nil {
		return ctx, func() {}, errors.New("CephFS filesystem is unavailable")
	}
	cluster := c.cluster
	if err := clusterutil.LockMutex(ctx, c.setupMu); err != nil {
		return ctx, func() {}, err
	}
	if spec.RecheckCanceled {
		if err := ctx.Err(); err != nil {
			c.setupMu.Unlock()
			return ctx, func() {}, err
		}
	}
	if err := servicehook.LockTopology(ctx, cluster); err != nil {
		c.setupMu.Unlock()
		return ctx, func() {}, err
	}
	timeout := servicehook.StartupTimeout(cluster)
	valid, err := spec.Check(ctx)
	servicehook.Unlock(cluster)
	if err != nil {
		c.setupMu.Unlock()
		return ctx, func() {}, err
	}
	if !valid {
		c.setupMu.Unlock()
		return ctx, func() {}, errors.New(spec.Refusal)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	return ctx, func() { cancel(); c.setupMu.Unlock() }, nil
}

// WithSetupGate runs read with the setup gate held, taking the gate through
// ctx, and releases it before it returns. It takes no other lock: a read of
// feature state that the setup gate guards, such as a restoration handle's
// confirmed state, uses it to return early without an admission, so it also
// works on a removed filesystem or a terminated cluster.
func (c *Core) WithSetupGate(ctx context.Context, read func()) error {
	if err := clusterutil.LockMutex(ctx, c.setupMu); err != nil {
		return err
	}
	defer c.setupMu.Unlock()
	read()
	return nil
}
