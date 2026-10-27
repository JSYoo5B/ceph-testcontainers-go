package override

import (
	"context"
	"errors"
	"math"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
)

// FullRatiosHandle restores all three global OSDMap thresholds as one owned
// fixture. Copies share lifecycle state. The native CLI changes one threshold
// per command, so apply and restore are not atomic. A handle returned together
// with an error for an uncertain outcome remains tracked; restore it or
// terminate the disposable cluster.
//
// SetFullRatios returns a non-nil handle for every outcome. A handle of a call
// that was refused or never applied owns nothing: its Restore returns nil, takes
// no lock and never reports a restoration. The zero value is unavailable, and
// its Restore returns an error.
type FullRatiosHandle struct {
	owner *ceph.Container
	state *fullRatiosState
}

type fullRatiosState struct {
	previous, expected ceph.FullRatios
	pending            *ceph.FullRatios
	fsid               string
	epoch              uint32
	restored           bool
	// token is the override's entry in the owner's registry of held overrides.
	token overridehook.Token
	// refused is why the call was not applied. A handle with it owns nothing
	// and has no token.
	refused error
}

// notAppliedFullRatios returns the handle of a call that changed nothing.
func notAppliedFullRatios(cause error) (*FullRatiosHandle, error) {
	return &FullRatiosHandle{state: &fullRatiosState{refused: cause}}, cause
}

// SetFullRatios applies finite thresholds satisfying
// 0 < NearFull < BackfillFull < Full <= 1 after native float32 normalization.
// The original map must also have this ordering. Overlapping handles are
// refused, including no-op leases. Each intermediate commit preserves ordering.
// Success verifies map values, not OSD reports or client write availability.
// External writers must not race apply/restore: Ceph has no threshold CAS and
// writes equal to owned values cannot be distinguished from this fixture.
//
// This is an explicit OSDMap policy override, with no expiry or automatic
// scope-exit restoration. Context cancellation does not undo committed changes.
// The caller must retain the handle of a call that returned an error after the
// first native step and call Restore with a fresh context when needed, or
// terminate the disposable cluster.
//
// The handle is non-nil for every outcome. A refusal returns a not-applied
// handle whose Restore is a no-op (see FullRatiosHandle). A nil cluster is
// refused with ceph.ErrInvalidRequest and a cluster that a SharedCluster shares
// with ceph.ErrSharedCluster, both with Op "override.SetFullRatios". A call that
// fails after the override was tracked, from the first native step on, returns
// the real handle with the error.
func SetFullRatios(ctx context.Context, c *ceph.Container, requested ceph.FullRatios) (*FullRatiosHandle, error) {
	const op = "override.SetFullRatios"
	if c == nil {
		return notAppliedFullRatios(&ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: op, Err: errors.New("the cluster is nil")})
	}
	if err := overridehook.Admit(c, op); err != nil {
		return notAppliedFullRatios(err)
	}
	if err := ctx.Err(); err != nil {
		return notAppliedFullRatios(err)
	}
	requested, err := normalizeFullRatios(requested)
	if err != nil {
		return notAppliedFullRatios(err)
	}
	ctx, cancel := fullRatiosDeadline(ctx, c)
	defer cancel()
	var (
		handle   *FullRatiosHandle
		applyErr error
	)
	if err := overridehook.WithTopology(ctx, c, func() error {
		handle, applyErr = setFullRatiosLocked(ctx, c, requested)
		return nil
	}); err != nil {
		return notAppliedFullRatios(err)
	}
	return handle, applyErr
}

// setFullRatiosLocked runs the apply with the cluster mutex held.
func setFullRatiosLocked(ctx context.Context, c *ceph.Container, requested ceph.FullRatios) (*FullRatiosHandle, error) {
	// A restored override has left the registry, so a held one is active.
	token, err := overridehook.Reserve(ctx, c, overridehook.FullRatios, "")
	if err != nil {
		return notAppliedFullRatios(err)
	}
	session, err := overridehook.OpenFullRatios(ctx, c)
	if err != nil {
		_, _ = overridehook.Settle(c, token, nil)
		return notAppliedFullRatios(err)
	}
	snapshot, err := session.Read(ctx)
	if err != nil {
		_, _ = overridehook.Settle(c, token, nil)
		return notAppliedFullRatios(err)
	}
	if !orderedFullRatios(snapshot.Ratios) {
		_, _ = overridehook.Settle(c, token, nil)
		return notAppliedFullRatios(errors.New("original native full ratios must have positive ascending thresholds"))
	}
	change := &FullRatiosHandle{owner: c, state: &fullRatiosState{
		previous: snapshot.Ratios, expected: snapshot.Ratios, fsid: snapshot.FSID, epoch: snapshot.Epoch, token: token,
	}}
	// Tracked before the first native step: an uncertain step still needs Restore.
	_, _ = overridehook.Settle(c, token, change)
	return change, change.transition(ctx, session, requested)
}

// Restore returns all three thresholds to their captured values. It is
// idempotent and reconciles uncertain native replies before another mutation.
// A different current tuple is refused rather than overwriting outside edits.
// Only the latest owned tuple or the two sides of one uncertain command are
// accepted; older apply prefixes do not authorize restoration. Unrelated flags,
// pool policies and central configuration are not changed. A handle of a call
// that was not applied returns nil without taking a lock. A cluster that has
// been terminated refuses the restore, and the override stays registered.
func (change *FullRatiosHandle) Restore(ctx context.Context) error {
	if change == nil {
		return errors.New("full ratios override is unavailable")
	}
	if change.state != nil && change.state.refused != nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if change.owner == nil || change.state == nil {
		return errors.New("full ratios override is unavailable")
	}
	c := change.owner
	ctx, cancel := fullRatiosDeadline(ctx, c)
	defer cancel()
	return overridehook.WithTopology(ctx, c, func() error { return change.restoreLocked(ctx, c) })
}

func (change *FullRatiosHandle) restoreLocked(ctx context.Context, c *ceph.Container) error {
	if change.state.restored {
		return nil
	}
	if !overridehook.Tracks(c, change.state.token) {
		return errors.New("full ratios override is not tracked by this cluster")
	}
	session, err := overridehook.OpenFullRatios(ctx, c)
	if err != nil {
		return err
	}
	if err := session.CheckIdentity(ctx, change.state.fsid); err != nil {
		return err
	}
	if err := change.transition(ctx, session, change.state.previous); err != nil {
		return err
	}
	change.state.restored = true
	overridehook.Release(c, change.state.token)
	return nil
}

func fullRatiosDeadline(ctx context.Context, c *ceph.Container) (context.Context, context.CancelFunc) {
	if timeout := overridehook.StartupTimeout(c); timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}
	return context.WithCancel(ctx)
}

func normalizeFullRatios(ratios ceph.FullRatios) (ceph.FullRatios, error) {
	for _, ratio := range []float64{ratios.NearFull, ratios.BackfillFull, ratios.Full} {
		if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio <= 0 || ratio > 1 {
			return ceph.FullRatios{}, overridehook.Invalid(errors.New("full ratios must be finite fractions greater than zero and at most one"))
		}
	}
	ratios = ceph.FullRatios{NearFull: float64(float32(ratios.NearFull)), BackfillFull: float64(float32(ratios.BackfillFull)), Full: float64(float32(ratios.Full))}
	if !orderedFullRatios(ratios) {
		return ceph.FullRatios{}, overridehook.Invalid(errors.New("full ratios must remain positive and ascending after native float32 normalization"))
	}
	return ratios, nil
}

func orderedFullRatios(ratios ceph.FullRatios) bool {
	return ratios.NearFull > 0 && ratios.NearFull < ratios.BackfillFull && ratios.BackfillFull < ratios.Full && ratios.Full <= 1
}

type fullRatioStep struct {
	name  string
	next  ceph.FullRatios
	value float64
}

// Decreases begin at the lower threshold; increases begin at the upper one.
// With ordered endpoints, every intermediate tuple stays ordered too.
func fullRatioSteps(current, target ceph.FullRatios) []fullRatioStep {
	steps := make([]fullRatioStep, 0, 3)
	fields := []struct {
		name string
		get  func(ceph.FullRatios) float64
		set  func(*ceph.FullRatios, float64)
	}{
		{"nearfull", func(r ceph.FullRatios) float64 { return r.NearFull }, func(r *ceph.FullRatios, value float64) { r.NearFull = value }},
		{"backfillfull", func(r ceph.FullRatios) float64 { return r.BackfillFull }, func(r *ceph.FullRatios, value float64) { r.BackfillFull = value }},
		{"full", func(r ceph.FullRatios) float64 { return r.Full }, func(r *ceph.FullRatios, value float64) { r.Full = value }},
	}
	add := func(index int, decrease bool) {
		field := fields[index]
		old, value := field.get(current), field.get(target)
		if old == value || (value < old) != decrease {
			return
		}
		field.set(&current, value)
		steps = append(steps, fullRatioStep{name: field.name, next: current, value: value})
	}
	for index := range fields {
		add(index, true)
	}
	for index := len(fields) - 1; index >= 0; index-- {
		add(index, false)
	}
	return steps
}

func (change *FullRatiosHandle) reconcile(snapshot ceph.FullRatioSnapshot) error {
	state := change.state
	if snapshot.Epoch < state.epoch || (snapshot.Ratios != state.expected && (state.pending == nil || snapshot.Ratios != *state.pending)) {
		return overridehook.Invalid(errors.New("native full ratios changed outside this override; refusing mutation"))
	}
	state.expected, state.pending, state.epoch = snapshot.Ratios, nil, snapshot.Epoch
	return nil
}

func (change *FullRatiosHandle) transition(ctx context.Context, session overridehook.FullRatiosSession, target ceph.FullRatios) error {
	snapshot, err := session.Read(ctx)
	if err != nil {
		return err
	}
	if err := change.reconcile(snapshot); err != nil {
		return err
	}
	for _, step := range fullRatioSteps(change.state.expected, target) {
		// Guard the complete tuple immediately before every native attempt. An
		// unrelated epoch advance is allowed; any threshold drift is refused.
		snapshot, err = session.Read(ctx)
		if err != nil {
			return err
		}
		if err := change.reconcile(snapshot); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		next := step.next
		change.state.pending = &next
		// Journal both possible outcomes before invocation. A command error,
		// canceled context or lost readback cannot prove absence of a commit.
		if err := session.SetRatio(ctx, step.name, step.value); err != nil {
			return err
		}
		snapshot, err = session.Read(ctx)
		if err != nil {
			return err
		}
		if snapshot.Ratios != next {
			return errors.New("native full ratios readback differs from requested state")
		}
		if err := change.reconcile(snapshot); err != nil {
			return err
		}
	}
	if change.state.expected != target {
		return errors.New("native full ratios did not reach the requested tuple")
	}
	return ctx.Err()
}
