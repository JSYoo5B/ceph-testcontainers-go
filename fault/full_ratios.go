package fault

import (
	"context"
	"errors"
	"math"
	"strconv"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
)

// FullRatiosOverride restores all three global OSDMap thresholds as one owned
// fixture. Copies share lifecycle state. The native CLI changes one threshold
// per command, so apply and restore are not atomic. A non-nil handle returned
// with an error remains tracked; restore it or terminate the disposable cluster.
type FullRatiosOverride struct {
	owner *ceph.Container
	state *fullRatiosOverrideState
}

type fullRatiosOverrideState struct {
	previous, expected ceph.FullRatios
	pending            *ceph.FullRatios
	fsid               string
	epoch              uint32
	restored           bool
	// token is the override's entry in the owner's fault registry.
	token faulthook.Token
}

// TemporaryFullRatios applies finite thresholds satisfying
// 0 < NearFull < BackfillFull < Full <= 1 after native float32 normalization.
// The original map must also have this ordering. Overlapping handles are
// refused, including no-op leases. Each intermediate commit preserves ordering.
// Success verifies map values, not OSD reports or client write availability.
// External writers must not race apply/restore: Ceph has no threshold CAS and
// writes equal to owned values cannot be distinguished from this fixture.
func TemporaryFullRatios(ctx context.Context, c *ceph.Container, requested ceph.FullRatios) (*FullRatiosOverride, error) {
	if gateErr := faulthook.Admit(c, "fault.TemporaryFullRatios"); gateErr != nil {
		return nil, gateErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c == nil {
		return nil, errors.New("ceph cluster is unavailable")
	}
	requested, err := normalizeFullRatios(requested)
	if err != nil {
		return nil, err
	}
	ctx, cancel := fullRatiosDeadline(ctx, c)
	defer cancel()
	var change *FullRatiosOverride
	err = faulthook.WithTopology(ctx, c, func() error {
		var applyErr error
		change, applyErr = temporaryFullRatiosLocked(ctx, c, requested)
		return applyErr
	})
	return change, err
}

func temporaryFullRatiosLocked(ctx context.Context, c *ceph.Container, requested ceph.FullRatios) (*FullRatiosOverride, error) {
	// A restored override has left the registry, so a held one is active.
	token, _, err := faulthook.Reserve(ctx, c, faulthook.FullRatios, "")
	if err != nil {
		return nil, err
	}
	session, err := faulthook.CaptureFullRatiosSession(ctx, c)
	if err != nil {
		_, _ = faulthook.Settle(c, token, faulthook.Held{})
		return nil, err
	}
	snapshot, err := session.Read(ctx)
	if err != nil {
		_, _ = faulthook.Settle(c, token, faulthook.Held{})
		return nil, err
	}
	if !orderedFullRatios(snapshot.Ratios) {
		_, _ = faulthook.Settle(c, token, faulthook.Held{})
		return nil, errors.New("original native full ratios must have positive ascending thresholds")
	}
	change := &FullRatiosOverride{owner: c, state: &fullRatiosOverrideState{
		previous: snapshot.Ratios, expected: snapshot.Ratios, fsid: snapshot.FSID, epoch: snapshot.Epoch, token: token,
	}}
	_, _ = faulthook.Settle(c, token, faulthook.Held{Handle: change})
	return change, change.transition(ctx, session, requested)
}

// Restore returns all three thresholds to their captured values. It is
// idempotent and reconciles uncertain native replies before another mutation.
// A different current tuple is refused rather than overwriting outside edits.
// Only the latest owned tuple or the two sides of one uncertain command are
// accepted; older apply prefixes do not authorize restoration. Unrelated flags,
// pool policies and central configuration are not changed.
func (change *FullRatiosOverride) Restore(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if change == nil || change.owner == nil || change.state == nil {
		return errors.New("full ratios override is unavailable")
	}
	c := change.owner
	ctx, cancel := fullRatiosDeadline(ctx, c)
	defer cancel()
	return faulthook.WithTopology(ctx, c, func() error { return change.restoreLocked(ctx) })
}

func (change *FullRatiosOverride) restoreLocked(ctx context.Context) error {
	c := change.owner
	if change.state.restored {
		return nil
	}
	if !faulthook.Tracks(c, change.state.token) {
		return errors.New("full ratios override is not tracked by this cluster")
	}
	session, err := faulthook.CaptureFullRatiosSession(ctx, c)
	if err != nil {
		return err
	}
	if session.Identity() != change.state.fsid {
		return clusterutil.OperationError(ctx, "full ratios override original bootstrap identity changed", nil)
	}
	if err := change.transition(ctx, session, change.state.previous); err != nil {
		return err
	}
	change.state.restored = true
	faulthook.Release(c, change.state.token)
	return nil
}

func fullRatiosDeadline(ctx context.Context, c *ceph.Container) (context.Context, context.CancelFunc) {
	if faulthook.StartupTimeout(c) > 0 {
		return context.WithTimeout(ctx, faulthook.StartupTimeout(c))
	}
	return context.WithCancel(ctx)
}

func normalizeFullRatios(ratios ceph.FullRatios) (ceph.FullRatios, error) {
	for _, ratio := range []float64{ratios.NearFull, ratios.BackfillFull, ratios.Full} {
		if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio <= 0 || ratio > 1 {
			return ceph.FullRatios{}, invalidRequest("full ratios must be finite fractions greater than zero and at most one")
		}
	}
	ratios = ceph.FullRatios{NearFull: float64(float32(ratios.NearFull)), BackfillFull: float64(float32(ratios.BackfillFull)), Full: float64(float32(ratios.Full))}
	if !orderedFullRatios(ratios) {
		return ceph.FullRatios{}, invalidRequest("full ratios must remain positive and ascending after native float32 normalization")
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

func (change *FullRatiosOverride) reconcile(snapshot ceph.FullRatioSnapshot) error {
	state := change.state
	if snapshot.Epoch < state.epoch || (snapshot.Ratios != state.expected && (state.pending == nil || snapshot.Ratios != *state.pending)) {
		return invalidRequest("native full ratios changed outside this override; refusing mutation")
	}
	state.expected, state.pending, state.epoch = snapshot.Ratios, nil, snapshot.Epoch
	return nil
}

func (change *FullRatiosOverride) transition(ctx context.Context, session faulthook.FullRatiosSession, target ceph.FullRatios) error {
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
		if _, err := session.Command(ctx, "osd", "set-"+step.name+"-ratio", strconv.FormatFloat(step.value, 'g', -1, 64)); err != nil {
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
