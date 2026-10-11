package ceph

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
)

// FullRatios are global OSDMap utilization thresholds, expressed as fractions
// of capacity. These are separate from central configuration and pool quotas.
// Native OSDMap storage uses float32; observations retain its normalized value.
type FullRatios struct {
	NearFull, BackfillFull, Full float64
}

// FullRatioSnapshot is one native OSDMap observation. Epoch is not a generation
// for these thresholds: unrelated OSDMap changes can advance it too. The values
// describe map policy, not whether any OSD has reported the corresponding state.
type FullRatioSnapshot struct {
	FSID   string
	Epoch  uint32
	Ratios FullRatios
}

// FullRatios reads native thresholds through one captured control CLI handle,
// checking the original bootstrap FSID before and after the map query. Native
// zero, equal and misordered thresholds remain observable. It makes no writes,
// retries or background workers and returns a zero snapshot on any failure.
// These separate reads are not atomic; external configuration writers must not
// race the observation. Poll HealthDetails for eventual OSD fullness reports.
func (c *Container) FullRatios(ctx context.Context) (FullRatioSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return FullRatioSnapshot{}, err
	}
	if c == nil {
		return FullRatioSnapshot{}, errors.New("ceph cluster is unavailable")
	}
	if err := c.lockTopology(ctx); err != nil {
		return FullRatioSnapshot{}, err
	}
	defer c.mu.Unlock()
	session, err := c.fullRatiosSession(ctx)
	if err != nil {
		return FullRatioSnapshot{}, err
	}
	return session.read(ctx)
}

// FullRatiosOverride restores all three global OSDMap thresholds as one owned
// fixture. Copies share lifecycle state. The native CLI changes one threshold
// per command, so apply and restore are not atomic. A non-nil handle returned
// with an error remains tracked; restore it or terminate the disposable cluster.
type FullRatiosOverride struct {
	owner *Container
	state *fullRatiosOverrideState
}

type fullRatiosOverrideState struct {
	previous, expected FullRatios
	pending            *FullRatios
	fsid               string
	epoch              uint32
	restored           bool
}

// TemporaryFullRatios applies finite thresholds satisfying
// 0 < NearFull < BackfillFull < Full <= 1 after native float32 normalization.
// The original map must also have this ordering. Overlapping handles are
// refused, including no-op leases. Each intermediate commit preserves ordering.
// Success verifies map values, not OSD reports or client write availability.
// External writers must not race apply/restore: Ceph has no threshold CAS and
// writes equal to owned values cannot be distinguished from this fixture.
func (c *Container) TemporaryFullRatios(ctx context.Context, requested FullRatios) (*FullRatiosOverride, error) {
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
	ctx, cancel := c.fullRatiosDeadline(ctx)
	defer cancel()
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	if c.fullRatiosOverride != nil && !c.fullRatiosOverride.state.restored {
		return nil, errors.New("restore the existing full ratios override first")
	}
	session, err := c.fullRatiosSession(ctx)
	if err != nil {
		return nil, err
	}
	snapshot, err := session.read(ctx)
	if err != nil {
		return nil, err
	}
	if !orderedFullRatios(snapshot.Ratios) {
		return nil, errors.New("original native full ratios must have positive ascending thresholds")
	}
	change := &FullRatiosOverride{owner: c, state: &fullRatiosOverrideState{
		previous: snapshot.Ratios, expected: snapshot.Ratios, fsid: snapshot.FSID, epoch: snapshot.Epoch,
	}}
	c.fullRatiosOverride = change
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
	ctx, cancel := c.fullRatiosDeadline(ctx)
	defer cancel()
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if change.state.restored {
		return nil
	}
	if c.fullRatiosOverride == nil || c.fullRatiosOverride.state != change.state {
		return errors.New("full ratios override is not tracked by this cluster")
	}
	session, err := c.fullRatiosSession(ctx)
	if err != nil {
		return err
	}
	if session.fsid != change.state.fsid {
		return clientOperationError(ctx, "full ratios override original bootstrap identity changed", nil)
	}
	if err := change.transition(ctx, session, change.state.previous); err != nil {
		return err
	}
	change.state.restored = true
	c.fullRatiosOverride = nil
	return nil
}

func (c *Container) fullRatiosDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.settings.startupTimeout > 0 {
		return context.WithTimeout(ctx, c.settings.startupTimeout)
	}
	return context.WithCancel(ctx)
}

type fullRatiosCLISession struct {
	control testcontainers.Container
	fsid    string
}

// The caller holds the topology gate. Capture both control and original
// bootstrap identity once; queries never switch to a replacement control handle.
func (c *Container) fullRatiosSession(ctx context.Context) (fullRatiosCLISession, error) {
	if c.closed {
		return fullRatiosCLISession{}, clientOperationError(ctx, "ceph cluster is terminated", nil)
	}
	control, err := c.ControlContainerContext(ctx)
	if err != nil {
		return fullRatiosCLISession{}, err
	}
	if control == nil {
		return fullRatiosCLISession{}, clientOperationError(ctx, "ceph control container is unavailable", nil)
	}
	if err := lockTopologyReadMutex(ctx, &c.configMu); err != nil {
		return fullRatiosCLISession{}, err
	}
	fsid, identityErr := monitorConfigClusterIdentity(c.config)
	confirmed := len(c.keyring) != 0
	c.configMu.RUnlock()
	if identityErr != nil || !confirmed {
		return fullRatiosCLISession{}, clientOperationError(ctx, "ceph original bootstrap identity is unavailable", identityErr)
	}
	return fullRatiosCLISession{control: control, fsid: fsid}, nil
}

func (session fullRatiosCLISession) command(ctx context.Context, args ...string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := command(ctx, session.control, append([]string{"ceph", "--connect-timeout", "5"}, args...)...)
	if err != nil || ctx.Err() != nil {
		return nil, clientOperationError(ctx, "native full ratios command failed", err)
	}
	return data, nil
}

func (session fullRatiosCLISession) checkFSID(ctx context.Context) error {
	data, err := session.command(ctx, "fsid")
	if err != nil {
		return err
	}
	native := strings.TrimSpace(string(data))
	parsed, err := uuid.Parse(native)
	if err != nil || parsed == uuid.Nil || parsed.String() != native || native != session.fsid {
		return clientOperationError(ctx, "native full ratios FSID differs from the original cluster", err)
	}
	return ctx.Err()
}

func (session fullRatiosCLISession) read(ctx context.Context) (FullRatioSnapshot, error) {
	if err := session.checkFSID(ctx); err != nil {
		return FullRatioSnapshot{}, err
	}
	data, err := session.command(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		return FullRatioSnapshot{}, err
	}
	snapshot, err := decodeFullRatios(data, session.fsid)
	if err != nil || ctx.Err() != nil {
		return FullRatioSnapshot{}, clientOperationError(ctx, "decode native full ratios failed", err)
	}
	if err := session.checkFSID(ctx); err != nil {
		return FullRatioSnapshot{}, err
	}
	return snapshot, nil
}

func decodeFullRatios(data []byte, fsid string) (FullRatioSnapshot, error) {
	var native struct {
		FSID         *string  `json:"fsid"`
		Epoch        *uint32  `json:"epoch"`
		NearFull     *float64 `json:"nearfull_ratio"`
		BackfillFull *float64 `json:"backfillfull_ratio"`
		Full         *float64 `json:"full_ratio"`
	}
	if err := healthDetailsJSON(data, &native); err != nil || native.FSID == nil || *native.FSID != fsid || native.Epoch == nil || native.NearFull == nil || native.BackfillFull == nil || native.Full == nil {
		return FullRatioSnapshot{}, errors.New("incomplete native full ratios")
	}
	ratios := FullRatios{NearFull: *native.NearFull, BackfillFull: *native.BackfillFull, Full: *native.Full}
	for _, ratio := range []float64{ratios.NearFull, ratios.BackfillFull, ratios.Full} {
		if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio < 0 || ratio > 1 {
			return FullRatioSnapshot{}, errors.New("invalid native full ratio")
		}
	}
	return FullRatioSnapshot{FSID: fsid, Epoch: *native.Epoch, Ratios: ratios}, nil
}

func normalizeFullRatios(ratios FullRatios) (FullRatios, error) {
	for _, ratio := range []float64{ratios.NearFull, ratios.BackfillFull, ratios.Full} {
		if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio <= 0 || ratio > 1 {
			return FullRatios{}, errors.New("full ratios must be finite fractions greater than zero and at most one")
		}
	}
	ratios = FullRatios{NearFull: float64(float32(ratios.NearFull)), BackfillFull: float64(float32(ratios.BackfillFull)), Full: float64(float32(ratios.Full))}
	if !orderedFullRatios(ratios) {
		return FullRatios{}, errors.New("full ratios must remain positive and ascending after native float32 normalization")
	}
	return ratios, nil
}

func orderedFullRatios(ratios FullRatios) bool {
	return ratios.NearFull > 0 && ratios.NearFull < ratios.BackfillFull && ratios.BackfillFull < ratios.Full && ratios.Full <= 1
}

type fullRatioStep struct {
	name  string
	next  FullRatios
	value float64
}

// Decreases begin at the lower threshold; increases begin at the upper one.
// With ordered endpoints, every intermediate tuple stays ordered too.
func fullRatioSteps(current, target FullRatios) []fullRatioStep {
	steps := make([]fullRatioStep, 0, 3)
	fields := []struct {
		name string
		get  func(FullRatios) float64
		set  func(*FullRatios, float64)
	}{
		{"nearfull", func(r FullRatios) float64 { return r.NearFull }, func(r *FullRatios, value float64) { r.NearFull = value }},
		{"backfillfull", func(r FullRatios) float64 { return r.BackfillFull }, func(r *FullRatios, value float64) { r.BackfillFull = value }},
		{"full", func(r FullRatios) float64 { return r.Full }, func(r *FullRatios, value float64) { r.Full = value }},
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

func (change *FullRatiosOverride) reconcile(snapshot FullRatioSnapshot) error {
	state := change.state
	if snapshot.Epoch < state.epoch || (snapshot.Ratios != state.expected && (state.pending == nil || snapshot.Ratios != *state.pending)) {
		return errors.New("native full ratios changed outside this override; refusing mutation")
	}
	state.expected, state.pending, state.epoch = snapshot.Ratios, nil, snapshot.Epoch
	return nil
}

func (change *FullRatiosOverride) transition(ctx context.Context, session fullRatiosCLISession, target FullRatios) error {
	snapshot, err := session.read(ctx)
	if err != nil {
		return err
	}
	if err := change.reconcile(snapshot); err != nil {
		return err
	}
	for _, step := range fullRatioSteps(change.state.expected, target) {
		// Guard the complete tuple immediately before every native attempt. An
		// unrelated epoch advance is allowed; any threshold drift is refused.
		snapshot, err = session.read(ctx)
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
		if _, err := session.command(ctx, "osd", "set-"+step.name+"-ratio", strconv.FormatFloat(step.value, 'g', -1, 64)); err != nil {
			return err
		}
		snapshot, err = session.read(ctx)
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
