package override

import (
	"context"
	"errors"
	"slices"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
)

// supportedOSDFlags are the reversible global flags that SetOSDFlag accepts:
// the ceph.OSDFlag constants. Composite pause and irreversible compatibility
// flags are not among them.
var supportedOSDFlags = []ceph.OSDFlag{
	ceph.OSDFlagNoUp, ceph.OSDFlagNoDown, ceph.OSDFlagNoOut, ceph.OSDFlagNoIn,
	ceph.OSDFlagNoBackfill, ceph.OSDFlagNoRebalance, ceph.OSDFlagNoRecover,
	ceph.OSDFlagNoScrub, ceph.OSDFlagNoDeepScrub, ceph.OSDFlagNoSnapTrim,
	ceph.OSDFlagNoAutoscale,
}

// OSDFlagHandle manages restoration of one global OSDMap flag, retaining
// prior state and ownership even when application is uncertain. Copies share
// lifecycle state.
// Restore explicitly when the condition should end; termination disposes of
// the entire OSDMap. A non-nil handle returned with an error for an uncertain
// outcome remains tracked and must be restored or the cluster terminated.
// Unrelated flags are never changed.
//
// SetOSDFlag returns a non-nil handle for every outcome. A handle of a call
// that was refused or never applied owns nothing: its Restore returns nil,
// takes no lock and never reports a restoration. The zero value is
// unavailable, and its Restore returns an error.
type OSDFlagHandle struct {
	owner *ceph.Container
	flag  ceph.OSDFlag
	state *osdFlagState
}

type osdFlagState struct {
	previous, applied, restored bool
	// token is the entry in the owner's registry of held overrides.
	token overridehook.Token
	// refused is why the call was not applied. A handle with it owns no flag
	// and has no token.
	refused error
}

// notAppliedOSDFlag returns the handle of a call that changed nothing.
func notAppliedOSDFlag(flag ceph.OSDFlag, cause error) (*OSDFlagHandle, error) {
	return &OSDFlagHandle{flag: flag, state: &osdFlagState{refused: cause}}, cause
}

// SetOSDFlag changes one reversible global flag while remembering its
// prior state. Supported flags are the ceph.OSDFlag constants:
// ceph.OSDFlagNoUp, ceph.OSDFlagNoDown, ceph.OSDFlagNoOut, ceph.OSDFlagNoIn,
// ceph.OSDFlagNoBackfill, ceph.OSDFlagNoRebalance, ceph.OSDFlagNoRecover,
// ceph.OSDFlagNoScrub, ceph.OSDFlagNoDeepScrub, ceph.OSDFlagNoSnapTrim and
// ceph.OSDFlagNoAutoscale. Any other value, including an untyped string
// constant that names another flag, is refused with an error. Composite pause
// and irreversible compatibility flags are excluded. Overlapping handles for a
// flag are refused. External edits to the same flag must not race
// apply/restore: a boolean flag has no generation and Ceph provides no CAS.
//
// The handle is non-nil for every outcome. A refusal returns a not-applied
// handle whose Restore is a no-op (see OSDFlagHandle). A nil cluster is
// refused with ceph.ErrInvalidRequest and Op "override.SetOSDFlag". A
// handle returned together with an error for an uncertain outcome is real and
// still restorable.
func SetOSDFlag(ctx context.Context, c *ceph.Container, flag ceph.OSDFlag, enabled bool) (*OSDFlagHandle, error) {
	const op = "override.SetOSDFlag"
	if c == nil {
		return notAppliedOSDFlag(flag, &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: op, Err: errors.New("the cluster is nil")})
	}

	if !slices.Contains(supportedOSDFlags, flag) {
		return notAppliedOSDFlag(flag, errors.New("unsupported reversible global OSD flag"))
	}
	var (
		handle   *OSDFlagHandle
		applyErr error
	)
	overridehook.WithTopologyNoContext(c, func() {
		handle, applyErr = setOSDFlagLocked(ctx, c, flag, enabled)
	})
	return handle, applyErr
}

// setOSDFlagLocked runs the apply with the cluster mutex held.
func setOSDFlagLocked(ctx context.Context, c *ceph.Container, flag ceph.OSDFlag, enabled bool) (*OSDFlagHandle, error) {
	if err := overridehook.Ready(c); err != nil {
		return notAppliedOSDFlag(flag, err)
	}
	// A restored override has left the registry, so a held one is active.
	token, err := overridehook.Reserve(ctx, c, overridehook.OSDFlag, string(flag))
	if err != nil {
		return notAppliedOSDFlag(flag, err)
	}
	ctx, cancel := context.WithTimeout(ctx, overridehook.StartupTimeout(c))
	defer cancel()
	flags, err := overridehook.OSDMapFlags(ctx, c)
	if err != nil {
		_, _ = overridehook.Settle(c, token, nil)
		return notAppliedOSDFlag(flag, err)
	}
	change := &OSDFlagHandle{owner: c, flag: flag, state: &osdFlagState{previous: slices.Contains(flags, string(flag)), applied: enabled, token: token}}
	_, _ = overridehook.Settle(c, token, change)
	if change.state.previous == enabled {
		return change, nil
	}
	if err := overridehook.SetOSDFlag(ctx, c, flag, enabled); err != nil {
		return change, err
	}
	flags, err = overridehook.OSDMapFlags(ctx, c)
	if err != nil {
		return change, err
	}
	if slices.Contains(flags, string(flag)) != enabled {
		return change, errors.New("OSD flag readback differs from requested state")
	}
	return change, nil
}

// Restore returns only this flag to its original state. It is idempotent and
// reconciles a lost restoration reply before another native mutation. A no-op
// lease refuses an outside change rather than overwriting it. Outside writes
// indistinguishable from applied/prior boolean states cannot be detected.
// A handle of a call that was not applied returns nil without taking a lock.
// A cluster that has been terminated refuses the restore with the error of an
// operation on a terminated cluster, and the flag stays registered.
func (change *OSDFlagHandle) Restore(ctx context.Context) error {
	if change == nil {
		return errors.New("OSD flag override is unavailable")
	}
	if change.state != nil && change.state.refused != nil {
		return nil
	}
	if change.owner == nil || change.state == nil {
		return errors.New("OSD flag override is unavailable")
	}
	c := change.owner
	var err error
	overridehook.WithTopologyNoContext(c, func() { err = change.restoreLocked(ctx, c) })
	return err
}

func (change *OSDFlagHandle) restoreLocked(ctx context.Context, c *ceph.Container) error {
	if change.state.restored {
		return nil
	}
	if err := overridehook.Ready(c); err != nil {
		return err
	}
	if !overridehook.Tracks(c, change.state.token) {
		return errors.New("OSD flag override is not tracked by this cluster")
	}
	ctx, cancel := context.WithTimeout(ctx, overridehook.StartupTimeout(c))
	defer cancel()
	flags, err := overridehook.OSDMapFlags(ctx, c)
	if err != nil {
		return err
	}
	current := slices.Contains(flags, string(change.flag))
	if current == change.state.previous {
		change.finish(c)
		return nil
	}
	if current != change.state.applied {
		return overridehook.Invalid(errors.New("OSD flag changed outside this override; refusing restoration"))
	}
	if err := overridehook.SetOSDFlag(ctx, c, change.flag, change.state.previous); err != nil {
		return err
	}
	flags, err = overridehook.OSDMapFlags(ctx, c)
	if err != nil {
		return err
	}
	if slices.Contains(flags, string(change.flag)) != change.state.previous {
		return errors.New("OSD flag restoration readback differs from original state")
	}
	change.finish(c)
	return nil
}

func (change *OSDFlagHandle) finish(c *ceph.Container) {
	change.state.restored = true
	overridehook.Release(c, change.state.token)
}
