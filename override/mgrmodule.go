package override

import (
	"context"
	"errors"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
)

// MGRModuleHandle manages restoration of one module's original enabled
// state, retaining ownership even when application is uncertain. Copies share
// lifecycle state. Restore explicitly when the test condition ends; termination
// disposes of the entire cluster. A real handle returned with an error must
// also be restored or the cluster terminated.
//
// SetMGRModule returns a non-nil handle for every outcome. A handle of a call
// that was refused or never applied owns nothing: its Restore returns nil, takes
// no lock and never reports a restoration. A handle returned together with an
// error for an uncertain outcome is real and still restorable. The zero value
// is unavailable, and its Restore returns an error.
type MGRModuleHandle struct {
	owner *ceph.Container
	name  string
	state *mgrModuleState
}

type mgrModuleState struct {
	previous, applied, alwaysOn, restored bool
	// token is the entry in the owner's registry of held overrides.
	token overridehook.Token
	// refused is why the call was not applied. A handle with it owns no module
	// and has no token.
	refused error
}

// mgrModuleNotApplied returns the handle of a call that changed nothing.
func mgrModuleNotApplied(name string, cause error) (*MGRModuleHandle, error) {
	return &MGRModuleHandle{name: name, state: &mgrModuleState{refused: cause}}, cause
}

// SetMGRModule changes configured membership without forcing unavailable
// dependencies. It rejects missing/cannot-run metadata on enable, overlapping
// handles, always-on disable, and disabling volumes or mirroring while relevant
// filesystems/mirror policies exist. An enabled always-on module accepts only a
// no-op true lease; a force-disabled always-on module cannot be temporarily
// enabled because restoring it would require force-disable. This function
// verifies membership, not command readiness: use ceph.Container.WaitMGRModuleReady
// or a native command probe before relying on the module. External edits must
// not race apply/restore; Ceph has no module membership CAS or per-module
// generation.
//
// The handle is non-nil for every outcome. A refusal returns a not-applied handle
// whose Restore is a no-op (see MGRModuleHandle). A nil cluster is refused with
// ceph.ErrInvalidRequest and a cluster that a SharedCluster shares with
// ceph.ErrSharedCluster, both with Op "override.SetMGRModule". A module that is
// already in the requested state returns a real handle and no error: it holds a
// lease with no write.
func SetMGRModule(ctx context.Context, c *ceph.Container, name string, enabled bool) (*MGRModuleHandle, error) {
	const op = "override.SetMGRModule"
	if c == nil {
		return mgrModuleNotApplied(name, &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: op, Err: errors.New("the cluster is nil")})
	}
	if err := overridehook.Admit(c, op); err != nil {
		return mgrModuleNotApplied(name, err)
	}
	if !overridehook.ValidMGRModuleName(name) {
		return mgrModuleNotApplied(name, errors.New("invalid Ceph manager module name"))
	}
	var (
		handle   *MGRModuleHandle
		applyErr error
	)
	overridehook.WithTopologyNoContext(c, func() {
		handle, applyErr = setMGRModuleLocked(ctx, c, name, enabled)
	})
	return handle, applyErr
}

// setMGRModuleLocked runs the apply with the cluster mutex held.
func setMGRModuleLocked(ctx context.Context, c *ceph.Container, name string, enabled bool) (*MGRModuleHandle, error) {
	if err := overridehook.Ready(c); err != nil {
		return mgrModuleNotApplied(name, err)
	}
	// A restored override has left the registry, so a held one is active.
	token, err := overridehook.Reserve(ctx, c, overridehook.MGRModule, name)
	if err != nil {
		return mgrModuleNotApplied(name, err)
	}
	timeout := overridehook.StartupTimeout(c)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	refuse := func(cause error) (*MGRModuleHandle, error) {
		_, _ = overridehook.Settle(c, token, nil)
		return mgrModuleNotApplied(name, cause)
	}
	modules, err := overridehook.MGRModulesSnapshot(ctx, c)
	if err != nil {
		return refuse(err)
	}
	module, ok := findMGRModule(modules, name)
	if !ok {
		return refuse(errors.New("MGR module is not configured or advertised by the active manager"))
	}
	if module.AlwaysOn && (!enabled || !module.Enabled) {
		return refuse(errors.New("always-on MGR modules cannot be disabled or temporarily re-enabled after force-disable"))
	}
	if enabled && (!module.Available || !module.CanRun) {
		return refuse(errors.New("MGR module has unavailable or cannot-run dependency metadata"))
	}
	if !enabled {
		if err := overridehook.ProtectMGRModule(ctx, c, name); err != nil {
			return refuse(err)
		}
	}
	change := &MGRModuleHandle{owner: c, name: name, state: &mgrModuleState{
		previous: module.Enabled, applied: enabled, alwaysOn: module.AlwaysOn, token: token,
	}}
	_, _ = overridehook.Settle(c, token, change)
	if module.Enabled == enabled {
		return change, nil
	}
	setErr := overridehook.SetMGRModule(ctx, c, name, enabled)
	// A lost reply cannot prove that the MON did not persist membership. Keep
	// the lease and reconcile with a fresh bounded read, retaining the error.
	readTimeout := 5 * time.Second
	if timeout > 0 && timeout < readTimeout {
		readTimeout = timeout
	}
	readCtx, readCancel := context.WithTimeout(context.WithoutCancel(ctx), readTimeout)
	defer readCancel()
	readErr := overridehook.WaitMGRModuleMembership(readCtx, c, name, enabled, module.AlwaysOn)
	return change, errors.Join(setErr, readErr)
}

// Restore returns only this module to its original membership. It is idempotent
// and reconciles an uncertain restoration reply before mutating again. It checks
// dependency metadata before enabling and current filesystem/mirror usage before
// disabling. No-op leases refuse an outside change. Outside writes equal to the
// applied or previous boolean state cannot be distinguished and must not race.
// A handle of a call that was not applied returns nil without taking a lock.
// A cluster that has been terminated refuses the restore with the error of an
// operation on a terminated cluster, and the module stays registered.
func (h *MGRModuleHandle) Restore(ctx context.Context) error {
	if h == nil {
		return errors.New("MGR module override is unavailable")
	}
	if h.state != nil && h.state.refused != nil {
		return nil
	}
	if h.owner == nil || h.state == nil {
		return errors.New("MGR module override is unavailable")
	}
	c := h.owner
	var err error
	overridehook.WithTopologyNoContext(c, func() { err = h.restoreLocked(ctx, c) })
	return err
}

func (h *MGRModuleHandle) restoreLocked(ctx context.Context, c *ceph.Container) error {
	if h.state.restored {
		return nil
	}
	if err := overridehook.Ready(c); err != nil {
		return err
	}
	if !overridehook.Tracks(c, h.state.token) {
		return errors.New("MGR module override is not tracked by this cluster")
	}
	ctx, cancel := context.WithTimeout(ctx, overridehook.StartupTimeout(c))
	defer cancel()
	modules, err := overridehook.MGRModulesSnapshot(ctx, c)
	if err != nil {
		return err
	}
	module, ok := findMGRModule(modules, h.name)
	if !ok || module.AlwaysOn != h.state.alwaysOn || module.ForceDisabled {
		return errors.New("MGR module availability or always-on policy changed outside this override")
	}
	if module.Enabled == h.state.previous {
		h.finish(c)
		return nil
	}
	if module.Enabled != h.state.applied {
		return overridehook.Invalid(errors.New("MGR module changed outside this override; refusing restoration"))
	}
	if h.state.previous {
		if !module.Available || !module.CanRun {
			return errors.New("MGR module cannot be re-enabled with unavailable or cannot-run dependency metadata")
		}
	} else {
		if module.AlwaysOn {
			return errors.New("always-on MGR module cannot be disabled")
		}
		if err := overridehook.ProtectMGRModule(ctx, c, h.name); err != nil {
			return err
		}
	}
	if err := overridehook.SetMGRModule(ctx, c, h.name, h.state.previous); err != nil {
		return err
	}
	if err := overridehook.WaitMGRModuleMembership(ctx, c, h.name, h.state.previous, h.state.alwaysOn); err != nil {
		return err
	}
	h.finish(c)
	return nil
}

func (h *MGRModuleHandle) finish(c *ceph.Container) {
	h.state.restored = true
	overridehook.Release(c, h.state.token)
}

func findMGRModule(modules []ceph.MGRModuleState, name string) (ceph.MGRModuleState, bool) {
	for _, module := range modules {
		if module.Name == name {
			return module, true
		}
	}
	return ceph.MGRModuleState{}, false
}
