package override

import (
	"context"
	"errors"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
)

// ConfigHandle manages restoration of one exact database entry, retaining
// prior state and ownership even when application is uncertain. Copies share
// lifecycle state. Restore explicitly when the test condition should end;
// cluster termination disposes of the entire configuration database. A handle
// returned with an error for an uncertain outcome must also be restored or
// the cluster terminated.
//
// SetConfig returns a non-nil handle for every outcome. A handle of a call
// that was refused or never applied owns nothing: its Restore returns nil,
// takes no lock and never reports a restoration. The zero value is
// unavailable, and its Restore returns an error.
type ConfigHandle struct {
	owner   *ceph.Container
	setting ceph.ConfigSetting
	state   *configState
}

type configState struct {
	previous *ceph.ConfigEntry
	applied  string
	known    bool
	restored bool
	// token is the override's entry in the owner's registry of held overrides.
	token overridehook.Token
	// refused is why the call was not applied. A handle with it owns nothing
	// and has no token.
	refused error
}

// String names the override's section and option without its values.
func (change ConfigHandle) String() string {
	return "Ceph config override " + overridehook.ConfigWho(change.setting) + " " + change.setting.Name
}

// GoString formats the override like String.
func (change ConfigHandle) GoString() string { return change.String() }

// notAppliedConfig returns the handle of a call that changed nothing.
func notAppliedConfig(setting ceph.ConfigSetting, cause error) (*ConfigHandle, error) {
	return &ConfigHandle{setting: setting, state: &configState{refused: cause}}, cause
}

// SetConfig sets one central database entry and records its native
// normalized value. It preserves absence separately from inherited defaults,
// and rejects overlapping handles for the same section/mask/name. On command
// or readback failure, the handle remains tracked for Restore. Never race
// external writes to this key with application/restoration; Ceph has no
// config CAS.
// MessengerV2Secure reserves its six ms_*_mode options and ms_bind_msgr1/2 in
// every section and mask: central overrides cannot change the fixed local
// bootstrap policy. MessengerV2Only reserves ms_bind_msgr1/2 the same way and
// leaves the mode options free. Select a ceph.MessengerMode when creating a
// new cluster instead.
//
// The handle is non-nil for every outcome. A refusal returns a not-applied
// handle whose Restore is a no-op (see ConfigHandle). A nil cluster is refused
// with ceph.ErrInvalidRequest and a cluster that a SharedCluster shares with
// ceph.ErrSharedCluster, both with Op "override.SetConfig". A call that fails
// after the entry was tracked, or whose write may have been applied, returns
// the real handle with the error.
func SetConfig(ctx context.Context, c *ceph.Container, setting ceph.ConfigSetting) (*ConfigHandle, error) {
	const op = "override.SetConfig"
	if c == nil {
		return notAppliedConfig(setting, &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: op, Err: errors.New("the cluster is nil")})
	}
	if err := overridehook.Admit(c, op); err != nil {
		return notAppliedConfig(setting, err)
	}
	return setConfig(ctx, c, setting)
}

// setConfig is SetConfig after the admission, shared with SetMessengerPolicy,
// which admits once under its own name.
func setConfig(ctx context.Context, c *ceph.Container, setting ceph.ConfigSetting) (*ConfigHandle, error) {
	if err := ctx.Err(); err != nil {
		return notAppliedConfig(setting, err)
	}
	setting, err := overridehook.NormalizeConfigSetting(setting)
	if err != nil {
		return notAppliedConfig(setting, err)
	}
	if err := overridehook.CheckConfigAllowed(c, setting); err != nil {
		return notAppliedConfig(setting, err)
	}
	var (
		handle   *ConfigHandle
		applyErr error
	)
	if err := overridehook.WithTopology(ctx, c, func() error {
		handle, applyErr = setConfigLocked(ctx, c, setting)
		return nil
	}); err != nil {
		return notAppliedConfig(setting, err)
	}
	return handle, applyErr
}

// setConfigLocked runs the apply with the cluster mutex held.
func setConfigLocked(ctx context.Context, c *ceph.Container, setting ceph.ConfigSetting) (*ConfigHandle, error) {
	if err := overridehook.Ready(c); err != nil {
		return notAppliedConfig(setting, err)
	}
	// A restored override has left the registry, so a held one is active.
	token, err := overridehook.Reserve(ctx, c, overridehook.Config, overridehook.ConfigKey(setting))
	if err != nil {
		return notAppliedConfig(setting, err)
	}
	ctx, cancel := configDeadline(ctx, c)
	defer cancel()
	entries, err := overridehook.ConfigEntries(ctx, c)
	if err != nil {
		_, _ = overridehook.Settle(c, token, nil)
		return notAppliedConfig(setting, err)
	}
	change := &ConfigHandle{owner: c, setting: setting, state: &configState{previous: findConfigEntry(entries, setting), token: token}}
	_, _ = overridehook.Settle(c, token, change)
	if change.state.previous != nil && change.state.previous.Value == setting.Value {
		change.state.applied, change.state.known = setting.Value, true
		return change, nil
	}
	setErr := overridehook.ConfigSet(ctx, c, setting, setting.Value)
	// A lost reply or cancellation cannot prove config set did not persist.
	// Record readback using a bounded fresh context, preserving the original
	// error. External writes must not race this native mutation/readback pair.
	readCtx, readCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer readCancel()
	entries, readErr := overridehook.ConfigEntries(readCtx, c)
	if readErr == nil {
		if current := findConfigEntry(entries, setting); current != nil {
			change.state.applied, change.state.known = current.Value, true
		}
	}
	if setErr != nil || readErr != nil {
		return change, errors.Join(setErr, readErr)
	}
	if !change.state.known {
		return change, errors.New("configuration set returned success without a stored entry")
	}
	return change, nil
}

// Restore returns the exact entry to its prior value, or removes it if it was
// absent. It refuses a different current value to preserve outside edits.
// After an uncertain restore reply, retrying reconciles the native state
// first. Same value outside writes cannot be distinguished; external edits
// must not race. A handle of a call that was not applied returns nil without
// taking a lock. A cluster that has been terminated refuses the restore.
func (change *ConfigHandle) Restore(ctx context.Context) error {
	if change == nil || change.state == nil {
		return errors.New("configuration override is unavailable")
	}
	if change.state.refused != nil {
		return nil
	}
	if change.owner == nil {
		return errors.New("configuration override is unavailable")
	}
	c := change.owner
	var err error
	overridehook.WithTopologyNoContext(c, func() { err = change.restoreLocked(ctx, c) })
	return err
}

func (change *ConfigHandle) restoreLocked(ctx context.Context, c *ceph.Container) error {
	if change.state.restored {
		return nil
	}
	if err := overridehook.Ready(c); err != nil {
		return err
	}
	if !overridehook.Tracks(c, change.state.token) {
		return errors.New("configuration override is not tracked by this cluster")
	}
	ctx, cancel := configDeadline(ctx, c)
	defer cancel()
	entries, err := overridehook.ConfigEntries(ctx, c)
	if err != nil {
		return err
	}
	current := findConfigEntry(entries, change.setting)
	if sameConfigValue(current, change.state.previous) {
		change.finish(c)
		return nil
	}
	expected := change.state.applied
	if !change.state.known {
		expected = change.setting.Value
		if current != nil && current.Value != expected {
			return errors.New("configuration apply readback was unavailable and the current value differs from the requested text; inspect native state or terminate the cluster")
		}
	}
	if current == nil || current.Value != expected {
		return overridehook.Invalid(errors.New("configuration entry changed outside this override; refusing restoration"))
	}
	var restoreErr error
	if change.state.previous == nil {
		restoreErr = overridehook.ConfigRemove(ctx, c, change.setting)
	} else {
		restoreErr = overridehook.ConfigSet(ctx, c, change.setting, change.state.previous.Value)
	}
	if restoreErr != nil {
		return restoreErr
	}
	entries, err = overridehook.ConfigEntries(ctx, c)
	if err != nil {
		return err
	}
	if !sameConfigValue(findConfigEntry(entries, change.setting), change.state.previous) {
		return errors.New("configuration restoration readback differs from previous entry")
	}
	change.finish(c)
	return nil
}

func (change *ConfigHandle) finish(c *ceph.Container) {
	change.state.restored = true
	overridehook.Release(c, change.state.token)
}

// configDeadline bounds one fixture operation by the cluster's startup
// timeout.
func configDeadline(ctx context.Context, c *ceph.Container) (context.Context, context.CancelFunc) {
	if timeout := overridehook.StartupTimeout(c); timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}
	return context.WithCancel(ctx)
}

func sameConfigValue(a, b *ceph.ConfigEntry) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Value == b.Value
}

func findConfigEntry(entries []ceph.ConfigEntry, setting ceph.ConfigSetting) *ceph.ConfigEntry {
	for _, entry := range entries {
		if entry.Section == setting.Section && entry.Mask == setting.Mask && entry.Name == setting.Name {
			copy := entry
			return &copy
		}
	}
	return nil
}
