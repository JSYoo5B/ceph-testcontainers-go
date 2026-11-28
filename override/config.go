package override

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/mutation"
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
	command  mutation.Fence
	previous *ceph.ConfigEntry
	applied  string
	known    bool
	restored bool
	// cleared records that a restoration of an originally absent MGR module
	// option stored the empty value that precedes its removal, so a retry
	// recognizes that value as this override's own.
	cleared bool
	// mgrDelivery owns a temporary per-manager setting until its restoration
	// succeeds. A retry settles it before changing the module entry again.
	mgrDelivery *ConfigHandle
	// mgrProbe must settle before retry can change the delivery marker.
	mgrProbe *ceph.NativeCommandUnstoppedError
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

// SetConfig writes one central database entry and records the value read back
// from Ceph for restoration. It preserves absence separately from inherited defaults,
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
// with ceph.ErrInvalidRequest and Op "override.SetConfig". A call that fails
// after the entry was tracked, or whose write may have been applied, returns
// the real handle with the error.
//
// Name may identify a daemon option or an MGR module key such as
// mgr/volumes/max_concurrent_clones. Ceph acknowledges the write without
// returning the normalized request. SetConfig confirms that the exact entry
// exists after the write; the readback alone cannot establish that its value
// matches the normalized request. Use ceph.Observer.Configuration to check
// the stored value when its native spelling is known. Local ceph.conf
// entries (including ceph.WithConfigFile), command-line arguments and
// runtime overrides take precedence over the database. Some options,
// such as osd_op_queue, apply only when a daemon starts. Check a running
// daemon's effective value with
// ceph.Container.Ceph(ctx, "config", "show", "osd.0", name). Configuration
// output can contain secrets; log only selected fields. Restore of an
// originally absent MGR module key publishes an empty value before removal
// (see ConfigHandle.Restore for the delivery limitation).
//
// Outcomes, as the handle it returns and the error:
//   - Write acknowledged and the exact entry found at readback: real handle; nil.
//   - The stored value already equals the request: real handle (a lease with
//     no write); nil.
//   - Nil cluster: not applied handle; ceph.ErrInvalidRequest, Op
//     override.SetConfig.
//   - Context already ended, or ended while waiting for the topology lock: not
//     applied handle; the context error.
//   - Invalid section, mask, name or value: not applied handle;
//     ceph.ErrInvalidRequest.
//   - Option fixed by the MessengerV2Secure or MessengerV2Only bootstrap: not
//     applied handle; ceph.ErrInvalidRequest.
//   - Terminated cluster or no control container: not applied handle; the
//     cluster's own error.
//   - This cluster already holds an override of the same section, mask and
//     name: not applied handle; plain error.
//   - The configuration cannot be read before the write: not applied handle
//     (reservation released); the native error.
//   - config set or the readback fails: real handle (uncertain); the joined
//     errors.
//   - Success without a stored entry: real handle; plain error.
//
//purpose:construction,condition
func SetConfig(ctx context.Context, c *ceph.Container, setting ceph.ConfigSetting) (*ConfigHandle, error) {
	const op = "override.SetConfig"
	if c == nil {
		return notAppliedConfig(setting, &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: op, Err: errors.New("the cluster is nil")})
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
	ctx, cancel := operationDeadline(ctx, c)
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
	setErr := change.state.command.Record(overridehook.ConfigSet(ctx, c, setting, setting.Value))
	// A lost reply or cancellation cannot prove config set did not persist.
	// Record readback using a bounded fresh context, preserving the original
	// error. External writes must not race this native mutation/readback pair.
	readCtx, readCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer readCancel()
	if err := change.state.command.Wait(readCtx); err != nil {
		return change, errors.Join(setErr, err)
	}
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
// After cancellation, retrying waits for native completion before reconciling
// the stored state. Same value outside writes cannot be distinguished; external edits
// must not race. A handle of a call that was not applied returns nil without
// taking a lock. A cluster that has been terminated refuses the restore. A
// context that ends before the topology lock is free returns the context
// error, and the entry stays registered.
//
// A removal alone does not clear a running MGR module's cached option.
// Restoring an originally absent mgr/ option therefore stores an empty value
// without option validation before removing the entry. A MGR that receives
// that intermediate update erases the cached value.
// Restore waits for each affected running owned manager to apply a MON
// snapshot before removing the empty entry. It temporarily uses
// mon_lease_ack_timeout_factor in the manager's exact section to observe
// delivery and restores that setting before proceeding. External writes to
// those entries must stay outside the restoration. A stopped manager will
// load the current configuration when it starts. If delivery is interrupted,
// retry Restore to settle the temporary setting and confirm delivery.
// Check the module's own effective value independently when it matters.
//
// A canceled call leaves its native write running under a separate bounded
// wait. Restore waits for its terminal reply before reconciling state.
// If transport failure or that bound loses completion evidence, Restore keeps
// ownership and returns an error; terminate the disposable cluster in that case.
//
//purpose:condition,lifetime
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
	return overridehook.WithTopology(ctx, c, func() error { return change.restoreLocked(ctx, c) })
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
	ctx, cancel := operationDeadline(ctx, c)
	defer cancel()
	if err := change.state.command.Wait(ctx); err != nil {
		return err
	}
	if err := change.restoreMGRDelivery(ctx, c); err != nil {
		return err
	}

	entries, err := overridehook.ConfigEntries(ctx, c)
	if err != nil {
		return err
	}
	current := findConfigEntry(entries, change.setting)
	if sameConfigValue(current, change.state.previous) {
		change.finish(c)
		return nil
	}
	// An earlier restoration that stored the empty module value and lost its
	// removal left that value behind.
	ownEmpty := change.state.cleared && current != nil && current.Value == ""
	if !ownEmpty {
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
	}
	var restoreErr error
	if change.state.previous == nil {
		if mgrModuleOption(change.setting.Name) && current.Value != "" {
			change.state.cleared = true
			if err := change.state.command.Record(overridehook.ConfigSetEmpty(ctx, c, change.setting)); err != nil {
				return err
			}
		}
		if mgrModuleOption(change.setting.Name) {
			if err := change.waitMGRDelivery(ctx, c); err != nil {
				return err
			}
		}
		restoreErr = change.state.command.Record(overridehook.ConfigRemove(ctx, c, change.setting))
	} else {
		restoreErr = change.state.command.Record(overridehook.ConfigSet(ctx, c, change.setting, change.state.previous.Value))
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

// mgrModuleOption reports whether name is an MGR module option, which the MGR
// consumes itself (mgr/<module>/<option>, or a localized
// mgr/<module>/<mgr id>/<option>) instead of its daemon configuration.
func mgrModuleOption(name string) bool { return strings.HasPrefix(name, "mgr/") }

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
