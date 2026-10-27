package ceph

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

// overrideHooks gives internal/overridehook, and through it the override
// package, the typed operations that the reversible overrides need from a
// cluster. Its methods are exported only so that internal/overridehook can
// call them through an interface; the type itself is unexported and never
// appears in a public package. There is no raw executor: each operation
// wraps one native command or one registry action, and none exposes the
// cluster mutex, the keyring or the configuration bytes.
type overrideHooks struct{}

func init() {
	hookreg.Override = overrideHooks{}
}

// overrideKindNames are the registry kinds that internal/overridehook names.
// The phase, overlap refusal and Terminate behavior of each stay fixed in
// faultKinds.
var overrideKindNames = map[string]faultKind{
	"Blocklist":  faultBlocklist,
	"FullRatios": faultFullRatios,
	"Config":     faultConfigOverride,
}

// Admit refuses the override operation op while a SharedCluster shares c,
// with the policy of sharedOverridePolicies. An operation without a policy
// is refused.
func (overrideHooks) Admit(c *Container, op string) error { return c.admitOverride(op) }

// WithTopology runs fn with the cluster mutex held, unless ctx ends first.
func (overrideHooks) WithTopology(ctx context.Context, c *Container, fn func() error) error {
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	return fn()
}

// WithTopologyNoContext runs fn with the cluster mutex held, waiting for it
// without a context as the blocklist addition and every lease release do.
func (overrideHooks) WithTopologyNoContext(c *Container, fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn()
}

// Ready refuses a terminated cluster or one without a control container with
// the identical errors of the operations that it replaces. The caller holds
// the mutex.
func (overrideHooks) Ready(c *Container) error { return c.poolPolicyReady() }

// StartupTimeout bounds one fixture operation.
func (overrideHooks) StartupTimeout(c *Container) time.Duration { return c.settings.startupTimeout }

// Terminated returns the error of an operation on a terminated cluster.
func (overrideHooks) Terminated() error { return errClusterTerminated }

// Invalid classifies err as ErrInvalidRequest, keeping its text.
func (overrideHooks) Invalid(err error) error { return invalid(err) }

// Reserve reserves key for an override of kind. The caller holds the mutex
// and has refused a closed cluster. The refusal of an overlap carries the
// kind's own text.
func (overrideHooks) Reserve(ctx context.Context, c *Container, kind, key string) (any, error) {
	k, ok := overrideKindNames[kind]
	if !ok {
		return nil, invalidRequest("unknown override kind %q", kind)
	}
	token, _, err := c.reserveFault(ctx, k, key)
	if err != nil {
		return nil, err
	}
	return token, nil
}

// Settle records handle for a reservation, or ends it when handle is nil,
// and reports whether Terminate has started. The caller holds the mutex.
func (overrideHooks) Settle(c *Container, token any, handle any) (bool, error) {
	entry, ok := token.(*faultEntry)
	if !ok || entry == nil {
		return false, errFaultToken
	}
	return c.settleFault(entry, "", handle, nil, nil), nil
}

// Release forgets the override of token while its key still maps to it. The
// caller holds the mutex.
func (overrideHooks) Release(c *Container, token any) bool {
	entry, _ := token.(*faultEntry)
	return c.releaseFault(entry)
}

// Tracks reports whether token is still the override of its key. The caller
// holds the mutex.
func (overrideHooks) Tracks(c *Container, token any) bool {
	entry, _ := token.(*faultEntry)
	return c.tracksFault(entry)
}

// NormalizeBlocklistSession validates and canonicalizes an exact client
// session address for fencing, with the errors of the former
// TemporaryBlocklist.
func (overrideHooks) NormalizeBlocklistSession(address string) (string, error) {
	return normalizeBlocklistSession(address, true)
}

// BlocklistEntries reads the native blocklist. The caller holds the mutex.
func (overrideHooks) BlocklistEntries(ctx context.Context, c *Container) ([]BlocklistEntry, error) {
	return c.blocklistEntries(ctx)
}

// BlocklistAdd adds one exact session to the native blocklist for duration.
// The caller holds the mutex.
func (overrideHooks) BlocklistAdd(ctx context.Context, c *Container, address string, duration time.Duration) error {
	_, err := c.cephCLI(ctx, "osd", "blocklist", "add", address, strconv.FormatFloat(duration.Seconds(), 'f', 6, 64))
	return err
}

// BlocklistRemove removes one exact session from the native blocklist. The
// caller holds the mutex.
func (overrideHooks) BlocklistRemove(ctx context.Context, c *Container, address string) error {
	_, err := c.cephCLI(ctx, "osd", "blocklist", "rm", address)
	return err
}

// OpenFullRatios captures the control container and the original bootstrap
// identity once and returns the session that every later native step of one
// full ratios override uses. It refuses a terminated cluster, a missing control
// container and an unconfirmed identity. The caller holds the mutex.
func (overrideHooks) OpenFullRatios(ctx context.Context, c *Container) (any, error) {
	session, err := c.fullRatiosSession(ctx)
	if err != nil {
		return nil, err
	}
	return session, nil
}

// FSID returns the cluster identity that the session captured.
func (session fullRatiosCLISession) FSID() string { return session.fsid }

// Read reads the three thresholds and the map epoch, guarded by the captured
// identity before and after the read.
func (session fullRatiosCLISession) Read(ctx context.Context) (FullRatioSnapshot, error) {
	return session.read(ctx)
}

// SetRatio sets one of the thresholds "nearfull", "backfillfull" or "full".
func (session fullRatiosCLISession) SetRatio(ctx context.Context, name string, value float64) error {
	switch name {
	case "nearfull", "backfillfull", "full":
	default:
		return invalidRequest("unknown full ratio %q", name)
	}
	_, err := session.command(ctx, "osd", "set-"+name+"-ratio", strconv.FormatFloat(value, 'g', -1, 64))
	return err
}

// CheckIdentity refuses a cluster whose original bootstrap identity is not fsid.
func (session fullRatiosCLISession) CheckIdentity(ctx context.Context, fsid string) error {
	if session.fsid != fsid {
		return clientOperationError(ctx, "full ratios override original bootstrap identity changed", nil)
	}
	return nil
}

// TestHeld snapshots the handles that one kind holds, for unit tests.
func (overrideHooks) TestHeld(c *Container, kind string) map[string]any {
	result := make(map[string]any)
	k, ok := overrideKindNames[kind]
	if !ok {
		return result
	}
	for id, entry := range c.faults.entries {
		if id.kind == k {
			result[id.key] = entry.handle
		}
	}
	return result
}

// NormalizeConfigSetting validates and canonicalizes one central
// configuration entry, with the errors of the former OverrideConfig.
func (overrideHooks) NormalizeConfigSetting(setting ConfigSetting) (ConfigSetting, error) {
	return normalizeConfigSetting(setting)
}

// CheckConfigAllowed refuses an option that the bootstrap messenger mode of c
// fixes for every section and mask. It reads settings that are immutable after
// construction and needs no lock.
func (overrideHooks) CheckConfigAllowed(c *Container, setting ConfigSetting) error {
	if c.settings.messengerMode == MessengerV2Secure && messengerBootstrapSetting(setting.Name) {
		return invalid(fmt.Errorf("%s is fixed in the MessengerV2Secure bootstrap configuration for every section and mask; choose WithMessengerMode when creating a new cluster", setting.Name))
	}
	if c.settings.messengerMode == MessengerV2Only && messengerProtocolSetting(setting.Name) {
		return invalid(fmt.Errorf("%s is fixed in the MessengerV2Only bootstrap configuration for every section and mask; choose WithMessengerMode when creating a new cluster", setting.Name))
	}
	return nil
}

// ConfigKey returns the registry key of a normalized setting.
func (overrideHooks) ConfigKey(setting ConfigSetting) string { return configOverrideKey(setting) }

// ConfigWho returns the native target of a normalized setting.
func (overrideHooks) ConfigWho(setting ConfigSetting) string { return configWho(setting) }

// ConfigEntries reads the central configuration database. The caller holds
// the mutex.
func (overrideHooks) ConfigEntries(ctx context.Context, c *Container) ([]ConfigEntry, error) {
	return c.configuration(ctx)
}

// ConfigSet stores value for a normalized setting. The caller holds the mutex.
func (overrideHooks) ConfigSet(ctx context.Context, c *Container, setting ConfigSetting, value string) error {
	_, err := c.configCommand(ctx, "set", configWho(setting), setting.Name, value)
	return err
}

// ConfigRemove removes the entry of a normalized setting. The caller holds the
// mutex.
func (overrideHooks) ConfigRemove(ctx context.Context, c *Container, setting ConfigSetting) error {
	_, err := c.configCommand(ctx, "rm", configWho(setting), setting.Name)
	return err
}

// MessengerPolicySettings returns the central configuration entries of a
// temporary messenger policy for c, with the refusals of the former
// OverrideMessengerPolicy in their order: an invalid mode list, a policy that
// selects no mode, and a cluster whose bootstrap fixes every mode.
func (overrideHooks) MessengerPolicySettings(c *Container, policy MessengerPolicy) ([]ConfigSetting, error) {
	settings, err := policy.settings()
	if err != nil {
		return nil, invalid(fmt.Errorf("messenger policy: %w", err))
	}
	if len(settings) == 0 {
		return nil, invalidRequest("messenger policy selects no mode")
	}
	if c.settings.messengerMode == MessengerV2Secure {
		return nil, invalidRequest("MessengerV2Secure fixes every connection mode in the local configuration")
	}
	return settings, nil
}
