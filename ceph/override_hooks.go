package ceph

import (
	"context"
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
	"MGRModule":  faultMGRModule,
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

// ValidMGRModuleName reports whether name can be a Ceph manager module name.
func (overrideHooks) ValidMGRModuleName(name string) bool {
	return mgrModuleNamePattern.MatchString(name)
}

// MGRModulesSnapshot waits for a coherent active-MGR snapshot of the modules
// and returns it. ctx carries the operation deadline. The caller holds the
// mutex.
func (overrideHooks) MGRModulesSnapshot(ctx context.Context, c *Container) ([]MGRModuleState, error) {
	return c.waitMGRModules(ctx)
}

// WaitMGRModuleMembership waits until the native listing shows name in the
// requested state with the given always-on property. The caller holds the
// mutex.
func (overrideHooks) WaitMGRModuleMembership(ctx context.Context, c *Container, name string, enabled, alwaysOn bool) error {
	return c.waitMGRModuleMembership(ctx, name, enabled, alwaysOn)
}

// SetMGRModule enables or disables one module with the native command. The
// caller holds the mutex.
func (overrideHooks) SetMGRModule(ctx context.Context, c *Container, name string, enabled bool) error {
	return c.setMGRModule(ctx, name, enabled)
}

// ProtectMGRModule refuses to disable a module that configured or owned
// CephFS filesystems, or native filesystems and mirror policies, still need.
// The caller holds the mutex.
func (overrideHooks) ProtectMGRModule(ctx context.Context, c *Container, name string) error {
	return c.protectMGRModule(ctx, name)
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
