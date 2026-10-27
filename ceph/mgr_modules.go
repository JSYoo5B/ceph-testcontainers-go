package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"
)

// MGRModuleState combines configured module membership with the active MGR's
// advertised module metadata. Available means metadata is present, not that
// commands are ready. CanRun is meaningful only when Available is true.
// AlwaysOn modules can be force-disabled outside this fixture; Enabled reflects
// ForceDisabled too. ErrorString is the native dependency/load diagnostic.
type MGRModuleState struct {
	Name                             string
	Enabled, AlwaysOn, ForceDisabled bool
	Available, CanRun                bool
	ErrorString                      string
}

// observedMGRModules implements Observer.MGRModules. It takes the topology lock and refuses
// a cluster without pool policy readiness before it reads.
func (c *Container) observedMGRModules(ctx context.Context) ([]MGRModuleState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	return c.mgrModules(ctx)
}

// MGRModuleOverrideHandle manages restoration of one module's original enabled
// state, retaining ownership even when application is uncertain. Copies share
// lifecycle state. Restore explicitly when the test condition ends; termination
// disposes of the entire cluster. A non-nil handle returned with an error must
// also be restored or the cluster terminated.
type MGRModuleOverrideHandle struct {
	owner *Container
	name  string
	state *mgrModuleOverrideState
}

type mgrModuleOverrideState struct {
	previous, applied, alwaysOn, restored bool
	// token is the registry entry of this override; see faultMGRModule.
	token *faultEntry
}

// OverrideMGRModule changes configured membership without forcing unavailable
// dependencies. It rejects missing/cannot-run metadata on enable, overlapping
// handles, always-on disable, and disabling volumes or mirroring while relevant
// filesystems/mirror policies exist. An enabled always-on module accepts only a
// no-op true lease; a force-disabled always-on module cannot be temporarily
// enabled because restoring it would require force-disable. This method verifies
// membership, not command readiness. External edits must not race apply/restore;
// Ceph has no module membership CAS or per-module generation.
func (c *Container) OverrideMGRModule(ctx context.Context, name string, enabled bool) (*MGRModuleOverrideHandle, error) {
	if gateErr := c.sharedGate("OverrideMGRModule"); gateErr != nil {
		return nil, gateErr
	}
	if !mgrModuleNamePattern.MatchString(name) {
		return nil, errors.New("invalid Ceph manager module name")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	// A restored override has left the registry, so a held one is active.
	token, _, err := c.reserveFault(ctx, faultMGRModule, name)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	modules, err := c.waitMGRModules(ctx)
	if err != nil {
		c.settleFault(token, "", nil, nil, nil)
		return nil, err
	}
	module, ok := findMGRModule(modules, name)
	if !ok {
		c.settleFault(token, "", nil, nil, nil)
		return nil, errors.New("MGR module is not configured or advertised by the active manager")
	}
	if module.AlwaysOn && (!enabled || !module.Enabled) {
		c.settleFault(token, "", nil, nil, nil)
		return nil, errors.New("always-on MGR modules cannot be disabled or temporarily re-enabled after force-disable")
	}
	if enabled && (!module.Available || !module.CanRun) {
		c.settleFault(token, "", nil, nil, nil)
		return nil, errors.New("MGR module has unavailable or cannot-run dependency metadata")
	}
	if !enabled {
		if err := c.protectMGRModule(ctx, name); err != nil {
			c.settleFault(token, "", nil, nil, nil)
			return nil, err
		}
	}
	change := &MGRModuleOverrideHandle{owner: c, name: name, state: &mgrModuleOverrideState{
		previous: module.Enabled, applied: enabled, alwaysOn: module.AlwaysOn, token: token,
	}}
	c.settleFault(token, "", change, nil, nil)
	if module.Enabled == enabled {
		return change, nil
	}
	setErr := c.setMGRModule(ctx, name, enabled)
	// A lost reply cannot prove that the MON did not persist membership. Keep
	// the lease and reconcile with a fresh bounded read, retaining the error.
	readTimeout := 5 * time.Second
	if c.settings.startupTimeout > 0 && c.settings.startupTimeout < readTimeout {
		readTimeout = c.settings.startupTimeout
	}
	readCtx, readCancel := context.WithTimeout(context.WithoutCancel(ctx), readTimeout)
	defer readCancel()
	readErr := c.waitMGRModuleMembership(readCtx, name, enabled, module.AlwaysOn)
	return change, errors.Join(setErr, readErr)
}

// Restore returns only this module to its original membership. It is idempotent
// and reconciles an uncertain restoration reply before mutating again. It checks
// dependency metadata before enabling and current filesystem/mirror usage before
// disabling. No-op leases refuse an outside change. Outside writes equal to the
// applied or previous boolean state cannot be distinguished and must not race.
func (change *MGRModuleOverrideHandle) Restore(ctx context.Context) error {
	if change == nil || change.owner == nil || change.state == nil {
		return errors.New("MGR module override is unavailable")
	}
	c := change.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if change.state.restored {
		return nil
	}
	if err := c.poolPolicyReady(); err != nil {
		return err
	}
	if !c.tracksFault(change.state.token) {
		return errors.New("MGR module override is not tracked by this cluster")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	modules, err := c.waitMGRModules(ctx)
	if err != nil {
		return err
	}
	module, ok := findMGRModule(modules, change.name)
	if !ok || module.AlwaysOn != change.state.alwaysOn || module.ForceDisabled {
		return errors.New("MGR module availability or always-on policy changed outside this override")
	}
	if module.Enabled == change.state.previous {
		change.finish()
		return nil
	}
	if module.Enabled != change.state.applied {
		return invalid(errors.New("MGR module changed outside this override; refusing restoration"))
	}
	if change.state.previous {
		if !module.Available || !module.CanRun {
			return errors.New("MGR module cannot be re-enabled with unavailable or cannot-run dependency metadata")
		}
	} else {
		if module.AlwaysOn {
			return errors.New("always-on MGR module cannot be disabled")
		}
		if err := c.protectMGRModule(ctx, change.name); err != nil {
			return err
		}
	}
	if err := c.setMGRModule(ctx, change.name, change.state.previous); err != nil {
		return err
	}
	if err := c.waitMGRModuleMembership(ctx, change.name, change.state.previous, change.state.alwaysOn); err != nil {
		return err
	}
	change.finish()
	return nil
}

func (change *MGRModuleOverrideHandle) finish() {
	change.state.restored = true
	change.owner.releaseFault(change.state.token)
}

// WaitMGRModuleReady waits for enabled/can-run metadata and successful native
// read-only commands. Supported modules are rbd_support (task and snapshot
// schedule lists) and volumes (volume list). Other modules need an application
// specific command probe. Each poll releases the cluster lock; cancellation and
// termination stop the wait. Success does not claim worker tasks have completed.
func (c *Container) WaitMGRModuleReady(ctx context.Context, name string) error {
	var probes [][]string
	switch name {
	case "rbd_support":
		probes = [][]string{{"rbd", "task", "list", "--format", "json"}, {"rbd", "mirror", "snapshot", "schedule", "list", "--format", "json"}}
	case "volumes":
		probes = [][]string{{"fs", "volume", "ls", "--format", "json"}}
	default:
		return errors.New("MGR module has no built-in readiness probe; execute the intended native command")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		if err := c.lockTopology(ctx); err != nil {
			return errors.Join(err, last)
		}
		if err := c.waitPolicyReady(ctx); err != nil {
			c.mu.Unlock()
			if ctx.Err() != nil {
				return errors.Join(ctx.Err(), last, err)
			}
			return err
		}
		if err := ctx.Err(); err != nil {
			c.mu.Unlock()
			return errors.Join(err, last)
		}
		modules, err := c.mgrModules(ctx)
		module, ok := findMGRModule(modules, name)
		if err == nil && (!ok || !module.Enabled || !module.Available || !module.CanRun) {
			err = errors.New("MGR module is not enabled with runnable native metadata")
		}
		if err == nil {
			for _, probe := range probes {
				data, probeErr := c.cephCLI(ctx, probe...)
				if probeErr != nil {
					err = probeErr
					break
				}
				var result any
				if json.Unmarshal(data, &result) != nil || result == nil {
					err = errors.New("MGR module readiness command returned invalid JSON")
					break
				}
			}
		}
		c.mu.Unlock()
		if err != nil {
			last = err
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(err, last)
		}
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), last)
		case <-ticker.C:
		}
	}
}

var mgrModuleNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,127}$`)

func findMGRModule(modules []MGRModuleState, name string) (MGRModuleState, bool) {
	for _, module := range modules {
		if module.Name == name {
			return module, true
		}
	}
	return MGRModuleState{}, false
}

func (c *Container) setMGRModule(ctx context.Context, name string, enabled bool) error {
	op := "disable"
	if enabled {
		op = "enable"
	}
	_, err := c.cephCLI(ctx, "mgr", "module", op, name)
	return err
}

// The caller holds c.mu and supplies the existing operation deadline. Native
// module changes can respawn the MGR after the MON has confirmed membership;
// wait for a coherent active-MGR snapshot before capturing or restoring state.
// Retry reads only, and do not admit a snapshot received after cancellation.
func (c *Container) waitMGRModules(ctx context.Context) ([]MGRModuleState, error) {
	var modules []MGRModuleState
	err := c.poll(ctx, func() (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		current, err := c.mgrModules(ctx)
		if err != nil {
			return false, err
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		modules = current
		return true, nil
	})
	return modules, err
}

// MGR module changes can briefly restart the active manager. A transient
// unavailable/old map is not proof that the MON membership mutation failed.
// The caller holds c.mu; only native reads are retried, never the mutation.
func (c *Container) waitMGRModuleMembership(ctx context.Context, name string, enabled, alwaysOn bool) error {
	return c.poll(ctx, func() (bool, error) {
		modules, err := c.mgrModules(ctx)
		if err != nil {
			return false, err
		}
		module, found := findMGRModule(modules, name)
		if !found || module.AlwaysOn != alwaysOn || module.ForceDisabled || module.Enabled != enabled {
			return false, errors.New("MGR module membership readback differs from requested state")
		}
		return true, nil
	})
}

func (c *Container) mgrModules(ctx context.Context) ([]MGRModuleState, error) {
	return readMGRModules(ctx, c.cephCLI)
}

func (c *Container) protectMGRModule(ctx context.Context, name string) error {
	if name != "volumes" && name != "mirroring" {
		return nil
	}
	ownedNeed := slices.ContainsFunc(extensionsWith[serviceModules](c), func(state serviceModules) bool { return state.RequiresMGRModule(name) })
	if name == "volumes" && (c.settings.requiresMGRModule("volumes") || ownedNeed) {
		return errors.New("MGR volumes module is required by configured or owned CephFS filesystems")
	}
	data, err := c.cephCLI(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		return err
	}
	var fsmap struct {
		Filesystems *[]struct {
			MirrorInfo json.RawMessage `json:"mirror_info"`
		} `json:"filesystems"`
	}
	if json.Unmarshal(data, &fsmap) != nil || fsmap.Filesystems == nil {
		return errors.New("decode FSMap before disabling MGR module")
	}
	if name == "volumes" && len(*fsmap.Filesystems) != 0 {
		return errors.New("MGR volumes module is required by native CephFS filesystems")
	}
	for _, fs := range *fsmap.Filesystems {
		if len(fs.MirrorInfo) != 0 {
			return fmt.Errorf("MGR %s module is required by native CephFS mirroring policy", name)
		}
	}
	return nil
}
