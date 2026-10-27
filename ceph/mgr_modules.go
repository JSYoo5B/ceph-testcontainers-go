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
