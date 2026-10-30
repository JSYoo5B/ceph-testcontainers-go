package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
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

// WaitMGRModuleReady waits for enabled/can-run metadata and successful native
// read-only commands. Supported modules are rbd_support (task and snapshot
// schedule lists) and volumes (volume list). Other modules need an application
// specific command probe. Each poll releases the cluster lock; cancellation and
// termination stop the wait. Success does not claim worker tasks have completed.
//
//purpose:observation
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
		modules, err := readMGRModules(ctx, c.conn().ceph)
		module, ok := findMGRModule(modules, name)
		if err == nil && (!ok || !module.Enabled || !module.Available || !module.CanRun) {
			err = errors.New("MGR module is not enabled with runnable native metadata")
		}
		if err == nil {
			for _, probe := range probes {
				data, probeErr := c.conn().ceph(ctx, probe...)
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
