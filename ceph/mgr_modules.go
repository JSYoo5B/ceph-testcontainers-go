package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"strings"
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

// readMGRModules is the MGR module observation: the configured membership
// from mgr module ls, then the active MGR's advertised status of each module
// from mgr dump, which must describe the same membership, sorted by name.
// Observer.MGRModules, waitMGRModuleReady and the override module hooks read
// the modules through it, so there is one command order and one combination
// rule.
func readMGRModules(ctx context.Context, run cliQuery) ([]MGRModuleState, error) {
	snapshot, err := readMGRModuleSnapshot(ctx, run)
	return snapshot.modules, err
}

// mgrModuleSnapshot is one MGR module observation together with the identity
// of the active MGR whose dump it combined.
type mgrModuleSnapshot struct {
	modules []MGRModuleState
	// activeGID is the active MGR's gid in the same dump. It is zero when the
	// dump does not name one.
	activeGID uint64
}

// readMGRModuleSnapshot is readMGRModules with the active MGR's gid from the
// same dump, so that a caller can tell a respawned active MGR from the one it
// observed before.
func readMGRModuleSnapshot(ctx context.Context, run cliQuery) (mgrModuleSnapshot, error) {
	data, err := run(ctx, "mgr", "module", "ls", "--format", "json")
	if err != nil {
		return mgrModuleSnapshot{}, err
	}
	modules, err := decodeMGRModuleMembership(data)
	if err != nil {
		return mgrModuleSnapshot{}, err
	}
	// module ls omits can_run for enabled modules. The active MGR's beacon
	// metadata in mgr dump advertises dependency status for every loaded module.
	data, err = run(ctx, "mgr", "dump", "--format", "json")
	if err != nil {
		return mgrModuleSnapshot{}, err
	}
	activeGID, err := modules.applyDump(data)
	if err != nil {
		return mgrModuleSnapshot{}, err
	}
	result := make([]MGRModuleState, 0, len(modules))
	for _, module := range modules {
		result = append(result, module)
	}
	slices.SortFunc(result, func(a, b MGRModuleState) int { return strings.Compare(a.Name, b.Name) })
	return mgrModuleSnapshot{modules: result, activeGID: activeGID}, nil
}

// mgrModuleSet is the configured MGR module membership, by name.
type mgrModuleSet map[string]MGRModuleState

// decodeMGRModuleMembership reads `mgr module ls`: each module once, except
// that an always-on module may also be force-disabled.
func decodeMGRModuleMembership(data []byte) (mgrModuleSet, error) {
	var listing struct {
		Enabled       *[]string `json:"enabled_modules"`
		AlwaysOn      *[]string `json:"always_on_modules"`
		ForceDisabled []string  `json:"force_disabled_modules"`
		Disabled      *[]struct {
			Name string `json:"name"`
		} `json:"disabled_modules"`
	}
	if json.Unmarshal(data, &listing) != nil || listing.Enabled == nil || listing.AlwaysOn == nil || listing.Disabled == nil {
		return nil, errors.New("decode MGR module membership: missing or invalid native listing")
	}
	modules := make(mgrModuleSet)
	for _, name := range *listing.Enabled {
		if err := modules.add(name, true, false, false); err != nil {
			return nil, err
		}
	}
	for _, name := range *listing.AlwaysOn {
		if err := modules.add(name, true, true, false); err != nil {
			return nil, err
		}
	}
	for _, name := range listing.ForceDisabled {
		if err := modules.add(name, false, true, true); err != nil {
			return nil, err
		}
	}
	for _, module := range *listing.Disabled {
		if err := modules.add(module.Name, false, false, false); err != nil {
			return nil, err
		}
	}
	return modules, nil
}

func (modules mgrModuleSet) add(name string, enabled, always, force bool) error {
	if !mgrModuleNamePattern.MatchString(name) {
		return errors.New("invalid native MGR module name")
	}
	module := modules[name]
	if module.Name != "" && !force {
		return errors.New("duplicate native MGR module membership")
	}
	if force && (!module.AlwaysOn || module.ForceDisabled) {
		return errors.New("invalid force-disabled MGR module membership")
	}
	modules[name] = MGRModuleState{Name: name, Enabled: enabled, AlwaysOn: always, ForceDisabled: force}
	return nil
}

// applyDump records the active MGR's advertised status of each module from
// `mgr dump`, and refuses a dump whose loaded modules differ from the
// membership read before it. It returns the dump's active MGR gid.
func (modules mgrModuleSet) applyDump(data []byte) (uint64, error) {
	var dump struct {
		Available        *bool     `json:"available"`
		ActiveGID        uint64    `json:"active_gid"`
		Modules          *[]string `json:"modules"`
		AvailableModules *[]struct {
			Name        string `json:"name"`
			CanRun      *bool  `json:"can_run"`
			ErrorString string `json:"error_string"`
		} `json:"available_modules"`
	}
	if json.Unmarshal(data, &dump) != nil || dump.Available == nil || dump.Modules == nil || dump.AvailableModules == nil {
		return 0, errors.New("decode active MGR module metadata: missing or invalid native dump")
	}
	if !*dump.Available {
		return 0, errors.New("active MGR is not available")
	}
	seen := make(map[string]bool)
	for _, native := range *dump.AvailableModules {
		if !mgrModuleNamePattern.MatchString(native.Name) || native.CanRun == nil || seen[native.Name] {
			return 0, errors.New("invalid or duplicate advertised MGR module metadata")
		}
		seen[native.Name] = true
		module, exists := modules[native.Name]
		if !exists {
			return 0, errors.New("MGR module metadata differs from configured membership; retry after convergence")
		}
		module.Available, module.CanRun, module.ErrorString = true, *native.CanRun, native.ErrorString
		modules[native.Name] = module
	}
	seen = make(map[string]bool)
	for _, name := range *dump.Modules {
		module, exists := modules[name]
		if !exists || !module.Enabled || seen[name] {
			return 0, errors.New("MGR module membership changed while reading; retry after convergence")
		}
		seen[name] = true
	}
	for _, module := range modules {
		if module.Enabled && !module.AlwaysOn && !seen[module.Name] {
			return 0, errors.New("MGR module membership changed while reading; retry after convergence")
		}
	}
	return dump.ActiveGID, nil
}

// waitMGRModuleReady implements the observer wait and is available to cluster startup.
func (c *Container) waitMGRModuleReady(ctx context.Context, name string) error {
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
