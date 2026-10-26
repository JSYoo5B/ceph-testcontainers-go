package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// The observations below also run only `ceph` commands, but their exported
// methods take the topology lock and the liveness check first. That acquisition
// stays on the methods. What moves here is the command order and the decoding,
// against the same cliQuery contract as observation_cli.go, so a fake query can
// drive them without a cluster. Internal callers that reuse a helper (the wait
// loops and the override families) go through the same functions, so there is
// one command order for each observation.

func readMGRModules(ctx context.Context, run cliQuery) ([]MGRModuleState, error) {
	data, err := run(ctx, "mgr", "module", "ls", "--format", "json")
	if err != nil {
		return nil, err
	}
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
	modules := make(map[string]MGRModuleState)
	add := func(name string, enabled, always, force bool) error {
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
	for _, name := range *listing.Enabled {
		if err := add(name, true, false, false); err != nil {
			return nil, err
		}
	}
	for _, name := range *listing.AlwaysOn {
		if err := add(name, true, true, false); err != nil {
			return nil, err
		}
	}
	for _, name := range listing.ForceDisabled {
		if err := add(name, false, true, true); err != nil {
			return nil, err
		}
	}
	for _, module := range *listing.Disabled {
		if err := add(module.Name, false, false, false); err != nil {
			return nil, err
		}
	}
	// module ls omits can_run for enabled modules. The active MGR's beacon
	// metadata in mgr dump advertises dependency status for every loaded module.
	data, err = run(ctx, "mgr", "dump", "--format", "json")
	if err != nil {
		return nil, err
	}
	var dump struct {
		Available        *bool     `json:"available"`
		Modules          *[]string `json:"modules"`
		AvailableModules *[]struct {
			Name        string `json:"name"`
			CanRun      *bool  `json:"can_run"`
			ErrorString string `json:"error_string"`
		} `json:"available_modules"`
	}
	if json.Unmarshal(data, &dump) != nil || dump.Available == nil || dump.Modules == nil || dump.AvailableModules == nil {
		return nil, errors.New("decode active MGR module metadata: missing or invalid native dump")
	}
	if !*dump.Available {
		return nil, errors.New("active MGR is not available")
	}
	seen := make(map[string]bool)
	for _, native := range *dump.AvailableModules {
		if !mgrModuleNamePattern.MatchString(native.Name) || native.CanRun == nil || seen[native.Name] {
			return nil, errors.New("invalid or duplicate advertised MGR module metadata")
		}
		seen[native.Name] = true
		module, exists := modules[native.Name]
		if !exists {
			return nil, errors.New("MGR module metadata differs from configured membership; retry after convergence")
		}
		module.Available, module.CanRun, module.ErrorString = true, *native.CanRun, native.ErrorString
		modules[native.Name] = module
	}
	seen = make(map[string]bool)
	for _, name := range *dump.Modules {
		module, exists := modules[name]
		if !exists || !module.Enabled || seen[name] {
			return nil, errors.New("MGR module membership changed while reading; retry after convergence")
		}
		seen[name] = true
	}
	for _, module := range modules {
		if module.Enabled && !module.AlwaysOn && !seen[module.Name] {
			return nil, errors.New("MGR module membership changed while reading; retry after convergence")
		}
	}
	result := make([]MGRModuleState, 0, len(modules))
	for _, module := range modules {
		result = append(result, module)
	}
	slices.SortFunc(result, func(a, b MGRModuleState) int { return strings.Compare(a.Name, b.Name) })
	return result, nil
}

func readManagerServices(ctx context.Context, run cliQuery) (map[string]string, error) {
	data, err := run(ctx, "mgr", "services", "--format", "json")
	if err != nil {
		return nil, err
	}
	return decodeManagerServices(data)
}

func readBlocklistEntries(ctx context.Context, run cliQuery) ([]BlocklistEntry, error) {
	data, err := run(ctx, "osd", "blocklist", "ls", "--format", "json")
	if err != nil {
		return nil, err
	}
	return decodeBlocklistEntries(data)
}

func readConfiguration(ctx context.Context, run cliQuery) ([]ConfigEntry, error) {
	data, err := runConfigCommand(ctx, run, "dump", "--format", "json")
	if err != nil {
		return nil, err
	}
	return decodeConfiguration(data)
}

// runConfigCommand runs `ceph config ...` and redacts a native failure; the
// native output can contain stored values. A cancelled context is reported as
// the context error.
func runConfigCommand(ctx context.Context, run cliQuery, args ...string) ([]byte, error) {
	data, err := run(ctx, append([]string{"config"}, args...)...)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("Ceph configuration command: %w", ctx.Err())
		}
		return nil, quiet(errors.New("Ceph configuration command failed; native output is redacted"), err)
	}
	return data, nil
}

func readOSDPolicyDump(ctx context.Context, run cliQuery) (osdPolicySnapshot, error) {
	data, err := run(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		return osdPolicySnapshot{osds: make(map[int]OSDState)}, err
	}
	return decodeOSDPolicyDump(data)
}

func readOSDFlags(ctx context.Context, run cliQuery) ([]OSDFlag, error) {
	dump, err := readOSDPolicyDump(ctx, run)
	if err != nil {
		return nil, err
	}
	flags := make([]OSDFlag, len(dump.flags))
	for i, flag := range dump.flags {
		flags[i] = OSDFlag(flag)
	}
	return flags, nil
}
