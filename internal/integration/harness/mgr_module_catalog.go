//go:build all || integration

package harness

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

// BaselineMGRModules is the result of CheckBaselineMGRModules for one mgr
// dump of the active manager.
type BaselineMGRModules struct {
	ActiveGID uint64
	// Runnable holds every checked module the catalog lists: the baseline
	// and the enabled modules the image ships, all able to run.
	Runnable map[string]bool
	// AbsentEnabled lists, sorted, the enabled modules that the catalog
	// omits.
	AbsentEnabled []string
}

// CheckBaselineMGRModules checks the MGR modules of one `ceph mgr dump
// --format json` against the essential role's baseline: the always-on
// modules of every release set the map publishes, and volumes, rbd_support,
// mirroring and snap_schedule, the set the image publication check requires.
// Each must be in the active manager's catalog and able to run.
//
// The enabled list also names the release's mgr_initial_modules defaults
// (iostat, nfs and nvmeof since Squid). The essential role need not ship
// them, and the manager stays HEALTH_OK without them, so an enabled module
// the catalog omits is only reported in AbsentEnabled. One that the catalog
// lists must still be able to run.
func CheckBaselineMGRModules(data []byte) (BaselineMGRModules, error) {
	var native struct {
		Available *bool               `json:"available"`
		ActiveGID *uint64             `json:"active_gid"`
		Modules   *[]string           `json:"modules"`
		AlwaysOn  map[string][]string `json:"always_on_modules"`
		Catalog   *[]struct {
			Name   *string `json:"name"`
			CanRun *bool   `json:"can_run"`
			Error  *string `json:"error_string"`
		} `json:"available_modules"`
	}
	if err := json.Unmarshal(data, &native); err != nil || native.Available == nil || !*native.Available || native.ActiveGID == nil ||
		*native.ActiveGID == 0 || native.Modules == nil || len(native.AlwaysOn) == 0 || native.Catalog == nil {
		return BaselineMGRModules{}, errors.New("strict active MGR catalog unavailable")
	}
	required := map[string]bool{"volumes": true, "rbd_support": true, "mirroring": true, "snap_schedule": true}
	for _, names := range native.AlwaysOn {
		if names == nil {
			return BaselineMGRModules{}, errors.New("null always-on release set")
		}
		for _, name := range names {
			if name == "" {
				return BaselineMGRModules{}, errors.New("empty always-on MGR module")
			}
			required[name] = true
		}
	}
	enabled := map[string]bool{}
	for _, name := range *native.Modules {
		if name == "" {
			return BaselineMGRModules{}, errors.New("empty enabled MGR module")
		}
		enabled[name] = true
	}
	seen, runnable := map[string]bool{}, map[string]bool{}
	for _, module := range *native.Catalog {
		if module.Name == nil || *module.Name == "" || seen[*module.Name] || module.CanRun == nil || module.Error == nil {
			return BaselineMGRModules{}, errors.New("MGR available catalog malformed")
		}
		seen[*module.Name] = true
		if !required[*module.Name] && !enabled[*module.Name] {
			continue
		}
		if !*module.CanRun || *module.Error != "" {
			return BaselineMGRModules{}, fmt.Errorf("manager payload/dependency unavailable; stop for fixed image-policy classification: module=%s can_run=%t error=%s", *module.Name, *module.CanRun, *module.Error)
		}
		runnable[*module.Name] = true
	}
	for name := range required {
		if !runnable[name] {
			return BaselineMGRModules{}, fmt.Errorf("baseline manager module absent; stop for fixed image-policy classification: %s", name)
		}
	}
	var absent []string
	for name := range enabled {
		if !seen[name] {
			absent = append(absent, name)
		}
	}
	slices.Sort(absent)
	return BaselineMGRModules{ActiveGID: *native.ActiveGID, Runnable: runnable, AbsentEnabled: absent}, nil
}
