//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// The always-on set that Squid, Tentacle and Umbrella publish for every
// release key, as the essential role images report it.
var baselineAlwaysOn = []string{"balancer", "crash", "devicehealth", "orchestrator", "pg_autoscaler", "progress", "rbd_support", "status", "telemetry", "volumes"}

// mgrDumpFixture is a mgr dump with the release's default enabled list and a
// catalog of the given modules, each able to run unless broken names it.
func mgrDumpFixture(t *testing.T, catalog []string, broken map[string]string) []byte {
	t.Helper()
	type module struct {
		Name   string `json:"name"`
		CanRun bool   `json:"can_run"`
		Error  string `json:"error_string"`
	}
	modules := []module{}
	for _, name := range catalog {
		reason, bad := broken[name]
		modules = append(modules, module{Name: name, CanRun: !bad, Error: reason})
	}
	always := map[string][]string{}
	for _, release := range []string{"squid", "tentacle", "umbrella"} {
		always[release] = baselineAlwaysOn
	}
	data, err := json.Marshal(map[string]any{"available": true, "active_gid": 4130, "modules": []string{"iostat", "nfs", "nvmeof"},
		"always_on_modules": always, "available_modules": modules})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// The 21.2.0 essential role image ships the baseline and nothing of the
// default enabled list; the 20.2.4 one also ships iostat, nfs and nvmeof.
var (
	umbrellaEssentialCatalog = append(slices.Clone(baselineAlwaysOn), "mirroring", "snap_schedule")
	tentacleEssentialCatalog = append(slices.Clone(umbrellaEssentialCatalog), "iostat", "nfs", "nvmeof", "prometheus")
)

func TestCheckBaselineMGRModulesAcceptsTheEssentialContract(t *testing.T) {
	for name, tc := range map[string]struct {
		catalog []string
		absent  []string
	}{
		"baseline only": {umbrellaEssentialCatalog, []string{"iostat", "nfs", "nvmeof"}},
		"wider image":   {tentacleEssentialCatalog, nil},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := harness.CheckBaselineMGRModules(mgrDumpFixture(t, tc.catalog, nil))
			if err != nil {
				t.Fatal(err)
			}
			if got.ActiveGID != 4130 || !slices.Equal(got.AbsentEnabled, tc.absent) {
				t.Fatalf("got %+v, want absent %v", got, tc.absent)
			}
			for _, module := range append(slices.Clone(baselineAlwaysOn), "mirroring") {
				if !got.Runnable[module] {
					t.Fatalf("%s not checked: %+v", module, got)
				}
			}
			if got.Runnable["snap_schedule"] || got.Runnable["prometheus"] {
				t.Fatalf("a module outside the baseline and the enabled list was checked: %+v", got)
			}
		})
	}
}

func TestCheckBaselineMGRModulesRefusesAMissingOrBrokenModule(t *testing.T) {
	without := func(name string) []string {
		return slices.DeleteFunc(slices.Clone(tentacleEssentialCatalog), func(s string) bool { return s == name })
	}
	for name, tc := range map[string]struct {
		data []byte
		want string
	}{
		"always-on module absent":     {mgrDumpFixture(t, without("volumes"), nil), "baseline manager module absent; stop for fixed image-policy classification: volumes"},
		"mirroring absent":            {mgrDumpFixture(t, without("mirroring"), nil), "baseline manager module absent; stop for fixed image-policy classification: mirroring"},
		"always-on module cannot run": {mgrDumpFixture(t, tentacleEssentialCatalog, map[string]string{"progress": "No module named 'x'"}), "module=progress can_run=false"},
		"shipped enabled module cannot run": {mgrDumpFixture(t, tentacleEssentialCatalog, map[string]string{"nfs": "No module named 'x'"}),
			"module=nfs can_run=false"},
		"not available":    {[]byte(`{"available":false,"active_gid":1,"modules":[],"always_on_modules":{"squid":["volumes"]},"available_modules":[]}`), "strict active MGR catalog unavailable"},
		"no release sets":  {[]byte(`{"available":true,"active_gid":1,"modules":[],"always_on_modules":{},"available_modules":[]}`), "strict active MGR catalog unavailable"},
		"null release set": {[]byte(`{"available":true,"active_gid":1,"modules":[],"always_on_modules":{"squid":null},"available_modules":[]}`), "null always-on release set"},
		"duplicate catalog entry": {[]byte(`{"available":true,"active_gid":1,"modules":[],"always_on_modules":{"squid":["volumes"]},"available_modules":[` +
			`{"name":"volumes","can_run":true,"error_string":""},{"name":"volumes","can_run":true,"error_string":""}]}`), "MGR available catalog malformed"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := harness.CheckBaselineMGRModules(tc.data)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %+v %v, want an error containing %q", got, err, tc.want)
			}
		})
	}
}
