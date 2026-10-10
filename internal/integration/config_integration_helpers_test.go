//go:build all || (integration && features)

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func testConfigurationOverrides(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	configFile := filepath.Join(t.TempDir(), "ceph.conf")
	if err := os.WriteFile(configFile, []byte(nativeConfigFile), 0o600); err != nil {
		t.Fatal(err)
	}
	opts := []testcontainers.ContainerCustomizer{
		ceph.WithInitialOSDs(ceph.OSDConfig{DeviceClass: "ssd"}, ceph.OSDConfig{DeviceClass: "ssd"}),
		ceph.WithConfigFile(configFile),
	}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	cluster, client := newServiceCluster(t, opts...)
	checkNativeConfigFile(t, ctx, cluster)
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-config", Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	const option = "osd_scrub_min_interval"
	equalSeconds := func(a, b string) bool {
		aValue, aErr := strconv.ParseFloat(strings.TrimSpace(a), 64)
		bValue, bErr := strconv.ParseFloat(b, 64)
		return aErr == nil && bErr == nil && aValue == bValue
	}
	cephCommand(t, ctx, cluster, "config", "set", "global", option, "3600")
	lookup := func(section, mask string) *ceph.ConfigEntry {
		t.Helper()
		entries, err := cluster.Configuration(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.Section == section && entry.Mask == mask && entry.Name == option {
				copy := entry
				return &copy
			}
		}
		return nil
	}
	apply := func(setting ceph.ConfigSetting) *ceph.ConfigOverride {
		t.Helper()
		change, err := cluster.TemporaryConfig(ctx, setting)
		if err != nil {
			if change != nil {
				_ = change.Restore(context.WithoutCancel(ctx))
			}
			t.Fatal(err)
		}
		return change
	}
	restore := func(change *ceph.ConfigOverride) {
		t.Helper()
		if err := change.Restore(ctx); err != nil {
			t.Fatal(err)
		}
	}
	osds := cluster.OSDs()
	who := fmt.Sprintf("osd.%d", osds[0].ID)
	checkRuntime := func(osd *ceph.OSDContainer, value string) {
		t.Helper()
		deadline := time.Now().Add(35 * time.Second)
		for {
			data, err := cluster.Ceph(ctx, "config", "show", fmt.Sprintf("osd.%d", osd.ID), option)
			if err == nil && equalSeconds(string(data), value) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("effective %s=%s not observed for osd.%d (output %q error %v)", option, value, osd.ID, data, err)
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(time.Second):
			}
		}
	}
	checkRuntime(osds[0], "3600")
	checkRuntime(osds[1], "3600")
	unmasked := apply(ceph.ConfigSetting{Section: "osd", Name: option, Value: "7200"})
	masked := apply(ceph.ConfigSetting{Section: "osd", Mask: "class:ssd", Name: option, Value: "14400"})
	// A device-class mask can outrank an unmasked per-daemon entry. Keep the
	// same native mask when exercising the more-specific daemon scope.
	exact := apply(ceph.ConfigSetting{Section: who, Mask: "class:ssd", Name: option, Value: "7200.0000"})
	if entry := lookup(who, "class:ssd"); entry == nil || !equalSeconds(entry.Value, "7200") {
		t.Fatalf("native number was not normalized: %+v", entry)
	}
	if _, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{Section: who, Mask: "class:ssd", Name: option, Value: "900"}); err == nil {
		t.Fatal("overlapping exact config override accepted")
	}
	checkRuntime(osds[0], "7200")
	checkRuntime(osds[1], "14400")
	copy := *exact
	restore(&copy)
	restore(exact)
	if lookup(who, "class:ssd") != nil {
		t.Fatal("missing entry was restored as an explicit inherited value")
	}
	checkRuntime(osds[0], "14400")
	restore(unmasked)
	if lookup("osd", "") != nil || lookup("osd", "class:ssd") == nil {
		t.Fatal("unmasked restoration changed masked entry")
	}
	checkRuntime(osds[0], "14400")
	restore(masked)
	checkRuntime(osds[0], "3600")
	checkRuntime(osds[1], "3600")
	global := apply(ceph.ConfigSetting{Section: "global", Name: option, Value: "7200.0000"})
	checkRuntime(osds[0], "7200")
	restore(global)
	if entry := lookup("global", ""); entry == nil || !equalSeconds(entry.Value, "3600") {
		t.Fatal("existing entry was removed or failed restoration")
	}
	guard := apply(ceph.ConfigSetting{Section: who, Name: option, Value: "1800"})
	cephCommand(t, ctx, cluster, "config", "set", who, option, "900")
	if err := guard.Restore(ctx); err == nil {
		t.Fatal("outside config edit overwritten")
	}
	if entry := lookup(who, ""); entry == nil || !equalSeconds(entry.Value, "900") {
		t.Fatal("outside value lost")
	}
	cephCommand(t, ctx, cluster, "config", "set", who, option, "1800")
	restore(guard)
	checkRuntime(osds[0], "3600")
	// Observe actual native object I/O after all policy changes/restorations.
	execCommand(t, ctx, client, "python3", "-c", `import rados
c=rados.Rados(conffile="/etc/ceph/ceph.conf")
c.conf_set("rados_osd_op_timeout","5")
c.connect(timeout=10)
try:
    with c.open_ioctx("tc-config") as io:
        payload=b"configuration restoration preserved native I/O"*1024
        io.write_full("shared",payload)
        assert io.read("shared",len(payload))==payload
finally: c.shutdown()
`)
	entries, err := cluster.Configuration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(entries)
	if strings.Contains(string(data), `"mask":"class:ssd"`) {
		t.Fatal("owned temporary masked entry left behind")
	}
	t.Log("native config: absent/explicit/inherited/masked entries preserved, canonical number readback, runtime scope precedence, copied handle and outside-edit guard; native RADOS I/O passed")
}

// nativeConfigFile replaces the fixture's own [osd] BlueStore cache size and
// adds OSD, MON and client settings that must reach bootstrap, later daemons
// and connection configs. Ceph ignores osd_memory_target below 896 MiB, so the
// value must differ from both that floor and the 4 GiB native default.
const nativeConfigFile = `# testdata-style ceph.conf
[osd]
bluestore cache size = 134217728
osd memory target = 1073741824
[mon]
mon_max_pg_per_osd = 320
[client.admin]
rados_osd_op_timeout = 25
`

func checkNativeConfigFile(t *testing.T, ctx context.Context, cluster *ceph.Container) {
	t.Helper()
	show := func(who, option string) string {
		t.Helper()
		data, err := cluster.Ceph(ctx, "config", "show", who, option)
		if err != nil {
			t.Fatalf("config show %s %s: %v", who, option, err)
		}
		return strings.TrimSpace(string(data))
	}
	for _, osd := range cluster.OSDs() {
		for option, want := range map[string]string{"bluestore_cache_size": "134217728", "osd_memory_target": "1073741824"} {
			if got := show(fmt.Sprintf("osd.%d", osd.ID), option); got != want {
				t.Fatalf("osd.%d %s = %q, want the config file value %s", osd.ID, option, got, want)
			}
		}
	}
	if got := show("mon.a", "mon_max_pg_per_osd"); got != "320" {
		t.Fatalf("mon.a mon_max_pg_per_osd = %q, want the config file value", got)
	}
	config, _, err := cluster.ConnectionConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(config), "[client.admin]\nrados_osd_op_timeout = 25\n") {
		t.Fatalf("connection config omitted client settings:\n%s", config)
	}
	if strings.Count(string(config), "bluestore cache size") != 0 || strings.Count(string(config), "bluestore_cache_size") != 1 {
		t.Fatalf("connection config kept the replaced fixture value:\n%s", config)
	}
	t.Log("WithConfigFile reached MON bootstrap, OSDs and connection config")
}
