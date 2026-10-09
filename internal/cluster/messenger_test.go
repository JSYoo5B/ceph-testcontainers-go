package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

var messengerSecurePolicy = map[string]string{
	"ms cluster mode": "secure", "ms service mode": "secure", "ms client mode": "secure",
	"ms mon cluster mode": "secure", "ms mon service mode": "secure", "ms mon client mode": "secure",
	"ms bind msgr1": "false", "ms bind msgr2": "true",
}

func TestMessengerModeValidationSelectionAndSnapshot(t *testing.T) {
	if MessengerDefault != 0 || MessengerV2Secure != 1 {
		t.Fatal("Messenger policy zero value changed")
	}
	settings := options{}
	if err := WithMessengerMode(MessengerV2Secure)(&settings); err != nil {
		t.Fatal(err)
	}
	for _, unknown := range []MessengerMode{-1, 2, 256} {
		if err := WithMessengerMode(unknown)(&settings); err == nil || settings.messengerMode != MessengerV2Secure {
			t.Fatal("unknown Messenger mode was accepted or replaced the selected policy")
		}
		if cluster, err := Run(t.Context(), "not-a-runnable-image", WithMessengerMode(unknown)); err == nil || cluster != nil {
			t.Fatal("unknown Messenger mode reached Docker allocation")
		}
	}
	if err := WithMessengerMode(MessengerDefault)(&settings); err != nil || settings.messengerMode != MessengerDefault {
		t.Fatal("explicit default did not replace the previous option")
	}
	for _, mode := range []MessengerMode{MessengerDefault, MessengerV2Secure} {
		cluster := &Container{settings: options{messengerMode: mode}}
		if cluster.MessengerMode() != mode {
			t.Fatal("accessor did not report the selected bootstrap mode")
		}
		cluster.closed = true
		cluster.config = []byte("[global]\nms client mode = crc\n")
		if cluster.MessengerMode() != mode {
			t.Fatal("bootstrap accessor depended on runtime state or termination")
		}
	}
	var unavailable *Container
	if unavailable.MessengerMode() != MessengerDefault {
		t.Fatal("nil Container did not report the default bootstrap selection")
	}
}

func TestMessengerSecureBootstrapRejectsCentralShadowInEveryScope(t *testing.T) {
	for key := range messengerSecurePolicy {
		canonical := strings.ReplaceAll(key, " ", "_")
		for _, name := range []string{canonical, strings.ReplaceAll(canonical, "_", "-")} {
			for _, scope := range []ConfigSetting{
				{Section: "global"}, {Section: "mon"}, {Section: "mon.b"},
				{Section: "mgr"}, {Section: "mgr.a"}, {Section: "osd"},
				{Section: "osd.0", Mask: "host:node-a/class:ssd"},
				{Section: "mds"}, {Section: "mds.a"}, {Section: "client"},
				{Section: "client.test", Mask: "host:node-a"},
			} {
				control := &configFixture{}
				cluster := configCluster(control)
				cluster.settings.messengerMode = MessengerV2Secure
				scope.Name, scope.Value = name, "PRIVATE-SETTING-VALUE"
				change, err := cluster.TemporaryConfig(t.Context(), scope)
				if err == nil || change != nil || len(control.calls) != 0 || len(cluster.configOverrides) != 0 {
					t.Fatalf("secure bootstrap %s/%s/%s acquired an override or ran native commands", scope.Section, scope.Mask, scope.Name)
				}
				if !strings.Contains(err.Error(), canonical) || !strings.Contains(err.Error(), "WithMessengerMode") || strings.Contains(err.Error(), scope.Value) {
					t.Fatal("bootstrap refusal omitted its precise reason or exposed the requested value")
				}
			}
		}
	}
	// Reserve the exact fixed keys only. Debug knobs, unrelated bind settings
	// and module-scoped names remain normal central database operations.
	for _, name := range []string{"debug_ms", "ms_bind_ipv6", "ms_cluster_mode_extra", "mgr/example/ms_client_mode"} {
		control := &configFixture{}
		cluster := configCluster(control)
		cluster.settings.messengerMode = MessengerV2Secure
		change, err := cluster.TemporaryConfig(t.Context(), ConfigSetting{Section: "global", Name: name, Value: "1"})
		if err != nil || change == nil {
			t.Fatalf("unrelated setting %s was treated as secure bootstrap policy: %v", name, err)
		}
		if err := change.Restore(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	control := &configFixture{}
	cluster := configCluster(control)
	change, err := cluster.TemporaryConfig(t.Context(), ConfigSetting{Section: "global", Name: "ms_client_mode", Value: "secure"})
	if err != nil || change == nil {
		t.Fatal("default bootstrap lost its existing central-configuration behavior", err)
	}
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestMessengerBootstrapGuardPreservesCallerContext(t *testing.T) {
	control := &configFixture{}
	cluster := configCluster(control)
	cluster.settings.messengerMode = MessengerV2Secure
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, setting := range []ConfigSetting{
		{Section: "global", Name: "ms_client_mode", Value: "crc"},
		{Section: "invalid", Name: "ms_client_mode", Value: "crc"},
		{Section: "global", Name: "debug_ms", Value: "1"},
	} {
		change, err := cluster.TemporaryConfig(ctx, setting)
		if !errors.Is(err, context.Canceled) || change != nil || len(control.calls) != 0 {
			t.Fatal("canceled caller was replaced by validation/policy errors or ran native commands", err)
		}
	}
	// Unrelated options still wait for serialized fixture work only until the
	// caller deadline. Refused bootstrap keys need no topology admission.
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	short, stop := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer stop()
	started := time.Now()
	change, err := cluster.TemporaryConfig(short, ConfigSetting{Section: "global", Name: "debug_ms", Value: "1"})
	if !errors.Is(err, context.DeadlineExceeded) || change != nil || len(control.calls) != 0 || time.Since(started) > time.Second {
		t.Fatal("configuration admission ignored the caller deadline", err)
	}
	if change, err := cluster.TemporaryConfig(t.Context(), ConfigSetting{Section: "global", Name: "ms_bind_msgr1", Value: "true"}); err == nil || change != nil || len(control.calls) != 0 {
		t.Fatal("secure bootstrap refusal waited for topology or ran a native command")
	}
	var unavailable *Container
	if change, err := unavailable.TemporaryConfig(ctx, ConfigSetting{Section: "global", Name: "ms_client_mode"}); !errors.Is(err, context.Canceled) || change != nil {
		t.Fatal("nil cluster hid the caller cancellation", err)
	}
	if change, err := unavailable.TemporaryConfig(t.Context(), ConfigSetting{Section: "global", Name: "ms_client_mode"}); err == nil || change != nil {
		t.Fatal("nil cluster acquired a central override")
	}
}

func TestMessengerBootstrapAndLateMonitorAddressPolicy(t *testing.T) {
	for _, fixture := range []struct {
		name, address, ports, vector string
		secure                       bool
	}{
		{"default bridge", "", "", "[v2:192.0.2.12:3300,v1:192.0.2.12:6789]", false},
		{"secure bridge", "", "", "[v2:192.0.2.12:3300]", true},
		{"default host", "127.0.0.1", "34111,34112", "[v2:127.0.0.1:34111,v1:127.0.0.1:34112]", false},
		{"secure host", "127.0.0.1", "34111", "[v2:127.0.0.1:34111]", true},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			settings := options{}
			if fixture.secure {
				if err := WithMessengerMode(MessengerV2Secure)(&settings); err != nil {
					t.Fatal(err)
				}
			}
			env := map[string]string{
				"CEPH_FSID": "fixture-fsid", "CEPH_OSD_BLOCK_SIZE": "1073741824",
				"CEPH_PUBLIC_ADDRESS": fixture.address, "CEPH_MON_ID": "b",
			}
			if fixture.secure {
				env[messengerV2SecureEnvironment] = "true"
			}
			if fixture.ports != "" {
				ports := []int{34111}
				if !fixture.secure {
					ports = append(ports, 34112)
				}
				if settings.monitorPortCount() != len(ports) {
					t.Fatal("host MON reserved unused or insufficient protocol ports")
				}
				for key, value := range settings.monitorPortEnvironment(ports) {
					env[key] = value
				}
			}
			config, calls := messengerRunScript(t, "mon", env, nil)
			if !strings.Contains(config, "mon host = "+fixture.vector+"\n") {
				t.Fatalf("bootstrap client config advertises the wrong protocols/ports: %s", config)
			}
			messengerAssertAddVector(t, calls, "a", fixture.vector)
			messengerAssertPolicy(t, config, fixture.secure)
			joinedConfig, calls := messengerRunScript(t, "mon-join", env, []byte(config))
			messengerAssertAddVector(t, calls, "b", fixture.vector)
			if joinedConfig != config {
				t.Fatal("late monitor replaced its inherited configuration")
			}
			wantPorts := []string{"3300/tcp", "6789/tcp"}
			if fixture.secure {
				wantPorts = wantPorts[:1]
			}
			if !slices.Equal(settings.monitorExposedPorts(), wantPorts) {
				t.Fatal("MON publishing does not match enabled listener protocols")
			}
		})
	}
}

func TestMessengerSecurePolicySurvivesMonitorRefreshAndClientAttachment(t *testing.T) {
	cluster, control := newMonitorConfigFixture()
	cluster.settings.messengerMode = MessengerV2Secure
	global := "[global]\nmon host = old\nfsid = retained-fsid\n"
	for key, value := range messengerSecurePolicy {
		global += key + " = " + value + "\n"
	}
	cluster.config = []byte(global)
	control.config = []byte(global + "[client.admin]\nprivate = retained\n")
	control.native = []byte(`{"quorum_names":["a","b","c"],"monmap":{"mons":[
{"name":"a","public_addrs":{"addrvec":[{"type":"v2","addr":"127.0.0.1:34111/0"}]}},
{"name":"b","public_addrs":{"addrvec":[{"type":"v2","addr":"127.0.0.1:34112/0"}]}},
{"name":"c","public_addrs":{"addrvec":[{"type":"v2","addr":"127.0.0.1:34113/0"}]}}]}}`)
	mon := newMonitorConfigArchive("mon-a", "")
	mgr := newMonitorConfigArchive("mgr-b", "")
	osd := newMonitorConfigArchive("osd-3", "")
	cluster.Container = mon
	cluster.controlPlane = control
	cluster.managers["b"] = &ManagerContainer{Container: mgr, DaemonName: "b"}
	cluster.osds[3] = &OSDContainer{Container: osd, ID: 3}
	for _, daemon := range []*monitorConfigArchive{mon, mgr, osd} {
		daemon.config = []byte(global)
	}
	if err := cluster.RefreshMonitorConfig(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, daemon := range []*monitorConfigArchive{control, mon, mgr, osd} {
		messengerAssertPolicy(t, string(daemon.config), true)
		if bytes.Contains(daemon.config, []byte("v1:")) || !bytes.Contains(daemon.config, []byte("[v2:127.0.0.1:34113]")) {
			t.Fatal("MON refresh changed the secure-only protocol policy")
		}
	}
	config, _, err := cluster.ConnectionConfig()
	if err != nil {
		t.Fatal(err)
	}
	messengerAssertPolicy(t, string(config), true)
	var req testcontainers.GenericContainerRequest
	if err := cluster.WithClient()(&req); err != nil {
		t.Fatal(err)
	}
	messengerAssertAttachedPolicy(t, req)

	// A restricted client must inherit the same global policy while receiving
	// its own keyring. Later attachment uses the refreshed cluster template.
	auth := &authFixtureContainer{}
	clientCluster := authFixtureCluster(auth)
	clientCluster.config = bytes.Clone(config)
	client, err := clientCluster.CreateClient(t.Context(), "reader", ClientCaps{Mon: "allow r", OSD: "allow r"})
	if err != nil {
		t.Fatal(err)
	}
	clientConfig, _, err := client.ConnectionConfig()
	if err != nil {
		t.Fatal(err)
	}
	messengerAssertPolicy(t, string(clientConfig), true)
	req = testcontainers.GenericContainerRequest{}
	if err := clientCluster.WithClientIdentity(client)(&req); err != nil {
		t.Fatal(err)
	}
	messengerAssertAttachedPolicy(t, req)
}

func messengerAssertAttachedPolicy(t *testing.T, req testcontainers.GenericContainerRequest) {
	t.Helper()
	for _, file := range req.Files {
		if file.ContainerFilePath != "/etc/ceph/ceph.conf" {
			continue
		}
		data, err := io.ReadAll(file.Reader)
		if err != nil {
			t.Fatal(err)
		}
		messengerAssertPolicy(t, string(data), true)
		return
	}
	t.Fatal("client attachment omitted the cluster configuration")
}

func messengerAssertPolicy(t *testing.T, config string, secure bool) {
	t.Helper()
	global := false
	values := map[string]string{}
	for _, line := range strings.Split(config, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "[") {
			global = line == "[global]"
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if _, relevant := messengerSecurePolicy[key]; !ok || !relevant {
			continue
		}
		if !global {
			t.Fatalf("messenger policy was scoped to one daemon: %s", key)
		}
		if _, duplicate := values[key]; duplicate {
			t.Fatalf("ambiguous duplicate messenger policy: %s", key)
		}
		values[key] = value
	}
	for key, expected := range messengerSecurePolicy {
		actual, found := values[key]
		if secure && (!found || actual != expected) {
			t.Fatalf("%s = %q, want singleton %q", key, actual, expected)
		}
		if !secure && found {
			t.Fatalf("default bootstrap overrides Ceph messenger defaults: %s", key)
		}
	}
}

func messengerAssertAddVector(t *testing.T, calls []string, id, vector string) {
	t.Helper()
	for i, value := range calls {
		if value == "--addv" && i+2 < len(calls) {
			if calls[i+1] != id || calls[i+2] != vector {
				t.Fatalf("native monmap admission used %q %q, want %q %q", calls[i+1], calls[i+2], id, vector)
			}
			return
		}
	}
	t.Fatal("MON bootstrap omitted explicit native monmap admission")
}

// Execute the embedded shell with every writable absolute path redirected to
// a test directory and native Ceph programs replaced with argv recorders.
// This exercises shell expansion and actual config output without Docker.
func messengerRunScript(t *testing.T, name string, env map[string]string, inherited []byte) (string, []string) {
	t.Helper()
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"ceph-authtool", "monmaptool", "ceph-mon"} {
		data := []byte("#!/bin/sh\nprintf '%s\\n' \"BEGIN:${0##*/}\" \"$@\" END >> \"$TC_COMMAND_LOG\"\n")
		if err := os.WriteFile(filepath.Join(bin, command), data, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(bin, "hostname"), []byte("#!/bin/sh\nprintf '%s\\n' 192.0.2.12\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	data, err := scripts.ReadFile("internal/scripts/" + name + ".sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(data)
	for _, path := range []string{"/etc/ceph", "/var/lib/ceph", "/var/run/ceph", "/tmp/monmap", "/tc/monmap"} {
		script = strings.ReplaceAll(script, path, root+path)
	}
	configPath := root + "/etc/ceph/ceph.conf"
	if inherited != nil {
		if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(configPath, inherited, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// monmaptool is a stub, but its output directory must be container-local.
	if err := os.MkdirAll(root+"/tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	scriptPath := filepath.Join(root, "script.sh")
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(root, "commands.log")
	cmd := exec.CommandContext(t.Context(), "/bin/sh", scriptPath)
	cmd.Env = []string{"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"), "TC_COMMAND_LOG=" + logPath}
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%s script failed: %v: %s", name, err, output)
	}
	config, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	log, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	return string(config), strings.Split(strings.TrimSpace(string(log)), "\n")
}
