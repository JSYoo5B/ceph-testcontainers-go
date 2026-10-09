package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const monitorConfigAddresses = "[v2:127.0.0.1:3311,v1:127.0.0.1:6791] [v2:127.0.0.1:3312,v1:127.0.0.1:6792] [v2:127.0.0.1:3313,v1:127.0.0.1:6793]"
const monitorConfigOld = "# fixture configuration\n[global]\nmon host = old\nfsid = retained-fsid\n[osd]\nosd memory target = 536870912\n"
const monitorConfigQuorum = `{"quorum_names":["a","b","c"],"monmap":{"mons":[
{"name":"a","public_addrs":{"addrvec":[{"type":"v2","addr":"127.0.0.1:3311/0"},{"type":"v1","addr":"127.0.0.1:6791/0"}]}},
{"name":"b","public_addrs":{"addrvec":[{"type":"v2","addr":"127.0.0.1:3312/0"},{"type":"v1","addr":"127.0.0.1:6792/0"}]}},
{"name":"c","public_addrs":{"addrvec":[{"type":"v2","addr":"127.0.0.1:3313/0"},{"type":"v1","addr":"127.0.0.1:6793/0"}]}}]}}`

func TestMonitorConfigReplacementPreservesPrivateEntriesAndComments(t *testing.T) {
	for _, example := range []struct {
		name, before, after string
	}{
		{"space alias", monitorConfigOld, strings.Replace(monitorConfigOld, "mon host = old", "mon host = "+monitorConfigAddresses, 1)},
		{"underscore alias and private override", "[global]\n\tmon_host\t=\t\"old\"  # retained bootstrap comment\n[rgw]\nmon_host = private-override\nrgw_frontends = \"beast port=7480 # quoted marker\"\n", "[global]\n\tmon_host\t=\t" + monitorConfigAddresses + "  # retained bootstrap comment\n[rgw]\nmon_host = private-override\nrgw_frontends = \"beast port=7480 # quoted marker\"\n"},
		{"CRLF and section comment", "; leading comment\r\n[ global ] # section note\r\n mon  host=old\t; note\r\n[mon.a]\r\npublic addr = 127.0.0.1\r\n", "; leading comment\r\n[ global ] # section note\r\n mon  host=" + monitorConfigAddresses + "\t; note\r\n[mon.a]\r\npublic addr = 127.0.0.1\r\n"},
		{"no trailing newline", "[global]\nmon_host=old", "[global]\nmon_host=" + monitorConfigAddresses},
		{"dash alias", "[global]\nmon-host = old\n[mds.a]\nmon-host = private\n", "[global]\nmon-host = " + monitorConfigAddresses + "\n[mds.a]\nmon-host = private\n"},
		{"quoted delimiter and escaped quote", "[global]\nmon host = \"old#value\\\"tail\" # retained\n[mds.test]\nmds_join_fs = test-fs\n", "[global]\nmon host = " + monitorConfigAddresses + " # retained\n[mds.test]\nmds_join_fs = test-fs\n"},
	} {
		t.Run(example.name, func(t *testing.T) {
			before := []byte(example.before)
			updated, err := replaceGlobalMonitorHost(before, monitorConfigAddresses)
			if err != nil || string(updated) != example.after || string(before) != example.before {
				t.Fatalf("global-only replacement changed private syntax or input: %v\n%s", err, updated)
			}
			second, err := replaceGlobalMonitorHost(updated, monitorConfigAddresses)
			if err != nil || !bytes.Equal(second, updated) {
				t.Fatal("repeated replacement changed an already updated file", err)
			}
		})
	}
}

func TestMonitorConfigReplacementRejectsAmbiguousOrMalformedInput(t *testing.T) {
	for _, raw := range []string{
		"", "mon host = old\n", "[global]\nfsid = retained\n", "[osd]\nmon_host = old\n",
		"[global]\nmon host = old\nmon_host = second\n", "[global]\nmon host = old\n[global]\nfsid = other\n",
		"[global] unexpected\nmon host = old\n", "[global\nmon host = old\n", "[global]\nmon host = \"unterminated\n",
		"[global]\nmon host = old\\\ncontinued\n", "[global]\nmon host = old\n!include /other/ceph.conf\n",
		"[global]\nmon host = old\ninclude = /other/ceph.conf\n",
		"[global]\nmon host = old\ninvalid standalone entry\n", "[global]\nmon host = old\n=unnamed entry\n",
		"[global]\nmon host = old\x00\n", "[global]\nmon host = old\xff\n",
		monitorConfigOld + strings.Repeat("#", monitorConfigMaxBytes),
	} {
		updated, err := replaceGlobalMonitorHost([]byte(raw), monitorConfigAddresses)
		if err == nil || updated != nil {
			t.Fatalf("ambiguous/malformed file was rewritten: input length=%d", len(raw))
		}
	}
	for _, addresses := range []string{"", "\n", "[v2:127.0.0.1:3300]\nother = injected", "[v2:127.0.0.1:3300] # injected", "x\x00y"} {
		if updated, err := replaceGlobalMonitorHost([]byte(monitorConfigOld), addresses); err == nil || updated != nil {
			t.Fatal("invalid bootstrap value could change configuration syntax")
		}
	}
}

func TestMonitorBootstrapAddressesValidateNativeMapAndQuorum(t *testing.T) {
	var valid QuorumStatus
	if err := json.Unmarshal([]byte(monitorConfigQuorum), &valid); err != nil {
		t.Fatal(err)
	}
	addresses, err := monitorBootstrapAddresses(valid)
	if err != nil || addresses != monitorConfigAddresses {
		t.Fatalf("valid native vectors did not retain protocols/ports: %s: %v", addresses, err)
	}
	for _, raw := range []string{
		`{}`, `{"quorum_names":["a"],"monmap":{"mons":[{"name":"a"}]}}`,
		strings.Replace(monitorConfigQuorum, `"name":"b"`, `"name":"a"`, 1),
		strings.Replace(monitorConfigQuorum, `"quorum_names":["a","b","c"]`, `"quorum_names":["a","a","a"]`, 1),
		strings.Replace(monitorConfigQuorum, `"quorum_names":["a","b","c"]`, `"quorum_names":["a","b","foreign"]`, 1),
		strings.Replace(monitorConfigQuorum, `"quorum_names":["a","b","c"]`, `"quorum_names":["a"]`, 1),
		strings.Replace(monitorConfigQuorum, `"type":"v2"`, `"type":"v3"`, 1),
		strings.Replace(monitorConfigQuorum, "127.0.0.1:3311/0", "0.0.0.0:3311/0", 1),
		strings.Replace(monitorConfigQuorum, "127.0.0.1:3311/0", "127.0.0.1:65536/0", 1),
		strings.Replace(monitorConfigQuorum, "127.0.0.1:3311/0", "localhost:3311/0", 1),
		strings.Replace(monitorConfigQuorum, "127.0.0.1:3311/0", "127.0.0.1:3311/injected", 1),
	} {
		var status QuorumStatus
		if err := json.Unmarshal([]byte(raw), &status); err != nil {
			t.Fatal(err)
		}
		if _, err := monitorBootstrapAddresses(status); err == nil {
			t.Fatal("malformed native bootstrap membership was accepted", raw)
		}
	}
}

func TestRefreshMonitorConfigUpdatesEveryOwnedRoleWhileStopped(t *testing.T) {
	cluster, control := newMonitorConfigFixture()
	mon := newMonitorConfigArchive("mon-a", "[mon.a]\npublic bind addr = 127.0.0.1\n")
	second := newMonitorConfigArchive("mon-b", "[mon.b]\nmon data = /private/b\n")
	mgr := newMonitorConfigArchive("mgr-a", "[mgr.a]\npublic addr = 127.0.0.2\n")
	osd := newMonitorConfigArchive("osd-3", "[osd.3]\ncluster addr = 172.20.0.8\n")
	rgw := newMonitorConfigArchive("rgw-default", "[client.admin]\nrgw frontends = beast port=7480\n")
	mds := newMonitorConfigArchive("mds-a", "[mds.a]\nmds join fs = test-fs\n")
	caller := newMonitorConfigArchive("caller-owned", "[client.test]\nkeyring = /private/keyring\n")
	cluster.Container = mon
	cluster.controlPlane = control
	cluster.monitors["b"] = &MonitorContainer{Container: second, DaemonName: "b"}
	cluster.managers["a"] = &ManagerContainer{Container: mgr, DaemonName: "a"}
	cluster.manager = mgr
	cluster.osds[3] = &OSDContainer{Container: osd, ID: 3}
	cluster.services["rgw.default"], cluster.services["mds.a"] = rgw, mds
	cluster.gateways["default"] = &RGWContainer{Container: rgw, GatewayName: "default"}
	cluster.filesystems["test-fs"] = &CephFSContainer{mdss: []*MDSContainer{{Container: mds, ID: "a"}}}
	all := []*monitorConfigArchive{control, mon, second, mgr, osd, rgw, mds}
	before := make(map[string]string)
	for _, daemon := range all {
		before[daemon.id] = string(daemon.config)
	}
	if err := cluster.RefreshMonitorConfig(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, daemon := range all {
		want := strings.Replace(before[daemon.id], "mon host = old", "mon host = "+monitorConfigAddresses, 1)
		if string(daemon.config) != want || daemon.reads != 1 || daemon.writes != 1 || daemon.starts != 0 || daemon.stops != 0 || daemon.terminations != 0 {
			t.Fatalf("stopped %s config/private data changed or daemon ran: reads=%d writes=%d", daemon.id, daemon.reads, daemon.writes)
		}
		if daemon != control && daemon.execs != 0 {
			t.Fatalf("%s was inspected through a native process rather than Docker archive", daemon.id)
		}
	}
	if caller.reads != 0 || caller.writes != 0 || !bytes.Contains(caller.config, []byte("mon host = old")) {
		t.Fatal("caller-owned config was implicitly adopted")
	}
	config, keyring, err := cluster.ConnectionConfig()
	if err != nil || !bytes.Contains(config, []byte(monitorConfigAddresses)) || bytes.Contains(config, []byte("control_private")) || string(keyring) != "retained-admin-keyring" {
		t.Fatal("future-client template imported control-only settings or lost bootstrap/keyring", err)
	}
	if err := cluster.RefreshMonitorConfig(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, daemon := range all {
		if daemon.reads != 2 || daemon.writes != 1 || daemon.starts != 0 || daemon.stops != 0 || daemon.terminations != 0 {
			t.Fatalf("idempotent refresh recopied or restarted %s", daemon.id)
		}
	}
	if control.execs != 2 {
		t.Fatal("repeated refresh did not use a fresh current quorum")
	}
}

func TestMonitorConfigInventorySkipsRemovedAndRetiredHandles(t *testing.T) {
	cluster, control := newMonitorConfigFixture()
	primary := newMonitorConfigArchive("retired-primary", "")
	removed := newMonitorConfigArchive("retired-secondary", "")
	retiredMgr := newMonitorConfigArchive("retired-manager", "")
	retiredOSD := newMonitorConfigArchive("retired-osd", "")
	cluster.Container, cluster.controlPlane = primary, control
	cluster.monitors["removed"] = &MonitorContainer{Container: removed, DaemonName: "removed"}
	cluster.monitors["uncreated"] = &MonitorContainer{DaemonName: "uncreated"}
	cluster.managers["a"] = &ManagerContainer{Container: retiredMgr, terminated: true}
	cluster.manager = retiredMgr
	cluster.osds[2] = &OSDContainer{Container: retiredOSD, purged: true}
	cluster.monitorTerminated = true
	targets := cluster.monitorConfigTargets(control, "removed")
	if len(targets) != 0 {
		t.Fatalf("retired or uncreated containers were queued for refresh: %v", targets)
	}
	cluster.monitorTerminated = false
	delete(cluster.monitors, "removed")
	if targets := cluster.monitorConfigTargets(control, "a"); len(targets) != 0 {
		t.Fatal("a just-terminated primary was queued during RemoveMonitor")
	}
	cluster.Container = control
	cluster.services["rgw.same"] = control
	if targets := cluster.monitorConfigTargets(control, ""); len(targets) != 0 {
		t.Fatal("control/primary/service aliases were recopied")
	}
}

func TestRefreshMonitorConfigPartialCopyPublishesControlAndRetries(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprintf("uncertain-copy-applied=%v", applied), func(t *testing.T) {
			cluster, control := newMonitorConfigFixture()
			bad := newMonitorConfigArchive("mgr-b", "[mgr.b]\nprivate = retained\n")
			other := newMonitorConfigArchive("osd-7", "[osd.7]\nprivate = neighbor\n")
			lost := errors.New("daemon archive copy response lost")
			bad.writeErr, bad.applyOnError = lost, applied
			cluster.managers["b"] = &ManagerContainer{Container: bad, DaemonName: "b"}
			cluster.osds[7] = &OSDContainer{Container: other, ID: 7}
			err := cluster.RefreshMonitorConfig(t.Context())
			if !errors.Is(err, lost) || !strings.Contains(err.Error(), "mgr.b") {
				t.Fatalf("partial copy did not retain its named error: %v", err)
			}
			config, _, err := cluster.ConnectionConfig()
			if err != nil || !bytes.Contains(config, []byte(monitorConfigAddresses)) || other.writes != 1 || control.writes != 1 || bad.writes != 1 {
				t.Fatal("partial daemon copy kept future clients stale or skipped another owned daemon", err)
			}
			bad.writeErr = nil
			if err := cluster.RefreshMonitorConfig(t.Context()); err != nil {
				t.Fatal(err)
			}
			wantWrites := 2
			if applied {
				wantWrites = 1
			}
			if control.writes != 1 || other.writes != 1 || bad.writes != wantWrites || bad.reads != 2 || !bytes.Contains(bad.config, []byte(monitorConfigAddresses)) || !bytes.Contains(bad.config, []byte("private = retained")) {
				t.Fatal("retry failed to re-read uncertain copy or rewrote already current files")
			}
		})
	}
}

func TestRefreshMonitorConfigUncertainControlCopyPublishesOnlyAfterConfirmation(t *testing.T) {
	cluster, control := newMonitorConfigFixture()
	daemon := newMonitorConfigArchive("osd-9", "[osd.9]\nprivate = retained\n")
	cluster.osds[9] = &OSDContainer{Container: daemon, ID: 9}
	before := bytes.Clone(cluster.config)
	lost := errors.New("control archive copy reply lost")
	control.writeErr, control.applyOnError = lost, true
	if err := cluster.RefreshMonitorConfig(t.Context()); !errors.Is(err, lost) || !bytes.Equal(cluster.config, before) || daemon.reads != 0 || !bytes.Contains(control.config, []byte(monitorConfigAddresses)) {
		t.Fatalf("uncertain control copy published an unconfirmed template or lost error: %v", err)
	}
	control.writeErr = nil
	if err := cluster.RefreshMonitorConfig(t.Context()); err != nil || control.writes != 1 || daemon.writes != 1 || !bytes.Contains(cluster.config, []byte(monitorConfigAddresses)) {
		t.Fatalf("fresh archive confirmation did not reconcile uncertain control copy: %v", err)
	}
}

func TestRefreshMonitorConfigRefusesMalformedDaemonButKeepsOtherCopies(t *testing.T) {
	cluster, control := newMonitorConfigFixture()
	bad := newMonitorConfigArchive("mgr-b", "")
	bad.config = []byte("[global]\nmon host = old\nmon_host = duplicate\n")
	before := bytes.Clone(bad.config)
	other := newMonitorConfigArchive("osd-8", "[osd.8]\nprivate = retained\n")
	cluster.managers["b"] = &ManagerContainer{Container: bad, DaemonName: "b"}
	cluster.osds[8] = &OSDContainer{Container: other, ID: 8}
	if err := cluster.RefreshMonitorConfig(t.Context()); err == nil || !strings.Contains(err.Error(), "duplicate") || !bytes.Equal(before, bad.config) || bad.writes != 0 || control.writes != 1 || other.writes != 1 {
		t.Fatalf("ambiguous daemon config was rewritten or successful copies rolled back: %v", err)
	}
	bad.config = []byte(monitorConfigOld + "[mgr.b]\nprivate = repaired\n")
	if err := cluster.RefreshMonitorConfig(t.Context()); err != nil || bad.writes != 1 || other.writes != 1 || control.writes != 1 {
		t.Fatalf("explicitly repaired config was not refreshable without unrelated copies: %v", err)
	}
}

func TestRefreshMonitorConfigRejectsMalformedTemplateBeforeArchiveCopies(t *testing.T) {
	cluster, control := newMonitorConfigFixture()
	cluster.config = []byte("[global]\nmon_host = old\nmon host = ambiguous\n")
	if err := cluster.RefreshMonitorConfig(t.Context()); err == nil || control.reads != 0 || control.writes != 0 {
		t.Fatalf("ambiguous template was published or Docker files changed: %v", err)
	}
}

func TestRefreshMonitorConfigArchiveReadCloseAndSizeErrorsArePreserved(t *testing.T) {
	for _, phase := range []string{"read", "close", "size"} {
		t.Run(phase, func(t *testing.T) {
			cluster, control := newMonitorConfigFixture()
			before := bytes.Clone(cluster.config)
			failed := errors.New("archive " + phase + " failure")
			switch phase {
			case "read":
				control.readErr = failed
			case "close":
				control.closeErr = failed
			case "size":
				control.config = []byte(monitorConfigOld + strings.Repeat("#", monitorConfigMaxBytes))
			}
			err := cluster.RefreshMonitorConfig(t.Context())
			if err == nil || (phase != "size" && !errors.Is(err, failed)) || control.writes != 0 || !bytes.Equal(cluster.config, before) {
				t.Fatalf("archive error lost or unconfirmed template published: %v", err)
			}
		})
	}
}

func TestRefreshMonitorConfigCanceledClosedAndContendedOperationsDoNotCopy(t *testing.T) {
	for _, phase := range []string{"canceled", "closed", "zero startup timeout", "contended mutex", "cancel during archive read"} {
		t.Run(phase, func(t *testing.T) {
			cluster, control := newMonitorConfigFixture()
			before := bytes.Clone(cluster.config)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch phase {
			case "canceled":
				cancel()
			case "closed":
				cluster.closed = true
			case "zero startup timeout":
				cluster.settings.startupTimeout = 0
			case "contended mutex":
				cluster.mu.Lock()
				defer cluster.mu.Unlock()
				short, stop := context.WithTimeout(ctx, 20*time.Millisecond)
				defer stop()
				ctx = short
			case "cancel during archive read":
				control.afterRead = cancel
			}
			started := time.Now()
			err := cluster.RefreshMonitorConfig(ctx)
			if err == nil || control.writes != 0 || !bytes.Equal(before, cluster.config) {
				t.Fatalf("expired/closed refresh copied configs or published template: %v", err)
			}
			if phase == "contended mutex" && (!errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second) {
				t.Fatalf("mutex contention ignored caller deadline: %v", err)
			}
			if phase != "cancel during archive read" && (control.reads != 0 || control.execs != 0) {
				t.Fatal("preflight refusal invoked Docker or native commands")
			}
		})
	}
	var unavailable *Container
	if err := unavailable.RefreshMonitorConfig(t.Context()); err == nil {
		t.Fatal("nil cluster refresh was accepted")
	}
}

func TestRefreshMonitorConfigCancellationAfterControlRetainsConfirmedTemplate(t *testing.T) {
	cluster, control := newMonitorConfigFixture()
	daemon := newMonitorConfigArchive("osd-5", "[osd.5]\nprivate = retained\n")
	cluster.osds[5] = &OSDContainer{Container: daemon, ID: 5}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	control.afterWrite = cancel
	if err := cluster.RefreshMonitorConfig(ctx); !errors.Is(err, context.Canceled) || control.writes != 1 || !bytes.Contains(cluster.config, []byte(monitorConfigAddresses)) || daemon.reads != 0 || daemon.writes != 0 {
		t.Fatalf("confirmed control/template update was lost or canceled daemon copy continued: %v", err)
	}
	control.afterWrite = nil
	if err := cluster.RefreshMonitorConfig(t.Context()); err != nil || control.writes != 1 || daemon.writes != 1 {
		t.Fatalf("fresh-context refresh did not finish remaining owned files: %v", err)
	}
}

func TestMonitorConfigInventoryIsDeterministicAndDeduplicated(t *testing.T) {
	cluster, control := newMonitorConfigFixture()
	a := newMonitorConfigArchive("a", "")
	b := newMonitorConfigArchive("b", "")
	cluster.services["rgw.z"], cluster.services["mds.a"] = a, b
	cluster.gateways["z"] = &RGWContainer{Container: a}
	cluster.filesystems["test"] = &CephFSContainer{mdss: []*MDSContainer{{Container: b, ID: "a"}}}
	targets := cluster.monitorConfigTargets(control, "")
	var names, ids []string
	for _, target := range targets {
		names = append(names, target.name)
		ids = append(ids, target.container.GetContainerID())
	}
	if len(targets) != 2 || !slices.IsSorted(names) || len(slices.Compact(slices.Clone(ids))) != 2 {
		t.Fatalf("unordered or duplicate archive targets: %v %v", names, ids)
	}
}

type monitorConfigArchive struct {
	testcontainers.Container
	id                          string
	config, native              []byte
	reads, writes, execs        int
	starts, stops, terminations int
	readErr, writeErr, closeErr error
	applyOnError                bool
	afterRead, afterWrite       func()
}

func newMonitorConfigArchive(id, private string) *monitorConfigArchive {
	return &monitorConfigArchive{id: id, config: []byte(monitorConfigOld + private)}
}

func newMonitorConfigFixture() (*Container, *monitorConfigArchive) {
	control := newMonitorConfigArchive("control", "[client.admin]\ncontrol_private = retained-only-on-control\n")
	control.native = []byte(monitorConfigQuorum)
	cluster := &Container{Container: control, settings: options{startupTimeout: time.Second},
		config: []byte(monitorConfigOld), keyring: []byte("retained-admin-keyring"),
		monitors: make(map[string]*MonitorContainer), managers: make(map[string]*ManagerContainer),
		osds: make(map[int]*OSDContainer), services: make(map[string]testcontainers.Container),
		gateways: make(map[string]*RGWContainer), filesystems: make(map[string]*CephFSContainer)}
	return cluster, control
}

func (archive *monitorConfigArchive) GetContainerID() string { return archive.id }

func (archive *monitorConfigArchive) CopyFileFromContainer(ctx context.Context, path string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	archive.reads++
	if path != "/etc/ceph/ceph.conf" {
		return nil, fmt.Errorf("unexpected archive path: %s", path)
	}
	if archive.readErr != nil {
		return nil, archive.readErr
	}
	reader := &monitorConfigReader{Reader: bytes.NewReader(bytes.Clone(archive.config)), closeErr: archive.closeErr}
	if archive.afterRead != nil {
		archive.afterRead()
	}
	return reader, nil
}

func (archive *monitorConfigArchive) CopyToContainer(ctx context.Context, config []byte, path string, mode int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if path != "/etc/ceph/ceph.conf" || mode != 0o644 {
		return errors.New("archive copy changed config path or fixture mode")
	}
	archive.writes++
	if archive.writeErr == nil || archive.applyOnError {
		archive.config = bytes.Clone(config)
	}
	if archive.afterWrite != nil {
		archive.afterWrite()
	}
	return archive.writeErr
}

func (archive *monitorConfigArchive) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	archive.execs++
	if !monitorQuorumTestCommand(args) || archive.native == nil {
		return 0, nil, errors.New("config refresh invoked an unexpected daemon process")
	}
	return monitorQuorumTestReader(args, 0, archive.native)
}

func (archive *monitorConfigArchive) Start(context.Context) error {
	archive.starts++
	return errors.New("config refresh must not start a daemon")
}

func (archive *monitorConfigArchive) Stop(context.Context, *time.Duration) error {
	archive.stops++
	return errors.New("config refresh must not stop a daemon")
}

func (archive *monitorConfigArchive) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	archive.terminations++
	return errors.New("config refresh must not terminate a daemon")
}

type monitorConfigReader struct {
	io.Reader
	closeErr error
}

func (reader *monitorConfigReader) Close() error { return reader.closeErr }
