package overridehook_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// TestRegistryThroughHooks calls every method of the seam that needs no
// native command. The blocklist commands themselves run against a fake control
// container in the tests of package override.
func TestRegistryThroughHooks(t *testing.T) {
	c := servicehook.TestCluster(nil, servicehook.TestConfig{StartupTimeout: 3 * time.Second})
	ctx := t.Context()
	if err := overridehook.Admit(c, "override.AddBlocklist"); err != nil {
		t.Fatalf("a cluster of its own refused an override: %v", err)
	}
	if err := overridehook.Admit(c, "override.SetMGRModule"); err != nil {
		t.Fatalf("a cluster of its own refused the MGR module override: %v", err)
	}
	if !overridehook.ValidMGRModuleName("mirroring") || overridehook.ValidMGRModuleName("--force") {
		t.Fatal("the MGR module name check changed")
	}
	if err := overridehook.Admit(c, "override.Unclassified"); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("an unclassified override was admitted: %v", err)
	}
	if got := overridehook.StartupTimeout(c); got != 3*time.Second {
		t.Fatalf("startup timeout %v", got)
	}
	if overridehook.Terminated() == nil || !errors.Is(overridehook.Terminated(), ceph.ErrInvalidRequest) {
		t.Fatal("the terminated error is missing or unclassified")
	}
	cause := errors.New("cause")
	if err := overridehook.Invalid(cause); !errors.Is(err, ceph.ErrInvalidRequest) || !errors.Is(err, cause) || err.Error() != cause.Error() {
		t.Fatalf("Invalid changed the text or lost the class: %v", err)
	}
	if got, err := overridehook.NormalizeBlocklistSession("v2:192.0.2.7:0/1234"); err != nil || got != "192.0.2.7:0/1234" {
		t.Fatalf("normalize: %q %v", got, err)
	}
	if _, err := overridehook.NormalizeBlocklistSession("192.0.2.7"); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a bare address was accepted: %v", err)
	}
	type handle struct{}
	var first overridehook.Token
	held := &handle{}
	err := overridehook.WithTopology(ctx, c, func() error {
		if err := overridehook.Ready(c); err == nil {
			t.Error("a cluster without a control container was ready")
		}
		token, err := overridehook.Reserve(ctx, c, overridehook.Blocklist, "192.0.2.7:0/1")
		if err != nil {
			return err
		}
		first = token
		if _, err := overridehook.Reserve(ctx, c, overridehook.Blocklist, "192.0.2.7:0/1"); err == nil {
			t.Error("a reserved key was reserved twice")
		}
		if closed, err := overridehook.Settle(c, token, held); err != nil || closed {
			t.Errorf("settle: closed=%v err=%v", closed, err)
		}
		if _, err := overridehook.Reserve(ctx, c, overridehook.Blocklist, "192.0.2.7:0/1"); err == nil {
			t.Error("a held key was reserved twice")
		}
		if _, err := overridehook.Reserve(ctx, c, overridehook.Kind("Other"), "x"); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("an unknown kind was reserved: %v", err)
		}
		if _, err := overridehook.Settle(c, overridehook.Token{}, held); err == nil {
			t.Error("a zero token settled")
		}
		if got := overridehook.TestHeld(c, overridehook.Blocklist); got["192.0.2.7:0/1"] != held || len(got) != 1 {
			t.Errorf("the held handle is %v", got)
		}
		// The OSD flag kind has its own keys and its own overlap text.
		flagToken, err := overridehook.Reserve(ctx, c, overridehook.OSDFlag, "noout")
		if err != nil {
			t.Errorf("an OSD flag key was refused next to a blocklist key: %v", err)
			return nil
		}
		if _, err := overridehook.Reserve(ctx, c, overridehook.OSDFlag, "noout"); err == nil || err.Error() != "restore the existing OSD flag override first" {
			t.Errorf("a reserved OSD flag was reserved twice: %v", err)
		}
		if _, err := overridehook.Settle(c, flagToken, held); err != nil {
			t.Errorf("settle of an OSD flag: %v", err)
		}
		if got := overridehook.TestHeld(c, overridehook.OSDFlag); got["noout"] != held || len(got) != 1 {
			t.Errorf("the held OSD flag handle is %v", got)
		}
		if !overridehook.Release(c, flagToken) {
			t.Error("the OSD flag was not released")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	overridehook.WithTopologyNoContext(c, func() {
		if !overridehook.Tracks(c, first) || !overridehook.Release(c, first) || overridehook.Tracks(c, first) || overridehook.Release(c, first) {
			t.Error("release through the hooks did not forget the token exactly once")
		}
		// The MGR module kind reserves its own key space.
		module, err := overridehook.Reserve(ctx, c, overridehook.MGRModule, "mirroring")
		if err != nil {
			t.Errorf("reserve MGR module: %v", err)
		}
		if _, err := overridehook.Reserve(ctx, c, overridehook.MGRModule, "mirroring"); err == nil || err.Error() != "restore the existing MGR module override first" {
			t.Errorf("a reserved module was reserved twice: %v", err)
		}
		if _, err := overridehook.Settle(c, module, nil); err != nil || overridehook.Tracks(c, module) {
			t.Errorf("a settled reservation without a handle stayed tracked: %v", err)
		}
		// Without a control container every native operation fails and none panics.
		bounded, stop := context.WithTimeout(ctx, 200*time.Millisecond)
		defer stop()
		if _, err := overridehook.MGRModulesSnapshot(bounded, c); err == nil {
			t.Error("MGR module snapshot without a control container")
		}
		if err := overridehook.SetMGRModule(ctx, c, "mirroring", true); err == nil {
			t.Error("MGR module change without a control container")
		}
		if err := overridehook.WaitMGRModuleMembership(bounded, c, "mirroring", true, false); err == nil {
			t.Error("MGR module readback without a control container")
		}
		if err := overridehook.ProtectMGRModule(ctx, c, "mirroring"); err == nil {
			t.Error("MGR module protection read without a control container")
		}
		if _, err := overridehook.BlocklistEntries(ctx, c); err == nil {
			t.Error("blocklist read without a control container")
		}
		if err := overridehook.BlocklistAdd(ctx, c, "192.0.2.7:0/1", time.Minute); err == nil {
			t.Error("blocklist add without a control container")
		}
		if err := overridehook.BlocklistRemove(ctx, c, "192.0.2.7:0/1"); err == nil {
			t.Error("blocklist removal without a control container")
		}
		if _, err := overridehook.OSDMapFlags(ctx, c); err == nil {
			t.Error("OSDMap flag read without a control container")
		}
		if err := overridehook.SetOSDFlag(ctx, c, ceph.OSDFlagNoOut, true); err == nil {
			t.Error("OSD flag write without a control container")
		}
	})
	// A context that has ended stops the wait for a held mutex.
	servicehook.Lock(c)
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := overridehook.WithTopology(short, c, func() error { t.Error("ran without the mutex"); return nil }); err == nil {
		t.Error("WithTopology ignored its context")
	}
	servicehook.Unlock(c)
	if err := c.Terminate(ctx); err != nil {
		t.Fatalf("Terminate undid a released override: %v", err)
	}
}

// inertControl stands for a control container that no test command reaches.
type inertControl struct{ testcontainers.Container }

// TestDebugHooks calls the debug level operations of the seam that need no
// native command. The window logic runs against a fake target in the tests of
// package override.
func TestDebugHooks(t *testing.T) {
	ctx := t.Context()
	c := servicehook.TestCluster(inertControl{}, servicehook.TestConfig{StartupTimeout: time.Second, OSDs: 1})
	if err := overridehook.Admit(c, "override.SetDebugLevel"); err != nil {
		t.Fatalf("a cluster of its own refused SetDebugLevel: %v", err)
	}
	shared := servicehook.TestCluster(nil, servicehook.TestConfig{Shared: true})
	if err := overridehook.Admit(shared, "override.SetDebugLevel"); !errors.Is(err, ceph.ErrSharedCluster) {
		t.Fatalf("a shared cluster admitted SetDebugLevel: %v", err)
	}
	if target, err := overridehook.OpenDebugTarget(ctx, c, "osd.0"); err != nil || target == nil {
		t.Fatalf("an owned OSD: %v %v", target, err)
	}
	for _, daemon := range []string{"osd.5", "mon.a", "osd.01"} {
		if target, err := overridehook.OpenDebugTarget(ctx, c, daemon); target != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: target %v err %v", daemon, target, err)
		}
	}
	closed := servicehook.TestCluster(inertControl{}, servicehook.TestConfig{Closed: true, OSDs: 1})
	if _, err := overridehook.OpenDebugTarget(ctx, closed, "osd.0"); !errors.Is(err, overridehook.Terminated()) {
		t.Errorf("a terminated cluster: %v", err)
	}
	// The lock is released again: the cluster mutex can be taken.
	overridehook.WithTopologyNoContext(c, func() {})
	registry := overridehook.DebugWindowRegistry(c)
	first, second := new(int), new(int)
	if err := registry.Reserve("osd.0/debug_osd", first); err != nil {
		t.Fatal(err)
	}
	if err := registry.Reserve("osd.0/debug_osd", second); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Errorf("a second window was admitted: %v", err)
	}
	registry.Release("osd.0/debug_osd", second)
	if err := registry.Reserve("osd.0/debug_osd", second); err == nil {
		t.Error("a release by a non-owner freed the slot")
	}
	registry.Release("osd.0/debug_osd", first)
	if err := registry.Reserve("osd.0/debug_osd", second); err != nil {
		t.Errorf("the slot was not freed: %v", err)
	}
	// The registry has a lock of its own: it works while the cluster mutex is
	// held by a long topology operation.
	servicehook.Lock(c)
	locked := make(chan struct{})
	go func() {
		defer close(locked)
		owner := new(int)
		if err := registry.Reserve("osd.0/debug_ms", owner); err != nil {
			t.Errorf("reserve under the cluster mutex: %v", err)
		}
		registry.Release("osd.0/debug_ms", owner)
	}()
	select {
	case <-locked:
	case <-time.After(5 * time.Second):
		t.Error("the registry waited for the cluster mutex")
	}
	servicehook.Unlock(c)
	if overridehook.DebugWindowRegistry(c) == overridehook.DebugWindowRegistry(shared) {
		t.Error("two clusters share one registry")
	}
	for name, test := range map[string]struct {
		owned, running, paused bool
		identity               string
		err                    error
		changed                bool
		want                   string
	}{
		"same":      {true, true, false, "id", nil, false, ""},
		"inspect":   {true, true, false, "id", errors.New("boom"), false, "inspect osd.0"},
		"not owned": {false, true, false, "id", nil, true, "no longer owned"},
		"other":     {true, true, false, "id2", nil, true, "another container generation"},
		"stopped":   {true, false, false, "id", nil, true, "is stopped"},
		"paused":    {true, true, true, "id", nil, false, "is paused"},
	} {
		changed, err := overridehook.CheckDebugGeneration(test.owned, test.identity, test.running, test.paused, test.err, "id", "osd.0")
		if changed != test.changed || (test.want == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), test.want)) {
			t.Errorf("%s: changed %v err %v", name, changed, err)
		}
	}
}

// fullRatiosControl answers the three commands of a full ratios session.
type fullRatiosControl struct {
	testcontainers.Container
	calls [][]string
}

const sessionFSID = "35e00c14-bc10-4357-aed3-107912b9bead"

func (f *fullRatiosControl) Exec(_ context.Context, argv []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	f.calls = append(f.calls, slices.Clone(argv))
	args := argv[3:]
	output := ""
	switch {
	case slices.Equal(args, []string{"fsid"}):
		output = sessionFSID + "\n"
	case slices.Equal(args, []string{"osd", "dump", "--format", "json"}):
		output = `{"fsid":"` + sessionFSID + `","epoch":4,"nearfull_ratio":0.85,"backfillfull_ratio":0.9,"full_ratio":0.95}`
	case len(args) == 3 && args[0] == "osd" && args[1] == "set-full-ratio":
	default:
		return 0, nil, errors.New("unexpected command")
	}
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	return 0, io.MultiReader(bytes.NewReader(header[:]), bytes.NewReader([]byte(output))), nil
}

// TestFullRatiosSessionThroughHooks calls every method of the full ratios
// session and the registry kind that tracks it.
func TestFullRatiosSessionThroughHooks(t *testing.T) {
	ctx := t.Context()
	bare := servicehook.TestCluster(nil, servicehook.TestConfig{StartupTimeout: time.Second})
	overridehook.WithTopologyNoContext(bare, func() {
		if _, err := overridehook.OpenFullRatios(ctx, bare); err == nil {
			t.Error("a session opened without a control container")
		}
	})
	control := &fullRatiosControl{}
	c := servicehook.TestCluster(control, servicehook.TestConfig{StartupTimeout: time.Second})
	state := servicehook.TestState(c)
	*state.Config = []byte("[global]\nfsid = " + sessionFSID + "\nmon_host = mon\n")
	*state.Keyring = []byte("confirmed")
	err := overridehook.WithTopology(ctx, c, func() error {
		token, err := overridehook.Reserve(ctx, c, overridehook.FullRatios, "")
		if err != nil {
			return err
		}
		if _, err := overridehook.Reserve(ctx, c, overridehook.FullRatios, ""); err == nil {
			t.Error("the full ratios kind was reserved twice")
		}
		if _, err := overridehook.Settle(c, token, nil); err != nil {
			return err
		}
		session, err := overridehook.OpenFullRatios(ctx, c)
		if err != nil {
			return err
		}
		if session.FSID() != sessionFSID {
			t.Errorf("session identity %q", session.FSID())
		}
		if err := session.CheckIdentity(ctx, sessionFSID); err != nil {
			t.Errorf("the captured identity was refused: %v", err)
		}
		if err := session.CheckIdentity(ctx, "8797c2db-bf63-40a1-990e-eb6658b50cf7"); err == nil {
			t.Error("another identity was accepted")
		}
		snapshot, err := session.Read(ctx)
		if err != nil || snapshot.Epoch != 4 || snapshot.Ratios != (ceph.FullRatios{NearFull: .85, BackfillFull: .9, Full: .95}) {
			t.Errorf("read: %+v %v", snapshot, err)
		}
		before := len(control.calls)
		for _, name := range []string{"", "nearfull ", "osd", "full-ratio", "../full"} {
			if err := session.SetRatio(ctx, name, .5); !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Errorf("the threshold %q was not refused: %v", name, err)
			}
		}
		if len(control.calls) != before {
			t.Error("a refused threshold name reached the native CLI")
		}
		if err := session.SetRatio(ctx, "full", .5); err != nil {
			t.Errorf("set: %v", err)
		}
		last := control.calls[len(control.calls)-1]
		if !slices.Equal(last[3:], []string{"osd", "set-full-ratio", "0.5"}) {
			t.Errorf("the native command is %v", last)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	servicehook.TestSetClosed(c, true)
	overridehook.WithTopologyNoContext(c, func() {
		if _, err := overridehook.OpenFullRatios(ctx, c); err == nil {
			t.Error("a session opened on a terminated cluster")
		}
	})
}

// TestConfigOperationsWithoutNativeCommands calls the pure operations of the
// configuration seam; the commands themselves run against a fake control
// container in the tests of package override.
func TestConfigOperationsWithoutNativeCommands(t *testing.T) {
	setting, err := overridehook.NormalizeConfigSetting(ceph.ConfigSetting{Section: "osd.0", Mask: "class:ssd/host:a", Name: "debug-ms", Value: "1"})
	if err != nil || setting.Name != "debug_ms" || setting.Mask != "host:a/class:ssd" {
		t.Fatalf("normalize: %+v %v", setting, err)
	}
	if got := overridehook.ConfigWho(setting); got != "osd.0/host:a/class:ssd" {
		t.Fatalf("who %q", got)
	}
	if got := overridehook.ConfigKey(setting); got != "osd.0/host:a/class:ssd/debug_ms" {
		t.Fatalf("key %q", got)
	}
	if _, err := overridehook.NormalizeConfigSetting(ceph.ConfigSetting{Section: "bad", Name: "debug_ms"}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("an invalid section was accepted: %v", err)
	}
	for mode, tc := range map[ceph.MessengerMode]struct {
		name    string
		allowed bool
	}{
		ceph.MessengerDefault:  {"ms_bind_msgr1", true},
		ceph.MessengerV2Secure: {"ms_service_mode", false},
		ceph.MessengerV2Only:   {"ms_service_mode", true},
	} {
		c := servicehook.TestCluster(nil, servicehook.TestConfig{MessengerMode: mode})
		err := overridehook.CheckConfigAllowed(c, ceph.ConfigSetting{Section: "global", Name: tc.name})
		if tc.allowed != (err == nil) || (err != nil && !errors.Is(err, ceph.ErrInvalidRequest)) {
			t.Errorf("mode %v option %s: %v", mode, tc.name, err)
		}
	}
	v2only := servicehook.TestCluster(nil, servicehook.TestConfig{MessengerMode: ceph.MessengerV2Only})
	if err := overridehook.CheckConfigAllowed(v2only, ceph.ConfigSetting{Section: "global", Name: "ms_bind_msgr2"}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Errorf("a fixed protocol option was allowed: %v", err)
	}
	settings, err := overridehook.MessengerPolicySettings(v2only, ceph.MessengerPolicy{OSDs: []ceph.MessengerConnectionMode{ceph.MessengerCRC}})
	if err != nil || len(settings) != 1 || settings[0].Name != "ms_service_mode" {
		t.Fatalf("policy settings: %+v %v", settings, err)
	}
	secure := servicehook.TestCluster(nil, servicehook.TestConfig{MessengerMode: ceph.MessengerV2Secure})
	for name, tc := range map[string]struct {
		c      *ceph.Container
		policy ceph.MessengerPolicy
	}{
		"empty":        {v2only, ceph.MessengerPolicy{}},
		"unknown mode": {v2only, ceph.MessengerPolicy{OSDs: []ceph.MessengerConnectionMode{"plain"}}},
		"secure":       {secure, ceph.MessengerPolicy{OSDs: []ceph.MessengerConnectionMode{ceph.MessengerCRC}}},
	} {
		if _, err := overridehook.MessengerPolicySettings(tc.c, tc.policy); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s policy: %v", name, err)
		}
	}
}
