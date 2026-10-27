package overridehook_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"slices"
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
	*state.Closed = true
	overridehook.WithTopologyNoContext(c, func() {
		if _, err := overridehook.OpenFullRatios(ctx, c); err == nil {
			t.Error("a session opened on a terminated cluster")
		}
	})
}
