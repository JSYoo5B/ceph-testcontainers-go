package finalize

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/diagnostics"
)

// staticReceipter is a foreign DebugLevelReceipter that returns a fixed receipt.
type staticReceipter struct{ receipt ceph.DebugLevelReceipt }

func (s *staticReceipter) Receipt() ceph.DebugLevelReceipt { return s.receipt }

// valueReceipter has a value-receiver Receipt, so a nil *valueReceipter in the
// interface panics in Go's generated wrapper when Receipt is called.
type valueReceipter struct{ receipt ceph.DebugLevelReceipt }

func (v valueReceipter) Receipt() ceph.DebugLevelReceipt { return v.receipt }

// panickyReceipter has a pointer-receiver Receipt that is NOT nil-safe.
type panickyReceipter struct{ receipt ceph.DebugLevelReceipt }

func (p *panickyReceipter) Receipt() ceph.DebugLevelReceipt { return p.receipt }

type mapReceipter map[string]ceph.DebugLevelReceipt

func (m mapReceipter) Receipt() ceph.DebugLevelReceipt { return m["x"] }

type funcReceipter func() ceph.DebugLevelReceipt

func (f funcReceipter) Receipt() ceph.DebugLevelReceipt { return f() }

func TestFinalizationDebugReceipter(t *testing.T) {
	absent := func(t *testing.T, name string, handle DebugLevelReceipter) {
		t.Helper()
		fx := newFinalizationFixture(t, Config{Archive: true})
		fx.TrackDebugLevel(handle)
		fx.tb.Errorf("workload failed")
		fx.run()
		windows := readManifest(t, fx.archives(t)[0]).DebugWindows
		if len(windows) != 1 {
			t.Fatalf("%s: %d windows", name, len(windows))
		}
		for _, snap := range []*finalizationDebugSnapshot{windows[0].Archive, windows[0].Final} {
			if snap == nil || snap.Present || snap.Receipt != nil {
				t.Fatalf("%s: snapshot %+v is not absent", name, snap)
			}
		}
	}
	t.Run("nil interface", func(t *testing.T) { absent(t, "nil interface", nil) })
	t.Run("typed nil", func(t *testing.T) {
		var h *fakeDebugWindow
		var r DebugLevelReceipter = h
		if isNilInterface(r) {
			t.Fatal("a typed nil must be a non-nil interface for this control")
		}
		absent(t, "typed nil", r)
	})
	t.Run("foreign typed nils", func(t *testing.T) {
		absent(t, "value receiver", (*valueReceipter)(nil))
		absent(t, "pointer receiver not nil-safe", (*panickyReceipter)(nil))
		absent(t, "nil map", mapReceipter(nil))
		absent(t, "nil func", funcReceipter(nil))
	})
	t.Run("foreign non-nil receipter is still read", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true})
		fx.TrackDebugLevel(valueReceipter{receipt: ceph.DebugLevelReceipt{Daemon: "osd.1", State: ceph.DebugLevelNotApplied, Reason: "r"}})
		fx.tb.Errorf("workload failed")
		fx.run()
		snap := readManifest(t, fx.archives(t)[0]).DebugWindows[0].Archive
		if snap == nil || !snap.Present || snap.Receipt.Reason != "r" {
			t.Fatalf("snapshot %+v", snap)
		}
	})
	t.Run("zero-value handle", func(t *testing.T) { absent(t, "zero-value handle", &fakeDebugWindow{}) })
	t.Run("zero receipt from a foreign receipter", func(t *testing.T) {
		absent(t, "foreign", &staticReceipter{})
	})
	t.Run("not-applied receipt", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true, Diagnostics: diagnostics.Config{RedactValues: []string{"s3cr3t"}}})
		fx.TrackDebugLevel(&staticReceipter{receipt: ceph.DebugLevelReceipt{Daemon: "osd.0", Name: "debug_osd", Requested: "20",
			State: ceph.DebugLevelNotApplied, Reason: "refused s3cr3t " + strings.Repeat("x", 4*maxFinalizationDebugError)}})
		fx.tb.Errorf("workload failed")
		fx.run()
		data := manifestText(t, fx.archives(t)[0])
		if strings.Contains(data, "s3cr3t") {
			t.Fatal("the reason kept a secret")
		}
		window := readManifest(t, fx.archives(t)[0]).DebugWindows[0]
		for _, snap := range []*finalizationDebugSnapshot{window.Archive, window.Final} {
			if snap == nil || !snap.Present || snap.Receipt == nil {
				t.Fatalf("snapshot %+v is not present", snap)
			}
			r := snap.Receipt
			if r.State != ceph.DebugLevelNotApplied || r.Daemon != "osd.0" || r.Requested != "20" || r.Applied != "" || r.Restored != "" {
				t.Fatalf("receipt %+v", r)
			}
			if len(r.Reason) > maxFinalizationDebugError+len(" [truncated]") || !strings.HasSuffix(r.Reason, " [truncated]") || !strings.HasPrefix(r.Reason, "refused ") {
				t.Fatalf("reason of %d bytes is not redacted and bounded", len(r.Reason))
			}
		}
	})
	t.Run("real window has no reason", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true})
		h := trackedDebugWindow(fx, "20")
		restoreStep(fx, h, "restore")
		fx.tb.Errorf("workload failed")
		fx.run()
		if data := manifestText(t, fx.archives(t)[0]); strings.Contains(data, `"reason"`) {
			t.Fatal("a real window wrote a reason field")
		}
		window := readManifest(t, fx.archives(t)[0]).DebugWindows[0]
		if window.Archive == nil || !window.Archive.Present || window.Archive.Receipt.State != ceph.DebugLevelApplied ||
			window.Final == nil || window.Final.Receipt.State != ceph.DebugLevelRestored {
			t.Fatalf("window %+v", window)
		}
	})
}

func manifestText(t *testing.T, directory string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(directory, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// isNilInterface compares the interface itself, as TrackDebugLevel's first
// check does; a typed nil pointer stored in it is not nil.
func isNilInterface(r DebugLevelReceipter) bool { return r == nil }
