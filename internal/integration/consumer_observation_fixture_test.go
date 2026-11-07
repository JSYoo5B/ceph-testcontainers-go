//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"regexp"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/consumerevidence"
)

func TestConsumerRunningInspectionControls(t *testing.T) {
	base := diagnostics.Artifact{Kind: "container-inspect", Role: "mgr", Name: "a", ContainerID: "ctr", Data: `{"id":"ctr","state":{"running":true}}`}
	for _, tc := range []struct {
		name   string
		change func(*diagnostics.Report)
		valid  bool
	}{
		{"running", func(*diagnostics.Report) {}, true},
		{"closed", func(r *diagnostics.Report) { r.Closed = true }, false},
		{"inventory only", func(r *diagnostics.Report) { r.Artifacts[0].Kind = "inventory" }, false},
		{"errored", func(r *diagnostics.Report) { r.Artifacts[0].Error = "inspect failed" }, false},
		{"stopped", func(r *diagnostics.Report) { r.Artifacts[0].Data = `{"id":"ctr","state":{"running":false}}` }, false},
		{"wrong container", func(r *diagnostics.Report) { r.Artifacts[0].Data = `{"id":"other","state":{"running":true}}` }, false},
	} {
		r := diagnostics.Report{Artifacts: []diagnostics.Artifact{base}}
		tc.change(&r)
		if got := consumerRunningInspection(r, "mgr", "a", "ctr"); (got == nil) != tc.valid {
			t.Errorf("%s: %v", tc.name, got)
		}
	}
}

func TestConsumerActiveControlNeedsAnAnsweredTell(t *testing.T) {
	r := consumerevidence.GateReceipt{Map: consumerevidence.ManagerMap{Available: true, Active: consumerevidence.Registration{Name: "a", GID: 7}}}
	if consumerActiveControl(r) {
		t.Fatal("accepted absent control")
	}
	r.Control = &consumerevidence.Control{Target: "mgr.a", Command: "tell version", Complete: true}
	if !consumerActiveControl(r) {
		t.Fatal("refused successful control")
	}
	r.Control.ExitCode = 13
	if consumerActiveControl(r) {
		t.Fatal("accepted refused native tell")
	}
}

func TestArchivedIntervalAndTailControls(t *testing.T) {
	// RFC3339Nano places the shorter fraction later lexically.
	a, err := time.Parse(time.RFC3339Nano, "2026-01-02T03:04:05.1Z")
	if err != nil {
		t.Fatal(err)
	}
	b := a.Add(10 * time.Millisecond)
	if !archivedIntervalOrdered(a, b, a, a) || archivedIntervalOrdered(b, a, a, b) || archivedIntervalOrdered(a, b, b, a) || archivedIntervalOrdered(time.Time{}, b, a, b) {
		t.Fatal("archive ordering did not enforce the two timestamp relations")
	}
	pattern := regexp.MustCompile("old interval")
	for _, tc := range []struct {
		a     diagnostics.Artifact
		valid bool
	}{
		{diagnostics.Artifact{Data: "new observation"}, true},
		{diagnostics.Artifact{}, false},
		{diagnostics.Artifact{Data: "new observation", Error: "stream interrupted"}, false},
		{diagnostics.Artifact{Data: "old interval"}, false},
	} {
		if osdTailPastInterval(tc.a, pattern) != tc.valid {
			t.Errorf("tail %+v, want %t", tc.a, tc.valid)
		}
	}
}
