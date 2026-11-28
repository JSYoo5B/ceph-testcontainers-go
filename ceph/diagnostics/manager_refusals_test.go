package diagnostics

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/internal/evidence"
)

const (
	mgrRefusedLine  = "monclient(hunting): handle_auth_bad_method failed to auth with my available methods: (13) Permission denied"
	reclaimBadLine  = "cephx server mgr.a:  attempt to reclaim global_id 14263 using bad ticket"
	unexpectedKey   = "cephx server mgr.a:  unexpected key: req.key=0123456789abcdef expected_key=fedcba9876543210"
	otherEntityLine = "cephx server mgr.b:  attempt to reclaim global_id 4179 without presenting ticket"
)

// refusalSnapshot is a cluster with the MGR m (mgr.a) and the MONs n (mon.b)
// and o (mon.a).
func refusalSnapshot() evidence.Snapshot[Target] {
	return evidence.Snapshot[Target]{Containers: []Target{
		{Role: "mgr", Name: "a", Container: &diagnosticsTestContainer{id: "m"}},
		{Role: "mon", Name: "b", Container: &diagnosticsTestContainer{id: "n"}},
		{Role: "mon", Name: "a", Container: &diagnosticsTestContainer{id: "o"}},
		{Role: "control", Name: "cli", Container: &diagnosticsTestContainer{id: "c"}},
	}}
}

func refusalManager() *fakeLogContainer {
	return &fakeLogContainer{started: []time.Duration{0}, running: true, entries: []fakeLogEntry{
		startEntry(time.Second, "ceph-mgr", 7),
		cephEntry(2*time.Second, "-1", mgrRefusedLine+" (an earlier process)"),
		cephEntry(3*time.Second, "-1", mgrRefusedLine),
		startEntry(10*time.Second, "ceph-mgr", 7),
		cephEntry(11*time.Second, "-1", "monclient(hunting): handle_auth_bad_method server allowed_methods [2] but i only support [2]"),
		cephEntry(12*time.Second, "-1", mgrRefusedLine),
		cephEntry(13*time.Second, " 0", "log_channel(cluster) log [DBG] : quoted "+mgrRefusedLine),
	}}
}

func TestManagerRefusalsAttributeTheLatestProcess(t *testing.T) {
	docker := newProcessLogDocker(map[string]*fakeLogContainer{
		"m": refusalManager(),
		"n": {started: []time.Duration{0}, running: true, entries: []fakeLogEntry{
			// Before the MGR's latest process started.
			cephEntry(5*time.Second, " 0", "cephx server mgr.a:  attempt to reclaim global_id 4161 using bad ticket"),
			cephEntry(11*time.Second, " 0", reclaimBadLine),
			cephEntry(11*time.Second, " 0", otherEntityLine),
			cephEntry(12*time.Second, " 0", unexpectedKey),
			cephEntry(13*time.Second, " 0", "log_channel(audit) log [DBG] : quoted "+reclaimBadLine),
		}},
		"o": {started: []time.Duration{0}, running: true},
	})
	result, err := readManagerRefusals(t.Context(), docker, refusalSnapshot(), logMember("mgr", "a", "m"), LogConfig{MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != LogComplete || !result.Start.Time.Equal(processLogEpoch.Add(10*time.Second)) ||
		len(result.Refusals) != 1 || !strings.HasSuffix(result.Refusals[0].Text, mgrRefusedLine) {
		t.Fatalf("manager = %+v, start %+v, refusals %+v", result.LogSource, result.Start, result.Refusals)
	}
	if want := result.Start.Time.Format(time.RFC3339Nano); docker.since["n"] != want || docker.since["o"] != want {
		t.Fatalf("MON windows start at %q and %q, want the MGR process start %q", docker.since["n"], docker.since["o"], want)
	}
	if len(result.Monitors) != 2 || result.Monitors[0].Name != "a" || result.Monitors[1].Name != "b" {
		t.Fatalf("monitors = %+v", result.Monitors)
	}
	empty, refused := result.Monitors[0], result.Monitors[1]
	if empty.Status != LogComplete || empty.Refusals == nil || len(empty.Refusals) != 0 || !empty.Since.Equal(result.Start.Time) {
		t.Fatalf("a MON without refusals = %+v", empty)
	}
	if refused.Status != LogComplete || len(refused.Refusals) != 2 {
		t.Fatalf("refusals = %+v", refused)
	}
	reclaim, key := refused.Refusals[0], refused.Refusals[1]
	if reclaim.Reason != "using bad ticket" || reclaim.GlobalID != 14263 || !strings.HasSuffix(reclaim.Line.Text, reclaimBadLine) ||
		!reclaim.Line.Time.Equal(processLogEpoch.Add(11*time.Second)) {
		t.Fatalf("reclaim refusal = %+v", reclaim)
	}
	if key.Reason != "unexpected key" || key.GlobalID != 0 || strings.Contains(key.Line.Text, "0123456789abcdef") {
		t.Fatalf("unexpected key refusal = %+v", key)
	}
}

func TestManagerRefusalsSourceStatus(t *testing.T) {
	monitor := func(entries ...fakeLogEntry) *fakeLogContainer {
		return &fakeLogContainer{started: []time.Duration{0}, running: true, entries: entries}
	}
	many := []fakeLogEntry{cephEntry(11*time.Second, " 0", strings.Repeat("x", 1000)), cephEntry(12*time.Second, " 0", reclaimBadLine)}
	for name, test := range map[string]struct {
		manager       *fakeLogContainer
		monitor       *fakeLogContainer
		max           int
		managerStatus LogStatus
		monitorStatus LogStatus
		refusals      int
	}{
		"complete":            {refusalManager(), monitor(many...), 1 << 20, LogComplete, LogComplete, 1},
		"MON window too long": {refusalManager(), monitor(many...), 800, LogComplete, LogTruncated, 0},
		"rotating MON log": {refusalManager(), &fakeLogContainer{started: []time.Duration{0}, driver: "json-file",
			options: map[string]string{"max-size": "10m"}, entries: many}, 1 << 20, LogComplete, LogStartNotFound, 1},
		"local MON log":           {refusalManager(), &fakeLogContainer{started: []time.Duration{0}, driver: "local", entries: many}, 1 << 20, LogComplete, LogStartNotFound, 1},
		"unbounded json-file MON": {refusalManager(), &fakeLogContainer{started: []time.Duration{0}, options: map[string]string{"max-size": "-1"}, entries: many}, 1 << 20, LogComplete, LogComplete, 1},
		"no MGR process start": {&fakeLogContainer{started: []time.Duration{0}, entries: []fakeLogEntry{cephEntry(12*time.Second, "-1", mgrRefusedLine)}},
			monitor(many...), 1 << 20, LogStartNotFound, LogStartNotFound, 1},
		"MGR restarted": {&fakeLogContainer{started: []time.Duration{0, 20 * time.Second}, entries: refusalManager().entries},
			monitor(many...), 1 << 20, LogChanged, LogComplete, 1},
	} {
		t.Run(name, func(t *testing.T) {
			docker := newProcessLogDocker(map[string]*fakeLogContainer{"m": test.manager, "n": test.monitor, "o": monitor()})
			result, err := readManagerRefusals(t.Context(), docker, refusalSnapshot(), logMember("mgr", "a", "m"), LogConfig{MaxBytes: test.max})
			if err != nil {
				t.Fatal(err)
			}
			got := result.Monitors[1]
			if result.Status != test.managerStatus || got.Name != "b" || got.Status != test.monitorStatus || len(got.Refusals) != test.refusals {
				t.Fatalf("manager %+v, monitor %+v", result.LogSource, got)
			}
			if test.managerStatus == LogStartNotFound && (len(result.Refusals) != 1 || result.Start.Text != "" || !got.Since.Equal(result.ContainerStartedAt)) {
				t.Fatalf("without a process start the windows start at the MGR container start: %+v", result)
			}
		})
	}
}

func TestManagerRefusalsFailOnAnUnreadMonitor(t *testing.T) {
	boom := errors.New("log driver none")
	docker := newProcessLogDocker(map[string]*fakeLogContainer{"m": refusalManager(), "n": {started: []time.Duration{0}, logErr: boom}, "o": {started: []time.Duration{0}}})
	if result, err := readManagerRefusals(t.Context(), docker, refusalSnapshot(), logMember("mgr", "a", "m"), LogConfig{MaxBytes: 1 << 20}); !errors.Is(err, boom) || result != nil {
		t.Fatalf("result = %+v, %v (an unread MON is no observed absence)", result, err)
	}
}

func TestManagerRefusalsRefuseANilManager(t *testing.T) {
	if _, err := CollectManagerRefusals(t.Context(), nil, nil, LogConfig{}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("nil manager = %v", err)
	}
	if _, err := CollectManagerRefusals(t.Context(), nil, &ceph.ManagerContainer{}, LogConfig{MaxBytes: -1}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("invalid limits = %v", err)
	}
}

func TestMonitorRefusal(t *testing.T) {
	stamp := "2026-11-27T04:00:00.000+0000 7f1b8c0a1640  0 "
	for text, want := range map[string]MonitorRefusal{
		stamp + "cephx server mgr.a:  attempt to reclaim global_id 14263 without presenting ticket": {Reason: "without presenting ticket", GlobalID: 14263},
		stamp + "cephx server mgr.a:  attempt to reclaim global_id 14263 using mismatching ticket":  {Reason: "using mismatching ticket", GlobalID: 14263},
		stamp + reclaimBadLine: {Reason: "using bad ticket", GlobalID: 14263},
		stamp + unexpectedKey:  {Reason: "unexpected key"},
	} {
		if got, ok := monitorRefusal(text, "mgr.a"); !ok || got != want {
			t.Errorf("%s = %+v, %t", text, got, ok)
		}
	}
	for _, text := range []string{
		stamp + otherEntityLine,
		stamp + "cephx server mgr.ab:  attempt to reclaim global_id 1 using bad ticket",
		stamp + "cephx server mgr.a:  attempt to reclaim global_id 0 using bad ticket",
		stamp + "cephx server mgr.a:  attempt to reclaim global_id 14263 using a new ticket",
		stamp + "log_channel(audit) log [DBG] : " + reclaimBadLine,
		reclaimBadLine,
	} {
		if got, ok := monitorRefusal(text, "mgr.a"); ok {
			t.Errorf("%s = %+v", text, got)
		}
	}
}
