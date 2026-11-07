package consumerevidence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
)

// MgrMaps and MON sessions of a native transition on quay.io/ceph/ceph:v20.2.4
// (the samples of internal/integration/manager_transition_fixture_test.go):
// mgr.a (GID 4161) was active and mgr.b (4233) its standby; after mgr.a's
// process stopped, mgr.b took over, and the restarted mgr.a registered again
// as 14263. mon.a held mgr.b's beacon and the module clients of the active
// mgr.a, whose global IDs are not its GID.
const (
	baselineDump = `{"epoch": 4, "active_gid": 4161, "active_name": "a", "available": true, "standbys": [{"gid": 4233, "name": "b"}]}`
	promotedDump = `{"epoch": 6, "active_gid": 4233, "active_name": "b", "available": true, "standbys": []}`
	restoredDump = `{"epoch": 7, "active_gid": 4233, "active_name": "b", "available": true, "standbys": [{"gid": 14263, "name": "a"}]}`
	monASessions = `[{"name": "mon.0", "entity_name": "", "con_type": "mon", "global_id": 0}, {"name": "client.?", "entity_name": "mgr.a", "con_type": "client", "global_id": 4179}, {"name": "client.?", "entity_name": "mgr.a", "con_type": "client", "global_id": 4185}, {"name": "mgr.?", "entity_name": "mgr.b", "con_type": "mgr", "global_id": 4233}]`
	monBSessions = `[{"name": "mon.0", "entity_name": "", "con_type": "mon", "global_id": 0}, {"name": "mgr.4161", "entity_name": "mgr.a", "con_type": "mgr", "global_id": 4161}]`
	monBRestored = `[{"name": "mon.0", "entity_name": "", "con_type": "mon", "global_id": 0}, {"name": "mgr.?", "entity_name": "mgr.a", "con_type": "mgr", "global_id": 14263}]`
	monCSessions = `[{"name": "mon.0", "entity_name": "", "con_type": "mon", "global_id": 0}]`
)

func managerMap(t *testing.T, dump string) ManagerMap {
	t.Helper()
	observed, err := decodeManagerMap([]byte(dump))
	if err != nil {
		t.Fatal(err)
	}
	return observed
}

func sessions(t *testing.T, outputs ...string) []MonitorSessions {
	t.Helper()
	var all []MonitorSessions
	for i, output := range outputs {
		observed, err := decodeSessions(string(rune('a'+i)), []byte(output))
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, observed)
	}
	return all
}

func TestDecodeManagerMap(t *testing.T) {
	observed := managerMap(t, restoredDump)
	if observed.Epoch != 7 || observed.Active != (Registration{"b", 4233}) || !slices.Equal(observed.Standbys, []Registration{{"a", 14263}}) {
		t.Fatalf("decoded %+v", observed)
	}
	for name, test := range map[string]struct {
		dump, status string
	}{
		"empty":            {"", StatusMissing},
		"without epoch":    {`{"active_gid": 1, "active_name": "a", "available": true, "standbys": []}`, StatusMissing},
		"truncated":        {`{"epoch": 4, "active_gid"`, StatusParse},
		"active by halves": {`{"epoch": 4, "active_gid": 0, "active_name": "a", "available": true, "standbys": []}`, StatusParse},
		"oversized":        {strings.Repeat(" ", managerDumpLimit+1), StatusTruncated},
	} {
		_, err := decodeManagerMap([]byte(test.dump))
		if status := queryStatus(err); status != test.status {
			t.Errorf("%s: status %s (%v), want %s", name, status, err, test.status)
		}
	}
}

// Beacon sessions are matched by entity and GID; module clients of the same
// entity do not count.
func TestBeaconMonitors(t *testing.T) {
	monitors := sessions(t, monASessions, monBSessions, monCSessions)
	if got := beaconMonitors(monitors, Registration{"a", 4161}); !slices.Equal(got, []string{"b"}) {
		t.Fatalf("beacon of mgr.a on %v", got)
	}
	if got := beaconMonitors(monitors, Registration{"a", 4179}); len(got) != 0 {
		t.Fatalf("a module client counted as a beacon on %v", got)
	}
	if got := moduleGlobalIDs(monitors, "a"); !slices.Equal(got, []uint64{4179, 4185}) {
		t.Fatalf("module global IDs %v", got)
	}
	if _, err := decodeSessions("a", []byte(" ")); queryStatus(err) != StatusMissing {
		t.Fatalf("empty sessions output: %v", err)
	}
	if observed, err := decodeSessions("a", []byte("[]")); err != nil || len(observed.Beacons) != 0 {
		t.Fatalf("an empty session list is an answer: %+v %v", observed, err)
	}
}

func TestQueryStatus(t *testing.T) {
	for err, want := range map[error]string{
		nil:                      StatusComplete,
		context.DeadlineExceeded: StatusTimedOut,
		fmt.Errorf("%w: timed out true, truncated false", ceph.ErrNativeCommandIncomplete): StatusTimedOut,
		fmt.Errorf("%w: timed out false, truncated true", ceph.ErrNativeCommandIncomplete): StatusTruncated,
		errors.New(`no valid command found; 10 closest matches`):                           StatusUnsupported,
		errors.New("connection refused"):                                                   StatusFailed,
	} {
		if got := queryStatus(err); got != want {
			t.Errorf("%v: %s, want %s", err, got, want)
		}
	}
}

// transitionCase builds the frozen baseline and one complete sample.
func transitionCase(t *testing.T, before, after string, monitors []string, instances []Instance) (TransitionFrozen, TransitionSample) {
	t.Helper()
	frozen := TransitionFrozen{Map: managerMap(t, baselineDump), Instances: []Instance{
		{Name: "a", Query: Query{Status: StatusComplete}, ContainerID: "ca", StartedAt: "t0", Running: true},
		{Name: "b", Query: Query{Status: StatusComplete}, ContainerID: "cb", StartedAt: "t0", Running: true},
	}}
	return frozen, TransitionSample{Before: managerMap(t, before), Monitors: sessions(t, monitors...), Instances: instances, After: managerMap(t, after)}
}

func running(name, container, started string) Instance {
	return Instance{Name: name, Query: Query{Status: StatusComplete}, ContainerID: container, StartedAt: started, Running: true}
}

// The native maps of a restart of the active mgr.a: the promotion alone and
// the stale registration are pending, the restored map with the new beacon is
// accepted, and changes that the restart cannot undo are violated.
func TestClassifyTransitionSample(t *testing.T) {
	monitors := []string{"a", "b", "c"}
	restored := []Instance{running("a", "ca", "t1"), running("b", "cb", "t0")}
	for name, test := range map[string]struct {
		before, after string
		sessions      []string
		instances     []Instance
		state         string
	}{
		"restored":           {restoredDump, restoredDump, []string{monASessions, monBRestored, monCSessions}, restored, SampleAccepted},
		"promoted only":      {promotedDump, promotedDump, []string{monASessions, monBSessions, monCSessions}, restored, SamplePending},
		"stale registration": {baselineDump, baselineDump, []string{monASessions, monBSessions, monCSessions}, restored, SamplePending},
		"no new beacon yet":  {restoredDump, restoredDump, []string{monASessions, monBSessions, monCSessions}, restored, SamplePending},
		"map moved":          {promotedDump, restoredDump, []string{monASessions, monBRestored, monCSessions}, restored, SampleTransitional},
		"not restarted yet":  {restoredDump, restoredDump, []string{monASessions, monBRestored, monCSessions}, []Instance{running("a", "ca", "t0"), running("b", "cb", "t0")}, SamplePending},
		"replaced container": {restoredDump, restoredDump, []string{monASessions, monBRestored, monCSessions}, []Instance{running("a", "other", "t1"), running("b", "cb", "t0")}, SampleViolated},
		"standby restarted":  {restoredDump, restoredDump, []string{monASessions, monBRestored, monCSessions}, []Instance{running("a", "ca", "t1"), running("b", "cb", "t2")}, SampleViolated},
		"one MON missing":    {restoredDump, restoredDump, []string{monASessions, monBRestored}, restored, SampleIncomplete},
	} {
		frozen, sample := transitionCase(t, test.before, test.after, test.sessions, test.instances)
		if state, reason := ClassifyTransitionSample(frozen, monitors, "a", sample); state != test.state {
			t.Errorf("%s: %s (%s), want %s", name, state, reason, test.state)
		}
	}
	frozen, sample := transitionCase(t, restoredDump, restoredDump, []string{monASessions, monBRestored, monCSessions}, restored)
	sample.Monitors[2].Query = Query{Name: "sessions of mon.c", Status: StatusTimedOut}
	if state, _ := ClassifyTransitionSample(frozen, monitors, "a", sample); state != SampleIncomplete {
		t.Fatalf("a timed-out sessions read gave %s", state)
	}
	unowned := `{"epoch": 7, "active_gid": 4233, "active_name": "b", "available": true, "standbys": [{"gid": 9, "name": "x"}]}`
	frozen, sample = transitionCase(t, unowned, unowned, []string{monASessions, monBRestored, monCSessions}, restored)
	if state, _ := ClassifyTransitionSample(frozen, monitors, "a", sample); state != SampleViolated {
		t.Fatalf("an unowned standby gave %s", state)
	}
}

// The receipt keeps every distinct map in order and counts the epochs that
// no read saw.
func TestTransitionReceiptKeepsChanges(t *testing.T) {
	receipt := TransitionReceipt{Counts: map[string]int{}, Changes: []ManagerMap{managerMap(t, baselineDump)}}
	for _, pair := range [][2]string{{baselineDump, baselineDump}, {promotedDump, promotedDump}, {promotedDump, restoredDump}, {restoredDump, restoredDump}, {restoredDump, restoredDump}, {restoredDump, restoredDump}} {
		receipt.observe(TransitionSample{Before: managerMap(t, pair[0]), After: managerMap(t, pair[1]), State: SamplePending})
	}
	var epochs []uint64
	for _, change := range receipt.Changes {
		epochs = append(epochs, change.Epoch)
	}
	if !slices.Equal(epochs, []uint64{4, 6, 7}) || epochsNotObserved(receipt.Changes) != 1 {
		t.Fatalf("changes %v, %d epochs not observed", epochs, epochsNotObserved(receipt.Changes))
	}
	if len(receipt.Samples) != transitionRetained || receipt.Counts[SamplePending] != 6 {
		t.Fatalf("%d samples kept, counts %v", len(receipt.Samples), receipt.Counts)
	}
}

func TestScanRefusals(t *testing.T) {
	log := strings.Join([]string{
		"2026-10-14T23:52:03.512+0000 7ff7ec39b640  0 cephx server mgr.b:  attempt to reclaim global_id 15059 using bad ticket",
		"2026-10-14T23:52:03.513+0000 7ff7ec39b640  0 cephx server client.admin:  attempt to reclaim global_id 22 without presenting ticket\r",
		"2026-10-14T23:52:03.514+0000 7ff7ec39b640  0 log_channel(audit) log [INF] : cmd=[cephx server mgr.b:  attempt to reclaim global_id 7 using bad ticket]",
		"2026-10-14T23:52:03.515+0000 7ff7ec39b640  1 mon.a@0(leader) e3 handle_auth_request failed to assign global_id",
	}, "\n") + "\n"
	found, err := scanRefusals("a", strings.NewReader(log), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	want := []Refusal{{Monitor: "a", Entity: "mgr.b", GlobalID: 15059}, {Monitor: "a", Entity: "client.admin", GlobalID: 22}}
	if len(found) != len(want) {
		t.Fatalf("found %+v", found)
	}
	for i := range want {
		if found[i].Monitor != want[i].Monitor || found[i].Entity != want[i].Entity || found[i].GlobalID != want[i].GlobalID {
			t.Fatalf("refusal %d %+v, want %+v", i, found[i], want[i])
		}
	}
	if _, err := scanRefusals("a", strings.NewReader(log), 16); queryStatus(err) != StatusTruncated {
		t.Fatalf("a log over its bound: %v", err)
	}
}

// gateCase is a complete gate observation of the restored native maps.
func gateCase(t *testing.T) GateReceipt {
	return GateReceipt{Map: managerMap(t, restoredDump), Monitors: sessions(t, monASessions, monBRestored, monCSessions),
		Control: &Control{Target: "mgr.b", Command: "tell version", Complete: true}, Queries: []Query{{Name: "log of mon.a", Status: StatusComplete}}}
}

func TestEvaluateGate(t *testing.T) {
	owned := []string{"a", "b"}
	healthy := EvaluateGate(gateCase(t), owned)
	if !healthy.Complete || healthy.FixtureError != "" || healthy.GenerationEquality != GenerationUnavailable || len(healthy.Daemons) != 2 {
		t.Fatalf("healthy gate %+v", healthy)
	}
	for name, test := range map[string]struct {
		change func(*GateReceipt)
		stale  string
	}{
		"registered GID refused": {func(r *GateReceipt) { r.Refusals = []Refusal{{Entity: "mgr.a", GlobalID: 14263}} }, SlotRefused},
		"module client refused":  {func(r *GateReceipt) { r.Refusals = []Refusal{{Entity: "mgr.a", GlobalID: 4179}} }, ""},
		"stopped process's GID":  {func(r *GateReceipt) { r.Refusals = []Refusal{{Entity: "mgr.a", GlobalID: 4161}} }, ""},
		"other entity, same ID":  {func(r *GateReceipt) { r.Refusals = []Refusal{{Entity: "client.x", GlobalID: 14263}} }, ""},
		"absent":                 {func(r *GateReceipt) { r.Map = managerMap(t, promotedDump) }, SlotAbsent},
		"no beacon":              {func(r *GateReceipt) { r.Monitors = sessions(t, monASessions, monBSessions, monCSessions) }, SlotNoBeacon},
	} {
		receipt := gateCase(t)
		test.change(&receipt)
		receipt = EvaluateGate(receipt, owned)
		var slots []string
		for _, daemon := range receipt.Daemons {
			if daemon.Stale {
				slots = append(slots, daemon.Slot)
			}
		}
		if test.stale == "" && (len(slots) != 0 || receipt.FixtureError != "") || test.stale != "" && (!slices.Equal(slots, []string{test.stale}) || receipt.FixtureError == "") {
			t.Errorf("%s: stale %v, fixture error %q", name, slots, receipt.FixtureError)
		}
	}
	for name, mut := range map[string]func(*GateReceipt){
		"unavailable MGR":            func(r *GateReceipt) { r.Map.Available = false },
		"absent active registration": func(r *GateReceipt) { r.Map.Active = Registration{} },
		"missing tell":               func(r *GateReceipt) { r.Control = nil },
		"tell of the standby":        func(r *GateReceipt) { r.Control.Target = "mgr.a" },
	} {
		r := gateCase(t)
		mut(&r)
		if got := EvaluateGate(r, owned); got.Complete || got.FixtureError == "" {
			t.Errorf("%s accepted as product evidence: %+v", name, got)
		}
	}
	refused := gateCase(t)
	refused.Control.ExitCode = 13
	if receipt := EvaluateGate(refused, owned); receipt.FixtureError == "" || !strings.Contains(receipt.FixtureError, "tell version") {
		t.Fatalf("a refused native control gave %q", receipt.FixtureError)
	}
	incomplete := gateCase(t)
	incomplete.Queries[0].Status = StatusTimedOut
	if receipt := EvaluateGate(incomplete, owned); receipt.Complete || receipt.Daemons != nil || receipt.FixtureError != "" {
		t.Fatalf("an incomplete gate was judged: %+v", receipt)
	}
}

// authCase is a complete bundle whose target kept its registration.
func authCase() AuthBundle {
	manager := AuthManager{Name: "a", GID: 4161, Active: true, ContainerID: "ca", StartedAt: "t0"}
	return AuthBundle{
		Expected:           Expected{FSID: "f", Monitors: []string{"b", "a"}, Manager: "a", GID: 4161},
		Rejection:          Rejection{Target: "mgr.a", TargetGID: 4161, Result: -13},
		Monitors:           []MonitorStatus{{Name: "a", FSID: "f"}, {Name: "b", FSID: "f"}},
		Managers:           []AuthManager{manager},
		ManagersAfter:      []AuthManager{manager},
		Controls:           []Control{{Target: "mgr.a", ExitCode: 0, Complete: true}},
		CollectionComplete: true,
		GenerationEquality: GenerationUnavailable,
	}
}

func TestClassifyAuthBundle(t *testing.T) {
	verdict := ClassifyAuthBundle(authCase())
	if !verdict.Complete || verdict.Target != TargetRegistered || !verdict.ControlsSucceeded || verdict.Result != -13 || verdict.GenerationEquality != GenerationUnavailable {
		t.Fatalf("verdict %+v", verdict)
	}
	for name, test := range map[string]struct {
		change   func(*AuthBundle)
		target   string
		complete bool
	}{
		"restarted": {func(b *AuthBundle) {
			b.ManagersAfter = []AuthManager{{Name: "a", GID: 4161, ContainerID: "ca", StartedAt: "t1"}}
		}, TargetChanged, true},
		"new GID": {func(b *AuthBundle) {
			b.ManagersAfter = []AuthManager{{Name: "a", GID: 9, ContainerID: "ca", StartedAt: "t0"}}
		}, TargetChanged, true},
		"absent":            {func(b *AuthBundle) { b.Managers = []AuthManager{{Name: "b", GID: 7}} }, TargetAbsent, true},
		"not observed":      {func(b *AuthBundle) { b.ManagersAfter = nil; b.CollectionComplete = false }, TargetUnavailable, false},
		"other FSID":        {func(b *AuthBundle) { b.Monitors[1].FSID = "g" }, TargetRegistered, false},
		"a MON missing":     {func(b *AuthBundle) { b.Monitors = b.Monitors[:1] }, TargetRegistered, false},
		"equality claimed":  {func(b *AuthBundle) { b.GenerationEquality = "equal" }, TargetRegistered, false},
		"other target":      {func(b *AuthBundle) { b.Rejection.TargetGID = 1 }, TargetRegistered, false},
		"control truncated": {func(b *AuthBundle) { b.Controls[0].Complete = false }, TargetRegistered, true},
	} {
		bundle := authCase()
		test.change(&bundle)
		verdict := ClassifyAuthBundle(bundle)
		if verdict.Target != test.target || verdict.Complete != test.complete || verdict.GenerationEquality != GenerationUnavailable {
			t.Errorf("%s: verdict %+v", name, verdict)
		}
	}
	if verdict := ClassifyAuthBundle(AuthBundle{Rejection: Rejection{Result: -13}}); verdict.Result != -13 || verdict.ControlsSucceeded {
		t.Fatalf("an empty bundle kept %+v", verdict)
	}
}

func TestParseMonitorStatus(t *testing.T) {
	status, err := parseMonitorStatus([]byte(`{"name": "a", "rank": 0, "state": "leader", "election_epoch": 8, "quorum": [0, 1, 2],
		"monmap": {"epoch": 3, "fsid": "f", "auth_epoch": 2}}`))
	if err != nil || status.MonmapEpoch != 3 || status.AuthEpoch == nil || *status.AuthEpoch != 2 {
		t.Fatalf("status %+v: %v", status, err)
	}
	if status, err := parseMonitorStatus([]byte(`{"name": "a", "monmap": {"epoch": 3, "fsid": "f"}}`)); err != nil || status.AuthEpoch != nil {
		t.Fatalf("a MON without auth_epoch: %+v %v", status, err)
	}
	for _, data := range []string{"", "{", `{"name": "a", "monmap": {"epoch": 3}}`} {
		if _, err := parseMonitorStatus([]byte(data)); err == nil {
			t.Errorf("%q was an observation", data)
		}
	}
}

// A bundle's JSON round-trips through its schema; one with another field
// fails the secrecy check before any redaction.
func TestAuthBundleSchema(t *testing.T) {
	data, err := json.Marshal(authCase())
	if err != nil {
		t.Fatal(err)
	}
	extended := strings.Replace(string(data), `"schema"`, `"debug_auth":"x","schema"`, 1)
	if err := CheckAuthBundleSecrecy(t.Context(), nil, []byte(extended)); err == nil || !strings.Contains(err.Error(), "beyond its schema") {
		t.Fatalf("an extended bundle: %v", err)
	}
}

// Finish collects and saves before Terminate only with a failure signal, and
// its error keeps that signal whatever the later steps return.
func TestFinish(t *testing.T) {
	signal := errors.New("driver failure signal")
	bounds := FinishBounds{Collect: 30 * time.Second, Save: 30 * time.Second, Terminate: 30 * time.Second}
	for name, test := range map[string]struct {
		signal                  error
		collectErr, terminateEr error
		steps                   []string
		failed                  bool
	}{
		"passing":              {nil, nil, nil, []string{"terminate"}, false},
		"passing, terminate":   {nil, nil, errors.New("gone"), []string{"terminate"}, true},
		"failed":               {signal, nil, nil, []string{"collect", "save", "terminate"}, true},
		"failed, partial":      {signal, errors.New("one query failed"), nil, []string{"collect", "save", "terminate"}, true},
		"failed, no terminate": {signal, nil, errors.New("gone"), []string{"collect", "save", "terminate"}, true},
	} {
		var steps []string
		err := finish(test.signal, func(ctx context.Context) (*diagnostics.Report, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Errorf("%s: collection without a deadline", name)
			}
			steps = append(steps, "collect")
			return &diagnostics.Report{}, test.collectErr
		}, func(context.Context, *diagnostics.Report) error {
			steps = append(steps, "save")
			return nil
		}, func(ctx context.Context) error {
			steps = append(steps, "terminate")
			return test.terminateEr
		}, bounds)
		if !slices.Equal(steps, test.steps) || (err != nil) != test.failed {
			t.Errorf("%s: steps %v, error %v", name, steps, err)
		}
		if test.signal != nil && !errors.Is(err, test.signal) {
			t.Errorf("%s: the failure signal was lost: %v", name, err)
		}
		if test.collectErr != nil && !errors.Is(err, test.collectErr) || test.terminateEr != nil && !errors.Is(err, test.terminateEr) {
			t.Errorf("%s: a step's error was lost: %v", name, err)
		}
	}
}

// A log with many refusals keeps one line per refused session, so repeated
// refusals of earlier MonClients never push out the current GID's refusal;
// more distinct sessions than the gate keeps is an error, never a short list.
func TestScanRefusalsKeepsEveryRefusedSession(t *testing.T) {
	line := func(entity string, id int) string {
		return fmt.Sprintf("2026-10-14T23:52:03.512+0000 7ff7ec39b640  0 cephx server %s:  attempt to reclaim global_id %d using bad ticket", entity, id)
	}
	var repeated []string
	for range 2 * maxRefusedSessions {
		repeated = append(repeated, line("mgr.a", 4179))
	}
	repeated = append(repeated, line("mgr.a", 14263))
	found, err := scanRefusals("a", strings.NewReader(strings.Join(repeated, "\n")+"\n"), 1<<20)
	if err != nil || len(found) != 2 || found[1].GlobalID != 14263 {
		t.Fatalf("repeated refusals then the current GID: %+v, %v", found, err)
	}
	var distinct []string
	for id := range maxRefusedSessions {
		distinct = append(distinct, line("client.admin", 100+id))
	}
	distinct = append(distinct, line("mgr.a", 14263))
	if found, err := scanRefusals("a", strings.NewReader(strings.Join(distinct, "\n")+"\n"), 1<<20); err == nil || queryStatus(err) == StatusComplete {
		t.Fatalf("%d distinct refused sessions returned %d without an error", maxRefusedSessions+1, len(found))
	}
}

// A failed MgrMap read before the sessions stays in the queries, so a gate
// whose bracket was never established is incomplete even when the sessions
// and the later MgrMap read succeed.
func TestSampleOneMapKeepsAFailedFirstRead(t *testing.T) {
	failed := ManagerMap{Query: Query{Name: "mgr dump", Status: StatusFailed, Error: "context deadline exceeded"}, Standbys: []Registration{}}
	complete := managerMap(t, restoredDump)
	reads := []ManagerMap{failed, complete}
	read := func() ManagerMap {
		next := reads[0]
		reads = reads[1:]
		return next
	}
	receipt := gateCase(t)
	monitors := receipt.Monitors
	receipt.Map, receipt.Monitors, receipt.Queries = sampleOneMap(read, func() []MonitorSessions { return monitors })
	if len(reads) != 0 || len(receipt.Queries) != 1 || receipt.Queries[0].Complete() {
		t.Fatalf("failed first read: %d reads left, queries %+v", len(reads), receipt.Queries)
	}
	if receipt = EvaluateGate(receipt, []string{"a", "b"}); receipt.Complete || receipt.Daemons != nil {
		t.Fatalf("a gate without its first MgrMap read is complete: %+v", receipt)
	}
	reads = []ManagerMap{complete, complete}
	if _, _, queries := sampleOneMap(read, func() []MonitorSessions { return monitors }); len(queries) != 0 {
		t.Fatalf("a matching bracket left queries %+v", queries)
	}
}
