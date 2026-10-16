//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// MgrMaps and MON sessions saved from a TestMessengerClientProfileManagerTransition
// run on quay.io/ceph/ceph:v20.2.4. Only the fields that the decoders read are
// kept; the values are native. mgr.a (GID 4161) was active and mgr.b (4233)
// its standby; after mgr.a's process stopped, mgr.b took over, and the
// restarted mgr.a registered again as 14263.
const (
	mgrBaselineDumpSample = `{"epoch": 4, "active_gid": 4161, "active_name": "a", "available": true, "standbys": [{"gid": 4233, "name": "b"}]}`
	mgrPromotedDumpSample = `{"epoch": 6, "active_gid": 4233, "active_name": "b", "available": true, "standbys": []}`
	mgrRestoredDumpSample = `{"epoch": 7, "active_gid": 4233, "active_name": "b", "available": true, "standbys": [{"gid": 14263, "name": "a"}]}`
	// mon.a held mgr.b's beacon and the module clients of the active mgr.a,
	// whose global IDs are not its GID; mon.b held mgr.a's beacon.
	mgrMonASessionsSample = `[{"name": "mon.0", "entity_name": "", "con_type": "mon", "global_id": 0}, {"name": "mon.1", "entity_name": "", "con_type": "mon", "global_id": 0}, {"name": "mon.2", "entity_name": "", "con_type": "mon", "global_id": 0}, {"name": "client.?", "entity_name": "mgr.a", "con_type": "client", "global_id": 4179}, {"name": "client.?", "entity_name": "mgr.a", "con_type": "client", "global_id": 4185}, {"name": "client.?", "entity_name": "mgr.a", "con_type": "client", "global_id": 4191}, {"name": "mgr.?", "entity_name": "mgr.b", "con_type": "mgr", "global_id": 4233}]`
	mgrMonBSessionsSample = `[{"name": "mon.0", "entity_name": "", "con_type": "mon", "global_id": 0}, {"name": "mon.1", "entity_name": "", "con_type": "mon", "global_id": 0}, {"name": "mon.2", "entity_name": "", "con_type": "mon", "global_id": 0}, {"name": "mgr.4161", "entity_name": "mgr.a", "con_type": "mgr", "global_id": 4161}]`
	// After the restart, mon.b held the new beacon of mgr.a.
	mgrMonBRestoredSessionsSample = `[{"name": "mon.0", "entity_name": "", "con_type": "mon", "global_id": 0}, {"name": "mon.1", "entity_name": "", "con_type": "mon", "global_id": 0}, {"name": "mon.2", "entity_name": "", "con_type": "mon", "global_id": 0}, {"name": "mgr.?", "entity_name": "mgr.a", "con_type": "mgr", "global_id": 14263}]`
	mgrMonCSessionsSample         = `[{"name": "mon.0", "entity_name": "", "con_type": "mon", "global_id": 0}, {"name": "mon.1", "entity_name": "", "con_type": "mon", "global_id": 0}, {"name": "mon.2", "entity_name": "", "con_type": "mon", "global_id": 0}]`
)

const mgrSampleFSID = "33d4128f-ef25-40e1-a84b-4cbe99970ba6"

func decodedMgrMap(t *testing.T, sample string) mgrMapObservation {
	t.Helper()
	observed, err := decodeMgrMap([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	return observed
}

func decodedMgrSessions(t *testing.T, monitor, sample string) mgrMonSessions {
	t.Helper()
	observed, err := decodeMgrSessions(monitor, []byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	return observed
}

func TestDecodeMgrMap(t *testing.T) {
	for sample, want := range map[string]mgrMapObservation{
		mgrBaselineDumpSample: {Epoch: 4, Available: true, Active: mgrRegistration{"a", 4161}, Standbys: []mgrRegistration{{"b", 4233}}},
		mgrPromotedDumpSample: {Epoch: 6, Available: true, Active: mgrRegistration{"b", 4233}, Standbys: []mgrRegistration{}},
		mgrRestoredDumpSample: {Epoch: 7, Available: true, Active: mgrRegistration{"b", 4233}, Standbys: []mgrRegistration{{"a", 14263}}},
		`{"epoch": 2, "active_gid": 0, "active_name": "", "available": false, "standbys": []}`: {Epoch: 2, Standbys: []mgrRegistration{}},
	} {
		got := decodedMgrMap(t, sample)
		want.Query = mgrQuery{Status: mgrQueryComplete}
		if !got.same(want) || got.Query != want.Query {
			t.Errorf("%s decoded to %+v", sample, got)
		}
	}
	// Output without the selected fields, truncated output and fields that
	// cannot be a registration are not an empty map.
	for sample, status := range map[string]string{
		"":                         mgrQueryMissing,
		"  \n":                     mgrQueryMissing,
		`{"epoch": 4}`:             mgrQueryMissing,
		`{}`:                       mgrQueryMissing,
		mgrBaselineDumpSample[:60]: mgrQueryParse,
		`[]`:                       mgrQueryParse,
		`{"epoch": "4", "active_gid": 1, "active_name": "a", "available": true, "standbys": []}`:                      mgrQueryParse,
		`{"epoch": 4, "active_gid": 0, "active_name": "a", "available": true, "standbys": []}`:                        mgrQueryParse,
		`{"epoch": 4, "active_gid": 1, "active_name": "a", "available": true, "standbys": [{"name": "b"}]}`:           mgrQueryParse,
		`{"epoch": 4, "active_gid": 1, "active_name": "a", "available": true, "standbys": []} {}`:                     mgrQueryParse,
		strings.Repeat(" ", mgrDumpLimit) + mgrBaselineDumpSample:                                                     mgrQueryTruncated,
		`{"epoch": 4, "active_gid": 1, "active_name": "a", "available": true, "standbys": [{"gid": 0, "name": "b"}]}`: mgrQueryParse,
	} {
		observed, err := decodeMgrMap([]byte(sample))
		if got := mgrQueryOutcome(err); got.Status != status {
			t.Errorf("%.60q decoded to %+v (%s), want %s", sample, observed, got.Status, status)
		}
	}
}

func TestDecodeMgrSessions(t *testing.T) {
	monA := decodedMgrSessions(t, "a", mgrMonASessionsSample)
	if !slices.Equal(monA.Beacons, []mgrSession{{"mgr.b", 4233}}) ||
		!slices.Equal(monA.Modules, []mgrSession{{"mgr.a", 4179}, {"mgr.a", 4185}, {"mgr.a", 4191}}) {
		t.Fatalf("mon.a sessions %+v", monA)
	}
	if monC := decodedMgrSessions(t, "c", mgrMonCSessionsSample); len(monC.Beacons) != 0 || len(monC.Modules) != 0 || !monC.Query.complete() {
		t.Fatalf("mon.c sessions %+v", monC)
	}
	for sample, status := range map[string]string{"": mgrQueryMissing, "null": mgrQueryMissing, "{}": mgrQueryParse, mgrMonASessionsSample[:80]: mgrQueryParse} {
		observed, err := decodeMgrSessions("a", []byte(sample))
		if got := mgrQueryOutcome(err); got.Status != status {
			t.Errorf("%.60q decoded to %+v (%s), want %s", sample, observed, got.Status, status)
		}
	}
	for sample, want := range map[string]int{"[]": 0, `[{"id": 1}]`: 1} {
		if got, err := decodeModuleTasks([]byte(sample)); err != nil || got != want {
			t.Errorf("tasks %s: %d, %v", sample, got, err)
		}
	}
	for _, sample := range []string{"", "null", "{}", "[{"} {
		if _, err := decodeModuleTasks([]byte(sample)); err == nil {
			t.Errorf("tasks %q decoded", sample)
		}
	}
}

func TestMgrQueryOutcome(t *testing.T) {
	for status, err := range map[string]error{
		mgrQueryComplete: nil,
		mgrQueryTimedOut: context.DeadlineExceeded,
		mgrQueryRejected: &ceph.NativeFailure{Program: "ceph", ExitCode: 22, Reason: "Error EINVAL"},
		mgrQueryFailed:   errors.New("exec ceph: connection refused"),
		mgrQueryMissing:  errMgrMissing,
	} {
		if got := mgrQueryOutcome(err); got.Status != status {
			t.Errorf("%v classified %s, want %s", err, got.Status, status)
		}
	}
	if got := mgrQueryOutcome(errors.New(strings.Repeat("x", 1000))); len(got.Error) != 256 {
		t.Errorf("error text of %d bytes", len(got.Error))
	}
}

// mgrSamples builds the three accepted samples of the native run.
func mgrSamples(t *testing.T) (mgrExpectation, mgrPhaseSample, mgrPhaseSample, mgrPhaseSample) {
	t.Helper()
	complete := mgrQuery{Status: mgrQueryComplete}
	instances := func(aStarted string, aRunning bool) []mgrInstance {
		return []mgrInstance{
			{Name: "a", Query: complete, ContainerID: "4c5eed2e4f15", StartedAt: aStarted, Running: aRunning},
			{Name: "b", Query: complete, ContainerID: "2723fc1c2181", StartedAt: "2026-10-15T23:53:06.772170091Z", Running: true},
		}
	}
	sample := func(dump string, monitors []mgrMonSessions, instances []mgrInstance) mgrPhaseSample {
		mgrMap := decodedMgrMap(t, dump)
		return mgrPhaseSample{Before: mgrMap, After: mgrMap, Monitors: monitors, Instances: instances,
			Quorum: mgrQuorum{Query: complete, FSID: mgrSampleFSID, Quorum: []string{"a", "b", "c"}}, Module: mgrModuleControl{Query: complete}}
	}
	monA, monB, monC := decodedMgrSessions(t, "a", mgrMonASessionsSample), decodedMgrSessions(t, "b", mgrMonBSessionsSample), decodedMgrSessions(t, "c", mgrMonCSessionsSample)
	baseline := sample(mgrBaselineDumpSample, []mgrMonSessions{monA, monB, monC}, instances("2026-10-15T23:53:03.668005798Z", true))
	promoted := sample(mgrPromotedDumpSample, []mgrMonSessions{monA, decodedMgrSessions(t, "b", mgrMonCSessionsSample), monC},
		instances("2026-10-15T23:53:03.668005798Z", false))
	restored := sample(mgrRestoredDumpSample, []mgrMonSessions{monA, decodedMgrSessions(t, "b", mgrMonBRestoredSessionsSample), monC},
		instances("2026-10-15T23:53:45.670360095Z", true))
	expect := mgrExpectation{FSID: mgrSampleFSID, Monitors: []string{"a", "b", "c"}, Managers: []string{"a", "b"},
		Baseline: mgrBaseline{Active: mgrRegistration{"a", 4161}, Standby: mgrRegistration{"b", 4233},
			ActiveInstance: baseline.Instances[0], StandbyInstance: baseline.Instances[1]}}
	return expect, baseline, promoted, restored
}

// clone copies a sample deeply enough for a case to change it.
func (s mgrPhaseSample) clone() mgrPhaseSample {
	s.Instances = slices.Clone(s.Instances)
	s.Monitors = slices.Clone(s.Monitors)
	for i := range s.Monitors {
		s.Monitors[i].Beacons = slices.Clone(s.Monitors[i].Beacons)
	}
	s.Before.Standbys, s.After.Standbys = slices.Clone(s.Before.Standbys), slices.Clone(s.After.Standbys)
	s.Quorum.Quorum = slices.Clone(s.Quorum.Quorum)
	return s
}

func TestClassifyMgrSample(t *testing.T) {
	expect, baseline, promoted, restored := mgrSamples(t)
	phases := map[string]mgrPhaseSample{mgrPhaseBaseline: baseline, mgrPhasePromoted: promoted, mgrPhaseRestored: restored}
	for _, test := range []struct {
		name, phase, state string
		change             func(*mgrPhaseSample)
	}{
		{"native baseline", mgrPhaseBaseline, mgrSampleAccepted, func(*mgrPhaseSample) {}},
		{"native promotion", mgrPhasePromoted, mgrSampleAccepted, func(*mgrPhaseSample) {}},
		{"native restoration", mgrPhaseRestored, mgrSampleAccepted, func(*mgrPhaseSample) {}},
		// mon.b no longer holds mgr.a's beacon; mon.a still holds the module
		// clients of mgr.a, the same entity with other global IDs.
		{"module sessions without the beacon", mgrPhaseBaseline, mgrSamplePending, func(s *mgrPhaseSample) { s.Monitors[1].Beacons = nil }},
		{"a beacon with another global ID", mgrPhaseRestored, mgrSamplePending, func(s *mgrPhaseSample) { s.Monitors[1].Beacons[0].GlobalID = 4161 }},
		{"a beacon of another entity", mgrPhaseBaseline, mgrSamplePending, func(s *mgrPhaseSample) { s.Monitors[1].Beacons[0].Entity = "mgr.b" }},
		{"the standby process changed", mgrPhasePromoted, mgrSampleViolated, func(s *mgrPhaseSample) { s.Instances[1].StartedAt = "2026-10-15T23:53:30Z" }},
		{"the standby container changed", mgrPhaseRestored, mgrSampleViolated, func(s *mgrPhaseSample) { s.Instances[1].ContainerID = "9a1c989e24dd" }},
		{"the standby stopped", mgrPhasePromoted, mgrSampleViolated, func(s *mgrPhaseSample) { s.Instances[1].Running = false }},
		{"the standby registered again", mgrPhasePromoted, mgrSampleViolated, func(s *mgrPhaseSample) { s.Before.Active.GID, s.After.Active.GID = 14250, 14250 }},
		{"the old GID came back", mgrPhaseRestored, mgrSampleViolated, func(s *mgrPhaseSample) { s.Before.Standbys[0].GID, s.After.Standbys[0].GID = 4161, 4161 }},
		{"the old container was replaced", mgrPhaseRestored, mgrSampleViolated, func(s *mgrPhaseSample) { s.Instances[0].ContainerID = "f4e61e53d322" }},
		{"an unowned active", mgrPhasePromoted, mgrSampleViolated, func(s *mgrPhaseSample) { s.Before.Active.Name, s.After.Active.Name = "x", "x" }},
		{"another FSID", mgrPhaseBaseline, mgrSampleViolated, func(s *mgrPhaseSample) { s.Quorum.FSID = "416c9ff7-ef0c-4bfd-993d-f44d3df63af5" }},
		// The restarted process returned with a new GID, but its container
		// still shows the start time before the stop.
		{"the old start time", mgrPhaseRestored, mgrSamplePending, func(s *mgrPhaseSample) { s.Instances[0].StartedAt = expect.Baseline.ActiveInstance.StartedAt }},
		{"the stop not yet seen", mgrPhasePromoted, mgrSamplePending, func(s *mgrPhaseSample) {
			mgrMap := decodedMgrMap(t, mgrBaselineDumpSample)
			s.Before, s.After = mgrMap, mgrMap
		}},
		{"the stopped MGR still listed", mgrPhasePromoted, mgrSamplePending, func(s *mgrPhaseSample) {
			s.Before.Standbys = []mgrRegistration{{"a", 4161}}
			s.After.Standbys = []mgrRegistration{{"a", 4161}}
		}},
		{"the stopped container still runs", mgrPhasePromoted, mgrSamplePending, func(s *mgrPhaseSample) { s.Instances[0].Running = true }},
		{"no standby yet", mgrPhaseRestored, mgrSamplePending, func(s *mgrPhaseSample) {
			mgrMap := decodedMgrMap(t, mgrPromotedDumpSample)
			s.Before, s.After = mgrMap, mgrMap
		}},
		{"two standbys", mgrPhaseBaseline, mgrSamplePending, func(s *mgrPhaseSample) {
			s.Before.Standbys = append(s.Before.Standbys, mgrRegistration{"c", 4300})
			s.After.Standbys = append(s.After.Standbys, mgrRegistration{"c", 4300})
		}},
		{"a MON out of the quorum", mgrPhaseBaseline, mgrSamplePending, func(s *mgrPhaseSample) { s.Quorum.Quorum = []string{"a", "b"} }},
		{"a task in the module's queue", mgrPhasePromoted, mgrSamplePending, func(s *mgrPhaseSample) { s.Module.Tasks = 1 }},
		{"the map moved during the sample", mgrPhasePromoted, mgrSampleTransitional, func(s *mgrPhaseSample) { s.Before = decodedMgrMap(t, mgrBaselineDumpSample) }},
		{"the module command timed out", mgrPhasePromoted, mgrSampleIncomplete, func(s *mgrPhaseSample) { s.Module.Query = mgrQueryOutcome(context.DeadlineExceeded) }},
		{"a MON's sessions timed out", mgrPhaseBaseline, mgrSampleIncomplete, func(s *mgrPhaseSample) { s.Monitors[2].Query = mgrQueryOutcome(context.DeadlineExceeded) }},
		{"a truncated MgrMap", mgrPhaseRestored, mgrSampleIncomplete, func(s *mgrPhaseSample) {
			_, err := decodeMgrMap([]byte(mgrRestoredDumpSample[:50]))
			s.After = mgrMapObservation{Query: mgrQueryOutcome(err)}
		}},
		{"a native rejection", mgrPhaseBaseline, mgrSampleIncomplete, func(s *mgrPhaseSample) {
			s.Quorum = mgrQuorum{Query: mgrQueryOutcome(&ceph.NativeFailure{Program: "ceph", ExitCode: 1})}
		}},
		{"a MON not observed", mgrPhaseBaseline, mgrSampleIncomplete, func(s *mgrPhaseSample) { s.Monitors = s.Monitors[:2] }},
	} {
		sample := phases[test.phase].clone()
		test.change(&sample)
		expect.Phase = test.phase
		if state, reason := classifyMgrSample(expect, sample); state != test.state {
			t.Errorf("%s: %s (%s), want %s", test.name, state, reason, test.state)
		}
	}
}

// fakeManager is the container of a MGR whose instance the test sets.
type fakeManager struct {
	testcontainers.Container
	id      string
	mu      sync.Mutex
	started string
	running bool
}

func (f *fakeManager) GetContainerID() string { return f.id }

func (f *fakeManager) Inspect(context.Context) (*container.InspectResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &container.InspectResponse{Image: "sha256:6bb1c8a42fbc", State: &container.State{StartedAt: f.started, Running: f.running}}, nil
}

// scriptedMgrSources answers every query from fixed samples, except "mgr
// dump", whose answers follow dumps; the last one repeats. A query that
// blocks waits for its context.
type scriptedMgrSources struct {
	mu       sync.Mutex
	dumps    []string
	sessions map[string]string
	block    string
}

func (s *scriptedMgrSources) Ceph(ctx context.Context, args ...string) ([]byte, error) {
	command := strings.Join(args[:2], " ")
	if command == s.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	switch command {
	case "mgr dump":
		s.mu.Lock()
		defer s.mu.Unlock()
		dump := s.dumps[0]
		if len(s.dumps) > 1 {
			s.dumps = s.dumps[1:]
		}
		return []byte(dump), nil
	case "rbd task":
		return []byte("[]"), nil
	}
	return nil, errors.New("unexpected command " + command)
}

func (s *scriptedMgrSources) AdminSocket(_ context.Context, daemon string, _ ...string) ([]byte, error) {
	return []byte(s.sessions[daemon]), nil
}

func (s *scriptedMgrSources) QuorumStatus(context.Context) (ceph.QuorumStatus, error) {
	var status ceph.QuorumStatus
	status.QuorumNames = []string{"c", "a", "b"}
	status.MonMap.FSID = mgrSampleFSID
	return status, nil
}

// A sample whose MgrMap moved is retained and followed by another one, and a
// phase that never settles ends at its deadline with its last samples.
func TestAwaitMgrPhase(t *testing.T) {
	a, b := &fakeManager{id: "4c5eed2e4f15", started: "start-a", running: true}, &fakeManager{id: "2723fc1c2181", started: "start-b", running: true}
	managers := []*ceph.ManagerContainer{{Container: a, DaemonName: "a"}, {Container: b, DaemonName: "b"}}
	sessions := map[string]string{"mon.a": mgrMonASessionsSample, "mon.b": mgrMonBSessionsSample, "mon.c": mgrMonCSessionsSample}
	expect := mgrExpectation{Phase: mgrPhaseBaseline, FSID: mgrSampleFSID, Monitors: []string{"a", "b", "c"}, Managers: []string{"a", "b"}}
	moved := strings.Replace(mgrBaselineDumpSample, `"epoch": 4`, `"epoch": 3`, 1)
	cluster := &scriptedMgrSources{dumps: []string{moved, mgrBaselineDumpSample}, sessions: sessions}
	record, sample := awaitMgrPhase(t.Context(), cluster, expect, managers, time.Minute, time.Second, time.Millisecond)
	if !record.Accepted || sample.State != mgrSampleAccepted || record.Counts[mgrSampleTransitional] != 1 || record.Counts[mgrSampleAccepted] != 1 ||
		len(record.Samples) != 2 || record.Samples[0].State != mgrSampleTransitional || record.Samples[0].Before.Epoch != 3 {
		t.Fatalf("record %+v", record)
	}

	// The standby never registers: the phase keeps the last samples.
	cluster = &scriptedMgrSources{dumps: []string{mgrPromotedDumpSample}, sessions: sessions}
	record, sample = awaitMgrPhase(t.Context(), cluster, expect, managers, 300*time.Millisecond, time.Second, 10*time.Millisecond)
	if record.Accepted || sample.State != mgrSamplePending || record.Counts[mgrSamplePending] <= mgrRetainedSamples || len(record.Samples) != mgrRetainedSamples {
		t.Fatalf("record %+v", record)
	}

	// The module command never answers: each sample ends within its bound
	// and the phase within its deadline, incomplete, not as an absence.
	cluster = &scriptedMgrSources{dumps: []string{mgrBaselineDumpSample}, sessions: sessions, block: "rbd task"}
	started := time.Now()
	record, sample = awaitMgrPhase(t.Context(), cluster, expect, managers, 500*time.Millisecond, 100*time.Millisecond, time.Millisecond)
	if elapsed := time.Since(started); elapsed > 5*time.Second || record.Accepted || sample.State != mgrSampleIncomplete || sample.Module.Query.Status != mgrQueryTimedOut {
		t.Fatalf("after %s: record %+v", elapsed, record)
	}

	// A changed standby process ends the phase at once.
	expect.Phase, expect.Baseline = mgrPhasePromoted, mgrBaseline{Active: mgrRegistration{"a", 4161}, Standby: mgrRegistration{"b", 4233},
		ActiveInstance: mgrInstance{ContainerID: a.id, StartedAt: "start-a"}, StandbyInstance: mgrInstance{ContainerID: b.id, StartedAt: "start-b"}}
	b.started, a.running = "start-b2", false
	cluster = &scriptedMgrSources{dumps: []string{mgrPromotedDumpSample}, sessions: sessions}
	record, sample = awaitMgrPhase(t.Context(), cluster, expect, managers, time.Minute, time.Second, time.Millisecond)
	if record.Accepted || sample.State != mgrSampleViolated || len(record.Samples) != 1 {
		t.Fatalf("record %+v", record)
	}
}
