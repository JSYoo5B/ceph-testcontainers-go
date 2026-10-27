//go:build all || (integration && features && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

// These fixtures replace the cluster with synthetic admin output shaped like
// the pinned Ceph source prints it (OSD::status, OpTracker dumps, "osd dump",
// "status"). They check the collector's rules; they are not native evidence of
// why a client call timed out.

type fakeRadosLateReader struct {
	mu       sync.Mutex
	commands []string
	answers  map[string]func(context.Context) ([]byte, error)
}

func (f *fakeRadosLateReader) answer(key string, ctx context.Context) ([]byte, error) {
	f.mu.Lock()
	f.commands = append(f.commands, key)
	call := f.answers[key]
	f.mu.Unlock()
	if call == nil {
		return nil, fmt.Errorf("no fixture for %s", key)
	}
	return call(ctx)
}

func (f *fakeRadosLateReader) Ceph(ctx context.Context, args ...string) ([]byte, error) {
	return f.answer("ceph "+strings.Join(args, " "), ctx)
}

func (f *fakeRadosLateReader) OSDAdminSocket(ctx context.Context, id int, args ...string) ([]byte, error) {
	return f.answer(fmt.Sprintf("osd.%d %s", id, strings.Join(args, " ")), ctx)
}

func fixedRadosLateAnswer(raw string) func(context.Context) ([]byte, error) {
	return func(context.Context) ([]byte, error) { return []byte(raw), nil }
}

const (
	radosLateKeyInFlight = "osd.0 dump_ops_in_flight"
	radosLateKeyHistoric = "osd.0 dump_historic_ops_by_duration"
	radosLateKeyOSD      = "osd.0 status"
	radosLateKeyDump     = "ceph osd dump --format json"
	radosLateKeyStatus   = "ceph status --format json"
)

func radosLateOpJSON(description string, age float64, flag string, events ...string) string {
	rendered := make([]string, 0, len(events))
	for index, event := range events {
		rendered = append(rendered, fmt.Sprintf(`{"event":%q,"time":"2026-10-27T04:11:%02d.250000+0000","duration":0.5}`, event, 8+index))
	}
	return fmt.Sprintf(`{"description":%q,"initiated_at":"x","age":%v,"duration":%v,"type_data":{"flag_point":%q,"client_info":{"client":"client.4218","tid":51},"events":[%s]}}`,
		description, age, age, flag, strings.Join(rendered, ","))
}

// healthyRadosLateReader answers like a one-OSD cluster that has the pool,
// with the OSD at the MON's epoch and nothing queued.
func healthyRadosLateReader(monEpoch, osdNewest int) *fakeRadosLateReader {
	return &fakeRadosLateReader{answers: map[string]func(context.Context) ([]byte, error){
		radosLateKeyInFlight: fixedRadosLateAnswer(`{"ops":[],"num_ops":0}`),
		radosLateKeyHistoric: fixedRadosLateAnswer(`{"size":20,"duration":600,"ops":[]}`),
		radosLateKeyOSD:      fixedRadosLateAnswer(fmt.Sprintf(`{"cluster_fsid":"x","osd_fsid":"y","whoami":0,"state":"active","oldest_map":"1","newest_map":"%d","num_pgs":2}`, osdNewest)),
		radosLateKeyDump:     fixedRadosLateAnswer(fmt.Sprintf(`{"epoch":%d,"modified":"2026-10-27T04:11:41.8Z","pools":[{"pool":1,"pool_name":".mgr"},{"pool":2,"pool_name":"tc-contracts"}],"osds":[{"osd":0,"up":1,"in":1}]}`, monEpoch)),
		radosLateKeyStatus:   fixedRadosLateAnswer(`{"osdmap":{"num_osds":1,"num_up_osds":1,"num_in_osds":1},"pgmap":{"num_pgs":2,"pgs_by_state":[{"state_name":"active+clean","count":2}]}}`),
	}}
}

func collectFixture(reader radosLateReader, label string, baseline *radosLateClusterState) *radosLateClusterState {
	return collectRadosLateClusterState(context.Background(), reader, label, "tc-contracts", 0, baseline)
}

func radosLateProbeError(t *testing.T, state *radosLateClusterState, name string) string {
	t.Helper()
	for _, probe := range state.Probes {
		if probe.Name == name {
			return probe.Error
		}
	}
	t.Fatalf("no probe %q in %+v", name, state.Probes)
	return ""
}

func TestRadosLateClusterStateHealthyPreLate(t *testing.T) {
	state := collectFixture(healthyRadosLateReader(15, 15), "pre-late", nil)
	if state.MonEpoch == nil || *state.MonEpoch != 15 || state.PoolID == nil || *state.PoolID != 2 || state.OSDNewestMap == nil || *state.OSDNewestMap != 15 {
		t.Fatalf("epochs or pool not read: %+v", state)
	}
	if state.OSDMapLag == nil || *state.OSDMapLag != 0 || state.Ready == nil || !*state.Ready {
		t.Fatalf("healthy cluster not ready: lag %v ready %v", state.OSDMapLag, state.Ready)
	}
	if state.InFlightTotal == nil || *state.InFlightTotal != 0 || state.HistoricTotal == nil || *state.HistoricTotal != 0 {
		t.Fatalf("empty dumps must be conclusive zero totals: %v %v", state.InFlightTotal, state.HistoricTotal)
	}
	if state.EpochAdvance != nil {
		t.Fatalf("no baseline, no advance: %v", *state.EpochAdvance)
	}
	for _, probe := range state.Probes {
		if probe.Error != "" || probe.Bytes == 0 || probe.EndUnix < probe.StartUnix || probe.StartUnix <= 0 {
			t.Fatalf("probe %+v", probe)
		}
	}
	if len(state.Probes) != 5 {
		t.Fatalf("want five probes, got %d", len(state.Probes))
	}
}

// The failing run's shape: the MON is one epoch ahead of the OSD, a reader
// stat is queued, and earlier reader calls are in the history.
func TestRadosLateClusterStateFailureShowsMapLagAndQueue(t *testing.T) {
	baseline := collectFixture(healthyRadosLateReader(15, 15), "pre-late", nil)
	reader := healthyRadosLateReader(16, 15)
	reader.answers[radosLateKeyInFlight] = fixedRadosLateAnswer(`{"ops":[` +
		radosLateOpJSON("osd_op(client.4218.0:51 2.0 2:abc:suite:late-compound [stat] snapc 0=[] e15)", 9.2, "delayed", "initiated", "queued_for_pg") +
		`],"num_ops":1}`)
	reader.answers[radosLateKeyHistoric] = fixedRadosLateAnswer(`{"size":20,"duration":600,"ops":[` +
		radosLateOpJSON("osd_op(client.4220.0:3 2.0 2:abc:suite:late-compound [cmpext 0~10,writefull 0~8,setxattr tc (2)] snapc 0=[] ondisk+write e15)", 14, "commit_sent", "initiated", "started", "commit_sent", "done") +
		`]}`)
	state := collectFixture(reader, "failure", baseline)
	if state.OSDMapLag == nil || *state.OSDMapLag != 1 || state.EpochAdvance == nil || *state.EpochAdvance != 1 {
		t.Fatalf("lag %v advance %v", state.OSDMapLag, state.EpochAdvance)
	}
	if state.Ready == nil || *state.Ready {
		t.Fatalf("an OSD behind the MON's map must be conclusively not ready: %v", state.Ready)
	}
	if len(state.InFlight) != 1 || state.InFlight[0].Client != "client.4218" || state.InFlight[0].Epoch == nil || *state.InFlight[0].Epoch != 15 ||
		state.InFlight[0].FlagPoint != "delayed" || len(state.InFlight[0].Events) != 2 || state.InFlight[0].Age == nil || *state.InFlight[0].Age != 9.2 {
		t.Fatalf("in-flight op: %+v", state.InFlight)
	}
	if len(state.Historic) != 1 || state.Historic[0].Client != "client.4220" || len(state.Historic[0].Events) != 4 {
		t.Fatalf("historic op: %+v", state.Historic)
	}
	encoded, err := json.Marshal(state)
	if err != nil || strings.Contains(string(encoded), "client_addr") {
		t.Fatalf("encode: %v %s", err, encoded)
	}
}

// An epoch bump with no CLI command behind it can be traced by comparing the
// map's change stamps, the pool's last_change and the OSD's up_thru.
func TestRadosLateClusterStateTracesWhatChangedBetweenEpochs(t *testing.T) {
	dump := func(epoch, upThru int, upChange, poolChange string) *fakeRadosLateReader {
		reader := healthyRadosLateReader(epoch, epoch)
		reader.answers[radosLateKeyDump] = fixedRadosLateAnswer(fmt.Sprintf(`{"epoch":%d,"modified":"m%d","flags":"sortbitwise","last_up_change":%q,"last_in_change":"in0",
			"pools":[{"pool":2,"pool_name":"tc-contracts","last_change":%q}],"osds":[{"osd":0,"up":1,"in":1,"up_from":5,"up_thru":%d}]}`, epoch, epoch, upChange, poolChange, upThru))
		return reader
	}
	before := collectFixture(dump(15, 12, "up0", "11"), "pre-late", nil)
	after := collectFixture(dump(16, 15, "up0", "11"), "failure", before)
	if len(after.OSDs) != 1 || after.OSDs[0].UpThru == nil || *after.OSDs[0].UpThru != 15 || before.OSDs[0].UpThru == nil || *before.OSDs[0].UpThru != 12 ||
		after.OSDs[0].UpFrom == nil || *after.OSDs[0].UpFrom != 5 {
		t.Fatalf("up_thru not carried: %+v / %+v", before.OSDs, after.OSDs)
	}
	if after.MonFlags != "sortbitwise" || after.LastUpChange != "up0" || after.LastInChange != "in0" || after.PoolLastChange != "11" || after.MonModified != "m16" {
		t.Fatalf("change stamps: %+v", after)
	}
	if after.EpochAdvance == nil || *after.EpochAdvance != 1 {
		t.Fatalf("advance %v", after.EpochAdvance)
	}
	// The same fields stay absent when the dump does not carry them.
	bare := healthyRadosLateReader(15, 15)
	bare.answers[radosLateKeyDump] = fixedRadosLateAnswer(`{"epoch":15,"osds":[{"osd":0,"up":1,"in":1}]}`)
	state := collectFixture(bare, "pre-late", nil)
	if state.OSDs[0].UpThru != nil || state.OSDs[0].UpFrom != nil || state.MonFlags != "" || state.PoolLastChange != "" {
		t.Fatalf("absent fields became values: %+v", state)
	}
}

func TestRadosLateClusterStateReadOnlyCommands(t *testing.T) {
	reader := healthyRadosLateReader(15, 15)
	collectFixture(reader, "pre-late", nil)
	want := []string{radosLateKeyInFlight, radosLateKeyHistoric, radosLateKeyOSD, radosLateKeyDump, radosLateKeyStatus}
	if strings.Join(reader.commands, "|") != strings.Join(want, "|") {
		t.Fatalf("commands %q, want exactly the read-only set %q in order", reader.commands, want)
	}
}

// Missing, null and mistyped inputs make the probe unavailable and leave its
// fields absent; they never become a value that counts as evidence.
func TestRadosLateClusterStateMalformedInputsStayInconclusive(t *testing.T) {
	type check func(*radosLateClusterState) bool
	cases := []struct {
		name, key, raw string
		absent         check
	}{
		{"dump epoch missing", radosLateKeyDump, `{"osds":[{"osd":0,"up":1,"in":1}]}`, func(s *radosLateClusterState) bool { return s.MonEpoch == nil && s.OSDsUpAndIn == nil }},
		{"dump epoch null", radosLateKeyDump, `{"epoch":null,"osds":[]}`, func(s *radosLateClusterState) bool { return s.MonEpoch == nil }},
		{"dump epoch string", radosLateKeyDump, `{"epoch":"15","osds":[{"osd":0,"up":1,"in":1}]}`, func(s *radosLateClusterState) bool { return s.MonEpoch == nil }},
		{"dump epoch zero", radosLateKeyDump, `{"epoch":0,"osds":[{"osd":0,"up":1,"in":1}]}`, func(s *radosLateClusterState) bool { return s.MonEpoch == nil }},
		{"dump epoch fraction", radosLateKeyDump, `{"epoch":15.5,"osds":[{"osd":0,"up":1,"in":1}]}`, func(s *radosLateClusterState) bool { return s.MonEpoch == nil }},
		{"dump osds missing", radosLateKeyDump, `{"epoch":15}`, func(s *radosLateClusterState) bool { return s.MonEpoch == nil && s.OSDsUpAndIn == nil }},
		{"dump osds null", radosLateKeyDump, `{"epoch":15,"osds":null}`, func(s *radosLateClusterState) bool { return s.OSDsUpAndIn == nil }},
		{"dump osd up missing", radosLateKeyDump, `{"epoch":15,"osds":[{"osd":0,"in":1}]}`, func(s *radosLateClusterState) bool { return s.OSDsUpAndIn == nil }},
		{"dump osd entry null", radosLateKeyDump, `{"epoch":15,"osds":[null]}`, func(s *radosLateClusterState) bool { return s.OSDsUpAndIn == nil }},
		{"dump pools wrong type", radosLateKeyDump, `{"epoch":15,"osds":[{"osd":0,"up":1,"in":1}],"pools":{}}`, func(s *radosLateClusterState) bool { return s.MonEpoch == nil && s.PoolID == nil }},
		{"dump pool id string", radosLateKeyDump, `{"epoch":15,"osds":[{"osd":0,"up":1,"in":1}],"pools":[{"pool":"2","pool_name":"tc-contracts"}]}`, func(s *radosLateClusterState) bool { return s.PoolID == nil && s.MonEpoch == nil }},
		{"dump osd id missing", radosLateKeyDump, `{"epoch":15,"osds":[{"up":1,"in":1}]}`, func(s *radosLateClusterState) bool { return s.OSDs == nil && s.OSDsUpAndIn == nil }},
		{"dump up_thru string", radosLateKeyDump, `{"epoch":15,"osds":[{"osd":0,"up":1,"in":1,"up_thru":"14"}]}`, func(s *radosLateClusterState) bool { return s.OSDs == nil && s.MonEpoch == nil }},
		{"dump up_from null", radosLateKeyDump, `{"epoch":15,"osds":[{"osd":0,"up":1,"in":1,"up_from":null}]}`, func(s *radosLateClusterState) bool { return s.OSDs == nil && s.MonEpoch == nil }},
		{"dump flags number", radosLateKeyDump, `{"epoch":15,"flags":3,"osds":[{"osd":0,"up":1,"in":1}]}`, func(s *radosLateClusterState) bool { return s.MonFlags == "" && s.MonEpoch == nil }},
		{"dump pool last_change number", radosLateKeyDump, `{"epoch":15,"osds":[{"osd":0,"up":1,"in":1}],"pools":[{"pool":2,"pool_name":"tc-contracts","last_change":11}]}`, func(s *radosLateClusterState) bool {
			return s.PoolLastChange == "" && s.PoolID == nil && s.MonEpoch == nil
		}},
		{"dump not an object", radosLateKeyDump, `[]`, func(s *radosLateClusterState) bool { return s.MonEpoch == nil }},
		{"dump null document", radosLateKeyDump, `null`, func(s *radosLateClusterState) bool { return s.MonEpoch == nil }},
		{"dump empty output", radosLateKeyDump, ``, func(s *radosLateClusterState) bool { return s.MonEpoch == nil }},
		{"dump trailing data", radosLateKeyDump, `{"epoch":15,"osds":[]} {}`, func(s *radosLateClusterState) bool { return s.MonEpoch == nil }},
		{"status pgmap missing", radosLateKeyStatus, `{"osdmap":{}}`, func(s *radosLateClusterState) bool { return s.PGsActiveClean == nil && s.PGStates == nil }},
		{"status pgs_by_state null", radosLateKeyStatus, `{"osdmap":{},"pgmap":{"num_pgs":2,"pgs_by_state":null}}`, func(s *radosLateClusterState) bool { return s.PGsActiveClean == nil }},
		{"status pg state missing", radosLateKeyStatus, `{"osdmap":{},"pgmap":{"num_pgs":2,"pgs_by_state":[{"count":2}]}}`, func(s *radosLateClusterState) bool { return s.PGsActiveClean == nil }},
		{"status pg count string", radosLateKeyStatus, `{"osdmap":{},"pgmap":{"num_pgs":2,"pgs_by_state":[{"state_name":"active+clean","count":"2"}]}}`, func(s *radosLateClusterState) bool { return s.PGsActiveClean == nil }},
		{"status num_pgs missing", radosLateKeyStatus, `{"osdmap":{},"pgmap":{"pgs_by_state":[{"state_name":"active+clean","count":2}]}}`, func(s *radosLateClusterState) bool { return s.PGsActiveClean == nil }},
		{"osd status newest_map number", radosLateKeyOSD, `{"state":"active","oldest_map":"1","newest_map":15}`, func(s *radosLateClusterState) bool { return s.OSDNewestMap == nil && s.OSDState == "" }},
		{"osd status newest_map missing", radosLateKeyOSD, `{"state":"active","oldest_map":"1"}`, func(s *radosLateClusterState) bool { return s.OSDNewestMap == nil && s.OSDState == "" }},
		{"osd status newest_map null", radosLateKeyOSD, `{"state":"active","oldest_map":"1","newest_map":null}`, func(s *radosLateClusterState) bool { return s.OSDNewestMap == nil }},
		{"osd status newest_map text", radosLateKeyOSD, `{"state":"active","oldest_map":"1","newest_map":"x"}`, func(s *radosLateClusterState) bool { return s.OSDNewestMap == nil }},
		{"osd status map range inverted", radosLateKeyOSD, `{"state":"active","oldest_map":"9","newest_map":"3"}`, func(s *radosLateClusterState) bool { return s.OSDNewestMap == nil }},
		{"osd status state empty", radosLateKeyOSD, `{"state":"","oldest_map":"1","newest_map":"15"}`, func(s *radosLateClusterState) bool { return s.OSDNewestMap == nil && s.OSDState == "" }},
		{"in-flight ops missing", radosLateKeyInFlight, `{"num_ops":0}`, func(s *radosLateClusterState) bool { return s.InFlightTotal == nil && s.InFlight == nil }},
		{"in-flight ops null", radosLateKeyInFlight, `{"ops":null}`, func(s *radosLateClusterState) bool { return s.InFlightTotal == nil }},
		{"in-flight ops object", radosLateKeyInFlight, `{"ops":{}}`, func(s *radosLateClusterState) bool { return s.InFlightTotal == nil }},
		{"in-flight op without description", radosLateKeyInFlight, `{"ops":[{"age":1}]}`, func(s *radosLateClusterState) bool { return s.InFlightTotal == nil && s.InFlight == nil }},
		{"in-flight op null", radosLateKeyInFlight, `{"ops":[null]}`, func(s *radosLateClusterState) bool { return s.InFlightTotal == nil }},
		{"in-flight op age string", radosLateKeyInFlight, `{"ops":[{"description":"osd_op(client.1.0:1 2.0 x e1)","age":"1"}]}`, func(s *radosLateClusterState) bool { return s.InFlightTotal == nil }},
		{"in-flight op events object", radosLateKeyInFlight, `{"ops":[{"description":"osd_op(client.1.0:1 2.0 x e1)","type_data":{"events":{}}}]}`, func(s *radosLateClusterState) bool { return s.InFlightTotal == nil }},
		{"in-flight event without time", radosLateKeyInFlight, `{"ops":[{"description":"osd_op(client.1.0:1 2.0 x e1)","type_data":{"events":[{"event":"started"}]}}]}`, func(s *radosLateClusterState) bool { return s.InFlightTotal == nil }},
		{"in-flight num_ops string", radosLateKeyInFlight, `{"ops":[],"num_ops":"0"}`, func(s *radosLateClusterState) bool { return s.InFlightTotal == nil }},
		{"historic ops missing", radosLateKeyHistoric, `{"size":20}`, func(s *radosLateClusterState) bool { return s.HistoricTotal == nil && s.Historic == nil }},
	}
	probeOf := map[string]string{radosLateKeyDump: "osd_dump", radosLateKeyStatus: "status", radosLateKeyOSD: "osd_status", radosLateKeyInFlight: "ops_in_flight", radosLateKeyHistoric: "historic_ops_by_duration"}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := healthyRadosLateReader(15, 15)
			reader.answers[tc.key] = fixedRadosLateAnswer(tc.raw)
			state := collectFixture(reader, "failure", nil)
			if radosLateProbeError(t, state, probeOf[tc.key]) == "" {
				t.Fatalf("probe accepted %q", tc.raw)
			}
			if !tc.absent(state) {
				t.Fatalf("an unusable answer left evidence behind: %+v", state)
			}
			if readyInput := map[string]bool{radosLateKeyDump: true, radosLateKeyStatus: true, radosLateKeyOSD: true}; readyInput[tc.key] && state.Ready != nil && *state.Ready {
				t.Fatalf("readiness claimed with an unavailable input")
			}
			// Probes are independent: the others still ran.
			if len(state.Probes) != 5 || len(reader.commands) != 5 {
				t.Fatalf("a failed probe must not stop the others: %d probes, %d commands", len(state.Probes), len(reader.commands))
			}
		})
	}
}

func TestRadosLateClusterStateCommandFailureIsRecordedNotFatal(t *testing.T) {
	reader := healthyRadosLateReader(15, 15)
	reader.answers[radosLateKeyOSD] = func(context.Context) ([]byte, error) {
		return []byte(`{"state":"active","oldest_map":"1","newest_map":"15"}`), fmt.Errorf("exit status 22")
	}
	state := collectFixture(reader, "failure", nil)
	if err := radosLateProbeError(t, state, "osd_status"); !strings.Contains(err, "exit status 22") {
		t.Fatalf("command error not kept: %q", err)
	}
	if state.OSDNewestMap != nil || state.OSDMapLag != nil || state.Ready != nil && *state.Ready {
		t.Fatalf("output of a failed command was used: %+v", state)
	}
}

// An answer that arrives after the probe's deadline is not evidence, even if
// it is well formed and says everything is fine.
func TestRadosLateClusterStateRejectsAnswerAfterDeadline(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := healthyRadosLateReader(15, 15)
	reader.answers[radosLateKeyOSD] = func(ctx context.Context) ([]byte, error) {
		cancel() // the diagnostic budget ends while the call is in flight
		<-ctx.Done()
		return []byte(`{"state":"active","oldest_map":"1","newest_map":"15"}`), nil
	}
	state := collectRadosLateClusterState(parent, reader, "failure", "tc-contracts", 0, nil)
	if err := radosLateProbeError(t, state, "osd_status"); !strings.Contains(err, "after the probe deadline") {
		t.Fatalf("late answer accepted or misattributed: %q", err)
	}
	if state.OSDNewestMap != nil || state.OSDState != "" {
		t.Fatalf("late answer used as evidence: %+v", state)
	}
	// The probes after the expiry did not run at all and say why.
	if err := radosLateProbeError(t, state, "osd_dump"); !strings.Contains(err, "not run") {
		t.Fatalf("probe after budget expiry: %q", err)
	}
	for _, key := range []string{radosLateKeyDump, radosLateKeyStatus} {
		for _, command := range reader.commands {
			if command == key {
				t.Fatalf("%s ran after the budget was spent", key)
			}
		}
	}
}

func TestRadosLateClusterStateSpentBudgetRunsNothing(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	reader := healthyRadosLateReader(15, 15)
	state := collectRadosLateClusterState(parent, reader, "failure", "tc-contracts", 0, nil)
	if len(reader.commands) != 0 || len(state.Probes) != 5 {
		t.Fatalf("commands %v probes %d", reader.commands, len(state.Probes))
	}
	for _, probe := range state.Probes {
		if !strings.Contains(probe.Error, "not run") {
			t.Fatalf("probe %+v", probe)
		}
	}
	if state.Ready != nil || state.MonEpoch != nil {
		t.Fatalf("evidence from nothing: %+v", state)
	}
}

func TestRadosLateClusterStateOutputLimit(t *testing.T) {
	reader := healthyRadosLateReader(15, 15)
	reader.answers[radosLateKeyHistoric] = fixedRadosLateAnswer(`{"ops":[],"pad":"` + strings.Repeat("x", radosLateAdminOutputSize) + `"}`)
	state := collectFixture(reader, "failure", nil)
	if err := radosLateProbeError(t, state, "historic_ops_by_duration"); !strings.Contains(err, "exceeds") || state.HistoricTotal != nil {
		t.Fatalf("oversized output accepted: %q %v", err, state.HistoricTotal)
	}
}

func TestRadosLateClusterStateReadyIsThreeValued(t *testing.T) {
	cases := []struct {
		name  string
		build func(*fakeRadosLateReader)
		want  string
	}{
		{"all conclusive and healthy", func(*fakeRadosLateReader) {}, "true"},
		{"osd down", func(r *fakeRadosLateReader) {
			r.answers[radosLateKeyDump] = fixedRadosLateAnswer(`{"epoch":15,"osds":[{"osd":0,"up":0,"in":1}]}`)
		}, "false"},
		{"osd out", func(r *fakeRadosLateReader) {
			r.answers[radosLateKeyDump] = fixedRadosLateAnswer(`{"epoch":15,"osds":[{"osd":0,"up":1,"in":0}]}`)
		}, "false"},
		{"no osds", func(r *fakeRadosLateReader) {
			r.answers[radosLateKeyDump] = fixedRadosLateAnswer(`{"epoch":15,"osds":[]}`)
		}, "false"},
		{"pg scrubbing", func(r *fakeRadosLateReader) {
			r.answers[radosLateKeyStatus] = fixedRadosLateAnswer(`{"osdmap":{},"pgmap":{"num_pgs":2,"pgs_by_state":[{"state_name":"active+clean","count":1},{"state_name":"active+clean+scrubbing","count":1}]}}`)
		}, "false"},
		{"pg counts do not add up", func(r *fakeRadosLateReader) {
			r.answers[radosLateKeyStatus] = fixedRadosLateAnswer(`{"osdmap":{},"pgmap":{"num_pgs":3,"pgs_by_state":[{"state_name":"active+clean","count":2}]}}`)
		}, "false"},
		{"no pgs", func(r *fakeRadosLateReader) {
			r.answers[radosLateKeyStatus] = fixedRadosLateAnswer(`{"osdmap":{},"pgmap":{"num_pgs":0,"pgs_by_state":[]}}`)
		}, "false"},
		{"osd not active", func(r *fakeRadosLateReader) {
			r.answers[radosLateKeyOSD] = fixedRadosLateAnswer(`{"state":"booting","oldest_map":"1","newest_map":"15"}`)
		}, "false"},
		{"osd behind the mon", func(r *fakeRadosLateReader) {
			r.answers[radosLateKeyOSD] = fixedRadosLateAnswer(`{"state":"active","oldest_map":"1","newest_map":"14"}`)
		}, "false"},
		{"unavailable input without a contradiction", func(r *fakeRadosLateReader) {
			r.answers[radosLateKeyStatus] = fixedRadosLateAnswer(`{}`)
		}, "unknown"},
		{"a conclusive no beats an unavailable input", func(r *fakeRadosLateReader) {
			r.answers[radosLateKeyStatus] = fixedRadosLateAnswer(`{}`)
			r.answers[radosLateKeyDump] = fixedRadosLateAnswer(`{"epoch":15,"osds":[{"osd":0,"up":0,"in":1}]}`)
		}, "false"},
		{"osd map ahead of the mon is not caught up", func(r *fakeRadosLateReader) {
			r.answers[radosLateKeyOSD] = fixedRadosLateAnswer(`{"state":"active","oldest_map":"1","newest_map":"16"}`)
		}, "false"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := healthyRadosLateReader(15, 15)
			tc.build(reader)
			got := "unknown"
			if state := collectFixture(reader, "pre-late", nil); state.Ready != nil {
				got = fmt.Sprint(*state.Ready)
			}
			if got != tc.want {
				t.Fatalf("ready %s, want %s", got, tc.want)
			}
		})
	}
}

func TestRadosLateClusterStateOperationFormsAndCaps(t *testing.T) {
	reader := healthyRadosLateReader(15, 15)
	var ops, many []string
	ops = append(ops, `{"description":"MOSDScrub2 something"}`, `{"description":"osd_op(client.9.0:1 2.0 x)"}`)
	for i := 0; i < radosLateOpLimit+3; i++ {
		many = append(many, radosLateOpJSON(fmt.Sprintf("osd_op(client.%d.0:1 2.0 x e15)", i), float64(i), "started"))
	}
	reader.answers[radosLateKeyInFlight] = fixedRadosLateAnswer(`{"ops":[` + strings.Join(ops, ",") + `],"num_ops":5}`)
	reader.answers[radosLateKeyHistoric] = fixedRadosLateAnswer(`{"ops":[` + strings.Join(many, ",") + `]}`)
	state := collectFixture(reader, "failure", nil)
	if len(state.InFlight) != 2 || state.InFlight[0].Client != "" || state.InFlight[0].Epoch != nil || state.InFlight[1].Client != "client.9" || state.InFlight[1].Epoch != nil {
		t.Fatalf("client and epoch are read only from a client op description that has them: %+v", state.InFlight)
	}
	if len(state.Notes) != 1 || !strings.Contains(state.Notes[0], "num_ops 5 differs") {
		t.Fatalf("num_ops disagreement not noted: %v", state.Notes)
	}
	if len(state.Historic) != radosLateOpLimit || state.HistoricDropped != 3 || state.HistoricTotal == nil || *state.HistoricTotal != radosLateOpLimit+3 {
		t.Fatalf("cap: kept %d dropped %d total %v", len(state.Historic), state.HistoricDropped, state.HistoricTotal)
	}
}

func TestRadosLateClusterStateEventCapAndByteBound(t *testing.T) {
	reader := healthyRadosLateReader(15, 15)
	events := make([]string, radosLateEventLimit+5)
	for i := range events {
		events[i] = fmt.Sprintf("event-%02d-%s", i, strings.Repeat("e", 40))
	}
	var many []string
	for i := 0; i < radosLateOpLimit; i++ {
		many = append(many, radosLateOpJSON(fmt.Sprintf("osd_op(client.%d.0:1 2.0 %s e15)", i, strings.Repeat("o", 200)), float64(i), "started", events...))
	}
	body := `{"ops":[` + strings.Join(many, ",") + `]}`
	reader.answers[radosLateKeyHistoric] = fixedRadosLateAnswer(body)
	reader.answers[radosLateKeyInFlight] = fixedRadosLateAnswer(body)
	state := collectFixture(reader, "failure", nil)
	encoded, err := json.Marshal(state)
	if err != nil || len(encoded) > radosLateStateLimit {
		t.Fatalf("encoded %d bytes (limit %d): %v", len(encoded), radosLateStateLimit, err)
	}
	if !state.Truncated {
		t.Fatalf("truncation not recorded")
	}
	// Whatever was dropped is counted, never silent: totals stay the real ones.
	if state.HistoricTotal == nil || *state.HistoricTotal != radosLateOpLimit || len(state.Historic)+state.HistoricDropped < 1 {
		t.Fatalf("totals: %v kept %d dropped %d", state.HistoricTotal, len(state.Historic), state.HistoricDropped)
	}
	for _, op := range state.Historic {
		if len(op.Events) > radosLateEventLimit || op.EventsDropped < 5 {
			t.Fatalf("event cap not recorded: %d kept, %d dropped", len(op.Events), op.EventsDropped)
		}
	}
}

func TestRadosLateClusterStatePoolNotFoundIsANoteNotAnID(t *testing.T) {
	reader := healthyRadosLateReader(15, 15)
	reader.answers[radosLateKeyDump] = fixedRadosLateAnswer(`{"epoch":15,"osds":[{"osd":0,"up":1,"in":1}],"pools":[{"pool":1,"pool_name":".mgr"}]}`)
	state := collectFixture(reader, "pre-late", nil)
	if state.PoolID != nil || state.MonEpoch == nil || len(state.Notes) != 1 || !strings.Contains(state.Notes[0], "tc-contracts") {
		t.Fatalf("pool id %v notes %v", state.PoolID, state.Notes)
	}
}

func TestRadosLateClusterStateProbeBoundsFitTheBudget(t *testing.T) {
	if radosLateProbeBound >= radosLateStateBound {
		t.Fatalf("probe bound %v and state bound %v", radosLateProbeBound, radosLateStateBound)
	}
	if radosLateStateBound > 30*time.Second {
		t.Fatalf("diagnostic bound %v is too long to follow a failed phase", radosLateStateBound)
	}
}

// Every probe reads one JSON document: a closing delimiter or a second value
// after it makes the answer unavailable, and only whitespace may follow.
func TestRadosLateClusterStateTrailingDataIsUnavailableForEveryProbe(t *testing.T) {
	probes := []struct{ key, name string }{
		{radosLateKeyInFlight, "ops_in_flight"}, {radosLateKeyHistoric, "historic_ops_by_duration"},
		{radosLateKeyOSD, "osd_status"}, {radosLateKeyDump, "osd_dump"}, {radosLateKeyStatus, "status"},
	}
	for _, probe := range probes {
		good, _ := healthyRadosLateReader(15, 15).answers[probe.key](context.Background())
		for _, suffix := range []string{"}", "]", " }", "{}", "[]", "x", "\n}\n", `{"a":1}`} {
			t.Run(probe.name+"/"+strings.TrimSpace(suffix), func(t *testing.T) {
				reader := healthyRadosLateReader(15, 15)
				reader.answers[probe.key] = fixedRadosLateAnswer(string(good) + suffix)
				state := collectFixture(reader, "failure", nil)
				if err := radosLateProbeError(t, state, probe.name); !strings.Contains(err, "trailing data") {
					t.Fatalf("suffix %q accepted: %q", suffix, err)
				}
				if state.MonEpoch != nil && probe.name == "osd_dump" || state.OSDNewestMap != nil && probe.name == "osd_status" || state.PGsActiveClean != nil && probe.name == "status" || state.InFlightTotal != nil && probe.name == "ops_in_flight" || state.HistoricTotal != nil && probe.name == "historic_ops_by_duration" {
					t.Fatalf("evidence published from an answer with trailing data: %+v", state)
				}
			})
		}
		t.Run(probe.name+"/whitespace", func(t *testing.T) {
			reader := healthyRadosLateReader(15, 15)
			reader.answers[probe.key] = fixedRadosLateAnswer(string(good) + " \n\t\r\n")
			if err := radosLateProbeError(t, collectFixture(reader, "failure", nil), probe.name); err != "" {
				t.Fatalf("trailing whitespace rejected: %q", err)
			}
		})
	}
}

func encodedRadosLateSize(t *testing.T, state *radosLateClusterState) int {
	t.Helper()
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return len(encoded)
}

// The advertised record budget holds for the final encoded record whatever
// field is large, including text that JSON escaping makes larger.
func TestRadosLateClusterStateFinalRecordStaysWithinBudget(t *testing.T) {
	huge := strings.Repeat("e", radosLateStateLimit+1)
	events := make([]string, radosLateEventLimit+4)
	for i := range events {
		events[i] = "event-" + strings.Repeat("v", 60)
	}
	opsBody := func() string {
		var ops []string
		for i := 0; i < radosLateOpLimit+5; i++ {
			ops = append(ops, radosLateOpJSON(fmt.Sprintf("osd_op(client.%d.0:1 2.0 %s e15)", i, strings.Repeat("o", 700)), float64(i), strings.Repeat("f", 300), events...))
		}
		return `{"ops":[` + strings.Join(ops, ",") + `]}`
	}
	cases := []struct {
		name  string
		build func(*fakeRadosLateReader)
		cut   bool
	}{
		{"large command error", func(r *fakeRadosLateReader) {
			r.answers[radosLateKeyOSD] = func(context.Context) ([]byte, error) { return nil, fmt.Errorf("%s", huge) }
		}, true},
		{"large non-operation field", func(r *fakeRadosLateReader) {
			r.answers[radosLateKeyDump] = fixedRadosLateAnswer(`{"epoch":15,"flags":"` + huge + `","last_up_change":"` + huge + `","osds":[{"osd":0,"up":1,"in":1}]}`)
		}, true},
		{"large pg state names", func(r *fakeRadosLateReader) {
			r.answers[radosLateKeyStatus] = fixedRadosLateAnswer(`{"osdmap":{},"pgmap":{"num_pgs":1,"pgs_by_state":[{"state_name":"` + huge + `","count":1}]}}`)
		}, true},
		{"many osds and pg states", func(r *fakeRadosLateReader) {
			var osds, states []string
			for i := 0; i < 500; i++ {
				osds = append(osds, fmt.Sprintf(`{"osd":%d,"up":1,"in":1,"up_from":1,"up_thru":2}`, i))
				states = append(states, fmt.Sprintf(`{"state_name":"s%d","count":1}`, i))
			}
			r.answers[radosLateKeyDump] = fixedRadosLateAnswer(`{"epoch":15,"osds":[` + strings.Join(osds, ",") + `]}`)
			r.answers[radosLateKeyStatus] = fixedRadosLateAnswer(`{"osdmap":{},"pgmap":{"num_pgs":500,"pgs_by_state":[` + strings.Join(states, ",") + `]}}`)
		}, true},
		{"large operation and event lists", func(r *fakeRadosLateReader) {
			r.answers[radosLateKeyInFlight] = fixedRadosLateAnswer(opsBody())
			r.answers[radosLateKeyHistoric] = fixedRadosLateAnswer(opsBody())
		}, true},
		{"escape-heavy error text", func(r *fakeRadosLateReader) {
			r.answers[radosLateKeyOSD] = func(context.Context) ([]byte, error) {
				return nil, fmt.Errorf("%s", strings.Repeat("\x00\"\\", 4*radosLateErrorLimit))
			}
			r.answers[radosLateKeyDump] = fixedRadosLateAnswer(`{"epoch":15,"flags":"` + strings.Repeat(`\u0000`, 40000) + `","osds":[{"osd":0,"up":1,"in":1}]}`)
		}, true},
		{"escape-heavy pg states and osd list", func(r *fakeRadosLateReader) {
			var states []string
			for i := 0; i < 100; i++ {
				states = append(states, fmt.Sprintf(`{"state_name":"%s","count":1}`, strings.Repeat(`\u0000`, 400)+fmt.Sprint(i)))
			}
			r.answers[radosLateKeyStatus] = fixedRadosLateAnswer(`{"osdmap":{},"pgmap":{"num_pgs":100,"pgs_by_state":[` + strings.Join(states, ",") + `]}}`)
			r.answers[radosLateKeyDump] = fixedRadosLateAnswer(`{"epoch":15,"flags":"` + strings.Repeat(`\u0000`, 400) + `","last_up_change":"` + strings.Repeat(`\u0000`, 400) + `","osds":[{"osd":0,"up":1,"in":1}]}`)
		}, true},
		{"everything at once", func(r *fakeRadosLateReader) {
			r.answers[radosLateKeyOSD] = func(context.Context) ([]byte, error) { return nil, fmt.Errorf("%s", huge) }
			r.answers[radosLateKeyDump] = fixedRadosLateAnswer(`{"epoch":15,"flags":"` + huge + `","modified":"` + huge + `","osds":[{"osd":0,"up":1,"in":1}]}`)
			r.answers[radosLateKeyStatus] = func(context.Context) ([]byte, error) { return nil, fmt.Errorf("%s", huge) }
			r.answers[radosLateKeyInFlight] = fixedRadosLateAnswer(opsBody())
			r.answers[radosLateKeyHistoric] = fixedRadosLateAnswer(opsBody())
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := healthyRadosLateReader(15, 15)
			tc.build(reader)
			state := collectFixture(reader, "failure", nil)
			if size := encodedRadosLateSize(t, state); size > radosLateStateLimit {
				t.Fatalf("final record is %d bytes, budget %d", size, radosLateStateLimit)
			}
			if tc.cut && !state.Truncated {
				t.Fatalf("cut not recorded")
			}
			// Each retained field is bounded on its own, not only the total.
			for name, text := range map[string]string{"flags": state.MonFlags, "last_up_change": state.LastUpChange, "last_in_change": state.LastInChange, "modified": state.MonModified, "pool_last_change": state.PoolLastChange} {
				if len(text) > radosLateTextLimit+64 {
					t.Fatalf("%s is %d bytes", name, len(text))
				}
			}
			for _, pg := range state.PGStates {
				if len(pg.State) > radosLateTextLimit+64 {
					t.Fatalf("pg state name is %d bytes", len(pg.State))
				}
			}
			if len(state.PGStates) > radosLateListLimit || len(state.OSDs) > radosLateListLimit || len(state.Notes) > radosLateNoteLimit {
				t.Fatalf("lists: %d pg states, %d osds, %d notes", len(state.PGStates), len(state.OSDs), len(state.Notes))
			}
			for _, probe := range state.Probes {
				if len(probe.Error) > radosLateErrorLimit+64 {
					t.Fatalf("probe %s error is %d bytes", probe.Name, len(probe.Error))
				}
			}
		})
	}
	t.Run("error just over the limit is marked", func(t *testing.T) {
		reader := healthyRadosLateReader(15, 15)
		reader.answers[radosLateKeyOSD] = func(context.Context) ([]byte, error) {
			return nil, fmt.Errorf("%s", strings.Repeat("x", radosLateErrorLimit+1))
		}
		state := collectFixture(reader, "failure", nil)
		if err := radosLateProbeError(t, state, "osd_status"); !strings.Contains(err, "[cut ") || !state.Truncated {
			t.Fatalf("unmarked cut: %q truncated=%v", err, state.Truncated)
		}
	})
	t.Run("small values are untouched", func(t *testing.T) {
		state := collectFixture(healthyRadosLateReader(15, 15), "pre-late", nil)
		if state.Truncated || strings.Contains(fmt.Sprint(state.Probes), "[cut") {
			t.Fatalf("healthy record cut: %+v", state)
		}
	})
}

// Capping keeps what fits: a long state name or a long OSD list is shortened
// and marked, not replaced by dropping the whole field.
func TestRadosLateClusterStateCappedFieldsAreKeptAndMarked(t *testing.T) {
	reader := healthyRadosLateReader(15, 15)
	var osds []string
	for i := 0; i < 500; i++ {
		osds = append(osds, fmt.Sprintf(`{"osd":%d,"up":1,"in":1}`, i))
	}
	reader.answers[radosLateKeyDump] = fixedRadosLateAnswer(`{"epoch":15,"osds":[` + strings.Join(osds, ",") + `]}`)
	reader.answers[radosLateKeyStatus] = fixedRadosLateAnswer(`{"osdmap":{},"pgmap":{"num_pgs":1,"pgs_by_state":[{"state_name":"` + strings.Repeat("s", 4*radosLateTextLimit) + `","count":1}]}}`)
	state := collectFixture(reader, "failure", nil)
	if len(state.OSDs) != radosLateListLimit || !state.Truncated {
		t.Fatalf("osd list: kept %d truncated=%v", len(state.OSDs), state.Truncated)
	}
	if len(state.PGStates) != 1 || !strings.Contains(state.PGStates[0].State, "[cut ") || len(state.PGStates[0].State) > radosLateTextLimit+32 {
		t.Fatalf("pg state: %+v", state.PGStates)
	}
	// Notes are bounded both in number and in length.
	notes := &radosLateClusterState{}
	for i := 0; i < 3*radosLateNoteLimit; i++ {
		notes.Notes = append(notes.Notes, strings.Repeat("n", 3*radosLateErrorLimit))
	}
	if !capRadosLateFields(notes) || len(notes.Notes) != radosLateNoteLimit || !strings.Contains(notes.Notes[0], "[cut ") || len(notes.Notes[0]) > radosLateErrorLimit+32 {
		t.Fatalf("notes: %d, first %d bytes", len(notes.Notes), len(notes.Notes[0]))
	}
	// A multi-byte rune is never split by the cut.
	text, cut := radosLateText(strings.Repeat("é", 100), 5)
	if !cut || !utf8.ValidString(text) {
		t.Fatalf("cut text %q valid=%v", text, utf8.ValidString(text))
	}
}
