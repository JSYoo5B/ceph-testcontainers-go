//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_reclaim_closing_open_test

import (
	"strings"
	"syscall"
	"testing"
)

const (
	testTarget    = 4217
	testReclaimer = 4225
	testUUID      = "closing-open-overlap"
)

// The MDS log lines of one overlap flow, as Tentacle 20.2.2 and 20.2.3
// write them at debug_mds 10 (addresses and pointers shortened).
var (
	testLogOpen    = "3 mds.0.server handle_client_session client_session(request_open) from client.4225"
	testLogClose   = "3 mds.0.server handle_client_session client_session(request_close) from client.4225"
	testLogJournal = "10 mds.0.server journal_close_session : client.4225 172.18.0.7:0/1 pending_prealloc_inos []"
	testLogLogged  = "10 mds.0.server _session_logged client.4225 172.18.0.7:0/1 state_seq 3 close 4 inos_to_free [] inotablev 0 inos_to_purge []"
	testLogRemoved = "10 mds.0.sessionmap remove_session s=0x5f name=client.4225"
	testLogReject  = "0 log_channel(cluster) log [WRN] : client session with duplicated session uuid 'closing-open-overlap' denied (client.4225 172.18.0.7:0/1)"
)

func testRC(rc int) *int { return &rc }

// testReclaimed returns the given steps of a flow that reached the
// adoption of the UUID.
func testReclaimed(uuid string) closingFlow {
	return closingFlow{
		Target: testTarget, Reclaimer: testReclaimer, UUID: uuid,
		TargetSessions:    []closingSession{{ID: testTarget, State: "open", UUID: uuid}},
		ReclaimedSessions: []closingSession{{ID: testReclaimer, State: "open"}},
	}
}

func testControl() closingFlow {
	f := testReclaimed("closing-open-control")
	f.CloseSeconds = 0.02
	f.ClosingSessions = nil
	f.SettledSessions = []closingSession{{ID: testReclaimer, State: "open", UUID: "closing-open-control"}}
	return f
}

// testOverlap returns the overlap flow up to the hold.
func testOverlap() closingFlow {
	f := testReclaimed(testUUID)
	f.PausedFlags = []string{"pauserd", "pausewr", "sortbitwise"}
	f.CloseSeconds = 10.0
	f.ClosingSessions = []closingSession{{ID: testReclaimer, State: "closing"}}
	f.ClientLog = []string{"1 client.4225 1 mds(s) did not respond to session close -- timing out."}
	f.Held = &closingHeld{Sessions: []closingSession{{ID: testReclaimer, State: "closing"}}}
	return f
}

// testDefect is the overlap flow of a build with the defect.
func testDefect() (closingFlow, []string) {
	f := testOverlap()
	f.Held.Returned, f.Held.RC, f.MountRC = true, testRC(-int(syscall.EPERM)), -int(syscall.EPERM)
	f.ClientLog = append(f.ClientLog, "-1 client.4225 mds.0 rejected us (duplicated session uuid)")
	return f, []string{testLogOpen, testLogClose, testLogJournal, testLogReject, testLogLogged, testLogRemoved}
}

// testContract is the overlap flow of a corrected build.
func testContract() (closingFlow, []string) {
	f := testOverlap()
	f.SettledSessions = []closingSession{{ID: testReclaimer, State: "open", UUID: testUUID}}
	return f, []string{testLogOpen, testLogClose, testLogJournal, testLogOpen, testLogLogged, testLogRemoved, testLogOpen}
}

func TestJudgeClosingOverlapVerdicts(t *testing.T) {
	f, log := testDefect()
	if reproduced, err := judgeClosingOverlap(f, testUUID, log); err != nil || !reproduced {
		t.Errorf("defect: reproduced=%t err=%v", reproduced, err)
	}
	f, log = testContract()
	if reproduced, err := judgeClosingOverlap(f, testUUID, log); err != nil || reproduced {
		t.Errorf("contract: reproduced=%t err=%v", reproduced, err)
	}
}

// Outcomes that are neither the contract nor the defect fail the test, never
// a verdict.
func TestJudgeClosingOverlapRejectsOtherOutcomes(t *testing.T) {
	for _, c := range []struct {
		name   string
		mutate func(*closingFlow, *[]string)
	}{
		{"mount timed out during the hold", func(f *closingFlow, _ *[]string) {
			f.Held.RC, f.MountRC = testRC(-int(syscall.ETIMEDOUT)), -int(syscall.ETIMEDOUT)
		}},
		{"mount accepted during the hold", func(f *closingFlow, _ *[]string) { f.Held.RC, f.MountRC = testRC(0), 0 }},
		{"EPERM without the MDS refusal", func(_ *closingFlow, log *[]string) { *log = (*log)[:3] }},
		{"EPERM refusing another UUID", func(_ *closingFlow, log *[]string) {
			(*log)[3] = strings.Replace((*log)[3], testUUID, "other", 1)
		}},
		{"EPERM refusing another client", func(_ *closingFlow, log *[]string) {
			(*log)[3] = strings.Replace((*log)[3], "client.4225", "client.4226", 1)
		}},
		{"EPERM refused before the close", func(_ *closingFlow, log *[]string) {
			*log = []string{testLogReject, testLogOpen, testLogClose, testLogLogged}
		}},
		{"EPERM without the client's refusal line", func(f *closingFlow, _ *[]string) { f.ClientLog = f.ClientLog[:1] }},
	} {
		f, log := testDefect()
		c.mutate(&f, &log)
		if reproduced, err := judgeClosingOverlap(f, testUUID, log); err == nil {
			t.Errorf("%s: reproduced=%t without an error", c.name, reproduced)
		}
	}
	for _, c := range []struct {
		name   string
		mutate func(*closingFlow, *[]string)
	}{
		{"mount failed after the unpause", func(f *closingFlow, _ *[]string) { f.MountRC = -int(syscall.EPERM) }},
		{"session no longer closing at the hold", func(f *closingFlow, _ *[]string) {
			f.Held.Sessions = []closingSession{{ID: testReclaimer, State: "opening", UUID: testUUID}}
		}},
		{"open not handled again", func(_ *closingFlow, log *[]string) { *log = (*log)[:6] }},
		{"open handled again before the close was logged", func(_ *closingFlow, log *[]string) {
			*log = []string{testLogClose, testLogOpen, testLogOpen, testLogLogged}
		}},
		{"refusal logged", func(_ *closingFlow, log *[]string) { *log = append(*log, testLogReject) }},
		{"settled session without the UUID", func(f *closingFlow, _ *[]string) {
			f.SettledSessions = []closingSession{{ID: testReclaimer, State: "open"}}
		}},
	} {
		f, log := testContract()
		c.mutate(&f, &log)
		if reproduced, err := judgeClosingOverlap(f, testUUID, log); err == nil {
			t.Errorf("%s: reproduced=%t without an error", c.name, reproduced)
		}
	}
}

func TestJudgeClosingConditionAndControl(t *testing.T) {
	if err := judgeClosingCondition(testOverlap(), testUUID, 10); err != nil {
		t.Errorf("condition: %v", err)
	}
	for _, c := range []struct {
		name   string
		mutate func(*closingFlow)
	}{
		{"not paused", func(f *closingFlow) { f.PausedFlags = []string{"pauserd"} }},
		{"close answered", func(f *closingFlow) { f.CloseSeconds = 0.5 }},
		{"close not timed out in the client", func(f *closingFlow) { f.ClientLog = nil }},
		{"session not closing", func(f *closingFlow) { f.ClosingSessions = nil }},
		{"no hold", func(f *closingFlow) { f.Held = nil }},
		{"RESET failed", func(f *closingFlow) { f.ReclaimRC = -int(syscall.ENOENT) }},
		{"target survived the RESET", func(f *closingFlow) {
			f.ReclaimedSessions = append(f.ReclaimedSessions, closingSession{ID: testTarget, State: "open", UUID: testUUID})
		}},
		{"target without the UUID", func(f *closingFlow) { f.TargetSessions[0].UUID = "" }},
		{"one handle for both", func(f *closingFlow) { f.Reclaimer = testTarget }},
	} {
		f := testOverlap()
		c.mutate(&f)
		if err := judgeClosingCondition(f, testUUID, 10); err == nil {
			t.Errorf("condition %s: no error", c.name)
		}
	}
	if err := judgeClosingControl(testControl(), "closing-open-control", 10); err != nil {
		t.Errorf("control: %v", err)
	}
	for _, c := range []struct {
		name   string
		mutate func(*closingFlow)
	}{
		{"close timed out", func(f *closingFlow) { f.CloseSeconds = 10 }},
		{"mount failed", func(f *closingFlow) { f.MountRC = -int(syscall.EPERM) }},
		{"session without the UUID", func(f *closingFlow) { f.SettledSessions[0].UUID = "" }},
		{"paused", func(f *closingFlow) { f.PausedFlags = []string{"pauserd", "pausewr"} }},
	} {
		f := testControl()
		c.mutate(&f)
		if err := judgeClosingControl(f, "closing-open-control", 10); err == nil {
			t.Errorf("control %s: no error", c.name)
		}
	}
}

// A client ID that has the reclaimer's as a prefix is another client.
func TestClosingLogMatchesWholeClientIDs(t *testing.T) {
	log := []string{
		strings.Replace(testLogClose, "client.4225", "client.42251", 1),
		strings.Replace(testLogOpen, "client.4225", "client.42251", 1),
		strings.Replace(testLogLogged, "client.4225", "client.42251", 1),
		strings.Replace(testLogOpen, "client.4225", "client.42251", 1),
	}
	if closingRetried(log, testReclaimer) || !closingRetried(log, 42251) {
		t.Error("the retry evidence is not tied to the whole client ID")
	}
}
