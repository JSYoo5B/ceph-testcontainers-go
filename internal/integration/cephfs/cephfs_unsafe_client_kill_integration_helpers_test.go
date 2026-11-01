//go:build all || integration

package cephfs_test

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	cephfssession "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/session"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// mdsOpsInFlight reads an MDS's dump_ops_in_flight and finds the client's
// rename with tid in it.
func mdsOpsInFlight(h *harness.CephFSUnsafeHold, t *testing.T, ctx context.Context, tid uint64) (harness.MDSRequestOp, bool) {
	t.Helper()
	data, err := h.Cluster.Ceph(ctx, "tell", "mds."+h.MDS.Name, "dump_ops_in_flight")
	if err != nil {
		t.Fatalf("dump_ops_in_flight of mds.%s: %v", h.MDS.Name, err)
	}
	ops, err := harness.ParseMDSRequestOps(data)
	if err != nil {
		t.Fatalf("dump_ops_in_flight of mds.%s: %v: %.1024s", h.MDS.Name, err, data)
	}
	return harness.FindMDSRequest(ops, h.Baseline.Client, "rename", tid)
}

// clientSession returns the actor client's session, or nil when the MDS has
// none.
func clientSession(h *harness.CephFSUnsafeHold, t *testing.T, ctx context.Context) *cephfssession.Session {
	t.Helper()
	sessions, err := cephfssession.List(ctx, h.FS)
	if err != nil {
		t.Fatal(err)
	}
	for _, session := range sessions {
		if session.ID == h.Baseline.Client {
			return &session
		}
	}
	return nil
}

// killActor SIGKILLs the actor's Python process inside the client container.
// The PID comes from the actor's receipts; /proc/<pid>/cmdline must carry the
// run's token, so no other process is killed. It waits, bounded, until
// RunNativeCommand returns and the process is gone, and returns how the
// command ended.
func killActor(h *harness.CephFSUnsafeHold, t *testing.T, ctx context.Context) string {
	t.Helper()
	pid := strconv.Itoa(h.Baseline.PID)
	cmdline := harness.ExecOutput(t, ctx, h.Client, "cat", "/proc/"+pid+"/cmdline")
	if args := strings.Split(cmdline, "\x00"); !slices.Contains(args, h.Actor.Token) || !strings.HasPrefix(args[0], "python3") {
		t.Fatalf("PID %s is not this run's actor: %q", pid, cmdline)
	}
	harness.ExecOutput(t, ctx, h.Client, "sh", "-c", `kill -9 "$1"`, "sh", pid)
	select {
	case <-h.Actor.Done:
	case <-time.After(time.Minute):
		t.Fatal("RunNativeCommand did not return after the actor was killed")
	}
	if code, _, err := harness.SessionExec(ctx, h.Client, "test", "-e", "/proc/"+pid); err != nil || code == 0 {
		t.Fatalf("killed actor PID %s is still present: code=%d err=%v", pid, code, err)
	}
	// The supervisor reports a signal as its negative number.
	exited := fmt.Sprintf("exit=%d timed_out=%t err=%v", h.Actor.Result.ExitCode, h.Actor.Result.TimedOut, h.Actor.Err)
	if h.Actor.Err != nil || h.Actor.Result.ExitCode != -9 || h.Actor.Result.TimedOut {
		t.Fatalf("killed actor did not end by SIGKILL: %s", exited)
	}
	return exited
}

// testCephFSClientKilledDuringUnsafeRequest holds one rename unsafe
// (HoldCephFSUnsafeRename) and SIGKILLs the client's process, so the client
// never sees the safe reply. Session timeouts are shortened to 30 s and 60 s
// through session.OverrideTimeouts before the kill. The MDS keeps the request
// in flight, unsafe, while the pause holds its journal. After the pause is
// restored the MDS journals the request and replies to the dead session; the
// MDS keeps its GID. The session stays open until autoclose, as for an idle
// client, and the MDS then evicts it and blocklists the client's address.
// An independent client finds the new name with the original inode and no
// old name.
func testCephFSClientKilledDuringUnsafeRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	h := harness.HoldCephFSUnsafeRename(t, ctx)
	tid := h.Held.Tid

	short := cephfssession.Limits{Timeout: 30 * time.Second, Autoclose: time.Minute}
	change, err := cephfssession.OverrideTimeouts(ctx, h.FS, short)
	if change != nil {
		t.Cleanup(func() {
			restoreCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := change.Restore(restoreCtx); err != nil {
				t.Errorf("restore session timeouts: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal("shorten session timeouts:", err)
	}
	session := clientSession(h, t, ctx)
	if session == nil || session.State != "open" || session.Address == "" {
		t.Fatalf("client.%d has no open session before the kill: %+v", h.Baseline.Client, session)
	}
	before := harness.CephFSRawSessionSummary(harness.CephFSRawSession(t, ctx, h.Cluster, h.MDS.Name, h.Baseline.Client))

	exited := killActor(h, t, ctx)
	killed := time.Now()

	// The pause still holds the journal: the dead client's request stays in
	// flight after its early reply.
	time.Sleep(3 * time.Second)
	held, inFlight := mdsOpsInFlight(h, t, ctx, tid)
	if !inFlight || harness.RequestSafety(held) != "unsafe" {
		t.Fatalf("after the kill the MDS does not hold rename %d unsafe: found=%t %q %v", tid, inFlight, held.Description, held.Events())
	}
	afterKill := clientSession(h, t, ctx)
	if afterKill == nil {
		t.Fatalf("the MDS dropped client.%d's session at the kill", h.Baseline.Client)
	}
	killedState := afterKill.State

	if err := h.Pause.Restore(ctx); err != nil {
		t.Fatal("restore mds_log_pause:", err)
	}
	h.WaitDaemonConfig(t, ctx, "mds_log_pause", "false")
	restored := time.Since(killed)
	var committed harness.MDSRequestOp
	deadline := time.Now().Add(time.Minute)
	for {
		_, flight := mdsOpsInFlight(h, t, ctx, tid)
		_, op, found := h.HistoricRename(t, ctx, h.MDS.Name, tid)
		if !flight && found && harness.RequestSafety(op) == "safe" {
			committed = op
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the dead client's rename did not complete: in flight=%t historic found=%t %q %v", flight, found, op.Description, op.Events())
		}
		time.Sleep(500 * time.Millisecond)
	}
	completed := time.Since(killed)

	// The session's states until the MDS drops it, bounded well past the
	// shortened autoclose, each sample with its time after the kill and the
	// session's address.
	var samples []killedSessionSample
	var evicted time.Duration
	deadline = time.Now().Add(3 * time.Minute)
	for {
		current := clientSession(h, t, ctx)
		if current == nil {
			evicted = time.Since(killed)
			break
		}
		samples = append(samples, killedSessionSample{State: current.State, Address: current.Address, At: time.Since(killed)})
		if time.Now().After(deadline) {
			t.Fatalf("the MDS kept the killed client's session, samples %v", samples)
		}
		time.Sleep(2 * time.Second)
	}
	states := killedSessionStates(samples)
	// Nothing contends for the dead client's capabilities, so the MDS keeps
	// the session open and evicts it at autoclose, as for an idle client:
	// kill_session journals the close in the killing state, and the session
	// is gone once the close is safe. The state contract and the deadline are
	// judged and reported separately.
	stateProblem, deadlineProblem := judgeKilledSession(samples, session.Address, evicted, short.Timeout, short.Autoclose+15*time.Second)
	if stateProblem != "" {
		t.Errorf("killed client's session states: %s (samples %v)", stateProblem, samples)
	}
	if deadlineProblem != "" {
		t.Errorf("killed client's session eviction: %s", deadlineProblem)
	}
	if t.Failed() {
		t.FailNow()
	}
	entries, err := harness.ObserveCluster(h.Cluster).BlocklistEntries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == session.Address }) {
		t.Fatalf("killed client %s is not blocklisted: %+v", session.Address, entries)
	}
	after, err := h.FS.MDSStatus(ctx)
	if err != nil || len(after.Active) != 1 || after.Active[0].GID != h.MDS.GID {
		t.Fatalf("mds.%s changed: %+v, %v", h.MDS.Name, after, err)
	}
	reader := h.ReadNamespace(t, ctx)
	t.Logf("CEPHFS_UNSAFE_KILL %s mds.%s gid=%d client.%d tid=%d inode=%d: rename returned in %d ms and was held unsafe on both sides; session before the kill %s; actor PID %d killed with SIGKILL (%s); 3 s later the session was %q and the MDS still held the request unsafe (events %v); pause restored %s after the kill; the request committed and replied by %s (events %v, %.1f s in the MDS); session states %q, gone %s after the kill with session_timeout=%s session_autoclose=%s; address %s blocklisted; reader client.%d found the target and ENOENT for the source",
		h.Version, h.MDS.Name, h.MDS.GID, h.Baseline.Client, tid, h.Baseline.Inode, h.Renamed.ElapsedMS, before, h.Baseline.PID, exited,
		killedState, held.Events(), restored.Round(time.Second), completed.Round(time.Second), committed.Events(), committed.Duration,
		states, evicted.Round(time.Second), short.Timeout, short.Autoclose, session.Address, reader)
}

// killedSessionSample is one observation of the killed client's MDS session:
// its state and address, and the time since the kill.
type killedSessionSample struct {
	State   string
	Address string
	At      time.Duration
}

func (s killedSessionSample) String() string {
	return fmt.Sprintf("%s@%s(%s)", s.State, s.At.Round(100*time.Millisecond), s.Address)
}

// killedSessionStates is the distinct states of samples, in order.
func killedSessionStates(samples []killedSessionSample) []string {
	var states []string
	for _, s := range samples {
		if len(states) == 0 || states[len(states)-1] != s.State {
			states = append(states, s.State)
		}
	}
	return states
}

// judgeKilledSession judges the killed client's session against the
// autoclose eviction of the MDS (Server::find_idle_sessions, then
// kill_session, which journals the close in STATE_KILLING and drops the
// session when the close is safe). The state contract: every sample is the
// session of address; it is open, then possibly killing, never anything else
// and never open again after killing; and killing starts no earlier than
// timeout, since the eviction cannot start before the session timeout. The
// deadline: the session is gone between timeout and latest. Each is returned
// on its own, so that a state failure is never read as a late eviction or the
// other way around.
func judgeKilledSession(samples []killedSessionSample, address string, gone, timeout, latest time.Duration) (stateProblem, deadlineProblem string) {
	killing := false
	for _, s := range samples {
		switch {
		case s.Address != address:
			stateProblem = fmt.Sprintf("a sample at %s has address %s, not the killed session's %s", s.At.Round(time.Second), s.Address, address)
		case s.State == "open" && killing:
			stateProblem = fmt.Sprintf("open again at %s after killing", s.At.Round(time.Second))
		case s.State == "killing" && !killing && s.At < timeout:
			stateProblem = fmt.Sprintf("killing at %s, before the session timeout %s", s.At.Round(time.Second), timeout)
		case s.State == "killing":
			killing = true
		case s.State != "open":
			stateProblem = fmt.Sprintf("state %q at %s is not on the autoclose eviction path", s.State, s.At.Round(time.Second))
		}
		if stateProblem != "" {
			break
		}
	}
	if stateProblem == "" && (len(samples) == 0 || samples[0].State != "open") {
		stateProblem = "the session was not open after the kill"
	}
	if gone < timeout || gone > latest {
		deadlineProblem = fmt.Sprintf("gone %s after the kill, want between %s and %s", gone.Round(time.Second), timeout, latest)
	}
	return stateProblem, deadlineProblem
}
