//go:build all || integration

package integration_test

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
)

// mdsOpsInFlight reads an MDS's dump_ops_in_flight and finds the client's
// rename with tid in it.
func (h *cephFSUnsafeHold) mdsOpsInFlight(t *testing.T, ctx context.Context, tid uint64) (mdsRequestOp, bool) {
	t.Helper()
	data, err := h.cluster.Ceph(ctx, "tell", "mds."+h.mds.Name, "dump_ops_in_flight")
	if err != nil {
		t.Fatalf("dump_ops_in_flight of mds.%s: %v", h.mds.Name, err)
	}
	ops, err := parseMDSRequestOps(data)
	if err != nil {
		t.Fatalf("dump_ops_in_flight of mds.%s: %v: %.1024s", h.mds.Name, err, data)
	}
	return findMDSRequest(ops, h.baseline.Client, "rename", tid)
}

// clientSession returns the actor client's session, or nil when the MDS has
// none.
func (h *cephFSUnsafeHold) clientSession(t *testing.T, ctx context.Context) *cephfs.Session {
	t.Helper()
	sessions, err := h.fs.Sessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, session := range sessions {
		if session.ID == h.baseline.Client {
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
func (h *cephFSUnsafeHold) killActor(t *testing.T, ctx context.Context) string {
	t.Helper()
	pid := strconv.Itoa(h.baseline.PID)
	cmdline := execOutput(t, ctx, h.client, "cat", "/proc/"+pid+"/cmdline")
	if args := strings.Split(cmdline, "\x00"); !slices.Contains(args, h.actor.token) || !strings.HasPrefix(args[0], "python3") {
		t.Fatalf("PID %s is not this run's actor: %q", pid, cmdline)
	}
	execOutput(t, ctx, h.client, "sh", "-c", `kill -9 "$1"`, "sh", pid)
	select {
	case <-h.actor.done:
	case <-time.After(time.Minute):
		t.Fatal("RunNativeCommand did not return after the actor was killed")
	}
	if code, _, err := sessionExec(ctx, h.client, "test", "-e", "/proc/"+pid); err != nil || code == 0 {
		t.Fatalf("killed actor PID %s is still present: code=%d err=%v", pid, code, err)
	}
	// The supervisor reports a signal as its negative number.
	exited := fmt.Sprintf("exit=%d timed_out=%t err=%v", h.actor.result.ExitCode, h.actor.result.TimedOut, h.actor.err)
	if h.actor.err != nil || h.actor.result.ExitCode != -9 || h.actor.result.TimedOut {
		t.Fatalf("killed actor did not end by SIGKILL: %s", exited)
	}
	return exited
}

// testCephFSClientKilledDuringUnsafeRequest holds one rename unsafe
// (holdCephFSUnsafeRename) and SIGKILLs the client's process, so the client
// never sees the safe reply. Session timeouts are shortened to 30 s and 60 s
// through OverrideSessionTimeouts before the kill. The MDS keeps the request
// in flight, unsafe, while the pause holds its journal. After the pause is
// restored the MDS journals the request and replies to the dead session; the
// MDS keeps its GID. The session stays open until autoclose, as for an idle
// client, and the MDS then evicts it and blocklists the client's address.
// An independent client finds the new name with the original inode and no
// old name.
func testCephFSClientKilledDuringUnsafeRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	h := holdCephFSUnsafeRename(t, ctx)
	tid := h.held.tid

	short := cephfs.SessionTimeouts{Timeout: 30 * time.Second, Autoclose: time.Minute}
	change, err := h.fs.OverrideSessionTimeouts(ctx, short)
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
	session := h.clientSession(t, ctx)
	if session == nil || session.State != "open" || session.Address == "" {
		t.Fatalf("client.%d has no open session before the kill: %+v", h.baseline.Client, session)
	}
	before := cephFSRawSessionSummary(cephFSRawSession(t, ctx, h.cluster, h.mds.Name, h.baseline.Client))

	exited := h.killActor(t, ctx)
	killed := time.Now()

	// The pause still holds the journal: the dead client's request stays in
	// flight after its early reply.
	time.Sleep(3 * time.Second)
	held, inFlight := h.mdsOpsInFlight(t, ctx, tid)
	if !inFlight || requestSafety(held) != "unsafe" {
		t.Fatalf("after the kill the MDS does not hold rename %d unsafe: found=%t %q %v", tid, inFlight, held.Description, held.events())
	}
	afterKill := h.clientSession(t, ctx)
	if afterKill == nil {
		t.Fatalf("the MDS dropped client.%d's session at the kill", h.baseline.Client)
	}
	killedState := afterKill.State

	if err := h.pause.Restore(ctx); err != nil {
		t.Fatal("restore mds_log_pause:", err)
	}
	h.waitDaemonConfig(t, ctx, "mds_log_pause", "false")
	restored := time.Since(killed)
	var committed mdsRequestOp
	deadline := time.Now().Add(time.Minute)
	for {
		_, flight := h.mdsOpsInFlight(t, ctx, tid)
		_, op, found := h.historicRename(t, ctx, h.mds.Name, tid)
		if !flight && found && requestSafety(op) == "safe" {
			committed = op
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the dead client's rename did not complete: in flight=%t historic found=%t %q %v", flight, found, op.Description, op.events())
		}
		time.Sleep(500 * time.Millisecond)
	}
	completed := time.Since(killed)

	// The session's states until the MDS drops it, bounded well past the
	// shortened autoclose.
	var states []string
	var evicted time.Duration
	deadline = time.Now().Add(3 * time.Minute)
	for {
		current := h.clientSession(t, ctx)
		if current == nil {
			evicted = time.Since(killed)
			break
		}
		if !slices.Contains(states, current.State) {
			states = append(states, current.State)
		}
		if time.Now().After(deadline) {
			t.Fatalf("the MDS kept the killed client's session, states %q", states)
		}
		time.Sleep(2 * time.Second)
	}
	// Nothing contends for the dead client's capabilities, so the MDS keeps
	// the session open and closes it at autoclose, as for an idle client.
	if !slices.Equal(states, []string{"open"}) || evicted < short.Timeout || evicted > short.Autoclose+15*time.Second {
		t.Fatalf("killed client's session: states %q, gone %s after the kill, want only open and gone between %s and %s", states, evicted, short.Timeout, short.Autoclose)
	}
	entries, err := observeCluster(h.cluster).BlocklistEntries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == session.Address }) {
		t.Fatalf("killed client %s is not blocklisted: %+v", session.Address, entries)
	}
	after, err := h.fs.MDSStatus(ctx)
	if err != nil || len(after.Active) != 1 || after.Active[0].GID != h.mds.GID {
		t.Fatalf("mds.%s changed: %+v, %v", h.mds.Name, after, err)
	}
	reader := h.readNamespace(t, ctx)
	t.Logf("CEPHFS_UNSAFE_KILL %s mds.%s gid=%d client.%d tid=%d inode=%d: rename returned in %d ms and was held unsafe on both sides; session before the kill %s; actor PID %d killed with SIGKILL (%s); 3 s later the session was %q and the MDS still held the request unsafe (events %v); pause restored %s after the kill; the request committed and replied by %s (events %v, %.1f s in the MDS); session states %q, gone %s after the kill with session_timeout=%s session_autoclose=%s; address %s blocklisted; reader client.%d found the target and ENOENT for the source",
		h.version, h.mds.Name, h.mds.GID, h.baseline.Client, tid, h.baseline.Inode, h.renamed.ElapsedMS, before, h.baseline.PID, exited,
		killedState, held.events(), restored.Round(time.Second), completed.Round(time.Second), committed.events(), committed.Duration,
		states, evicted.Round(time.Second), short.Timeout, short.Autoclose, session.Address, reader)
}
