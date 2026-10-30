//go:build all || integration

package cephfs_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
)

// cephFSStandbyFailover is a cluster whose filesystem has one active, owned
// MDS and one owned standby.
type cephFSStandbyFailover struct {
	*harness.CephFSSingleMDS
	standby cephfs.MDSStatus
}

func startCephFSStandbyFailover(t *testing.T, ctx context.Context) *cephFSStandbyFailover {
	t.Helper()
	m := harness.StartCephFSSingleMDS(t, ctx, cephfs.WithFilesystems(cephfs.Config{StandbyMDS: 1}))
	status, err := m.FS.MDSStatus(ctx)
	if err != nil || len(status.Standby) != 1 || len(status.StandbyReplay) != 0 || !status.Standby[0].Owned || !m.MDS.Owned || len(m.FS.MDSs()) != 2 {
		t.Fatalf("want one owned active MDS and one owned standby: %+v, %v", status, err)
	}
	return &cephFSStandbyFailover{CephFSSingleMDS: m, standby: status.Standby[0]}
}

// failToStandby SIGKILLs the active MDS, which leaves it no chance to hand
// off its rank or flush anything, and fails its GID. It observes the standby
// take rank 0 with its own GID and become active, and from then on m.mds is
// the new active MDS. It returns the killed daemon's status and how long the
// takeover took. The killed daemon stays stopped until the fixture ends.
func (m *cephFSStandbyFailover) failToStandby(t *testing.T, ctx context.Context) (cephfs.MDSStatus, time.Duration) {
	t.Helper()
	old := m.MDS
	var daemon *cephfs.MDS
	for _, member := range m.FS.MDSs() {
		if member.ID == old.Name {
			daemon = member
		}
	}
	if daemon == nil {
		t.Fatalf("active mds.%s has no owned daemon: %+v", old.Name, m.FS.MDSs())
	}
	kill := time.Duration(0)
	if err := daemon.Stop(ctx, &kill); err != nil {
		t.Fatal("kill MDS:", err)
	}
	if state, err := daemon.State(ctx); err != nil || state.Running {
		t.Fatalf("killed MDS still runs: %+v, %v", state, err)
	}
	if _, err := m.Cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(old.GID, 10)); err != nil {
		t.Fatal("fail MDS GID:", err)
	}
	failed := time.Now()
	seen, err := harness.Observe(ctx, 3*time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) (*cephfs.FilesystemStatus, bool, error) {
		status, err := m.FS.MDSStatus(probeCtx)
		if err != nil {
			return nil, false, nil
		}
		active, ok := harness.ActiveRank(status, 0)
		return status, ok && len(status.Active) == 1 && active.State == "up:active" && active.GID != old.GID, nil
	})
	if err != nil {
		t.Fatalf("the standby did not take rank 0: last %+v: %v", seen.Value, err)
	}
	current, _ := harness.ActiveRank(seen.Value, 0)
	if current.Name != m.standby.Name || current.GID != m.standby.GID || !current.Owned {
		t.Fatalf("rank 0 is not served by standby mds.%s (GID %d): %+v", m.standby.Name, m.standby.GID, seen.Value)
	}
	m.MDS = current
	return old, time.Since(failed)
}

// noEviction requires that every client keeps its open rank 0 session, now
// on the current MDS, with the address and PID it had before, and that no
// blocklist entry names its address.
func (m *cephFSStandbyFailover) noEviction(t *testing.T, ctx context.Context, before map[uint64]cephfs.Session) {
	t.Helper()
	entries, err := harness.ObserveCluster(m.Cluster).BlocklistEntries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for client, session := range before {
		if after := harness.CephFSRetainedSession(t, ctx, m.FS, client, 0); after.Address != session.Address || after.PID != session.PID {
			t.Fatalf("client.%d session %+v, before %+v", client, after, session)
		}
		if slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == session.Address }) {
			t.Fatalf("client.%d %s was blocklisted: %+v", client, session.Address, entries)
		}
	}
}

// historicFlock finds client's setfilelock request in the current MDS's
// dump_historic_ops, so that the test knows which daemon answered it. The MDS
// replies before it cleans the request up and moves it into its history, so
// a dump read right after the client's reply can still lack it; the history
// is observed until the request is there.
func (m *cephFSStandbyFailover) historicFlock(t *testing.T, ctx context.Context, client uint64) harness.MDSRequestOp {
	t.Helper()
	var last []byte
	seen, err := harness.Observe(ctx, time.Minute, 30*time.Second, 500*time.Millisecond, func(probeCtx context.Context) (harness.MDSRequestOp, bool, error) {
		data, err := m.Cluster.Ceph(probeCtx, "tell", "mds."+m.MDS.Name, "dump_historic_ops")
		if err != nil {
			return harness.MDSRequestOp{}, false, err
		}
		last = data
		ops, err := harness.ParseMDSRequestOps(data)
		if err != nil {
			return harness.MDSRequestOp{}, false, fmt.Errorf("dump_historic_ops: %v: %.1024s", err, data)
		}
		op, found := harness.FindMDSRequest(ops, client, "setfilelock", 0)
		return op, found, nil
	})
	if err != nil {
		t.Fatalf("mds.%s has no setfilelock of client.%d in its history: %v: %.2048s", m.MDS.Name, client, err, last)
	}
	return seen.Value
}

// testCephFSFlockAcrossMDSFailover keeps a flock across an MDS failover. Two
// live clients, in two containers, open one file; the holder takes an
// exclusive flock and the contender's non-blocking one returns EWOULDBLOCK.
// The active MDS is SIGKILLed and the standby takes rank 0. It never saw the
// lock, so it can only know it from the clients' reconnects, which carry
// their flock state. On the new MDS, with the same sessions, the contender's
// non-blocking lock again returns EWOULDBLOCK, and the new MDS's own history
// holds that request. The contender's blocking lock waits in the new MDS's
// dump_ops_in_flight at "failed to add lock, waiting" without returning; when
// the holder unlocks, the lock is granted and leaves the MDS, and the former
// holder's non-blocking lock returns EWOULDBLOCK. Neither client is evicted or
// blocklisted.
func testCephFSFlockAcrossMDSFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	m := startCephFSStandbyFailover(t, ctx)
	const path = "/failover-locks/file"
	holder := startFlockActor(t, ctx, m.CephFSSingleMDS, m.Client, "/tmp/failover-holder", path, "ex-nb", "un", "ex-nb")
	contender := startFlockActor(t, ctx, m.CephFSSingleMDS, harness.ToolkitContainer(t, m.Cluster), "/tmp/failover-contender", path, "ex-nb", "ex-nb", "ex", "un")
	if holder.client == contender.client || holder.inode != contender.inode {
		t.Fatalf("holder client.%d inode %d, contender client.%d inode %d", holder.client, holder.inode, contender.client, contender.inode)
	}
	sessions := map[uint64]cephfs.Session{
		holder.client:    harness.CephFSRetainedSession(t, ctx, m.FS, holder.client, 0),
		contender.client: harness.CephFSRetainedSession(t, ctx, m.FS, contender.client, 0),
	}

	if locked := holder.run(t, ctx, 0); locked.Errno != 0 {
		t.Fatalf("holder's exclusive lock: %+v", locked)
	}
	if refused := contender.run(t, ctx, 0); refused.Errno != 11 {
		t.Fatalf("contender's non-blocking lock before the failover: %+v, want EWOULDBLOCK (11)", refused)
	}
	before := m.historicFlock(t, ctx, contender.client)

	old, took := m.failToStandby(t, ctx)
	m.noEviction(t, ctx, sessions)
	raw := map[uint64]string{}
	for client := range sessions {
		raw[client] = harness.CephFSRawSessionSummary(harness.CephFSRawSession(t, ctx, m.Cluster, m.MDS.Name, client))
	}

	refused := contender.run(t, ctx, 1)
	if refused.Errno != 11 {
		t.Fatalf("contender's non-blocking lock on the new mds.%s: %+v, want EWOULDBLOCK (11): the holder's lock was lost in the failover", m.MDS.Name, refused)
	}
	answered := m.historicFlock(t, ctx, contender.client)
	inode := fmt.Sprintf("#0x%x ", holder.inode)
	if !strings.Contains(answered.Description, "rule 2, type 2,") || !strings.Contains(answered.Description, "wait 0 ") || !strings.Contains(answered.Description, inode) {
		t.Fatalf("new mds.%s did not answer the contender's non-blocking flock on inode %d: %q", m.MDS.Name, holder.inode, answered.Description)
	}

	contender.start(t, ctx, 2)
	waiting := mdsWaitingFlock(t, ctx, m.CephFSSingleMDS, contender.client, true)
	if waiting.flagPoint != "failed to add lock, waiting" || !strings.Contains(waiting.description, "rule 2, type 2,") ||
		!strings.Contains(waiting.description, "wait 1 ") || !strings.Contains(waiting.description, inode) {
		t.Fatalf("new mds.%s does not hold client.%d's exclusive flock waiting on inode %d: %+v", m.MDS.Name, contender.client, holder.inode, waiting)
	}
	// The receipt is checked after the MDS showed the request waiting, so a
	// lock granted at once would already have published it.
	if contender.Published(ctx, 4) {
		t.Fatal("the contender's blocking lock returned while the holder held the lock")
	}
	if released := holder.run(t, ctx, 1); released.Errno != 0 {
		t.Fatalf("holder's unlock: %+v", released)
	}
	granted := contender.Receipt(t, ctx, 4)
	if granted.Errno != 0 {
		t.Fatalf("contender's blocking lock: %+v, want 0 after the unlock", granted)
	}
	mdsWaitingFlock(t, ctx, m.CephFSSingleMDS, contender.client, false)
	reversed := holder.run(t, ctx, 2)
	if reversed.Errno != 11 {
		t.Fatalf("former holder's non-blocking lock while the contender holds it: %+v, want EWOULDBLOCK (11)", reversed)
	}
	if released := contender.run(t, ctx, 3); released.Errno != 0 {
		t.Fatalf("contender's unlock: %+v", released)
	}
	m.noEviction(t, ctx, sessions)
	holder.close(t, ctx)
	contender.close(t, ctx)
	t.Logf("CEPHFS_FLOCK_FAILOVER %s inode=%d: holder client.%d LOCK_EX|LOCK_NB=0 on mds.%s/%d, contender client.%d refused there (%q); mds.%s SIGKILLed, standby mds.%s/%d active after %s; sessions kept: holder %s, contender %s; the new MDS refused the contender's LOCK_EX|LOCK_NB with 11 (%q), held its blocking LOCK_EX as %q at %q (events %v) and granted it after %d ms, once the holder unlocked; former holder then refused with %d; neither client blocklisted",
		m.Version, holder.inode, holder.client, old.Name, old.GID, contender.client, before.Description, old.Name, m.MDS.Name, m.MDS.GID, took.Round(time.Second),
		raw[holder.client], raw[contender.client], answered.Description, waiting.description, waiting.flagPoint, waiting.events, granted.ElapsedMS, reversed.Errno)
}

// mdsLogCounters is part of an MDS's "mds_log" perf counters: events
// submitted and replayed, and the journal's write and expire positions.
type mdsLogCounters struct {
	Submitted *uint64 `json:"evadd"`
	Replayed  *uint64 `json:"replayed"`
	WritePos  *uint64 `json:"wrpos"`
	ExpirePos *uint64 `json:"expos"`
}

func mdsLog(m *harness.CephFSSingleMDS, t *testing.T, ctx context.Context) mdsLogCounters {
	t.Helper()
	out, err := m.Cluster.Ceph(ctx, "tell", "mds."+m.MDS.Name, "perf", "dump", "mds_log")
	if err != nil {
		t.Fatalf("perf dump mds_log of mds.%s: %v", m.MDS.Name, err)
	}
	var dump struct {
		Log *mdsLogCounters `json:"mds_log"`
	}
	if err := json.Unmarshal(out, &dump); err != nil || dump.Log == nil || dump.Log.Submitted == nil || dump.Log.Replayed == nil || dump.Log.WritePos == nil || dump.Log.ExpirePos == nil {
		t.Fatalf("perf dump mds_log of mds.%s: %v: %.1024s", m.MDS.Name, err, out)
	}
	return *dump.Log
}

func (c mdsLogCounters) String() string {
	return fmt.Sprintf("evadd=%d replayed=%d wrpos=%d expos=%d", *c.Submitted, *c.Replayed, *c.WritePos, *c.ExpirePos)
}

// dirfragDentries lists the dentries that the metadata pool holds for an
// unfragmented directory: the omap keys of its dirfrag object, without the
// "_head" suffix. It reads RADOS directly, not through an MDS.
func dirfragDentries(m *harness.CephFSSingleMDS, t *testing.T, ctx context.Context, directory uint64) []string {
	t.Helper()
	object := fmt.Sprintf("%x.00000000", directory)
	out := harness.ExecOutput(t, ctx, m.Client, "rados", "-p", m.FS.MetadataPool, "listomapkeys", object)
	var names []string
	for _, key := range strings.Fields(out) {
		name, ok := strings.CutSuffix(key, "_head")
		if !ok {
			t.Fatalf("dirfrag %s has key %q", object, key)
		}
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// flushJournal makes the current MDS flush its journal and write every dirty
// object back to the metadata pool.
func flushJournal(m *harness.CephFSSingleMDS, t *testing.T, ctx context.Context) {
	t.Helper()
	if out, err := m.Cluster.Ceph(ctx, "tell", "mds."+m.MDS.Name, "flush", "journal"); err != nil {
		t.Fatalf("flush journal on mds.%s: %v: %.512s", m.MDS.Name, err, out)
	}
}

// testCephFSJournaledRenameAcrossMDSCrash crashes the MDS after one rename
// reached its journal and before the MDS wrote it back to the directory
// object. A live client creates and fsyncs a file in a new directory, and the
// MDS flushes its journal, so the directory's dirfrag object in the metadata
// pool holds the source name. The client renames the file once. The MDS's
// history shows the rename journaled ("journal_committed") and replied, the
// client holds no pending request, and the MDS's journal write position has
// moved, while the dirfrag object still holds only the source name: the
// rename exists in the journal alone. The MDS is then SIGKILLed and the
// standby takes rank 0; its own counters show it replayed journal events from
// an expire position before the rename's entry to at least the killed
// daemon's write position. The client keeps its session, finds
// the target through its own mount, and an independent client finds the
// target with the original inode and no source. After the new MDS flushes,
// the dirfrag object holds the target name instead of the source.
func testCephFSJournaledRenameAcrossMDSCrash(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	m := startCephFSStandbyFailover(t, ctx)
	const workdir = "/tmp/journaled-actor"
	h := &harness.CephFSUnsafeHold{CephFSSingleMDS: m.CephFSSingleMDS, Asok: workdir + "/client.asok"}
	h.Actor = harness.StartCephFSActor(t, ctx, m.Cluster, m.Client, harness.CephFSUnsafeActor, m.FS.FilesystemName, workdir, "/unsafe", "/unsafe/source", "/unsafe/target", h.Asok)
	h.Baseline = h.Actor.Receipt(t, ctx, 1)
	if h.Baseline.Client == 0 || h.Baseline.Inode == 0 || h.Baseline.DirInode == 0 || h.Baseline.Size != int64(len("unsafe-window")) {
		t.Fatalf("baseline receipt = %+v", h.Baseline)
	}
	session := harness.CephFSRetainedSession(t, ctx, m.FS, h.Baseline.Client, 0)
	flushJournal(m.CephFSSingleMDS, t, ctx)
	if written := dirfragDentries(m.CephFSSingleMDS, t, ctx, h.Baseline.DirInode); !slices.Equal(written, []string{"source"}) {
		t.Fatalf("after a journal flush the dirfrag of directory %d holds %v, want [source]", h.Baseline.DirInode, written)
	}
	logBefore := mdsLog(m.CephFSSingleMDS, t, ctx)

	h.Actor.Open(t, ctx, "rename")
	h.Renamed = h.Actor.Receipt(t, ctx, 2)
	if h.Renamed.Errno != 0 || h.Renamed.Error != "" || h.Renamed.PID != h.Baseline.PID {
		t.Fatalf("rename receipt = %+v", h.Renamed)
	}
	// The rename is journaled when the MDS's history has it committed and
	// replied and the client holds no request any more.
	type journaled struct {
		op      harness.MDSRequestOp
		pending []harness.ClientMDSRequest
	}
	seen, err := harness.Observe(ctx, time.Minute, 30*time.Second, 500*time.Millisecond, func(probeCtx context.Context) (journaled, bool, error) {
		data, err := m.Cluster.Ceph(probeCtx, "tell", "mds."+m.MDS.Name, "dump_historic_ops")
		if err != nil {
			return journaled{}, false, err
		}
		ops, err := harness.ParseMDSRequestOps(data)
		if err != nil {
			return journaled{}, false, fmt.Errorf("%w: %.512s", err, data)
		}
		requests, err := harness.QueryClientMDSRequests(probeCtx, m.Cluster, m.Client, h.Asok)
		if err != nil {
			return journaled{}, false, err
		}
		op, found := harness.FindMDSRequest(ops, h.Baseline.Client, "rename", 0)
		state := journaled{op: op, pending: requests}
		return state, found && harness.RenameSafe(harness.JoinUnsafeRename(nil, requests, h.Baseline.Client, op.TypeData.ReqID.TID), ops, h.Baseline.Client), nil
	})
	if err != nil {
		t.Fatalf("the rename was not journaled and replied: last %q %v, client %+v: %v", seen.Value.op.Description, seen.Value.op.Events(), seen.Value.pending, err)
	}
	committed := seen.Value.op
	tid := committed.TypeData.ReqID.TID
	logAfter := mdsLog(m.CephFSSingleMDS, t, ctx)
	if *logAfter.Submitted <= *logBefore.Submitted || *logAfter.WritePos <= *logBefore.WritePos {
		t.Fatalf("mds.%s journal did not grow with the rename: before %s, after %s", m.MDS.Name, logBefore, logAfter)
	}
	if written := dirfragDentries(m.CephFSSingleMDS, t, ctx, h.Baseline.DirInode); !slices.Equal(written, []string{"source"}) {
		t.Fatalf("the rename reached the dirfrag of directory %d before the crash: %v", h.Baseline.DirInode, written)
	}

	old, took := m.failToStandby(t, ctx)
	replayed := mdsLog(m.CephFSSingleMDS, t, ctx)
	// The replay starts at the journal's expire position, which must lie
	// before the rename's entry, and ends at or past its end.
	if *replayed.Replayed == 0 || *replayed.ExpirePos > *logBefore.WritePos || *replayed.WritePos < *logAfter.WritePos {
		t.Fatalf("new mds.%s did not replay the journal up to %d: %s", m.MDS.Name, *logAfter.WritePos, replayed)
	}
	m.noEviction(t, ctx, map[uint64]cephfs.Session{h.Baseline.Client: session})
	raw := harness.CephFSRawSessionSummary(harness.CephFSRawSession(t, ctx, m.Cluster, m.MDS.Name, h.Baseline.Client))
	h.Finish(t, ctx)
	reader := h.ReadNamespace(t, ctx)
	flushJournal(m.CephFSSingleMDS, t, ctx)
	written := dirfragDentries(m.CephFSSingleMDS, t, ctx, h.Baseline.DirInode)
	if !slices.Equal(written, []string{"target"}) {
		t.Fatalf("after the new MDS flushed, the dirfrag of directory %d holds %v, want [target]", h.Baseline.DirInode, written)
	}
	t.Logf("CEPHFS_JOURNALED_CRASH %s client.%d tid=%d inode=%d dir=%d: rename returned in %d ms; mds.%s/%d journaled and replied it (events %v), journal %s -> %s, dirfrag still [source]; mds.%s SIGKILLed, standby mds.%s/%d active after %s with %s; client session kept (%s); client found the target through its own mount; reader client.%d found the target and ENOENT for the source; dirfrag after flush %v",
		m.Version, h.Baseline.Client, tid, h.Baseline.Inode, h.Baseline.DirInode, h.Renamed.ElapsedMS, old.Name, old.GID, committed.Events(), logBefore, logAfter,
		old.Name, m.MDS.Name, m.MDS.GID, took.Round(time.Second), replayed, raw, reader, written)
}
