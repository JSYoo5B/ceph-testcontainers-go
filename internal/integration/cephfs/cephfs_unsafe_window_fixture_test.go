//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// MDS and client dumps saved from TestCephFSUnsafeRequestWindow runs on
// quay.io/ceph/ceph:v20.2.4, and the client dump on v19.2.5. Only the fields
// that the parsers read are kept; the values are native. Client 4239's TID 2
// is the actor's mkdir, TID 3 its create and TID 4 its rename.
const (
	// The rename while mds_log_pause holds the journal.
	unsafeInFlightSample = `{"ops": [{"description": "client_request(client.4239:4 rename #0x10000000000/target #0x10000000000/source 2026-10-15T15:51:25.344400+0000 caller_uid=0, caller_gid=0{})", "duration": 0.538486, "type_data": {"flag_point": "submit entry: journal_and_reply", "reqid": {"entity": {"type": "client", "num": 4239}, "tid": 4}, "events": [{"event": "initiated"}, {"event": "header_read"}, {"event": "throttled"}, {"event": "all_read"}, {"event": "dispatched"}, {"event": "acquired locks"}, {"event": "acquired locks"}, {"event": "early_replied"}, {"event": "submit entry: journal_and_reply"}]}}], "num_ops": 1}`
	// The same rename in dump_historic_ops after the pause was restored, and
	// the actor's earlier mkdir, which allocated an inode and got no early
	// reply.
	unsafeHistoricSample = `{"ops": [{"description": "client_request(client.4239:2 mkdir owner_uid=0, owner_gid=0 #0x1/unsafe 2026-10-15T15:51:24.112615+0000 caller_uid=0, caller_gid=0{})", "duration": 0.002597, "type_data": {"flag_point": "done", "reqid": {"entity": {"type": "client", "num": 4239}, "tid": 2}, "events": [{"event": "initiated"}, {"event": "header_read"}, {"event": "throttled"}, {"event": "all_read"}, {"event": "dispatched"}, {"event": "acquired locks"}, {"event": "submit entry: journal_and_reply"}, {"event": "journal_committed: "}, {"event": "replying"}, {"event": "finishing request"}, {"event": "cleaned up request"}, {"event": "done"}]}}, {"description": "client_request(client.4239:4 rename #0x10000000000/target #0x10000000000/source 2026-10-15T15:51:25.344400+0000 caller_uid=0, caller_gid=0{})", "duration": 7.2515, "type_data": {"flag_point": "done", "reqid": {"entity": {"type": "client", "num": 4239}, "tid": 4}, "events": [{"event": "initiated"}, {"event": "header_read"}, {"event": "throttled"}, {"event": "all_read"}, {"event": "dispatched"}, {"event": "acquired locks"}, {"event": "acquired locks"}, {"event": "early_replied"}, {"event": "submit entry: journal_and_reply"}, {"event": "journal_committed: "}, {"event": "replying"}, {"event": "finishing request"}, {"event": "cleaned up request"}, {"event": "done"}]}}]}`
	// The client's pending rename: Tentacle prints a "requests" array, Squid
	// one "request" object per request.
	tentacleClientRequestsSample = `{"requests": [{"tid": 4, "op": "rename", "ino": "0x10000000000", "target_ino": "0x10000000001", "mds": 0, "got_unsafe": 1}]}`
	squidClientRequestsSample    = `{"request": {"tid": 4, "op": "rename", "ino": "0x10000000000", "target_ino": "0x10000000001", "mds": 0, "got_unsafe": 1}}`
)

func TestRequestSafety(t *testing.T) {
	inFlight, err := harness.ParseMDSRequestOps([]byte(unsafeInFlightSample))
	if err != nil {
		t.Fatal(err)
	}
	historic, err := harness.ParseMDSRequestOps([]byte(unsafeHistoricSample))
	if err != nil {
		t.Fatal(err)
	}
	held, found := harness.FindMDSRequest(inFlight, 4239, "rename", 0)
	if !found || held.TypeData.ReqID.TID != 4 || harness.RequestSafety(held) != "unsafe" {
		t.Fatalf("held rename %+v found=%t safety=%s", held, found, harness.RequestSafety(held))
	}
	committed, found := harness.FindMDSRequest(historic, 4239, "rename", 4)
	if !found || committed.TypeData.ReqID.TID != 4 || harness.RequestSafety(committed) != "safe" || committed.Duration != 7.2515 {
		t.Fatalf("committed rename %+v found=%t safety=%s", committed, found, harness.RequestSafety(committed))
	}
	if mkdir, found := harness.FindMDSRequest(historic, 4239, "mkdir", 0); !found || harness.RequestSafety(mkdir) != "no-early-reply" {
		t.Fatalf("mkdir %+v found=%t safety=%s", mkdir, found, harness.RequestSafety(mkdir))
	}
	// Another client, or a description that only starts like the operation,
	// is not the request.
	for _, miss := range []struct {
		client uint64
		op     string
	}{{4240, "rename"}, {4239, "renam"}, {4239, "create"}} {
		if op, found := harness.FindMDSRequest(inFlight, miss.client, miss.op, 0); found {
			t.Errorf("client.%d %s matched %q", miss.client, miss.op, op.Description)
		}
	}
	// A commit without the final reply, or one recorded before the early
	// reply, is neither state.
	out := held
	out.TypeData.Events = append(append(out.TypeData.Events[:0:0], held.TypeData.Events...), struct {
		Event string `json:"event"`
	}{"journal_committed: "})
	if safety := harness.RequestSafety(out); safety != "inconsistent" {
		t.Errorf("a commit without its reply is %s", safety)
	}
	for _, malformed := range []string{`{}`, `{"ops": 1}`, `[]`} {
		if _, err := harness.ParseMDSRequestOps([]byte(malformed)); err == nil {
			t.Errorf("%s parsed", malformed)
		}
	}
}

func TestParseClientMDSRequests(t *testing.T) {
	for name, sample := range map[string]string{"tentacle": tentacleClientRequestsSample, "squid": squidClientRequestsSample} {
		requests, err := harness.ParseClientMDSRequests([]byte(sample))
		if err != nil || len(requests) != 1 {
			t.Fatalf("%s: %+v, %v", name, requests, err)
		}
		if request := requests[0]; request.TID != 4 || request.Op != "rename" || request.MDS != 0 || request.TargetIno != "0x10000000001" || !bool(request.GotUnsafe) {
			t.Errorf("%s: %+v", name, request)
		}
	}
	// Squid repeats the key for each pending request.
	repeated := `{"request": {"tid": 4, "op": "rename", "got_unsafe": 1}, "request": {"tid": 5, "op": "unlink", "got_unsafe": false}}`
	if requests, err := harness.ParseClientMDSRequests([]byte(repeated)); err != nil || len(requests) != 2 || requests[1].TID != 5 || bool(requests[1].GotUnsafe) {
		t.Errorf("repeated keys: %+v, %v", requests, err)
	}
	for _, empty := range []string{`{}`, `{"requests": []}`} {
		if requests, err := harness.ParseClientMDSRequests([]byte(empty)); err != nil || len(requests) != 0 {
			t.Errorf("%s: %+v, %v", empty, requests, err)
		}
	}
	for _, malformed := range []string{`[]`, `{"other": 1}`, `{"request": {"got_unsafe": 2}}`, `{"requests": []} {}`, `{"request": {}`} {
		if _, err := harness.ParseClientMDSRequests([]byte(malformed)); err == nil {
			t.Errorf("%s parsed", malformed)
		}
	}
}

// The completion of a held rename is judged per TID on each side: a client
// that still holds the TID, or an MDS that still has it in flight, keeps it
// unfinished, whatever the other side shows.
func TestRenameSafe(t *testing.T) {
	parseOps := func(data string) []harness.MDSRequestOp {
		t.Helper()
		ops, err := harness.ParseMDSRequestOps([]byte(data))
		if err != nil {
			t.Fatal(err)
		}
		return ops
	}
	parseRequests := func(data string) []harness.ClientMDSRequest {
		t.Helper()
		requests, err := harness.ParseClientMDSRequests([]byte(data))
		if err != nil {
			t.Fatal(err)
		}
		return requests
	}
	inFlight, historic, cleared := parseOps(unsafeInFlightSample), parseOps(unsafeHistoricSample), parseOps(`{"ops": [], "num_ops": 0}`)
	held, otherPending, none := parseRequests(tentacleClientRequestsSample),
		parseRequests(strings.Replace(tentacleClientRequestsSample, `"tid": 4`, `"tid": 5`, 1)), parseRequests(`{"requests": []}`)
	// The first look names the TID from the MDS and finds it on both sides.
	if state := harness.JoinUnsafeRename(inFlight, held, 4239, 0); state.Tid != 4 || !state.InFlight || !state.Pending || !bool(state.Request.GotUnsafe) {
		t.Fatalf("held: %+v", state)
	}
	if state := harness.JoinUnsafeRename(cleared, held, 4239, 0); state.Tid != 0 || state.Pending {
		t.Fatalf("no MDS request names no TID: %+v", state)
	}
	for name, test := range map[string]struct {
		ops      []harness.MDSRequestOp
		requests []harness.ClientMDSRequest
		history  []harness.MDSRequestOp
		safe     bool
	}{
		// The MDS finished, but the client still holds TID 4 as unsafe.
		"client still pending": {cleared, held, historic, false},
		"MDS still in flight":  {inFlight, none, historic, false},
		"only another TID":     {cleared, otherPending, historic, true},
		"both cleared":         {cleared, none, historic, true},
		"not in the history":   {cleared, none, parseOps(`{"ops": []}`), false},
		"history unsafe":       {cleared, none, inFlight, false},
	} {
		state := harness.JoinUnsafeRename(test.ops, test.requests, 4239, 4)
		if got := harness.RenameSafe(state, test.history, 4239); got != test.safe {
			t.Errorf("%s: safe %t, want %t (%+v)", name, got, test.safe, state)
		}
	}
}

// The rename of client 4245, TID 4, in dump_historic_ops of the MDS that
// restarted after a SIGKILL while the request was held unsafe, from a
// TestCephFSUnsafeRequestReplay run on quay.io/ceph/ceph:v20.2.4. The run
// logged the description and events, the fields that ReplayedRequest reads.
const replayedHistoricSample = `{"ops": [{"description": "client_request(client.4245:4 rename #0x10000000000/target #0x10000000000/source 2026-10-16T07:54:30.497182+0000 RETRY=1 REPLAY QUEUED_FOR_REPLAY caller_uid=0, caller_gid=0{})", "type_data": {"reqid": {"entity": {"type": "client", "num": 4245}, "tid": 4}, "events": [{"event": "initiated"}, {"event": "header_read"}, {"event": "throttled"}, {"event": "all_read"}, {"event": "dispatched"}, {"event": "acquired locks"}, {"event": "failed to xlock, waiting"}, {"event": "acquired locks"}, {"event": "submit entry: journal_and_reply"}, {"event": "journal_committed: "}, {"event": "replying"}, {"event": "finishing request"}, {"event": "cleaned up request"}, {"event": "done"}]}}]}`

// A replay is the client's own resend of an unsafe request to a later MDS
// generation: the REPLAY flag, no early reply, and a journal commit before
// the reply.
func TestReplayedRequest(t *testing.T) {
	ops, err := harness.ParseMDSRequestOps([]byte(replayedHistoricSample))
	if err != nil {
		t.Fatal(err)
	}
	replayed, found := harness.FindMDSRequest(ops, 4245, "rename", 4)
	if !found || !harness.ReplayedRequest(replayed) {
		t.Fatalf("native replay %q %v found=%t", replayed.Description, replayed.Events(), found)
	}
	// The first generation's own commit after its early reply is no replay.
	historic, err := harness.ParseMDSRequestOps([]byte(unsafeHistoricSample))
	if err != nil {
		t.Fatal(err)
	}
	if committed, _ := harness.FindMDSRequest(historic, 4239, "rename", 4); harness.ReplayedRequest(committed) {
		t.Errorf("an early-replied commit counts as a replay: %q", committed.Description)
	}
	withEvents := func(op harness.MDSRequestOp, drop string, add ...string) harness.MDSRequestOp {
		op.TypeData.Events = op.TypeData.Events[:0:0]
		for _, event := range append(replayed.Events(), add...) {
			if event != drop {
				op.TypeData.Events = append(op.TypeData.Events, struct {
					Event string `json:"event"`
				}{event})
			}
		}
		return op
	}
	for name, op := range map[string]harness.MDSRequestOp{
		"queued only": func() harness.MDSRequestOp {
			op := replayed
			op.Description = strings.Replace(op.Description, " REPLAY ", " ", 1)
			return op
		}(),
		"not committed":       withEvents(replayed, "journal_committed: "),
		"not replied":         withEvents(replayed, "replying"),
		"early reply":         withEvents(replayed, "", "early_replied"),
		"reply before commit": withEvents(replayed, "journal_committed: ", "journal_committed: "),
	} {
		if harness.ReplayedRequest(op) {
			t.Errorf("%s: counted as a replay: %q %v", name, op.Description, op.Events())
		}
	}
}

// A client's dump_cache from TestCephFSFlushingCapReplay runs on
// quay.io/ceph/ceph:v20.2.4, with the fields that ParseClientCache reads;
// v19.2.5 printed the same shape and values. In the first the fsync's cap
// flush of inode 0x10000000001 waits for its ack, behind a dentry that also
// names the inode; in the second the MDS has acknowledged it.
const (
	clientCacheFlushingSample = `{"cache": [{"name": "flush", "dir": "0x1", "ino": "0x10000000000"}, {"ino": "0x10000000000", "snapid": "head", "size": 0, "dirty_caps": "-"}, {"name": "file", "dir": "0x10000000000", "ino": "0x10000000001"}, {"ino": "0x10000000001", "snapid": "head", "size": 20497, "dirty_caps": "-", "flushings_caps": "Fw", "flushing_cap_tid": {"Fw": 3}}]}`
	clientCacheFlushedSample  = `{"cache": [{"ino": "0x10000000001", "snapid": "head", "size": 20497, "dirty_caps": "-"}]}`
)

func TestParseClientCache(t *testing.T) {
	const inode = 0x10000000001
	held, found, err := harness.ParseClientCache([]byte(clientCacheFlushingSample), inode)
	if err != nil || !found || !held.Flushing() || held.Size != 20497 || held.FlushingCaps != "Fw" || held.FlushingTIDs["Fw"] != 3 {
		t.Fatalf("held flush: %+v found=%t err=%v", held, found, err)
	}
	acked, found, err := harness.ParseClientCache([]byte(clientCacheFlushedSample), inode)
	if err != nil || !found || acked.Flushing() || acked.Size != 20497 {
		t.Fatalf("acknowledged flush: %+v found=%t err=%v", acked, found, err)
	}
	// The same entries as a bare array.
	bare := strings.TrimSuffix(strings.TrimPrefix(clientCacheFlushingSample, `{"cache": `), `}`)
	if caps, found, err := harness.ParseClientCache([]byte(bare), inode); err != nil || !found || !caps.Flushing() {
		t.Fatalf("bare array: %+v found=%t err=%v", caps, found, err)
	}
	// Only a dentry names the inode, the head is another snapshot, or the
	// inode is not cached: not found.
	for name, sample := range map[string]string{
		"dentry only":   `{"cache": [{"name": "file", "dir": "0x10000000000", "ino": "0x10000000001"}]}`,
		"snapshot only": `{"cache": [{"ino": "0x10000000001", "snapid": "2", "size": 20497, "dirty_caps": "-"}]}`,
		"another inode": `{"cache": [{"ino": "0x10000000000", "snapid": "head", "size": 0, "dirty_caps": "-"}]}`,
		"empty":         `{"cache": []}`,
	} {
		if caps, found, err := harness.ParseClientCache([]byte(sample), inode); err != nil || found {
			t.Errorf("%s: %+v found=%t err=%v", name, caps, found, err)
		}
	}
	// A flushing cap without a flush TID is no pending flush.
	if caps, _, _ := harness.ParseClientCache([]byte(`{"cache": [{"ino": "0x10000000001", "snapid": "head", "flushings_caps": "Fw"}]}`), inode); caps.Flushing() {
		t.Errorf("flushing caps without a TID: %+v", caps)
	}
	for _, malformed := range []string{`{}`, `{"other": []}`, `"cache"`, `{"cache": [1]}`, `{"cache": [{"ino": 1}]}`} {
		if _, _, err := harness.ParseClientCache([]byte(malformed), inode); err == nil {
			t.Errorf("%s parsed", malformed)
		}
	}
}
