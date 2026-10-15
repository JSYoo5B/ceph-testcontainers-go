//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"strings"
	"testing"
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
	inFlight, err := parseMDSRequestOps([]byte(unsafeInFlightSample))
	if err != nil {
		t.Fatal(err)
	}
	historic, err := parseMDSRequestOps([]byte(unsafeHistoricSample))
	if err != nil {
		t.Fatal(err)
	}
	held, found := findMDSRequest(inFlight, 4239, "rename", 0)
	if !found || held.TypeData.ReqID.TID != 4 || requestSafety(held) != "unsafe" {
		t.Fatalf("held rename %+v found=%t safety=%s", held, found, requestSafety(held))
	}
	committed, found := findMDSRequest(historic, 4239, "rename", 4)
	if !found || committed.TypeData.ReqID.TID != 4 || requestSafety(committed) != "safe" || committed.Duration != 7.2515 {
		t.Fatalf("committed rename %+v found=%t safety=%s", committed, found, requestSafety(committed))
	}
	if mkdir, found := findMDSRequest(historic, 4239, "mkdir", 0); !found || requestSafety(mkdir) != "no-early-reply" {
		t.Fatalf("mkdir %+v found=%t safety=%s", mkdir, found, requestSafety(mkdir))
	}
	// Another client, or a description that only starts like the operation,
	// is not the request.
	for _, miss := range []struct {
		client uint64
		op     string
	}{{4240, "rename"}, {4239, "renam"}, {4239, "create"}} {
		if op, found := findMDSRequest(inFlight, miss.client, miss.op, 0); found {
			t.Errorf("client.%d %s matched %q", miss.client, miss.op, op.Description)
		}
	}
	// A commit without the final reply, or one recorded before the early
	// reply, is neither state.
	out := held
	out.TypeData.Events = append(append(out.TypeData.Events[:0:0], held.TypeData.Events...), struct {
		Event string `json:"event"`
	}{"journal_committed: "})
	if safety := requestSafety(out); safety != "inconsistent" {
		t.Errorf("a commit without its reply is %s", safety)
	}
	for _, malformed := range []string{`{}`, `{"ops": 1}`, `[]`} {
		if _, err := parseMDSRequestOps([]byte(malformed)); err == nil {
			t.Errorf("%s parsed", malformed)
		}
	}
}

func TestParseClientMDSRequests(t *testing.T) {
	for name, sample := range map[string]string{"tentacle": tentacleClientRequestsSample, "squid": squidClientRequestsSample} {
		requests, err := parseClientMDSRequests([]byte(sample))
		if err != nil || len(requests) != 1 {
			t.Fatalf("%s: %+v, %v", name, requests, err)
		}
		if request := requests[0]; request.TID != 4 || request.Op != "rename" || request.MDS != 0 || request.TargetIno != "0x10000000001" || !bool(request.GotUnsafe) {
			t.Errorf("%s: %+v", name, request)
		}
	}
	// Squid repeats the key for each pending request.
	repeated := `{"request": {"tid": 4, "op": "rename", "got_unsafe": 1}, "request": {"tid": 5, "op": "unlink", "got_unsafe": false}}`
	if requests, err := parseClientMDSRequests([]byte(repeated)); err != nil || len(requests) != 2 || requests[1].TID != 5 || bool(requests[1].GotUnsafe) {
		t.Errorf("repeated keys: %+v, %v", requests, err)
	}
	for _, empty := range []string{`{}`, `{"requests": []}`} {
		if requests, err := parseClientMDSRequests([]byte(empty)); err != nil || len(requests) != 0 {
			t.Errorf("%s: %+v, %v", empty, requests, err)
		}
	}
	for _, malformed := range []string{`[]`, `{"other": 1}`, `{"request": {"got_unsafe": 2}}`, `{"requests": []} {}`, `{"request": {}`} {
		if _, err := parseClientMDSRequests([]byte(malformed)); err == nil {
			t.Errorf("%s parsed", malformed)
		}
	}
}

// The completion of a held rename is judged per TID on each side: a client
// that still holds the TID, or an MDS that still has it in flight, keeps it
// unfinished, whatever the other side shows.
func TestRenameSafe(t *testing.T) {
	parseOps := func(data string) []mdsRequestOp {
		t.Helper()
		ops, err := parseMDSRequestOps([]byte(data))
		if err != nil {
			t.Fatal(err)
		}
		return ops
	}
	parseRequests := func(data string) []clientMDSRequest {
		t.Helper()
		requests, err := parseClientMDSRequests([]byte(data))
		if err != nil {
			t.Fatal(err)
		}
		return requests
	}
	inFlight, historic, cleared := parseOps(unsafeInFlightSample), parseOps(unsafeHistoricSample), parseOps(`{"ops": [], "num_ops": 0}`)
	held, otherPending, none := parseRequests(tentacleClientRequestsSample),
		parseRequests(strings.Replace(tentacleClientRequestsSample, `"tid": 4`, `"tid": 5`, 1)), parseRequests(`{"requests": []}`)
	// The first look names the TID from the MDS and finds it on both sides.
	if state := joinUnsafeRename(inFlight, held, 4239, 0); state.tid != 4 || !state.inFlight || !state.pending || !bool(state.request.GotUnsafe) {
		t.Fatalf("held: %+v", state)
	}
	if state := joinUnsafeRename(cleared, held, 4239, 0); state.tid != 0 || state.pending {
		t.Fatalf("no MDS request names no TID: %+v", state)
	}
	for name, test := range map[string]struct {
		ops      []mdsRequestOp
		requests []clientMDSRequest
		history  []mdsRequestOp
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
		state := joinUnsafeRename(test.ops, test.requests, 4239, 4)
		if got := renameSafe(state, test.history, 4239); got != test.safe {
			t.Errorf("%s: safe %t, want %t (%+v)", name, got, test.safe, state)
		}
	}
}
