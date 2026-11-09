package harness

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// OSD descriptions contain the full hobject and a bracketed vector of
// operations. Extract whole RBD object names and include only data reads.
var rbdDataObjectToken = regexp.MustCompile(`[[:alnum:]_.-]+`)
var rbdDataReadOp = regexp.MustCompile(`(\[|,)[ ]*(read|sparse-read)[ ]`)

// RBDDataReadsInHistory selects reads of exact RBD object names. Each key
// identifies a native request by its description and start time.
func RBDDataReadsInHistory(out []byte, objects map[string]bool) (map[string]string, error) {
	var history struct {
		Ops *[]struct {
			Description string `json:"description"`
			Started     string `json:"initiated_at"`
		} `json:"ops"`
	}
	if err := json.Unmarshal(out, &history); err != nil {
		return nil, err
	}
	if history.Ops == nil {
		return nil, errors.New("OSD history is missing ops")
	}
	reads := map[string]string{}
	for _, op := range *history.Ops {
		if !strings.HasPrefix(op.Description, "osd_op(") || !rbdDataReadOp.MatchString(op.Description) {
			continue
		}
		for _, token := range rbdDataObjectToken.FindAllString(op.Description, -1) {
			if objects[token] {
				if op.Started == "" {
					return nil, errors.New("RBD data read is missing its start time")
				}
				reads[op.Started+" "+op.Description] = token
				break
			}
		}
	}
	return reads, nil
}

// OSDReadCounters is the read part of one OSD's `perf dump osd`. The
// counters sum every read the OSD served, of any object and from any client,
// so they cannot identify the request that served one read; they only bound
// what the request-scoped evidence may claim.
type OSDReadCounters struct {
	OpR, OpROutBytes int64
	// ReplicaReadServed is nil when the OSD does not report the counter.
	ReplicaReadServed *int64
}

// ParseOSDReadCounters decodes `perf dump osd`. op_r and op_r_out_bytes are
// required; replica_read_served is optional. A counter that is present must
// be a non-negative integer; JSON null is not a sample.
func ParseOSDReadCounters(out []byte) (OSDReadCounters, error) {
	var dump struct {
		OSD map[string]json.RawMessage `json:"osd"`
	}
	if err := json.Unmarshal(out, &dump); err != nil {
		return OSDReadCounters{}, err
	}
	counter := func(name string, required bool) (*int64, error) {
		raw, ok := dump.OSD[name]
		if !ok {
			if required {
				return nil, errors.New("perf dump osd lacks " + name)
			}
			return nil, nil
		}
		var value int64
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return nil, fmt.Errorf("perf dump osd %s is null, not a non-negative integer", name)
		}
		if err := json.Unmarshal(raw, &value); err != nil || value < 0 {
			return nil, fmt.Errorf("perf dump osd %s is %s, not a non-negative integer", name, raw)
		}
		return &value, nil
	}
	var counters OSDReadCounters
	for _, c := range []struct {
		name string
		dst  *int64
	}{{"op_r", &counters.OpR}, {"op_r_out_bytes", &counters.OpROutBytes}} {
		value, err := counter(c.name, true)
		if err != nil {
			return OSDReadCounters{}, err
		}
		*c.dst = *value
	}
	served, err := counter("replica_read_served", false)
	if err != nil {
		return OSDReadCounters{}, err
	}
	counters.ReplicaReadServed = served
	return counters, nil
}

// RBDReadReply is one successful reply an OSD sent to a read of an RBD data
// object, from the OSD's debug_ms 1 log. The messenger prints a received
// osd_op before decoding it, without its object, so the reply is the logged
// request-scoped evidence: it names the object, the request's transaction ID
// and the requester's address, whose nonce is per client instance.
type RBDReadReply struct {
	Peer     string
	TID      string
	OutBytes int64
}

// A sent reply is logged as
// "-- <own addrs> --> <peer> -- osd_op_reply(<tid> <object> [<ops>] v<e>'<v> uv<v> ... = <result>) [v<n> ]-- 0x...".
var (
	msgrOSDOpReply = regexp.MustCompile(` --> (\S+) -- osd_op_reply\((\d+) (\S+) \[(.*)\] v\d+'\d+ uv\d+.* = (-?\d+)\)(?: v\d+)? -- `)
	msgrReadOut    = regexp.MustCompile(`^ *(?:read|sparse-read) \d+~\d+ .*out=(\d+)b$`)
)

// RBDDataReadReplies selects, from an OSD log window, the successful replies
// to reads of the exact RBD object that went to client. Replies to other
// peers are not the client's read and are left out.
func RBDDataReadReplies(log []byte, object string, client netip.Addr) ([]RBDReadReply, error) {
	var replies []RBDReadReply
	for line := range strings.SplitSeq(string(log), "\n") {
		m := msgrOSDOpReply.FindStringSubmatch(line)
		if m == nil || m[3] != object || m[5] != "0" {
			continue
		}
		peer, err := msgrPeerAddr(m[1])
		if err != nil {
			return nil, fmt.Errorf("reply %q: %w", line, err)
		}
		if peer.Addr() != client {
			continue
		}
		var out int64
		read := false
		for _, op := range splitTopLevel(m[4]) {
			r := msgrReadOut.FindStringSubmatch(op)
			if r == nil {
				continue
			}
			n, err := strconv.ParseInt(r[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("reply %q: %w", line, err)
			}
			out += n
			read = true
		}
		if read {
			replies = append(replies, RBDReadReply{Peer: m[1], TID: m[2], OutBytes: out})
		}
	}
	return replies, nil
}

// msgrPeerAddr parses a messenger address, "[v2:ip:port/nonce,...]",
// "v2:ip:port/nonce" or "ip:port/nonce", to its first IP and port.
func msgrPeerAddr(s string) (netip.AddrPort, error) {
	if strings.HasPrefix(s, "[") && len(s) > 1 && s[1] != ':' && (s[1] < '0' || s[1] > '9') {
		s = strings.TrimSuffix(s[1:], "]")
		s, _, _ = strings.Cut(s, ",")
	}
	for _, prefix := range []string{"v1:", "v2:", "any:"} {
		s = strings.TrimPrefix(s, prefix)
	}
	s, _, _ = strings.Cut(s, "/")
	return netip.ParseAddrPort(s)
}

// splitTopLevel splits an operation vector at commas outside brackets.
func splitTopLevel(s string) []string {
	var parts []string
	depth, start := 0, 0
	for i, r := range s {
		switch r {
		case '[':
			depth++
		case ']':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, s[start:])
}

// RBDReadEvidence is what one OSD showed between the observations before
// and after a single read of one RBD data object.
type RBDReadEvidence struct {
	// History holds the op history entries for the object that were not
	// there before the read.
	History []string
	// Replies are the OSD's replies to reads of the object in the window.
	Replies []RBDReadReply
	// Before and After are the OSD's read counters around the read.
	Before, After OSDReadCounters
}

// OutBytes is the number of bytes the OSD returned to read requests of any
// object during the read.
func (e RBDReadEvidence) OutBytes() int64 { return e.After.OpROutBytes - e.Before.OpROutBytes }

// ReplyBytes is the number of bytes the OSD's replies to the object returned.
func (e RBDReadEvidence) ReplyBytes() int64 {
	var n int64
	for _, r := range e.Replies {
		n += r.OutBytes
	}
	return n
}

// ReplicaServed is the number of replica reads the OSD served during the
// read; ok is false when the OSD does not report the counter.
func (e RBDReadEvidence) ReplicaServed() (served int64, ok bool) {
	if e.Before.ReplicaReadServed == nil || e.After.ReplicaReadServed == nil {
		return 0, false
	}
	return *e.After.ReplicaReadServed - *e.Before.ReplicaReadServed, true
}

// ErrRBDReadUnattributed reports a read that no OSD's request-scoped
// evidence names.
var ErrRBDReadUnattributed = errors.New("no OSD has a history entry or a reply for the read")

// RBDReadServers attributes a read of a data object of size bytes to the OSDs
// with request-scoped evidence for it: a new op history entry for a read of
// the object, or a reply to a read request of the object. Counters alone
// never attribute a read, because unrelated reads add to them; a read that
// only the counters show is ErrRBDReadUnattributed. The counters bound the
// evidence instead: counters that went backwards, replies to the object that
// returned fewer than size bytes or more than the OSD's counters, and a
// serving OSD whose counters returned fewer than size bytes are errors.
func RBDReadServers(evidence map[int]RBDReadEvidence, size int64) ([]int, error) {
	var servers []int
	for id, e := range evidence {
		if e.After.OpR < e.Before.OpR || e.OutBytes() < 0 {
			return nil, fmt.Errorf("osd.%d read counters went backwards: %+v, then %+v", id, e.Before, e.After)
		}
		if served, ok := e.ReplicaServed(); ok && served < 0 {
			return nil, fmt.Errorf("osd.%d replica_read_served went backwards by %d", id, -served)
		}
		if len(e.Replies) > 0 && e.ReplyBytes() < size {
			return nil, fmt.Errorf("osd.%d replies %+v returned %d bytes, less than the %d-byte object", id, e.Replies, e.ReplyBytes(), size)
		}
		if e.ReplyBytes() > e.OutBytes() {
			return nil, fmt.Errorf("osd.%d replies returned %d bytes but its counters %d", id, e.ReplyBytes(), e.OutBytes())
		}
		if len(e.History) == 0 && len(e.Replies) == 0 {
			continue
		}
		if e.OutBytes() < size {
			return nil, fmt.Errorf("osd.%d history gained %v and replies %+v but the OSD returned %d bytes, less than the %d-byte object", id, e.History, e.Replies, e.OutBytes(), size)
		}
		servers = append(servers, id)
	}
	if len(servers) == 0 {
		return nil, ErrRBDReadUnattributed
	}
	slices.Sort(servers)
	return servers, nil
}
