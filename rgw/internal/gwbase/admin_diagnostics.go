package gwbase

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

// What a radosgw-admin run by the gateway's supervisor keeps about itself,
// so that one that is stopped at the supervisor's limit, or that crashes,
// says what it waited for:
//
//   - Its admin socket, in the supervisor's directory under the command's
//     process ID. The supervisor asks it for the command's outstanding RADOS
//     requests right before it stops the command (waitReport).
//   - The objecter's events at level 20, in memory only. Ceph's handler of a
//     fatal signal, the stop's SIGABRT included, writes its recent events to
//     standard error, and eventReport reads the objecter's from there. The
//     other subsystems keep their default memory levels.
//   - recentEvents events instead of 500: a command that waits for two
//     minutes logs about a thousand at these levels, and the events of the
//     request it waits for are the oldest of them.
//
// Nothing of this is written while a command runs normally.
const (
	adminSocketPath = supervised.MarkerDir + "/radosgw-admin." + supervised.DeadlineQueryPID + ".asok"
	// waitRequest asks an admin socket for the objecter's requests.
	waitRequest  = `{"prefix":"objecter_requests"}`
	recentEvents = 10000
)

// diagnosticArgv returns the radosgw-admin command line argv with the
// options above. Ceph replaces $pid itself. Options that the caller gave
// come later and win.
func diagnosticArgv(argv []string) []string {
	if len(argv) == 0 || argv[0] != "radosgw-admin" {
		return argv
	}
	out := make([]string, 0, len(argv)+3)
	out = append(out, argv[0],
		"--admin-socket="+strings.ReplaceAll(adminSocketPath, supervised.DeadlineQueryPID, "$pid"),
		"--debug-objecter=0/20", fmt.Sprintf("--log-max-recent=%d", recentEvents))
	return append(out, argv[1:]...)
}

const (
	// maxWaitRequests bounds the RADOS requests that waitReport describes.
	maxWaitRequests = 4
	// maxWaitNameBytes bounds an object name, an operation or a pool name.
	maxWaitNameBytes = 64
	// maxEvents and maxEventBytes bound the events of eventReport.
	maxEvents     = 32
	maxEventBytes = 160
)

// objecterRequests is the answer of a Ceph client's admin socket to
// waitRequest, as far as waitReport reads it.
type objecterRequests struct {
	Ops []struct {
		Tid      uint64          `json:"tid"`
		PG       string          `json:"pg"`
		OSD      int             `json:"osd"`
		Object   string          `json:"object_id"`
		Locator  string          `json:"object_locator"`
		Paused   json.RawMessage `json:"paused"`
		Age      float64         `json:"age"`
		Attempts int             `json:"attempts"`
		OSDOps   []string        `json:"osd_ops"`
	} `json:"ops"`
	LingerOps []json.RawMessage `json:"linger_ops"`
	PoolOps   []struct {
		Tid  uint64 `json:"tid"`
		Pool int64  `json:"pool"`
		Type string `json:"operation_type"`
	} `json:"pool_ops"`
	PoolStatOps []json.RawMessage `json:"pool_stat_ops"`
	StatfsOps   []json.RawMessage `json:"statfs_ops"`
	CommandOps  []json.RawMessage `json:"command_ops"`
}

// waitReport says which RADOS requests a command had outstanding when the
// supervisor asked it, from the answer to waitRequest: for at most
// maxWaitRequests object requests the operations, the object, its pool and
// placement group, the OSD they were sent to, how often and how long ago;
// then pool operations and the numbers of the other kinds. A watch is no
// request that a command waits for, and is only counted. It returns "" for
// no answer. Object names are names of RGW metadata and data objects; the
// name of an object in a key namespace is an access key or a Swift user,
// and is left out.
func waitReport(reply []byte, cut bool) string {
	if len(reply) == 0 {
		return ""
	}
	var requests objecterRequests
	if cut || json.Unmarshal(reply, &requests) != nil {
		return "its answer about its RADOS requests could not be read"
	}
	var parts []string
	for i, op := range requests.Ops {
		if i == maxWaitRequests {
			parts = append(parts, fmt.Sprintf("%d more", len(requests.Ops)-i))
			break
		}
		object := printable(op.Object, maxWaitNameBytes)
		if keyNamespace(op.Locator) {
			object = "a key object"
		}
		target := fmt.Sprintf("osd.%d", op.OSD)
		if op.OSD < 0 {
			target = "no OSD"
		}
		paused := ""
		if state := strings.TrimSpace(string(op.Paused)); state != "" && state != "0" && state != "false" {
			paused = ", paused"
		}
		operations := make([]string, 0, len(op.OSDOps))
		for j, name := range op.OSDOps {
			if j == maxWaitRequests {
				operations = append(operations, "...")
				break
			}
			operations = append(operations, printable(name, maxWaitNameBytes))
		}
		parts = append(parts, fmt.Sprintf("tid %d [%s] of %s in %s, pg %s, on %s%s, sent %d times, %.1fs old", op.Tid, strings.Join(operations, ","),
			object, printable(op.Locator, maxWaitNameBytes), printable(op.PG, maxWaitNameBytes), target, paused, op.Attempts, op.Age))
	}
	text := "it had no RADOS object request outstanding"
	if len(requests.Ops) > 0 {
		text = fmt.Sprintf("it waited for %d RADOS object requests: %s", len(requests.Ops), strings.Join(parts, "; "))
	}
	var others []string
	for i, op := range requests.PoolOps {
		if i == maxWaitRequests {
			break
		}
		others = append(others, fmt.Sprintf("pool operation tid %d %s of pool %d", op.Tid, printable(op.Type, maxWaitNameBytes), op.Pool))
	}
	for _, kind := range []struct {
		name  string
		count int
	}{
		{"pool operations", max(0, len(requests.PoolOps)-maxWaitRequests)}, {"pool statistics requests", len(requests.PoolStatOps)},
		{"statfs requests", len(requests.StatfsOps)}, {"commands", len(requests.CommandOps)}, {"watches", len(requests.LingerOps)},
	} {
		if kind.count > 0 {
			others = append(others, fmt.Sprintf("%d %s", kind.count, kind.name))
		}
	}
	if len(others) > 0 {
		text += ", and had " + strings.Join(others, ", ")
	}
	return text
}

// keyNamespace reports whether text names the RGW namespace of access keys
// or of Swift keys, whose object names are the keys' identifiers.
func keyNamespace(text string) bool {
	return strings.Contains(text, "users.keys") || strings.Contains(text, "users.swift")
}

// printable returns at most limit bytes of text, with every byte that is not
// printable ASCII replaced.
func printable(text string, limit int) string {
	cutAt := min(len(text), limit)
	out := []byte(text[:cutAt])
	for i, b := range out {
		if b < ' ' || b > '~' {
			out[i] = '?'
		}
	}
	if cutAt < len(text) {
		return string(out) + "..."
	}
	return string(out)
}

// The dump of recent events that Ceph's fatal signal handler writes to
// standard error: numbered lines, each with its time, thread and level, and
// the message, which for the objecter starts with the client's name.
var (
	recentEvent   = regexp.MustCompile(`^ *-?\d{1,6}> \d{4}-\d\d-\d\dT(\d\d:\d\d:\d\d\.\d{3})\S* [0-9a-f]{1,16} +-?\d{1,2} (.*)$`)
	objecterEvent = regexp.MustCompile(`^client\.[0-9?]{1,20}\.objecter (.*)$`)
	// Session reference counting, logged several times for each request.
	sessionEvent = regexp.MustCompile(`^(?:_?get_session|put_session|_session_op_assign|_session_op_remove) `)
	pointer      = regexp.MustCompile(`0x[0-9a-f]+`)
	elapsed      = regexp.MustCompile(`\d+\.\d+s`)
)

const (
	recentEventsBegin = "--- begin dump of recent events ---"
	recentEventsEnd   = "--- logging levels ---"
)

// eventReport summarizes the objecter's events in the first dump of recent
// events in stderr: the last maxEvents different ones in the order of their
// first occurrence, each with its time and at most maxEventBytes of its
// text. An event that repeats, as the objecter's tick does every five
// seconds of a wait, is one entry with its count and its last time, so the
// entries before it are the requests that preceded the wait. The events name
// what the objecter did: the OSD map epochs it received, each request with
// its object, pool, placement group and OSD, and each reply. An event that
// names a key namespace is replaced, session reference counting is left
// out, and events of other subsystems, whose text this package cannot
// vouch for, are only counted. It returns "" when stderr holds no dump.
func eventReport(stderr []byte) string {
	_, dump, found := strings.Cut(string(stderr), recentEventsBegin)
	if !found {
		return ""
	}
	dump, _, _ = strings.Cut(dump, recentEventsEnd)
	type entry struct {
		first, last, text string
		count             int
	}
	var entries []*entry
	seen := map[string]*entry{}
	all, objecter := 0, 0
	for line := range strings.SplitSeq(dump, "\n") {
		event := recentEvent.FindStringSubmatch(line)
		if event == nil {
			continue
		}
		all++
		message := objecterEvent.FindStringSubmatch(event[2])
		if message == nil {
			continue
		}
		objecter++
		text := strings.TrimSpace(pointer.ReplaceAllString(message[1], "0x"))
		switch {
		case sessionEvent.MatchString(text):
			continue
		case keyNamespace(text):
			text = "an event of a key object"
		}
		key := elapsed.ReplaceAllString(text, "#s")
		if known := seen[key]; known != nil {
			known.count++
			known.last, known.text = event[1], text
			continue
		}
		added := &entry{first: event[1], last: event[1], text: text, count: 1}
		seen[key] = added
		entries = append(entries, added)
	}
	if objecter == 0 {
		return fmt.Sprintf("none of its %d recent events is of the objecter", all)
	}
	shown := entries[max(0, len(entries)-maxEvents):]
	parts := make([]string, 0, len(shown))
	for _, e := range shown {
		part := e.first + " " + printable(e.text, maxEventBytes)
		if e.count > 1 {
			part += fmt.Sprintf(" (%d times until %s)", e.count, e.last)
		}
		parts = append(parts, part)
	}
	return fmt.Sprintf("the last %d of the %d different objecter events among its %d recent events (%d of the objecter): %s",
		len(shown), len(entries), all, objecter, strings.Join(parts, " | "))
}
