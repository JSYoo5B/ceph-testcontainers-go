package gwbase

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// What a radosgw-admin run by the gateway's supervisor keeps about itself,
// so that one that is stopped at the supervisor's limit, or that crashes,
// says what it waited for:
//
//   - The objecter's events at level 20, in memory only. Ceph's handler of a
//     fatal signal, the stop's SIGABRT included, writes its recent events to
//     standard error, and eventReport reads the objecter's from there. The
//     other subsystems keep their default memory levels.
//   - recentEvents events instead of 500: a command that waits for two
//     minutes logs thousands at these levels while OSD maps change, and the
//     events of the request it waits for are the oldest of them.
//
// Nothing of this is written while a command runs normally.
//
// The command's admin socket is no source for this. A radosgw-admin has
// several RADOS clients (one reads the configuration, another the buckets),
// and its objecter_requests command answers for the first one only.
const recentEvents = 10000

// diagnosticArgv returns the radosgw-admin command line argv with the
// options above. Options that the caller gave come later and win.
func diagnosticArgv(argv []string) []string {
	if len(argv) == 0 || argv[0] != "radosgw-admin" {
		return argv
	}
	out := make([]string, 0, len(argv)+2)
	out = append(out, argv[0], "--debug-objecter=0/20", fmt.Sprintf("--log-max-recent=%d", recentEvents))
	return append(out, argv[1:]...)
}

const (
	// maxOpenRequests bounds the requests without a reply that eventReport
	// describes. A bucket index read has one for each of the bucket's
	// shards, eleven by default.
	maxOpenRequests = 12
	// maxThreadEvents, maxOtherEvents and maxEventBytes bound the events of
	// eventReport.
	maxThreadEvents = 24
	maxOtherEvents  = 16
	maxEventBytes   = 160
	// maxNameBytes bounds an object name, a locator or an operation list.
	maxNameBytes = 80
)

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
// standard error after its report: numbered lines, each with its time,
// thread and level, and the message, which for an objecter starts with its
// client's name. The report names the thread that took the signal.
var (
	signalThread  = regexp.MustCompile(`(?m)^ *in thread ([0-9a-f]{1,16}) thread_name:`)
	recentEvent   = regexp.MustCompile(`^ *-?\d{1,6}> \d{4}-\d\d-\d\dT(\d\d:\d\d:\d\d\.\d{3})\S* ([0-9a-f]{1,16}) +-?\d{1,2} (.*)$`)
	objecterEvent = regexp.MustCompile(`^client\.([0-9?]{1,20})\.objecter (.*)$`)
	// A request with its object, locator, operations, transaction ID and
	// OSD; where it was sent; and the end of a transaction.
	submitEvent = regexp.MustCompile(`^_op_submit oid (.+) '([^']*)' '[^']*' \[(.*)\] tid (\d{1,20}) osd\.(-?\d{1,10})$`)
	sendEvent   = regexp.MustCompile(`^_send_op (\d{1,20}) to (\S+) on osd\.-?\d{1,10}$`)
	finishEvent = regexp.MustCompile(`^_finish_op (\d{1,20})$`)
	// What the objecter logs for every request besides the lines that name
	// its object, target and transaction: session reference counting, the
	// request's address, the hashed placement group, the request count and
	// the reply's bookkeeping after the line that carries its result.
	routineEvent = regexp.MustCompile(`^(?:_?get_session |put_session |_session_op_assign |_session_op_remove |_op_submit op |_calc_target target |\d+ in flight$|handle_osd_op_reply |_finish_op )`)
	// Where an event names a request's transaction ID.
	requestID   = regexp.MustCompile(`(?:\btid |^_send_op |osd_op_reply\(|\bop )(\d{1,20})\b`)
	mapEvent    = regexp.MustCompile(`^handle_osd_map got epochs `)
	pointer     = regexp.MustCompile(`0x[0-9a-f]+`)
	elapsedTime = regexp.MustCompile(`\d+\.\d+s`)
)

const (
	recentEventsBegin = "--- begin dump of recent events ---"
	recentEventsEnd   = "--- logging levels ---"
)

// eventReport says what the thread that took the signal, which for a stopped
// command is the thread that waited, asked of RADOS and did not get, from
// the objecter's events in the first dump of recent events in stderr. A
// command has several RADOS clients, each with its own transaction IDs; the
// report is about the one that the thread used last:
//
//   - how many requests the thread submitted in these events and which have
//     no reply, at most maxOpenRequests: the transaction ID, operations,
//     object, locator, placement group and OSD, and when it was submitted.
//     None without a reply means that the thread waits for something else,
//     or for a request older than the events;
//   - the thread's last maxThreadEvents events, which give the OSD map
//     epoch each request was computed for;
//   - of the other threads' events of that client from then on, at most
//     maxOtherEvents: those that name one of these transactions (a reply
//     with its result, a resend, a laggy mark) and the OSD maps received.
//
// The latest events alone would not do: while a command waits, its watches
// are pinged every five seconds and OSD maps keep arriving, which pushes the
// request it waits for out of any short tail. An event that repeats is one
// entry with its count and last time. Each entry has its time and at most
// maxEventBytes of its text. An event or request that names a key namespace
// has its text replaced, and events of other subsystems, whose text this
// package cannot vouch for, are only counted. Without an event of the
// signal's thread the last maxThreadEvents events of all threads and clients
// are shown. It returns "" when stderr holds no dump.
func eventReport(stderr []byte) string {
	head, dump, found := strings.Cut(string(stderr), recentEventsBegin)
	if !found {
		return ""
	}
	dump, _, _ = strings.Cut(dump, recentEventsEnd)
	thread := ""
	if match := signalThread.FindStringSubmatch(head); match != nil {
		thread = match[1]
	}
	type event struct{ time, thread, client, text string }
	var events []event
	all := 0
	for line := range strings.SplitSeq(dump, "\n") {
		match := recentEvent.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		all++
		if message := objecterEvent.FindStringSubmatch(match[3]); message != nil {
			events = append(events, event{time: match[1], thread: match[2], client: message[1], text: strings.TrimSpace(pointer.ReplaceAllString(message[2], "0x"))})
		}
	}
	if len(events) == 0 {
		return fmt.Sprintf("none of its %d recent events is of the objecter", all)
	}
	summary := fmt.Sprintf("of its %d recent events, %d of the objecter", all, len(events))
	// shown is what an entry says of an event.
	shown := func(e event) string {
		if keyNamespace(e.text) {
			return "an event of a key object"
		}
		return e.text
	}
	// collect lists events in order, a repeated one once.
	collect := func(selected []event, limit int, fromEnd bool) (parts []string, total int) {
		type entry struct {
			first, last, text string
			count             int
		}
		var entries []*entry
		seen := map[string]*entry{}
		for _, e := range selected {
			text := shown(e)
			key := elapsedTime.ReplaceAllString(text, "#s")
			if known := seen[key]; known != nil {
				known.count++
				known.last, known.text = e.time, text
				continue
			}
			added := &entry{first: e.time, last: e.time, text: text, count: 1}
			seen[key] = added
			entries = append(entries, added)
		}
		total = len(entries)
		if fromEnd {
			entries = entries[max(0, total-limit):]
		} else {
			entries = entries[:min(total, limit)]
		}
		for _, e := range entries {
			part := e.first + " " + printable(e.text, maxEventBytes)
			if e.count > 1 {
				part += fmt.Sprintf(" (%d times until %s)", e.count, e.last)
			}
			parts = append(parts, part)
		}
		return parts, total
	}
	notRoutine := func(e event) bool { return !routineEvent.MatchString(e.text) }
	client := ""
	for _, e := range events {
		if e.thread == thread {
			client = e.client
		}
	}
	if client == "" {
		var rest []event
		for _, e := range events {
			if notRoutine(e) {
				rest = append(rest, e)
			}
		}
		parts, total := collect(rest, maxThreadEvents, true)
		return fmt.Sprintf("%s, none of the signalled thread, the last %d of %d different ones: %s", summary, len(parts), total, strings.Join(parts, " | "))
	}
	// The requests of that client: the thread's own, where each was sent,
	// and which ended.
	type request struct {
		id, time, object, locator, operations, osd, group string
		own                                               bool
	}
	var requests []*request
	byID := map[string]*request{}
	for _, e := range events {
		if e.client != client {
			continue
		}
		if match := submitEvent.FindStringSubmatch(e.text); match != nil {
			r := &request{id: match[4], time: e.time, object: match[1], locator: match[2], operations: match[3], osd: match[5], own: e.thread == thread}
			requests, byID[r.id] = append(requests, r), r
		} else if match := sendEvent.FindStringSubmatch(e.text); match != nil && byID[match[1]] != nil {
			byID[match[1]].group = match[2]
		} else if match := finishEvent.FindStringSubmatch(e.text); match != nil {
			delete(byID, match[1])
		}
	}
	submitted, othersOpen := 0, 0
	named := map[string]bool{}
	var open []string
	for _, r := range requests {
		if r.own {
			submitted++
		}
		switch {
		case byID[r.id] != r:
		case !r.own:
			othersOpen++
		default:
			named[r.id] = true
			if len(open) == maxOpenRequests {
				continue
			}
			object, target := printable(r.object, maxNameBytes), "osd."+r.osd
			if keyNamespace(r.locator) {
				object = "a key object"
			}
			if strings.HasPrefix(r.osd, "-") {
				target = "no OSD"
			}
			if r.group != "" {
				target = "pg " + printable(r.group, maxNameBytes) + " on " + target
			}
			open = append(open, fmt.Sprintf("tid %s [%s] of %s in %s to %s at %s", r.id, printable(r.operations, maxNameBytes), object, printable(r.locator, maxNameBytes), target, r.time))
		}
	}
	text := fmt.Sprintf("%s; through client.%s the signalled thread submitted %d requests in them, %d without a reply", summary, client, submitted, len(named))
	if len(open) > 0 {
		text += ": " + strings.Join(open, "; ")
		if len(named) > len(open) {
			text += fmt.Sprintf("; %d more", len(named)-len(open))
		}
	}
	if othersOpen > 0 {
		text += fmt.Sprintf(" (other threads: %d without a reply)", othersOpen)
	}
	// The thread's last events of that client, and where they start.
	var last []event
	start := 0
	for i := len(events) - 1; i >= 0 && len(last) < maxThreadEvents; i-- {
		if e := events[i]; e.thread == thread && e.client == client && notRoutine(e) {
			last = append(last, e)
			start = i
		}
	}
	slices.Reverse(last)
	for _, e := range last {
		for _, match := range requestID.FindAllStringSubmatch(e.text, -1) {
			named[match[1]] = true
		}
	}
	var others []event
	for _, e := range events[start:] {
		if e.thread == thread || e.client != client || !notRoutine(e) {
			continue
		}
		about := mapEvent.MatchString(e.text)
		for _, match := range requestID.FindAllStringSubmatch(e.text, -1) {
			about = about || named[match[1]]
		}
		if about {
			others = append(others, e)
		}
	}
	parts, _ := collect(last, maxThreadEvents, true)
	text += fmt.Sprintf("; its last %d events: %s", len(parts), strings.Join(parts, " | "))
	if parts, total := collect(others, maxOtherEvents, false); total > 0 {
		text += fmt.Sprintf("; then of other threads, for these requests and OSD maps, the first %d of %d: %s", len(parts), total, strings.Join(parts, " | "))
	} else {
		text += "; then no event of another thread for these requests, and no OSD map"
	}
	return text
}
