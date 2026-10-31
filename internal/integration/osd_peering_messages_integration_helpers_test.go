//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// peeringSymbols maps the name a message prints itself with to its native
// message type, for the PG creation, peering, lease and scrub messages.
var peeringSymbols = map[string]string{
	"pg_create2":    "MSG_OSD_PG_CREATE2",
	"pg_query2":     "MSG_OSD_PG_QUERY2",
	"pg_notify2":    "MSG_OSD_PG_NOTIFY2",
	"PGlog":         "MSG_OSD_PG_LOG",
	"pg_info2":      "MSG_OSD_PG_INFO2",
	"pg_lease":      "MSG_OSD_PG_LEASE",
	"pg_lease_ack":  "MSG_OSD_PG_LEASE_ACK",
	"replica_scrub": "MSG_OSD_REP_SCRUB",
	"rep_scrubmap":  "MSG_OSD_REP_SCRUBMAP",
}

// peeringEvent is one line of a daemon's debug_ms 1 and debug_osd 10 log
// that the peering judge reads: a message of peeringSymbols that the logging
// messenger received ("in") or sent ("out"), the entry into a peering state
// ("enter") or a scrubber state ("scrub") of one PG.
type peeringEvent struct {
	Line int    `json:"line"`
	Kind string `json:"kind"`
	// Local is the v2 address of the logging messenger, Addr the v2 address
	// of the other end and Peer the entity that sent a received message.
	Local string `json:"local,omitempty"`
	Addr  string `json:"addr,omitempty"`
	Peer  string `json:"peer,omitempty"`
	Seq   uint64 `json:"seq,omitempty"`
	// Type is the message's print name and Message what it printed; State
	// is the entered state.
	Type    string   `json:"type,omitempty"`
	Message string   `json:"message,omitempty"`
	State   string   `json:"state,omitempty"`
	PGIDs   []string `json:"pgids"`
	Text    string   `json:"text"`
}

var (
	peeringInLine    = regexp.MustCompile(`^\S+ \S+ +\d+ -- (\S+) <== (\S+) (\S+) (\d+) ==== (.*?) ==== \d+\+\d+\+\d+ \(`)
	peeringOutLine   = regexp.MustCompile(`^\S+ \S+ +\d+ -- (\S+) --> (\S+) -- (.*) -- 0x[0-9a-f]+ con 0x[0-9a-f]+$`)
	peeringName      = regexp.MustCompile(`^([A-Za-z_0-9]+)\(`)
	peeringCreatePGs = regexp.MustCompile(`[{,](\d+\.[0-9a-f]+)=`)
	peeringScrubPG   = regexp.MustCompile(`^replica_scrub\(pg: ([^,]+),`)
	peeringFirstPG   = regexp.MustCompile(`^[A-Za-z_0-9]+\((\d+\.[0-9a-f]+(?:s\d+)?) `)
	peeringEnter     = regexp.MustCompile(`^\S+ \S+ +5 osd\.\d+ pg_epoch: \d+ pg\[(\d+\.[0-9a-f]+(?:s\d+)?)\(.*\] enter (\S+)$`)
	peeringScrubber  = regexp.MustCompile(`^\S+ \S+ +10 osd\.\d+ pg_epoch: \d+ pg\[(\d+\.[0-9a-f]+(?:s\d+)?)\(.*\] scrubber<[^>]*>: FSM: +-- state -->> (\S+)$`)
)

// v2Addr returns the v2 address of an address or address vector as a
// messenger prints it, without its "v2:" prefix.
func v2Addr(addr string) string {
	for _, part := range strings.Split(strings.Trim(addr, "[]"), ",") {
		if rest, ok := strings.CutPrefix(part, "v2:"); ok {
			return rest
		}
	}
	return ""
}

// peeringPGIDs returns the PGs a message of peeringSymbols concerns.
func peeringPGIDs(name, text string) []string {
	switch name {
	case "pg_create2":
		var pgids []string
		for _, m := range peeringCreatePGs.FindAllStringSubmatch(text, -1) {
			pgids = append(pgids, m[1])
		}
		return pgids
	case "replica_scrub":
		if m := peeringScrubPG.FindStringSubmatch(text); m != nil {
			return []string{m[1]}
		}
	default:
		if m := peeringFirstPG.FindStringSubmatch(text); m != nil {
			return []string{m[1]}
		}
	}
	return nil
}

// parsePeeringLog picks the peering events out of one daemon's log. A message
// of peeringSymbols whose PG cannot be read, or a line naming a v1-only peer,
// is unusable rather than skipped.
func parsePeeringLog(lines []string) (events []peeringEvent, unusable []string) {
	for index, line := range lines {
		event := peeringEvent{Line: index, Text: line}
		switch m := peeringInLine.FindStringSubmatch(line); {
		case m != nil:
			event.Kind, event.Local, event.Peer, event.Addr, event.Message = "in", v2Addr(m[1]), m[2], v2Addr(m[3]), m[5]
			event.Seq, _ = strconv.ParseUint(m[4], 10, 64)
		default:
			m := peeringOutLine.FindStringSubmatch(line)
			if m != nil {
				event.Kind, event.Local, event.Addr, event.Message = "out", v2Addr(m[1]), v2Addr(m[2]), m[3]
				break
			}
			if m := peeringEnter.FindStringSubmatch(line); m != nil {
				events = append(events, peeringEvent{Line: index, Kind: "enter", State: m[2], PGIDs: []string{m[1]}, Text: line})
			} else if m := peeringScrubber.FindStringSubmatch(line); m != nil {
				events = append(events, peeringEvent{Line: index, Kind: "scrub", State: m[2], PGIDs: []string{m[1]}, Text: line})
			}
			continue
		}
		name := peeringName.FindStringSubmatch(event.Message)
		if name == nil || peeringSymbols[name[1]] == "" {
			continue
		}
		event.Type, event.PGIDs = name[1], peeringPGIDs(name[1], event.Message)
		if len(event.PGIDs) == 0 || event.Local == "" || event.Addr == "" {
			unusable = append(unusable, line)
			continue
		}
		events = append(events, event)
	}
	return events, unusable
}

// peeringDaemon is one daemon's side of the judged messages: its entity, the
// v2 address of every messenger it logged with, and its parsed log.
type peeringDaemon struct {
	Name   string
	Addrs  []string
	Events []peeringEvent
}

// concerns reports whether the event concerns pgid.
func (e peeringEvent) concerns(pgid string) bool {
	return slices.Contains(e.PGIDs, pgid)
}

// peeringPair is one message that its sender logged as sent and its receiver
// as received.
type peeringPair struct {
	Type     string       `json:"type"`
	Symbol   string       `json:"symbol"`
	From     string       `json:"from"`
	To       string       `json:"to"`
	Sent     peeringEvent `json:"sent"`
	Received peeringEvent `json:"received"`
}

// pairPeeringMessages joins the messages of one type about pgid that from
// sent to one of to's messengers with the ones to received from from at one
// of from's messengers with the same printed message. Each received message joins the
// first sent one it can, in order, and each sent one joins at most one.
func pairPeeringMessages(from, to peeringDaemon, name, pgid string) []peeringPair {
	var pairs []peeringPair
	used := map[int]bool{}
	for _, in := range to.Events {
		if in.Kind != "in" || in.Type != name || !in.concerns(pgid) || in.Peer != from.Name || !slices.Contains(from.Addrs, in.Addr) || !slices.Contains(to.Addrs, in.Local) {
			continue
		}
		for _, out := range from.Events {
			if out.Kind == "out" && !used[out.Line] && out.Type == name && out.Message == in.Message && out.Addr == in.Local && out.Local == in.Addr {
				used[out.Line] = true
				pairs = append(pairs, peeringPair{Type: name, Symbol: peeringSymbols[name], From: from.Name, To: to.Name, Sent: out, Received: in})
				break
			}
		}
	}
	return pairs
}

// peeringFlow is one message type that must pass between two daemons.
type peeringFlow struct {
	name     string
	from, to *peeringDaemon
}

// peeringJudgement is what the judge joined: the first pair of every flow,
// the states each OSD entered for the PG, and why anything is missing.
type peeringJudgement struct {
	Pairs         map[string]peeringPair `json:"pairs"`
	PairCounts    map[string]int         `json:"pair_counts"`
	PrimaryStates []string               `json:"primary_states"`
	ReplicaStates []string               `json:"replica_states"`
	Problems      []string               `json:"problems,omitempty"`
}

// firstIndex returns the line of the first event of daemon d that ok accepts
// at or after line from, or -1.
func firstIndex(d peeringDaemon, from int, ok func(peeringEvent) bool) int {
	for _, e := range d.Events {
		if e.Line >= from && ok(e) {
			return e.Line
		}
	}
	return -1
}

// judgePeering requires every message of peeringSymbols about pgid to pass
// in its direction: the MON's pg_create2 to the primary, the primary's
// pg_query2, PGlog, pg_lease and replica_scrub to the replica, and the
// replica's pg_notify2, pg_info2, pg_lease_ack and rep_scrubmap to the
// primary, each logged as sent by one and received by the other. It then
// places the messages among the PG's states in each OSD's own log: the
// primary sends pg_query2 and receives pg_notify2 between entering
// Peering/GetInfo and Peering/GetLog, and sends PGlog after entering Active
// and receives the replica's pg_info2 before entering Active/Clean; the
// replica receives PGlog before entering ReplicaActive; the primary sends
// replica_scrub after its scrubber enters ActiveScrubbing and receives
// rep_scrubmap after it enters WaitReplicas, and the replica sends
// rep_scrubmap after its scrubber enters ReplicaBuildingMap for the received
// replica_scrub.
func judgePeering(mon, primary, replica peeringDaemon, pgid string) peeringJudgement {
	judgement := peeringJudgement{Pairs: map[string]peeringPair{}, PairCounts: map[string]int{}}
	problem := func(format string, args ...any) {
		judgement.Problems = append(judgement.Problems, fmt.Sprintf(format, args...))
	}
	flows := []peeringFlow{
		{"pg_create2", &mon, &primary},
		{"pg_query2", &primary, &replica}, {"pg_notify2", &replica, &primary},
		{"PGlog", &primary, &replica}, {"pg_info2", &replica, &primary},
		{"pg_lease", &primary, &replica}, {"pg_lease_ack", &replica, &primary},
		{"replica_scrub", &primary, &replica}, {"rep_scrubmap", &replica, &primary},
	}
	for _, flow := range flows {
		pairs := pairPeeringMessages(*flow.from, *flow.to, flow.name, pgid)
		judgement.PairCounts[flow.name] = len(pairs)
		if len(pairs) == 0 {
			problem("no %s about %s from %s to %s", flow.name, pgid, flow.from.Name, flow.to.Name)
			continue
		}
		judgement.Pairs[flow.name] = pairs[0]
	}
	for _, e := range primary.Events {
		if e.Kind == "enter" && e.concerns(pgid) {
			judgement.PrimaryStates = append(judgement.PrimaryStates, e.State)
		}
	}
	for _, e := range replica.Events {
		if e.Kind == "enter" && e.concerns(pgid) {
			judgement.ReplicaStates = append(judgement.ReplicaStates, e.State)
		}
	}
	if len(judgement.Problems) != 0 {
		return judgement
	}

	enter := func(d peeringDaemon, from int, state string) int {
		return firstIndex(d, from, func(e peeringEvent) bool { return e.Kind == "enter" && e.State == state && e.concerns(pgid) })
	}
	scrubber := func(d peeringDaemon, from int, suffix string) int {
		return firstIndex(d, from, func(e peeringEvent) bool {
			return e.Kind == "scrub" && strings.HasSuffix(e.State, suffix) && e.concerns(pgid)
		})
	}
	// within requires line to lie after begin and, when end is not -1,
	// before it.
	within := func(what string, line, begin, end int) {
		switch {
		case begin < 0:
			problem("%s: the bounding state was never entered", what)
		case line <= begin, end >= 0 && line >= end:
			problem("%s at line %d is not between lines %d and %d", what, line, begin, end)
		}
	}
	pairs := judgement.Pairs

	getInfo := enter(primary, 0, "Started/Primary/Peering/GetInfo")
	getLog := enter(primary, max(getInfo, 0), "Started/Primary/Peering/GetLog")
	if getLog < 0 {
		problem("the primary never entered Started/Primary/Peering/GetLog")
	}
	within("the primary's pg_query2", pairs["pg_query2"].Sent.Line, getInfo, getLog)
	within("the primary's receipt of pg_notify2", pairs["pg_notify2"].Received.Line, getInfo, getLog)

	active := enter(primary, max(getLog, 0), "Started/Primary/Active")
	clean := enter(primary, max(active, 0), "Started/Primary/Active/Clean")
	if clean < 0 {
		problem("the primary never entered Started/Primary/Active/Clean")
	}
	within("the primary's PGlog", pairs["PGlog"].Sent.Line, active, clean)
	within("the primary's receipt of pg_info2", pairs["pg_info2"].Received.Line, pairs["PGlog"].Sent.Line, clean)
	within("the replica's ReplicaActive", enter(replica, 0, "Started/ReplicaActive"), pairs["PGlog"].Received.Line, -1)
	within("the replica's pg_info2", pairs["pg_info2"].Sent.Line, pairs["PGlog"].Received.Line, -1)

	scrubbing := scrubber(primary, 0, "ActiveScrubbing")
	within("the primary's replica_scrub", pairs["replica_scrub"].Sent.Line, scrubbing, -1)
	within("the primary's receipt of rep_scrubmap", pairs["rep_scrubmap"].Received.Line, scrubber(primary, max(scrubbing, 0), "WaitReplicas"), -1)
	building := scrubber(replica, pairs["replica_scrub"].Received.Line, "ReplicaBuildingMap")
	within("the replica's rep_scrubmap", pairs["rep_scrubmap"].Sent.Line, building, -1)
	return judgement
}

// peeringDebugConfig makes the MONs log their messages and the OSDs their
// messages and the entered peering and scrubber states.
const peeringDebugConfig = "[mon]\ndebug_ms = 1\n[osd]\ndebug_ms = 1\ndebug_osd = 10\n"

// peeringPool and peeringObject are the pool whose one PG the test creates
// and the object it scrubs.
const (
	peeringPool   = "peering-messages"
	peeringObject = "peering-object"
)

// osdPeeringReceipt is the evidence of one PG's creation, peering, lease and
// deep scrub messages. It is evidence for this release and image only.
type osdPeeringReceipt struct {
	Release   string           `json:"release"`
	Platform  string           `json:"platform"`
	Run       string           `json:"run,omitempty"`
	PGID      string           `json:"pgid"`
	MON       string           `json:"mon"`
	Primary   string           `json:"primary"`
	Replica   string           `json:"replica"`
	Clean     string           `json:"clean"`
	Scrubbed  string           `json:"scrubbed"`
	Unusable  []string         `json:"unusable,omitempty"`
	Judgement peeringJudgement `json:"judgement"`
}

// testOSDPeeringMessages creates a pool with one PG on two OSDs, writes an
// object to it and deep-scrubs the PG while the MON and both OSDs log their
// messages at debug_ms 1 and the OSDs their peering and scrubber states at
// debug_osd 10, then judges the logs with judgePeering.
func testOSDPeeringMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	config := filepath.Join(t.TempDir(), "ceph.conf")
	if err := os.WriteFile(config, []byte(peeringDebugConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	cluster, client := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(2)), ceph.WithConfigFile(config))
	receipt := osdPeeringReceipt{Platform: runtime.GOOS + "/" + runtime.GOARCH}
	if run := os.Getenv("GITHUB_RUN_ID"); run != "" {
		receipt.Run = run + "/" + os.Getenv("GITHUB_RUN_ATTEMPT")
	}
	defer func() {
		data, err := json.Marshal(receipt)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("OSD_PEERING_MESSAGES_RECEIPT %s", data)
	}()
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: peeringPool, PGNum: 1, Replicas: 2, MinSize: 1, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	put, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv: []string{"rados", "-p", peeringPool, "put", peeringObject, "/etc/os-release"}, Timeout: time.Minute,
		MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "peering object",
	})
	if err != nil || put.ExitCode != 0 || !put.Complete() {
		t.Fatalf("put: exit %d: %v: %.2048s", put.ExitCode, err, put.Stderr)
	}
	snapshot, err := harness.PoolHandle(t, ctx, cluster, peeringPool).PGs(ctx)
	if err != nil || len(snapshot.PGs) != 1 {
		t.Fatalf("PGs of %s: %v, %d PGs", peeringPool, err, len(snapshot.PGs))
	}
	pg := snapshot.PGs[0]
	if len(pg.Acting) != 2 || !slices.Equal(pg.Up, pg.Acting) {
		t.Fatalf("PG %s is %s on %v, up %v", pg.PGID, pg.State, pg.Acting, pg.Up)
	}
	receipt.PGID, receipt.Clean = pg.PGID.String(), pg.State
	if err := cluster.DeepScrubPG(ctx, pg.PGID); err != nil {
		t.Fatal(err)
	}
	snapshot, err = harness.PoolHandle(t, ctx, cluster, peeringPool).PGs(ctx)
	if err != nil || len(snapshot.PGs) != 1 || snapshot.PGs[0].ActingPrimary != pg.ActingPrimary || !slices.Equal(snapshot.PGs[0].Acting, pg.Acting) {
		t.Fatalf("the PG moved during the scrub: %v: %+v", err, snapshot.PGs)
	}
	receipt.Scrubbed = snapshot.PGs[0].State

	daemon := func(name string, events []peeringEvent) peeringDaemon {
		d := peeringDaemon{Name: name, Events: events}
		for _, e := range events {
			if e.Local != "" && !slices.Contains(d.Addrs, e.Local) {
				d.Addrs = append(d.Addrs, e.Local)
			}
		}
		return d
	}
	parse := func(name string, lines []string) peeringDaemon {
		events, unusable := parsePeeringLog(lines)
		for _, line := range unusable {
			receipt.Unusable = append(receipt.Unusable, name+": "+line)
		}
		return daemon(name, events)
	}
	monitors := clusteraccess.Monitors(t, cluster)
	if len(monitors) != 1 {
		t.Fatalf("%d MONs, want 1", len(monitors))
	}
	receipt.MON = "mon.0"
	mon := parse(receipt.MON, backoffDaemonLog(t, ctx, monitors[0].Container))
	var primary, replica peeringDaemon
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		name := fmt.Sprintf("osd.%d", osd.ID)
		switch {
		case osd.ID == pg.ActingPrimary:
			primary = parse(name, backoffDaemonLog(t, ctx, osd.Container))
		case slices.Contains(pg.Acting, osd.ID):
			replica = parse(name, backoffDaemonLog(t, ctx, osd.Container))
		}
	}
	if primary.Name == "" || replica.Name == "" {
		t.Fatalf("no primary and replica OSD containers for %+v", pg)
	}
	receipt.Primary, receipt.Replica = primary.Name, replica.Name
	version := targetedJSON[map[string]string](t, ctx, cluster, "tell", primary.Name, "version")
	receipt.Release = version["version"]
	receipt.Judgement = judgePeering(mon, primary, replica, receipt.PGID)
	if len(receipt.Unusable) != 0 {
		t.Fatalf("unusable peering message lines: %q", receipt.Unusable)
	}
	if len(receipt.Judgement.Problems) != 0 {
		t.Fatalf("peering messages of %s: %s", receipt.PGID, strings.Join(receipt.Judgement.Problems, "; "))
	}
}
