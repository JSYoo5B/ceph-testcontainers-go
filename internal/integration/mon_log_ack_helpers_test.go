//go:build all || integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// This file reads the evidence of TestMONLogAckThroughPeon (issue 389): the
// MLogAck that a daemon's LogClient waits for after its cluster log entries
// reach the MONs. A daemon sends its entries in an MLog to the MON it has a
// session with. A peon forwards it to the leader, which commits the entries
// and routes an MLogAck back through the peon. A 20.2.4 leader also calls
// no_reply for that forwarded MLog: the peon receives an empty route first,
// erases its routed request, and discards the MLogAck that follows ("don't
// have routed request"), so the daemon's queue is never trimmed. A 19.2.2
// leader does not.

// logAckMonStatus is the part of a MON's local mon_status that the test needs.
type logAckMonStatus struct {
	Name          string `json:"name"`
	State         string `json:"state"`
	Rank          int    `json:"rank"`
	Quorum        []int  `json:"quorum"`
	ElectionEpoch int    `json:"election_epoch"`
}

// logAckSession is one session of a MON's admin socket sessions listing.
type logAckSession struct {
	Name       string `json:"name"`
	EntityName string `json:"entity_name"`
	ConType    string `json:"con_type"`
	GlobalID   uint64 `json:"global_id"`
}

// logAckSessionID is one OSD's MON session: the MON and the session's
// global_id, so a new session with the same MON is a different session.
type logAckSessionID struct {
	Mon      string
	GlobalID uint64
}

func (id logAckSessionID) String() string { return fmt.Sprintf("mon.%s#%d", id.Mon, id.GlobalID) }

// osdMonSessions maps each OSD named in the MONs' sessions listings to its
// session. An OSD listed by two MONs, or twice by one, is reported: its
// session is moving, and the arrangement cannot be used.
func osdMonSessions(listings map[string][]byte) (map[string]logAckSessionID, error) {
	owners := map[string]logAckSessionID{}
	names := make([]string, 0, len(listings))
	for mon := range listings {
		names = append(names, mon)
	}
	slices.Sort(names)
	for _, mon := range names {
		var sessions []logAckSession
		if err := json.Unmarshal(listings[mon], &sessions); err != nil {
			return nil, fmt.Errorf("mon.%s sessions: %w", mon, err)
		}
		for _, s := range sessions {
			if s.ConType != "osd" {
				continue
			}
			if other, ok := owners[s.EntityName]; ok {
				return nil, fmt.Errorf("%s has sessions %s and mon.%s#%d", s.EntityName, other, mon, s.GlobalID)
			}
			owners[s.EntityName] = logAckSessionID{Mon: mon, GlobalID: s.GlobalID}
		}
	}
	return owners, nil
}

// logAckRole is how an OSD reaches the leader: directly, or through a peon.
func logAckRole(sessionMon, leader string) string {
	if sessionMon == leader {
		return "leader"
	}
	return "peon"
}

var (
	// LogClient at debug_monc 10: "will send <stamp> osd.0 (osd.0) 5 : cluster [DBG] ..."
	logClientSend = regexp.MustCompile(`log_client\s+will send \S+ (\S+) \(\S+\) (\d+) : `)
	// LogClient at debug_monc 10: "handle_log_ack log(last 5)"
	logClientAck = regexp.MustCompile(`log_client\s+handle_log_ack log\(last (\d+)\)`)
	// A peon at debug_mon 10: "handle_route tid 198 log(last 34)" and then
	// " don't have routed request tid 198".
	peonRouteAck  = regexp.MustCompile(`handle_route tid (\d+) log\(last (\d+)\)`)
	peonRouteLost = regexp.MustCompile(`don't have routed request tid (\d+)`)
)

// logClientProgress is what one daemon's log shows of its cluster log
// entries: the highest sequence it sent and the highest one acknowledged.
type logClientProgress struct {
	Sent  uint64 `json:"sent"`
	Acked uint64 `json:"acked"`
	Acks  int    `json:"acks"`
}

// readLogClientProgress reads the LogClient lines of entity in a daemon log.
func readLogClientProgress(log []byte, entity string) logClientProgress {
	var p logClientProgress
	for _, m := range logClientSend.FindAllSubmatch(log, -1) {
		if string(m[1]) != entity {
			continue
		}
		if seq, err := strconv.ParseUint(string(m[2]), 10, 64); err == nil && seq > p.Sent {
			p.Sent = seq
		}
	}
	for _, m := range logClientAck.FindAllSubmatch(log, -1) {
		p.Acks++
		if seq, err := strconv.ParseUint(string(m[1]), 10, 64); err == nil && seq > p.Acked {
			p.Acked = seq
		}
	}
	return p
}

// lostRoutedAcks lists the MLogAcks that a peon's log shows it received for a
// forwarded request it no longer had, as "tid:last".
func lostRoutedAcks(log []byte) []string {
	acks := map[string]string{}
	for _, m := range peonRouteAck.FindAllSubmatch(log, -1) {
		acks[string(m[1])] = string(m[2])
	}
	var lost []string
	for _, m := range peonRouteLost.FindAllSubmatch(log, -1) {
		if last, ok := acks[string(m[1])]; ok {
			lost = append(lost, string(m[1])+":"+last)
		}
	}
	return lost
}

// monClientTransitions lists the MonClient lines of a daemon log that show its
// MON session being reset, reopened or moved: at debug_monc 10 every such
// change logs one of them, even one that ends on the same MON.
func monClientTransitions(log []byte) []string {
	var found []string
	for _, line := range strings.Split(string(log), "\n") {
		if strings.Contains(line, "monclient") && monClientTransition.MatchString(line) {
			found = append(found, strings.TrimSpace(line))
		}
	}
	return found
}

var (
	monClientTransition = regexp.MustCompile(`_reopen_session|picked mon\.|_finish_hunting|found mon\.|ms_handle_reset|continuing hunt`)
	// A MON at debug_mon 10: "preprocess_query log(2 entries from seq 4 at <stamp>) from osd.0 <addr>"
	monLogQuery = regexp.MustCompile(`preprocess_query (log\(\d+ entries from seq \d+ at \S+\)) from (\S+) `)
	// A peon at debug_mon 10: "forward_request 198 request log(1 entries from seq 34 at <stamp>) features ..."
	peonForward = regexp.MustCompile(`forward_request (\d+) request (log\(\d+ entries from seq \d+ at \S+\))`)
)

// routedLogAck binds an OSD's acknowledgement to the peon path: in the peon's
// log, an MLog of entity is forwarded under a tid, and the MLogAck routed
// back under that tid covers sent and is not discarded. It returns the tid,
// or why no forwarded MLog of entity was acknowledged through the peon.
func routedLogAck(log []byte, entity string, sent uint64) (string, string) {
	mine := map[string]bool{}
	for _, m := range monLogQuery.FindAllSubmatch(log, -1) {
		if string(m[2]) == entity {
			mine[string(m[1])] = true
		}
	}
	tids := map[string]bool{}
	for _, m := range peonForward.FindAllSubmatch(log, -1) {
		if mine[string(m[2])] {
			tids[string(m[1])] = true
		}
	}
	if len(tids) == 0 {
		return "", "no MLog of " + entity + " was forwarded"
	}
	lost := map[string]bool{}
	for _, m := range peonRouteLost.FindAllSubmatch(log, -1) {
		lost[string(m[1])] = true
	}
	why := fmt.Sprintf("no MLogAck routed back for the %d forwarded MLogs of %s", len(tids), entity)
	for _, m := range peonRouteAck.FindAllSubmatch(log, -1) {
		tid := string(m[1])
		last, err := strconv.ParseUint(string(m[2]), 10, 64)
		switch {
		case !tids[tid] || err != nil:
		case lost[tid]:
			why = "the MLogAck of tid " + tid + " was discarded"
		case last >= sent:
			return tid, ""
		}
	}
	return "", why
}
