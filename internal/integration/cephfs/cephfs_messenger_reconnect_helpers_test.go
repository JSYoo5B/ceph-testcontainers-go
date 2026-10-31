//go:build all || integration

package cephfs_test

import (
	"fmt"
	"strconv"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// reconnectVerdict is the answer of a reconnect or reset judge.
type reconnectVerdict int

const (
	// reconnectInconclusive means the evidence cannot support either answer.
	// It is the default, so a forgotten branch can never pass.
	reconnectInconclusive reconnectVerdict = iota
	// reconnectObserved means the session reconnected or reset as claimed.
	reconnectObserved
	// reconnectContradicted means the evidence shows the opposite: a lossless
	// session that was replaced or restarted its sequence, or a lossy session
	// that resumed.
	reconnectContradicted
)

func (v reconnectVerdict) String() string {
	return map[reconnectVerdict]string{reconnectInconclusive: "inconclusive", reconnectObserved: "observed", reconnectContradicted: "contradicted"}[v]
}

func (v reconnectVerdict) MarshalText() ([]byte, error) { return []byte(v.String()), nil }

// reconnectOutcome is a verdict with its reason and the READY sessions that
// support it.
type reconnectOutcome struct {
	Verdict  reconnectVerdict       `json:"verdict"`
	Reason   string                 `json:"reason"`
	Sessions []harness.ReadySession `json:"sessions,omitempty"`
}

// clientSessions selects the READY sessions of client with the given policy.
func clientSessions(sessions []harness.ReadySession, client uint64, lossy bool) []harness.ReadySession {
	id := strconv.FormatUint(client, 10)
	var result []harness.ReadySession
	for _, s := range sessions {
		if s.PeerType == "client" && s.PeerID == id && s.Lossy == lossy {
			result = append(result, s)
		}
	}
	return result
}

// judgeLosslessReconnect decides from the READY sessions that the accepting
// daemon logged for client before and during a fault window whether its
// lossless session reconnected. Before the window the client must have one
// established session with both cookies. Every READY of that client in the
// window must then be the same session: the same connection object and
// cookies, a larger connect_seq and peer global sequence than the one before
// it, and in_seq and out_seq that never go back, so that what was received
// and sent before a fault still counts after it. connect_seq can skip values,
// because an attempt that faults before READY also takes one.
func judgeLosslessReconnect(before, window []harness.ReadySession, client uint64) reconnectOutcome {
	base := clientSessions(before, client, false)
	if len(base) != 1 {
		return reconnectOutcome{Reason: fmt.Sprintf("%d lossless sessions of client.%d before the window, want 1", len(base), client)}
	}
	established := base[0]
	if established.ConnectSeq != 0 || established.ClientCookie == "0" || established.ServerCookie == "0" {
		return reconnectOutcome{Reason: fmt.Sprintf("the session before the window is not a new lossless session: cs=%d cookies %s/%s",
			established.ConnectSeq, established.ClientCookie, established.ServerCookie)}
	}
	reconnects := clientSessions(window, client, false)
	if len(reconnects) == 0 {
		return reconnectOutcome{Reason: fmt.Sprintf("no READY of client.%d's lossless session in the window", client), Sessions: base}
	}
	previous := established
	for i, s := range reconnects {
		switch {
		case !s.Revision1:
			return reconnectOutcome{Verdict: reconnectContradicted, Reason: fmt.Sprintf("reconnect %d is not msgr2.1", i), Sessions: append(base, s)}
		case s.ClientCookie != established.ClientCookie || s.ServerCookie != established.ServerCookie:
			return reconnectOutcome{Verdict: reconnectContradicted, Reason: fmt.Sprintf("reconnect %d has cookies %s/%s, not %s/%s: the session was replaced",
				i, s.ClientCookie, s.ServerCookie, established.ClientCookie, established.ServerCookie), Sessions: append(base, s)}
		case s.Conn != established.Conn:
			return reconnectOutcome{Verdict: reconnectContradicted, Reason: fmt.Sprintf("reconnect %d was logged by connection %s, not %s", i, s.Conn, established.Conn), Sessions: append(base, s)}
		case s.ConnectSeq <= previous.ConnectSeq || s.PeerGlobalSeq <= previous.PeerGlobalSeq:
			return reconnectOutcome{Verdict: reconnectContradicted, Reason: fmt.Sprintf("reconnect %d has cs=%d pgs=%d after cs=%d pgs=%d",
				i, s.ConnectSeq, s.PeerGlobalSeq, previous.ConnectSeq, previous.PeerGlobalSeq), Sessions: append(base, s)}
		case s.InSeq < previous.InSeq || s.OutSeq < previous.OutSeq:
			return reconnectOutcome{Verdict: reconnectContradicted, Reason: fmt.Sprintf("reconnect %d has in_seq=%d out_seq=%d after %d/%d: the sequence restarted",
				i, s.InSeq, s.OutSeq, previous.InSeq, previous.OutSeq), Sessions: append(base, s)}
		}
		previous = s
	}
	if previous.InSeq == 0 || previous.OutSeq == 0 {
		return reconnectOutcome{Reason: fmt.Sprintf("the last reconnect has in_seq=%d out_seq=%d: no exchanged message is carried over", previous.InSeq, previous.OutSeq),
			Sessions: append(base, reconnects...)}
	}
	return reconnectOutcome{Verdict: reconnectObserved, Reason: fmt.Sprintf("%d reconnects of one session, cs %d, in_seq %d, out_seq %d",
		len(reconnects), previous.ConnectSeq, previous.InSeq, previous.OutSeq), Sessions: append(base, reconnects...)}
}

// judgeLossyResets decides from the READY sessions that the accepting
// daemons logged for client before and during a fault window whether its
// lossy sessions were reset. A lossy session is never resumed: every READY in
// the window must be a new session, with connect_seq 0, no server cookie, a
// client cookie that no earlier session had, and in_seq and out_seq 0.
func judgeLossyResets(before, window []harness.ReadySession, client uint64) reconnectOutcome {
	base := clientSessions(before, client, true)
	if len(base) == 0 {
		return reconnectOutcome{Reason: fmt.Sprintf("no lossy session of client.%d before the window", client)}
	}
	resets := clientSessions(window, client, true)
	if len(resets) == 0 {
		return reconnectOutcome{Reason: fmt.Sprintf("no READY of a lossy session of client.%d in the window", client), Sessions: base}
	}
	for i, s := range resets {
		switch {
		case !s.Revision1:
			return reconnectOutcome{Verdict: reconnectContradicted, Reason: fmt.Sprintf("reset %d is not msgr2.1", i), Sessions: []harness.ReadySession{s}}
		case s.ConnectSeq != 0 || s.ServerCookie != "0" || s.InSeq != 0 || s.OutSeq != 0:
			return reconnectOutcome{Verdict: reconnectContradicted, Reason: fmt.Sprintf("reset %d resumed: cs=%d server_cookie=%s in_seq=%d out_seq=%d",
				i, s.ConnectSeq, s.ServerCookie, s.InSeq, s.OutSeq), Sessions: []harness.ReadySession{s}}
		case s.ClientCookie == "0":
			return reconnectOutcome{Reason: fmt.Sprintf("reset %d has no client cookie to tell it from another session", i), Sessions: []harness.ReadySession{s}}
		}
	}
	// Each reset's fresh state (cs=0, server_cookie=0, seq 0/0) proves it is a
	// new session rather than a resumed one; a lossy client may reuse a client
	// cookie for a new connection to the same peer, so cookie reuse across
	// resets is not a contradiction.
	return reconnectOutcome{Verdict: reconnectObserved, Reason: fmt.Sprintf("%d new lossy sessions", len(resets)), Sessions: append(base, resets...)}
}

// judgeDumpedReconnect compares the accepting daemon's messenger dump of
// client's lossless connection before and after the window with the last
// logged reconnect: the same connection object and peer, now READY with that
// reconnect's connect_seq.
func judgeDumpedReconnect(before, after []harness.MessengerConnection, client uint64, last harness.ReadySession) reconnectOutcome {
	pick := func(connections []harness.MessengerConnection) []harness.MessengerConnection {
		var result []harness.MessengerConnection
		for _, c := range connections {
			if c.PeerType == "client" && c.GlobalID == client && c.Ready() {
				result = append(result, c)
			}
		}
		return result
	}
	was, is := pick(before), pick(after)
	if len(was) != 1 || len(is) != 1 {
		return reconnectOutcome{Reason: fmt.Sprintf("%d READY connections of client.%d before and %d after, want 1 and 1", len(was), client, len(is))}
	}
	switch {
	case is[0].ConnID != was[0].ConnID || is[0].PeerNonce != was[0].PeerNonce:
		return reconnectOutcome{Verdict: reconnectContradicted, Reason: fmt.Sprintf("connection %d of nonce %d became %d of nonce %d", was[0].ConnID, was[0].PeerNonce, is[0].ConnID, is[0].PeerNonce)}
	case !is[0].Revision1:
		return reconnectOutcome{Verdict: reconnectContradicted, Reason: "the reconnected connection is not msgr2.1"}
	case was[0].ConnectSeq != 0 || is[0].ConnectSeq != last.ConnectSeq || is[0].PeerGlobalSeq != last.PeerGlobalSeq:
		return reconnectOutcome{Verdict: reconnectContradicted, Reason: fmt.Sprintf("dumped connect_seq %d then %d and peer_global_seq %d, logged cs=%d pgs=%d",
			was[0].ConnectSeq, is[0].ConnectSeq, is[0].PeerGlobalSeq, last.ConnectSeq, last.PeerGlobalSeq)}
	}
	return reconnectOutcome{Verdict: reconnectObserved, Reason: fmt.Sprintf("connection %d, connect_seq 0 then %d", is[0].ConnID, is[0].ConnectSeq)}
}
