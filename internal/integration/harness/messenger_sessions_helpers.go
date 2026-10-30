//go:build all || integration

package harness

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// MessengerConnection is one connection in a daemon's "messenger dump": its
// messenger, peer, protocol state, connection mode, the crypto and
// compression handlers of the session, and its identity and sequence state.
// ConnID names the connection object for the life of the daemon process,
// ConnectSeq counts the reconnects of its session and PeerGlobalSeq is the
// peer's global sequence of its latest connection attempt.
type MessengerConnection struct {
	Messenger     string `json:"messenger"`
	PeerType      string `json:"peer_type"`
	PeerID        int64  `json:"peer_id"`
	GlobalID      uint64 `json:"global_id"`
	PeerNonce     uint64 `json:"peer_nonce"`
	ConnID        int64  `json:"conn_id"`
	State         string `json:"state"`
	Connected     bool   `json:"connected"`
	Mode          string `json:"mode"`
	Revision1     bool   `json:"rev1"`
	ConnectSeq    uint64 `json:"connect_seq"`
	PeerGlobalSeq uint64 `json:"peer_global_seq"`
	CryptoRx      string `json:"crypto_rx"`
	CryptoTx      string `json:"crypto_tx"`
	CompressionRx string `json:"compression_rx"`
	CompressionTx string `json:"compression_tx"`
}

// Ready reports whether the connection is an established msgr2 session.
func (c MessengerConnection) Ready() bool { return c.Connected && c.State == "READY" }

// MessengerAddress is one entry of an address vector in "messenger dump".
type MessengerAddress struct {
	Type  string `json:"type"`
	Addr  string `json:"addr"`
	Nonce uint64 `json:"nonce"`
}

// MessengerSelf is what one messenger reports about itself: the addresses it
// advertises and how many sockets it listens on. A client messenger, such as
// one of radosgw's, advertises an "any" address and listens on none.
type MessengerSelf struct {
	Name          string             `json:"name"`
	Addrs         []MessengerAddress `json:"addrs"`
	ListenSockets int                `json:"listen_sockets"`
}

// ParseMessengerCatalog returns the messenger names of "messenger dump"
// without an argument. Ceph wraps the result in "status".
func ParseMessengerCatalog(data []byte) ([]string, error) {
	var dump struct {
		Status *struct {
			Messengers []string `json:"messengers"`
		} `json:"status"`
		Messengers []string `json:"messengers"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		return nil, err
	}
	names := dump.Messengers
	if dump.Status != nil {
		names = dump.Status.Messengers
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("messenger dump lists no messengers: %.300s", data)
	}
	return names, nil
}

type dumpedAsyncConnection struct {
	State  string `json:"state"`
	ConnID *int64 `json:"conn_id"`
	Status struct {
		Connected bool `json:"connected"`
	} `json:"status"`
	Peer struct {
		Type     string `json:"type"`
		ID       int64  `json:"id"`
		GlobalID uint64 `json:"global_id"`
		Addr     struct {
			Addrvec []MessengerAddress `json:"addrvec"`
		} `json:"addr"`
	} `json:"peer"`
	Protocol struct {
		V2 *struct {
			State         string `json:"state"`
			ConMode       string `json:"con_mode"`
			Rev1          bool   `json:"rev1"`
			ConnectSeq    uint64 `json:"connect_seq"`
			PeerGlobalSeq uint64 `json:"peer_global_seq"`
			Crypto        struct {
				Rx string `json:"rx"`
				Tx string `json:"tx"`
			} `json:"crypto"`
			Compression struct {
				Rx string `json:"rx"`
				Tx string `json:"tx"`
			} `json:"compression"`
		} `json:"v2"`
	} `json:"protocol"`
}

type dumpedMessenger struct {
	MyAddrs struct {
		Addrvec []MessengerAddress `json:"addrvec"`
	} `json:"my_addrs"`
	ListenSockets []json.RawMessage `json:"listen_sockets"`
	Connections   []struct {
		Async dumpedAsyncConnection `json:"async_connection"`
	} `json:"connections"`
	AnonConns []dumpedAsyncConnection `json:"anon_conns"`
}

func parseDumpedMessenger(data []byte) (string, *dumpedMessenger, error) {
	var dump struct {
		Status *struct {
			Name      string           `json:"name"`
			Messenger *dumpedMessenger `json:"messenger"`
		} `json:"status"`
		Name      string           `json:"name"`
		Messenger *dumpedMessenger `json:"messenger"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		return "", nil, err
	}
	name, body := dump.Name, dump.Messenger
	if dump.Status != nil {
		name, body = dump.Status.Name, dump.Status.Messenger
	}
	if body == nil {
		return "", nil, fmt.Errorf("messenger dump has no messenger: %.300s", data)
	}
	return name, body, nil
}

// ParseMessengerConnections reads "messenger dump <name>": the connections
// and anonymous connections of one messenger.
func ParseMessengerConnections(data []byte) ([]MessengerConnection, error) {
	name, body, err := parseDumpedMessenger(data)
	if err != nil {
		return nil, err
	}
	all := make([]dumpedAsyncConnection, 0, len(body.Connections)+len(body.AnonConns))
	for _, c := range body.Connections {
		all = append(all, c.Async)
	}
	all = append(all, body.AnonConns...)
	var result []MessengerConnection
	for _, c := range all {
		connection := MessengerConnection{Messenger: name, PeerType: c.Peer.Type, PeerID: c.Peer.ID, GlobalID: c.Peer.GlobalID,
			State: c.State, Connected: c.Status.Connected, ConnID: -1}
		if c.ConnID != nil {
			connection.ConnID = *c.ConnID
		}
		if len(c.Peer.Addr.Addrvec) > 0 {
			connection.PeerNonce = c.Peer.Addr.Addrvec[0].Nonce
		}
		if v2 := c.Protocol.V2; v2 != nil {
			connection.State, connection.Mode, connection.Revision1 = v2.State, v2.ConMode, v2.Rev1
			connection.ConnectSeq, connection.PeerGlobalSeq = v2.ConnectSeq, v2.PeerGlobalSeq
			connection.CryptoRx, connection.CryptoTx = v2.Crypto.Rx, v2.Crypto.Tx
			connection.CompressionRx, connection.CompressionTx = v2.Compression.Rx, v2.Compression.Tx
		}
		result = append(result, connection)
	}
	return result, nil
}

// ParseMessengerSelf reads the advertised addresses and listening sockets of
// "messenger dump <name>".
func ParseMessengerSelf(data []byte) (MessengerSelf, error) {
	name, body, err := parseDumpedMessenger(data)
	if err != nil {
		return MessengerSelf{}, err
	}
	return MessengerSelf{Name: name, Addrs: body.MyAddrs.Addrvec, ListenSockets: len(body.ListenSockets)}, nil
}

// ReadyLogConnection is one msgr2 session that a daemon logged as READY at
// debug_ms 1. A release without messenger dump prints the session's crypto
// and compression handlers there only as pointers, so the log tells whether
// a handler exists, not which algorithm it runs.
type ReadyLogConnection struct {
	Mode        string `json:"mode"`
	Crypto      bool   `json:"crypto"`
	Compression bool   `json:"compression"`
	PeerType    string `json:"peer_type"`
	PeerID      string `json:"peer_id"`
}

var readyLogPattern = regexp.MustCompile(`conn\(\S+ \S+ (\w+) :\S+ s=READY .*? crypto rx=(\S+) tx=(\S+) comp rx=(\S+) tx=(\S+)\)\.ready entity=(\w+)\.(\S+)`)

// ParseReadyLogLines returns the READY sessions in daemon log lines.
func ParseReadyLogLines(lines []string) []ReadyLogConnection {
	null := func(pointer string) bool { return pointer == "0" || pointer == "0x0" || pointer == "(nil)" }
	var result []ReadyLogConnection
	for _, line := range lines {
		match := readyLogPattern.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		mode, cryptoRx, cryptoTx, compRx, compTx, peerType, peerID := match[1], match[2], match[3], match[4], match[5], match[6], match[7]
		result = append(result, ReadyLogConnection{Mode: mode, Crypto: !null(cryptoRx) && !null(cryptoTx),
			Compression: !null(compRx) && !null(compTx), PeerType: peerType, PeerID: peerID})
	}
	return result
}

// ReadyRevision is the msgr2 revision a daemon printed for one READY session.
// "unavailable" is not revision 0: it means the line gave no usable rev1 field.
type ReadyRevision string

const (
	ReadyRevision1           ReadyRevision = "rev1"
	ReadyRevision0           ReadyRevision = "rev0"
	ReadyRevisionUnavailable ReadyRevision = "unavailable"
)

// ReadyRevisionBinding names the daemon, process generation and capture the
// log lines came from. The log line alone does not say which process printed
// it, so a missing field leaves the observation unbound.
type ReadyRevisionBinding struct {
	Daemon     string
	Generation string
	Capture    string
}

// Complete reports whether every binding field is set.
func (b ReadyRevisionBinding) Complete() bool {
	return b.Daemon != "" && b.Generation != "" && b.Capture != ""
}

// ReadyRevisionObservation is one selected READY line with its revision field.
// Line is the original selected line, kept for the enclosing receipt.
type ReadyRevisionObservation struct {
	Binding  ReadyRevisionBinding
	Line     string
	Revision ReadyRevision
	Reason   string
	Mode     string
	PeerType string
	PeerID   string
}

// readyRevisionLinePattern selects the same READY lines as readyLogPattern but
// keeps the text between "s=READY" and "crypto rx=" for the revision field.
var readyRevisionLinePattern = regexp.MustCompile(`conn\(\S+ \S+ (\w+) :\S+ s=READY (.*?)crypto rx=\S+ tx=\S+ comp rx=\S+ tx=\S+\)\.ready entity=(\w+)\.(\S+)`)

var readyRevisionFieldPattern = regexp.MustCompile(`(?:^|\s)rev1=(\S*)`)

// ParseReadyRevisionLines returns one observation per READY line. A rev1 field
// that is absent, repeated, empty or not exactly 0 or 1 is unavailable; it is
// never read as revision 0 and never inferred from crypto or compression.
func ParseReadyRevisionLines(binding ReadyRevisionBinding, lines []string) []ReadyRevisionObservation {
	var result []ReadyRevisionObservation
	for _, line := range lines {
		match := readyRevisionLinePattern.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		observation := ReadyRevisionObservation{Binding: binding, Line: line, Mode: match[1], PeerType: match[3], PeerID: match[4]}
		fields := readyRevisionFieldPattern.FindAllStringSubmatch(" "+match[2], -1)
		switch {
		case len(fields) == 0:
			observation.Revision, observation.Reason = ReadyRevisionUnavailable, "no rev1 field"
		case len(fields) > 1:
			observation.Revision, observation.Reason = ReadyRevisionUnavailable, "repeated rev1 field"
		case fields[0][1] == "1":
			observation.Revision = ReadyRevision1
		case fields[0][1] == "0":
			observation.Revision = ReadyRevision0
		default:
			observation.Revision, observation.Reason = ReadyRevisionUnavailable, "unrecognized rev1 value "+strings.TrimSpace(fields[0][1])
		}
		result = append(result, observation)
	}
	return result
}

// ReadyRevision1Problems states why observations cannot support "every READY
// session reports msgr2.1". Unbound, unavailable and revision 0 observations
// are separate problems; none counts as evidence.
func ReadyRevision1Problems(observations []ReadyRevisionObservation) []string {
	if len(observations) == 0 {
		return []string{"no READY revision observation"}
	}
	var problems []string
	for i, o := range observations {
		switch {
		case !o.Binding.Complete():
			problems = append(problems, fmt.Sprintf("observation %d has no daemon, generation and capture binding", i))
		case o.Revision == ReadyRevisionUnavailable:
			problems = append(problems, fmt.Sprintf("observation %d revision unavailable: %s", i, o.Reason))
		case o.Revision == ReadyRevision0:
			problems = append(problems, fmt.Sprintf("observation %d reports revision 0", i))
		}
	}
	return problems
}

// ReadySession is the session state that ProtocolV2 prints when a session
// becomes READY at debug_ms 1, on first connection and on every reconnect.
// Conn is the connection object's address in the logging process, Lossy the
// connection policy, ConnectSeq and PeerGlobalSeq the "cs" and "pgs" fields.
// The cookies identify the session: a lossless session keeps both across
// reconnects, a lossy one has no server cookie and each new session has a
// new client cookie. InSeq and OutSeq are the sequence numbers of the last
// message received from and sent to the peer when the session became READY.
// Every field is required; a line without one is not a ReadySession.
type ReadySession struct {
	Line          string `json:"line"`
	Conn          string `json:"conn"`
	Mode          string `json:"mode"`
	PeerGlobalSeq uint64 `json:"pgs"`
	ConnectSeq    uint64 `json:"cs"`
	Lossy         bool   `json:"lossy"`
	Revision1     bool   `json:"rev1"`
	PeerType      string `json:"peer_type"`
	PeerID        string `json:"peer_id"`
	ClientCookie  string `json:"client_cookie"`
	ServerCookie  string `json:"server_cookie"`
	InSeq         uint64 `json:"in_seq"`
	OutSeq        uint64 `json:"out_seq"`
}

var readySessionPattern = regexp.MustCompile(`conn\((\S+) \S+ (\w+) :\S+ s=READY (.*?) crypto rx=\S+ tx=\S+ comp rx=\S+ tx=\S+\)\.ready entity=(\w+)\.(\S+) (.*)$`)

// fields reads space-separated key=value pairs and refuses a repeated key.
func fields(text string) (map[string]string, error) {
	result := map[string]string{}
	for _, field := range strings.Fields(text) {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		if _, repeated := result[key]; repeated {
			return nil, fmt.Errorf("repeated field %s", key)
		}
		result[key] = value
	}
	return result, nil
}

// ParseReadySessions returns the READY sessions in daemon log lines, and the
// READY lines that lack or repeat a required field or carry an unreadable
// value, with the reason, so that a caller can count them as unavailable
// rather than drop them.
func ParseReadySessions(lines []string) ([]ReadySession, []string) {
	var sessions []ReadySession
	var unusable []string
	for _, line := range lines {
		match := readySessionPattern.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		session, err := readySession(line, match)
		if err != nil {
			unusable = append(unusable, err.Error()+": "+line)
			continue
		}
		sessions = append(sessions, session)
	}
	return sessions, unusable
}

func readySession(line string, match []string) (ReadySession, error) {
	state, err := fields(match[3])
	if err != nil {
		return ReadySession{}, err
	}
	tail, err := fields(match[6])
	if err != nil {
		return ReadySession{}, err
	}
	number := func(from map[string]string, key string) (uint64, error) {
		value, ok := from[key]
		if !ok {
			return 0, fmt.Errorf("no %s field", key)
		}
		n, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("field %s=%q", key, value)
		}
		return n, nil
	}
	flag := func(key string) (bool, error) {
		switch state[key] {
		case "0":
			return false, nil
		case "1":
			return true, nil
		}
		return false, fmt.Errorf("field %s=%q", key, state[key])
	}
	cookie := func(key string) (string, error) {
		value, ok := tail[key]
		if _, err := strconv.ParseUint(value, 16, 64); !ok || err != nil {
			return "", fmt.Errorf("field %s=%q", key, value)
		}
		return value, nil
	}
	session := ReadySession{Line: line, Conn: match[1], Mode: match[2], PeerType: match[4], PeerID: match[5]}
	if session.PeerGlobalSeq, err = number(state, "pgs"); err != nil {
		return ReadySession{}, err
	}
	if session.ConnectSeq, err = number(state, "cs"); err != nil {
		return ReadySession{}, err
	}
	if session.Lossy, err = flag("l"); err != nil {
		return ReadySession{}, err
	}
	if session.Revision1, err = flag("rev1"); err != nil {
		return ReadySession{}, err
	}
	if session.ClientCookie, err = cookie("client_cookie"); err != nil {
		return ReadySession{}, err
	}
	if session.ServerCookie, err = cookie("server_cookie"); err != nil {
		return ReadySession{}, err
	}
	if session.InSeq, err = number(tail, "in_seq"); err != nil {
		return ReadySession{}, err
	}
	if session.OutSeq, err = number(tail, "out_seq"); err != nil {
		return ReadySession{}, err
	}
	return session, nil
}
