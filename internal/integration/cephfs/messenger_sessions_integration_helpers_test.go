//go:build all || integration

package cephfs_test

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// readySession is the session state that ProtocolV2 prints when a session
// becomes READY at debug_ms 1, on first connection and on every reconnect.
// Conn is the connection object's address in the logging process, Lossy the
// connection policy, ConnectSeq and PeerGlobalSeq the "cs" and "pgs" fields.
// The cookies identify the session: a lossless session keeps both across
// reconnects, a lossy one has no server cookie and each new session has a
// new client cookie. InSeq and OutSeq are the sequence numbers of the last
// message received from and sent to the peer when the session became READY.
// Every field is required; a line without one is not a readySession.
type readySession struct {
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

// parseReadySessions returns the READY sessions in daemon log lines, and the
// READY lines that lack or repeat a required field or carry an unreadable
// value, with the reason, so that a caller can count them as unavailable
// rather than drop them.
func parseReadySessions(lines []string) ([]readySession, []string) {
	var sessions []readySession
	var unusable []string
	for _, line := range lines {
		match := readySessionPattern.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		session, err := parseReadySession(line, match)
		if err != nil {
			unusable = append(unusable, err.Error()+": "+line)
			continue
		}
		sessions = append(sessions, session)
	}
	return sessions, unusable
}

func parseReadySession(line string, match []string) (readySession, error) {
	state, err := fields(match[3])
	if err != nil {
		return readySession{}, err
	}
	tail, err := fields(match[6])
	if err != nil {
		return readySession{}, err
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
	session := readySession{Line: line, Conn: match[1], Mode: match[2], PeerType: match[4], PeerID: match[5]}
	if session.PeerGlobalSeq, err = number(state, "pgs"); err != nil {
		return readySession{}, err
	}
	if session.ConnectSeq, err = number(state, "cs"); err != nil {
		return readySession{}, err
	}
	if session.Lossy, err = flag("l"); err != nil {
		return readySession{}, err
	}
	if session.Revision1, err = flag("rev1"); err != nil {
		return readySession{}, err
	}
	if session.ClientCookie, err = cookie("client_cookie"); err != nil {
		return readySession{}, err
	}
	if session.ServerCookie, err = cookie("server_cookie"); err != nil {
		return readySession{}, err
	}
	if session.InSeq, err = number(tail, "in_seq"); err != nil {
		return readySession{}, err
	}
	if session.OutSeq, err = number(tail, "out_seq"); err != nil {
		return readySession{}, err
	}
	return session, nil
}
