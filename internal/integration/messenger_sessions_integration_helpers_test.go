//go:build all || integration

package integration_test

import (
	"regexp"
)

// readyLogConnection is one msgr2 session that a daemon logged as READY at
// debug_ms 1. A release without messenger dump prints the session's crypto
// and compression handlers there only as pointers, so the log tells whether
// a handler exists, not which algorithm it runs.
type readyLogConnection struct {
	Mode        string `json:"mode"`
	Crypto      bool   `json:"crypto"`
	Compression bool   `json:"compression"`
	PeerType    string `json:"peer_type"`
	PeerID      string `json:"peer_id"`
}

var readyLogPattern = regexp.MustCompile(`conn\(\S+ \S+ (\w+) :\S+ s=READY .*? crypto rx=(\S+) tx=(\S+) comp rx=(\S+) tx=(\S+)\)\.ready entity=(\w+)\.(\S+)`)

// parseReadyLogLines returns the READY sessions in daemon log lines.
func parseReadyLogLines(lines []string) []readyLogConnection {
	null := func(pointer string) bool { return pointer == "0" || pointer == "0x0" || pointer == "(nil)" }
	var result []readyLogConnection
	for _, line := range lines {
		match := readyLogPattern.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		mode, cryptoRx, cryptoTx, compRx, compTx, peerType, peerID := match[1], match[2], match[3], match[4], match[5], match[6], match[7]
		result = append(result, readyLogConnection{Mode: mode, Crypto: !null(cryptoRx) && !null(cryptoTx),
			Compression: !null(compRx) && !null(compTx), PeerType: peerType, PeerID: peerID})
	}
	return result
}
