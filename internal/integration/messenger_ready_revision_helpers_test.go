//go:build all || integration

package integration_test

import (
	"fmt"
	"regexp"
	"strings"
)

// readyRevision is the msgr2 revision a daemon printed for one READY session.
// "unavailable" is not revision 0: it means the line gave no usable rev1 field.
type readyRevision string

const (
	readyRevision1           readyRevision = "rev1"
	readyRevision0           readyRevision = "rev0"
	readyRevisionUnavailable readyRevision = "unavailable"
)

// readyRevisionBinding names the daemon, process generation and capture the
// log lines came from. The log line alone does not say which process printed
// it, so a missing field leaves the observation unbound.
type readyRevisionBinding struct {
	Daemon     string
	Generation string
	Capture    string
}

func (b readyRevisionBinding) complete() bool {
	return b.Daemon != "" && b.Generation != "" && b.Capture != ""
}

// readyRevisionObservation is one selected READY line with its revision field.
// Line is the original selected line, kept for the enclosing receipt.
type readyRevisionObservation struct {
	Binding  readyRevisionBinding
	Line     string
	Revision readyRevision
	Reason   string
	Mode     string
	PeerType string
	PeerID   string
}

// readyRevisionLinePattern selects the same READY lines as readyLogPattern but
// keeps the text between "s=READY" and "crypto rx=" for the revision field.
var readyRevisionLinePattern = regexp.MustCompile(`conn\(\S+ \S+ (\w+) :\S+ s=READY (.*?)crypto rx=\S+ tx=\S+ comp rx=\S+ tx=\S+\)\.ready entity=(\w+)\.(\S+)`)

var readyRevisionFieldPattern = regexp.MustCompile(`(?:^|\s)rev1=(\S*)`)

// parseReadyRevisionLines returns one observation per READY line. A rev1 field
// that is absent, repeated, empty or not exactly 0 or 1 is unavailable; it is
// never read as revision 0 and never inferred from crypto or compression.
func parseReadyRevisionLines(binding readyRevisionBinding, lines []string) []readyRevisionObservation {
	var result []readyRevisionObservation
	for _, line := range lines {
		match := readyRevisionLinePattern.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		observation := readyRevisionObservation{Binding: binding, Line: line, Mode: match[1], PeerType: match[3], PeerID: match[4]}
		fields := readyRevisionFieldPattern.FindAllStringSubmatch(" "+match[2], -1)
		switch {
		case len(fields) == 0:
			observation.Revision, observation.Reason = readyRevisionUnavailable, "no rev1 field"
		case len(fields) > 1:
			observation.Revision, observation.Reason = readyRevisionUnavailable, "repeated rev1 field"
		case fields[0][1] == "1":
			observation.Revision = readyRevision1
		case fields[0][1] == "0":
			observation.Revision = readyRevision0
		default:
			observation.Revision, observation.Reason = readyRevisionUnavailable, "unrecognized rev1 value "+strings.TrimSpace(fields[0][1])
		}
		result = append(result, observation)
	}
	return result
}

// readyRevision1Problems states why observations cannot support "every READY
// session reports msgr2.1". Unbound, unavailable and revision 0 observations
// are separate problems; none counts as evidence.
func readyRevision1Problems(observations []readyRevisionObservation) []string {
	if len(observations) == 0 {
		return []string{"no READY revision observation"}
	}
	var problems []string
	for i, o := range observations {
		switch {
		case !o.Binding.complete():
			problems = append(problems, fmt.Sprintf("observation %d has no daemon, generation and capture binding", i))
		case o.Revision == readyRevisionUnavailable:
			problems = append(problems, fmt.Sprintf("observation %d revision unavailable: %s", i, o.Reason))
		case o.Revision == readyRevision0:
			problems = append(problems, fmt.Sprintf("observation %d reports revision 0", i))
		}
	}
	return problems
}
