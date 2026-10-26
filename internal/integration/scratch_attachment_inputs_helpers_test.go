//go:build all || integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"time"
)

// Producers for the attachment judge's inputs (issue 63). Every function turns
// raw tool output (an `osd dump` document, a container inspection, a client
// ledger) into the value the judge consumes. A missing, null or wrong-typed
// field is an error here and never an empty value: the judge treats an empty
// string, a nil receipt or a nil map as "not observed", so decoding a defect
// into one would hide it.
//
// The producers are Docker-free and have been exercised on synthetic output
// only. Nothing here starts a cluster, so no claim about native output formats
// beyond the pinned source is made.

// exactObject decodes one JSON object and looks fields up by exact name.
// encoding/json would otherwise match "Full_Ratio" to "full_ratio".
func exactObject(data []byte) (map[string]json.RawMessage, error) {
	data = bytes.TrimSpace(data)
	dec := json.NewDecoder(bytes.NewReader(data))
	var m map[string]json.RawMessage
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	if m == nil {
		return nil, errors.New("document is null, not an object")
	}
	if dec.More() {
		return nil, errors.New("trailing data after the JSON document")
	}
	return m, nil
}

func rawString(m map[string]json.RawMessage, name string) (string, error) {
	raw, ok := m[name]
	if !ok {
		return "", fmt.Errorf("field %q is missing", name)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", fmt.Errorf("field %q is not a string", name)
	}
	return s, nil
}

func rawBool(m map[string]json.RawMessage, name string) (bool, error) {
	raw, ok := m[name]
	if !ok {
		return false, fmt.Errorf("field %q is missing", name)
	}
	var b bool
	if err := json.Unmarshal(raw, &b); err != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return false, fmt.Errorf("field %q is not a boolean", name)
	}
	return b, nil
}

func rawInt(m map[string]json.RawMessage, name string) (int, error) {
	raw, ok := m[name]
	if !ok {
		return 0, fmt.Errorf("field %q is missing", name)
	}
	var n int
	if err := json.Unmarshal(raw, &n); err != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0, fmt.Errorf("field %q is not an integer", name)
	}
	return n, nil
}

// attachPolicyOf renders the three fullness thresholds of an `osd dump
// --format json` document as the canonical string the judge compares at the
// start and the end of a run. The pinned OSDMap::dump emits full_ratio,
// backfillfull_ratio and nearfull_ratio as floats. The cluster fsid must match
// the one the caller attached to, so a dump of another cluster is refused. The
// ratios must be finite and ordered 0 < nearfull < backfillfull < full <= 1;
// anything else is an error, not a policy.
func attachPolicyOf(dump []byte, fsid string) (string, error) {
	if fsid == "" {
		return "", errors.New("expected fsid is empty")
	}
	m, err := exactObject(dump)
	if err != nil {
		return "", fmt.Errorf("osd dump: %w", err)
	}
	got, err := rawString(m, "fsid")
	if err != nil {
		return "", fmt.Errorf("osd dump: %w", err)
	}
	if got != fsid {
		return "", errors.New("osd dump: fsid differs from the attached cluster")
	}
	var r [3]float64
	for i, name := range []string{"nearfull_ratio", "backfillfull_ratio", "full_ratio"} {
		raw, ok := m[name]
		if !ok {
			return "", fmt.Errorf("osd dump: field %q is missing", name)
		}
		if err := json.Unmarshal(raw, &r[i]); err != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return "", fmt.Errorf("osd dump: field %q is not a number", name)
		}
		if math.IsNaN(r[i]) || math.IsInf(r[i], 0) {
			return "", fmt.Errorf("osd dump: field %q is not finite", name)
		}
	}
	if !(0 < r[0] && r[0] < r[1] && r[1] < r[2] && r[2] <= 1) {
		return "", errors.New("osd dump: ratios are not ordered 0 < nearfull < backfillfull < full <= 1")
	}
	f := func(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }
	return "n=" + f(r[0]) + " b=" + f(r[1]) + " f=" + f(r[2]), nil
}

// attachContainerState is the part of a container inspection the attachment
// needs. Each field is required.
type attachContainerState struct {
	Status     string
	Running    bool
	Paused     bool
	Restarting bool
	OOMKilled  bool
	ExitCode   int
	StartedAt  string
}

// parseAttachContainerState reads the State object of a container inspection
// (the document has a top-level "State"). StartedAt must be an RFC 3339
// instant that is not the zero time: a never-started container reports the
// zero time, and that is not an identity.
func parseAttachContainerState(inspect []byte) (*attachContainerState, error) {
	top, err := exactObject(inspect)
	if err != nil {
		return nil, fmt.Errorf("container inspection: %w", err)
	}
	raw, ok := top["State"]
	if !ok {
		return nil, errors.New("container inspection: State is missing")
	}
	m, err := exactObject(raw)
	if err != nil {
		return nil, fmt.Errorf("container inspection State: %w", err)
	}
	var s attachContainerState
	if s.Status, err = rawString(m, "Status"); err != nil {
		return nil, fmt.Errorf("container inspection State: %w", err)
	}
	for _, f := range []struct {
		name string
		to   *bool
	}{{"Running", &s.Running}, {"Paused", &s.Paused}, {"Restarting", &s.Restarting}, {"OOMKilled", &s.OOMKilled}} {
		if *f.to, err = rawBool(m, f.name); err != nil {
			return nil, fmt.Errorf("container inspection State: %w", err)
		}
	}
	if s.ExitCode, err = rawInt(m, "ExitCode"); err != nil {
		return nil, fmt.Errorf("container inspection State: %w", err)
	}
	if s.StartedAt, err = rawString(m, "StartedAt"); err != nil {
		return nil, fmt.Errorf("container inspection State: %w", err)
	}
	at, err := time.Parse(time.RFC3339Nano, s.StartedAt)
	if err != nil || at.IsZero() || at.Year() <= 1 {
		return nil, errors.New("container inspection State: StartedAt is not a real start instant")
	}
	return &s, nil
}

// attachKeeperIdentity produces the judge's keeper inputs from the inspection
// taken at attachment and the one taken when recovery is evaluated. The
// attachment observation must show a running keeper. A keeper that is not
// running at evaluation time yields running=false and the judge refuses it; a
// restarted keeper keeps its new start instant and the judge refuses the
// mismatch. Neither is hidden here.
func attachKeeperIdentity(atAttach, atEvaluation *attachContainerState) (started [2]string, running bool, err error) {
	if atAttach == nil || atEvaluation == nil {
		return started, false, errors.New("keeper state is missing at one of the two points")
	}
	if !atAttach.Running || atAttach.Paused || atAttach.Restarting {
		return started, false, errors.New("keeper was not running at attachment")
	}
	live := atEvaluation.Running && !atEvaluation.Paused && !atEvaluation.Restarting
	return [2]string{atAttach.StartedAt, atEvaluation.StartedAt}, live, nil
}

// sigabrtExit is the exit status a shell reports for a process killed by
// SIGABRT (128+6), the signal Ceph's assertion and BlueFS allocation-failure
// paths raise through abort().
const sigabrtExit = 134

// attachDaemonState classifies the OSD container after the filler was
// released, against the inspection taken when the daemon was attached.
//
//   - up: running, not paused or restarting, and the same start instant. A
//     different start instant is another process instance, so it is unknown.
//   - aborted: the same instance exited with status 134 and no OOM kill.
//     Status 134 is also what an external SIGABRT yields, so the result is "the
//     process aborted", not proof that BlueFS caused it.
//   - unknown: anything else, including 137 (SIGKILL from OOM, docker kill or
//     the harness), OOMKilled, other exit codes and a paused or restarting
//     container. Those are not capacity outcomes.
func attachDaemonState(attached, now *attachContainerState) (string, error) {
	if attached == nil || now == nil {
		return "", errors.New("daemon state is missing at one of the two points")
	}
	if !attached.Running || attached.Paused || attached.Restarting {
		return "", errors.New("daemon was not running at attachment")
	}
	if now.StartedAt != attached.StartedAt {
		return daemonUnknown, nil
	}
	switch {
	case now.Running && !now.Paused && !now.Restarting && !now.OOMKilled:
		return daemonUp, nil
	case !now.Running && !now.Paused && !now.Restarting && !now.OOMKilled && now.Status == "exited" && now.ExitCode == sigabrtExit:
		return daemonAborted, nil
	default:
		return daemonUnknown, nil
	}
}

var attachDigestRE = regexp.MustCompile(`^[0-9a-f]{64}$`)

type attachLedgerEntry struct {
	Key     *string `json:"key"`
	Outcome *string `json:"outcome"`
	SHA256  *string `json:"sha256"`
}

// parseAttachLedger reads the client's mutation ledger, a JSON array with one
// entry per submitted mutation. Every entry needs a nonempty unique key, one of
// the three known outcomes and the digest of the bytes the client sent
// (64 lowercase hexadecimal characters). An empty ledger is an error. The
// outcome is taken from the ledger as written and is never changed by a later
// readback.
func parseAttachLedger(out []byte) ([]attachMutation, error) {
	var entries []attachLedgerEntry
	if err := decodeStrict(out, &entries); err != nil {
		return nil, fmt.Errorf("mutation ledger: %w", err)
	}
	if len(entries) == 0 {
		return nil, errors.New("mutation ledger: no entries")
	}
	seen := map[string]bool{}
	ledger := make([]attachMutation, 0, len(entries))
	for i, e := range entries {
		if e.Key == nil || *e.Key == "" || e.Outcome == nil || e.SHA256 == nil {
			return nil, fmt.Errorf("mutation ledger: entry %d has a missing, null or empty field", i)
		}
		switch *e.Outcome {
		case mutAcked, mutUnknown, mutFailed:
		default:
			return nil, fmt.Errorf("mutation ledger: entry %d has outcome %q", i, *e.Outcome)
		}
		if !attachDigestRE.MatchString(*e.SHA256) {
			return nil, fmt.Errorf("mutation ledger: entry %d digest is not 64 lowercase hex characters", i)
		}
		if seen[*e.Key] {
			return nil, fmt.Errorf("mutation ledger: key %q appears twice", *e.Key)
		}
		seen[*e.Key] = true
		ledger = append(ledger, attachMutation{Key: *e.Key, Outcome: *e.Outcome, SHA256: *e.SHA256})
	}
	return ledger, nil
}

const (
	readbackPresent = "present"
	readbackAbsent  = "absent"
	readbackError   = "error" // the read itself failed: not the same as a missing object
)

type attachReadbackEntry struct {
	Key    *string `json:"key"`
	Status *string `json:"status"`
	SHA256 *string `json:"sha256"`
}

// attachRecoveredOf joins the readback taken after the release to the ledger.
// Every acknowledged key needs exactly one readback record: a key that was not
// read is inconclusive and never counts as lost or intact. A read error is
// inconclusive too. An absent object is left out of the map so the judge
// reports it as missing. Records for keys outside the ledger are refused. The
// readback is only required to cover acknowledged keys; unknown and failed
// mutations may be read but the judge never promotes them.
func attachRecoveredOf(ledger []attachMutation, out []byte) (map[string]string, error) {
	var entries []attachReadbackEntry
	if err := decodeStrict(out, &entries); err != nil {
		return nil, fmt.Errorf("readback: %w", err)
	}
	inLedger := map[string]string{}
	for _, m := range ledger {
		inLedger[m.Key] = m.Outcome
	}
	recovered := map[string]string{}
	seen := map[string]bool{}
	for i, e := range entries {
		if e.Key == nil || *e.Key == "" || e.Status == nil {
			return nil, fmt.Errorf("readback: entry %d has a missing, null or empty field", i)
		}
		if _, ok := inLedger[*e.Key]; !ok {
			return nil, fmt.Errorf("readback: key %q is not in the ledger", *e.Key)
		}
		if seen[*e.Key] {
			return nil, fmt.Errorf("readback: key %q appears twice", *e.Key)
		}
		seen[*e.Key] = true
		switch *e.Status {
		case readbackPresent:
			if e.SHA256 == nil || !attachDigestRE.MatchString(*e.SHA256) {
				return nil, fmt.Errorf("readback: present key %q lacks a valid digest", *e.Key)
			}
			recovered[*e.Key] = *e.SHA256
		case readbackAbsent:
			if e.SHA256 != nil {
				return nil, fmt.Errorf("readback: absent key %q carries a digest", *e.Key)
			}
		case readbackError:
			if inLedger[*e.Key] == mutAcked {
				return nil, fmt.Errorf("readback: acknowledged key %q could not be read", *e.Key)
			}
		default:
			return nil, fmt.Errorf("readback: entry %d has status %q", i, *e.Status)
		}
	}
	for _, m := range ledger {
		if m.Outcome == mutAcked && !seen[m.Key] {
			return nil, fmt.Errorf("readback: acknowledged key %q was not read", m.Key)
		}
	}
	return recovered, nil
}

// attachRawInputs is the raw output of every earlier observation, as the
// native scenario will hold it. The two store views, the block backing
// receipt, the fill and release receipts keep the parsers they already have.
type attachRawInputs struct {
	KeeperView, OSDView []byte
	Token               string
	CeilingBytes        int64
	BlockBacking        []byte

	FSID                       string
	PolicyBefore, PolicyAtEval []byte

	Fill, Release []byte
	SeedSHA       string
	WriterJoined  bool

	KeeperAtAttach, KeeperAtEval []byte
	DaemonAtAttach, DaemonAtEval []byte

	Ledger, Readback []byte
}

// buildAttachRun parses every raw input and assembles the judge's input. A
// parse failure anywhere is an error, so the judge only ever sees well-formed
// evidence. The readback is consulted only when the daemon is up, matching the
// judge; otherwise it is ignored here.
func buildAttachRun(in *attachRawInputs) (*attachRun, error) {
	if in == nil {
		return nil, errors.New("attachment inputs are nil")
	}
	run := &attachRun{SeedSHA: in.SeedSHA, WriterJoined: in.WriterJoined}

	keeperView, err := parseStoreView(in.KeeperView)
	if err != nil {
		return nil, fmt.Errorf("keeper view: %w", err)
	}
	osdView, err := parseStoreView(in.OSDView)
	if err != nil {
		return nil, fmt.Errorf("osd view: %w", err)
	}
	if run.Link, err = classifyStoreLink(keeperView, osdView, in.Token, in.CeilingBytes); err != nil {
		return nil, fmt.Errorf("store link: %w", err)
	}
	backing, err := parseBlockBacking(in.BlockBacking)
	if err != nil {
		return nil, err
	}
	if run.Backing, err = classifyBlockBacking(backing); err != nil {
		return nil, fmt.Errorf("block backing: %w", err)
	}
	if run.Policy[0], err = attachPolicyOf(in.PolicyBefore, in.FSID); err != nil {
		return nil, fmt.Errorf("policy before: %w", err)
	}
	if run.Policy[1], err = attachPolicyOf(in.PolicyAtEval, in.FSID); err != nil {
		return nil, fmt.Errorf("policy at evaluation: %w", err)
	}
	if run.Fill, err = parseScratchFillReceipt(in.Fill); err != nil {
		return nil, err
	}
	if run.Release, err = parseScratchRelease(in.Release); err != nil {
		return nil, err
	}

	keeperAttach, err := parseAttachContainerState(in.KeeperAtAttach)
	if err != nil {
		return nil, fmt.Errorf("keeper at attachment: %w", err)
	}
	keeperEval, err := parseAttachContainerState(in.KeeperAtEval)
	if err != nil {
		return nil, fmt.Errorf("keeper at evaluation: %w", err)
	}
	if run.KeeperStarted, run.KeeperRunning, err = attachKeeperIdentity(keeperAttach, keeperEval); err != nil {
		return nil, err
	}
	daemonAttach, err := parseAttachContainerState(in.DaemonAtAttach)
	if err != nil {
		return nil, fmt.Errorf("daemon at attachment: %w", err)
	}
	daemonEval, err := parseAttachContainerState(in.DaemonAtEval)
	if err != nil {
		return nil, fmt.Errorf("daemon at evaluation: %w", err)
	}
	if run.Daemon, err = attachDaemonState(daemonAttach, daemonEval); err != nil {
		return nil, err
	}
	if run.Ledger, err = parseAttachLedger(in.Ledger); err != nil {
		return nil, err
	}
	if run.Daemon == daemonUp {
		if run.Recovered, err = attachRecoveredOf(run.Ledger, in.Readback); err != nil {
			return nil, err
		}
	}
	return run, nil
}
