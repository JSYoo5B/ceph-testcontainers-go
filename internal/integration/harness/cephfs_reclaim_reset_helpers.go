//go:build all || integration

package harness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

// This file holds the Docker-free part of the CephFS session-RESET recipe: a
// strict reader for the ctypes ABI probe, a reader for the active MDS's session
// listing, and a judge that decides from timed observations whether a
// UUID-targeted RESET did what the pinned source says it does.
//
// Source basis (Tentacle 20.2.4, 7f793731):
//
//   - Server::reclaim_session accepts only flags == CEPH_RECLAIM_RESET (1) and
//     answers anything else with -EINVAL, so flags 0 is never RESET evidence.
//     Flags 0 is the state-preserving request (adopt the old session's
//     capabilities and locks): the client sends it, the MDS replies -EINVAL
//     without touching the target, and Client::start_reclaim returns that
//     reply's result. The same check is at v19.2.5.
//   - For a UUID that no session carries, the server sends its default success
//     reply without target addresses. Client::start_reclaim turns that into
//     -ENOENT only when its remembered reclaim_target_addrs is empty.
//   - Client::start_reclaim clears reclaim_errno but not reclaim_target_addrs,
//     and finish_reclaim does not clear them either. A missing-UUID attempt on a
//     handle that already completed a RESET can therefore return 0. Each judged
//     attempt needs its own freshly initialized handle.
//   - Server::finish_reclaim_session sends the reply from the completion of
//     kill_session or evict_client, so the reply, and hence the return of
//     ceph_start_reclaim, follows the removal of the target session.
//   - The blocklist and OSD epoch effects of that eviction are not observed
//     here and are not judged.

// resetCall is the declared ctypes shape of one libcephfs entry point. Restype
// "None" is a void function.
type resetCall struct {
	Restype  string
	Argtypes []string
}

// ResetABIContract lists the eleven public C symbols the actor must resolve and
// the ctypes types derived from src/include/cephfs/libcephfs.h at the pin.
// ceph_create writes a handle through a pointer; ceph_start_reclaim returns a
// signed negative errno; ceph_finish_reclaim and ceph_set_uuid are void;
// ceph_get_instance_id is uint64_t; ceph_version returns const char *.
var ResetABIContract = map[string]resetCall{
	"ceph_create":            {"c_int", []string{"POINTER(c_void_p)", "c_char_p"}},
	"ceph_conf_read_file":    {"c_int", []string{"c_void_p", "c_char_p"}},
	"ceph_conf_set":          {"c_int", []string{"c_void_p", "c_char_p", "c_char_p"}},
	"ceph_init":              {"c_int", []string{"c_void_p"}},
	"ceph_select_filesystem": {"c_int", []string{"c_void_p", "c_char_p"}},
	"ceph_set_uuid":          {"None", []string{"c_void_p", "c_char_p"}},
	"ceph_start_reclaim":     {"c_int", []string{"c_void_p", "c_char_p", "c_uint"}},
	"ceph_finish_reclaim":    {"None", []string{"c_void_p"}},
	"ceph_release":           {"c_int", []string{"c_void_p"}},
	"ceph_get_instance_id":   {"c_uint64", []string{"c_void_p"}},
	"ceph_version":           {"c_char_p", []string{"POINTER(c_int)", "POINTER(c_int)", "POINTER(c_int)"}},
}

// CephReclaimReset is CEPH_RECLAIM_RESET in libcephfs.h.
const CephReclaimReset = 1

var (
	resetSHA256Hex = regexp.MustCompile(`^[0-9a-f]{64}$`)
	resetImageID   = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

// ResetABIProbe is the parsed, fully present output of the eligibility probe.
type ResetABIProbe struct {
	ImageConfigID          string
	LibraryPath            string
	LibrarySHA256          string
	ExtensionSHA256        string
	Major, Minor, Patch    int64
	Calls                  map[string]resetCall
	CreateReturn           int64
	ReleaseReturn          int64
	ProbeHandleInitialized bool
}

// ParseResetABIProbe reads the probe's JSON. Every field is required and must
// have its exact type. A missing symbol, a symbol reported as unresolved, a
// missing or null restype or argtypes list, or a version that is not three
// integers is an error, never an empty value that could pass as evidence.
func ParseResetABIProbe(data []byte) (ResetABIProbe, error) {
	var p ResetABIProbe
	top, err := probeObject(data, "probe")
	if err != nil {
		return p, err
	}
	if p.ImageConfigID, err = probeString(top, "image_config_id"); err != nil {
		return p, err
	}
	if !resetImageID.MatchString(p.ImageConfigID) {
		return p, fmt.Errorf("image_config_id is not a sha256 digest: %.80s", p.ImageConfigID)
	}
	if p.ExtensionSHA256, err = probeString(top, "extension_sha256"); err != nil {
		return p, err
	}
	if !resetSHA256Hex.MatchString(p.ExtensionSHA256) {
		return p, fmt.Errorf("extension_sha256 is not a hex digest")
	}
	lib, err := probeObject(top["library"], "library")
	if err != nil {
		return p, err
	}
	if p.LibraryPath, err = probeString(lib, "path"); err != nil || p.LibraryPath == "" {
		return p, fmt.Errorf("library path is missing or empty (%v)", err)
	}
	if p.LibrarySHA256, err = probeString(lib, "sha256"); err != nil {
		return p, err
	}
	if !resetSHA256Hex.MatchString(p.LibrarySHA256) {
		return p, fmt.Errorf("library sha256 is not a hex digest")
	}
	ver, err := probeObject(lib["version"], "library version")
	if err != nil {
		return p, err
	}
	for name, dst := range map[string]*int64{"major": &p.Major, "minor": &p.Minor, "patch": &p.Patch} {
		if *dst, err = probeInt(ver, name); err != nil {
			return p, fmt.Errorf("version: %w", err)
		}
	}
	syms, err := probeObject(top["symbols"], "symbols")
	if err != nil {
		return p, err
	}
	if len(syms) != len(ResetABIContract) {
		return p, fmt.Errorf("probe reports %d symbols, want exactly %d", len(syms), len(ResetABIContract))
	}
	p.Calls = map[string]resetCall{}
	for name := range ResetABIContract {
		raw, ok := syms[name]
		if !ok {
			return p, fmt.Errorf("symbol %q is missing from the probe", name)
		}
		sym, err := probeObject(raw, "symbol "+name)
		if err != nil {
			return p, err
		}
		resolved, err := probeBool(sym, "resolved")
		if err != nil {
			return p, fmt.Errorf("symbol %q: %w", name, err)
		}
		if !resolved {
			return p, fmt.Errorf("symbol %q did not resolve", name)
		}
		var c resetCall
		if c.Restype, err = probeString(sym, "restype"); err != nil {
			return p, fmt.Errorf("symbol %q: %w", name, err)
		}
		argRaw, ok := sym["argtypes"]
		if !ok {
			return p, fmt.Errorf("symbol %q: argtypes is missing", name)
		}
		var args []string
		if err := json.Unmarshal(argRaw, &args); err != nil || args == nil {
			return p, fmt.Errorf("symbol %q: argtypes is not a list of strings (%v)", name, err)
		}
		c.Argtypes = args
		p.Calls[name] = c
	}
	probe, err := probeObject(top["probe_handle"], "probe_handle")
	if err != nil {
		return p, err
	}
	if p.CreateReturn, err = probeInt(probe, "create_rc"); err != nil {
		return p, err
	}
	if p.ReleaseReturn, err = probeInt(probe, "release_rc"); err != nil {
		return p, err
	}
	if p.ProbeHandleInitialized, err = probeBool(probe, "initialized"); err != nil {
		return p, err
	}
	return p, nil
}

// CheckResetABIProbe is the eligibility gate. It compares every declared type
// with the contract and requires the probe's own unmounted, uninitialized
// handle to have been created and released with return 0 and 0, and the
// library to report the expected release.
func CheckResetABIProbe(p ResetABIProbe, major, minor, patch int64) error {
	if p.Major != major || p.Minor != minor || p.Patch != patch {
		return fmt.Errorf("library reports %d.%d.%d, want %d.%d.%d", p.Major, p.Minor, p.Patch, major, minor, patch)
	}
	for name, want := range ResetABIContract {
		got := p.Calls[name]
		if got.Restype != want.Restype || len(got.Argtypes) != len(want.Argtypes) {
			return fmt.Errorf("%s declared as %v, want %v", name, got, want)
		}
		for i := range want.Argtypes {
			if got.Argtypes[i] != want.Argtypes[i] {
				return fmt.Errorf("%s argument %d declared %q, want %q", name, i, got.Argtypes[i], want.Argtypes[i])
			}
		}
	}
	if p.CreateReturn != 0 || p.ReleaseReturn != 0 {
		return fmt.Errorf("probe handle create/release returned %d/%d, want 0/0", p.CreateReturn, p.ReleaseReturn)
	}
	if p.ProbeHandleInitialized {
		return fmt.Errorf("probe handle was initialized; the eligibility handle must stay unmounted and uninitialized")
	}
	return nil
}

func probeObject(raw json.RawMessage, what string) (map[string]json.RawMessage, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		return nil, fmt.Errorf("%s is missing or not a JSON object (%v)", what, err)
	}
	return m, nil
}

func probeString(m map[string]json.RawMessage, name string) (string, error) {
	raw, ok := m[name]
	if !ok {
		return "", fmt.Errorf("field %q is missing", name)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil || raw[0] != '"' {
		return "", fmt.Errorf("field %q is not a string: %.64s", name, raw)
	}
	return s, nil
}

func probeBool(m map[string]json.RawMessage, name string) (bool, error) {
	raw, ok := m[name]
	if !ok {
		return false, fmt.Errorf("field %q is missing", name)
	}
	switch string(bytes.TrimSpace(raw)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	return false, fmt.Errorf("field %q is not a boolean: %.64s", name, raw)
}

// probeInt accepts only a signed decimal integer literal. Null, a string, a
// fraction and an exponent are errors.
func probeInt(m map[string]json.RawMessage, name string) (int64, error) {
	raw, ok := m[name]
	if !ok {
		return 0, fmt.Errorf("field %q is missing", name)
	}
	v, err := strconv.ParseInt(string(bytes.TrimSpace(raw)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("field %q is not an integer: %.64s", name, raw)
	}
	return v, nil
}

// ResetSessions is one parsed "session ls" read: client ID to session state.
type ResetSessions map[uint64]string

// ParseResetSessions reads the output of "tell mds.<name> session ls". The
// document must be an array. Every row needs an integer "id" and a string
// "state" (Session::dump), and two rows for one client are an error, so a row
// that cannot be attributed never passes as unrelated. An empty array is a
// valid, empty session set; a missing or null document is an error.
func ParseResetSessions(data []byte) (ResetSessions, error) {
	var rows []json.RawMessage
	if err := json.Unmarshal(data, &rows); err != nil || rows == nil {
		return nil, fmt.Errorf("session listing is not a JSON array (%v)", err)
	}
	out := ResetSessions{}
	for i, rawRow := range rows {
		row, err := probeObject(rawRow, fmt.Sprintf("session row %d", i))
		if err != nil {
			return nil, err
		}
		id, err := probeInt(row, "id")
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("session row %d has no positive integer id (%v)", i, err)
		}
		state, err := probeString(row, "state")
		if err != nil {
			return nil, fmt.Errorf("session row %d: %w", i, err)
		}
		if _, dup := out[uint64(id)]; dup {
			return nil, fmt.Errorf("duplicate session row for client.%d", id)
		}
		out[uint64(id)] = state
	}
	return out, nil
}

// status classifies a client's row. Only open and stale sessions count as
// present. The transitional states "closing" and "killing" are neither
// present nor absent evidence.
func (s ResetSessions) Status(id uint64) resetPresence {
	state, ok := s[id]
	switch {
	case !ok:
		return ResetAbsent
	case state == "open" || state == "stale":
		return ResetPresent
	}
	return ResetTransitional
}

type resetPresence int

const (
	resetPresenceUnknown resetPresence = iota
	ResetAbsent
	ResetPresent
	ResetTransitional
)

// ResetLock is the result of an independent non-blocking lock attempt. Only
// EWOULDBLOCK means held and only a granted lock means free; any other error is
// unknown and is never promoted to either.
type ResetLock int

const (
	ResetLockUnknown ResetLock = iota
	ResetLockHeld
	ResetLockFree
)

// ResetObservation is one session listing plus the two independent lock
// probes. Start and End bracket the whole observation, because the daemon and
// the lock holder answer somewhere inside that interval.
type ResetObservation struct {
	Start, End time.Time
	Sessions   ResetSessions
	ALock      ResetLock
	ULock      ResetLock
}

// ResetAttempt is one ceph_start_reclaim call on one native handle. RC is
// nil when the return value was not recorded. PriorReclaimCalls counts the
// reclaim calls the same handle made before this one. Replies holds the
// results of the client_reclaim_reply messages that the handle's client log
// shows arriving from the MDS, nil when the log was not read.
type ResetAttempt struct {
	Handle            string
	Initialized       bool
	PriorReclaimCalls int
	Flags             uint
	Target            string
	OwnUUID           string
	RC                *int
	Replies           []int
	Start, End        time.Time
}

// ResetSequence is everything the judge needs. A is the killed client, U the
// unrelated live client, B the reclaimer. Observations are grouped by the phase
// that bounds them; the judge does not trust the grouping and re-checks every
// time.
type ResetSequence struct {
	A, U, B        uint64
	AUUID, UUUID   string
	AKilledAt      time.Time
	AExitStatus    *int // supervised exit status, -9 for SIGKILL
	AfterKill      []ResetObservation
	Missing        ResetAttempt
	AfterMissing   []ResetObservation
	Preserve       ResetAttempt
	AfterPreserve  []ResetObservation
	Reset          ResetAttempt
	AfterReset     []ResetObservation
	MountStart     time.Time
	SeedSize       int
	SeedSHA256     string
	ReadSize       int
	ReadSHA256     string
	ReadFromNewFd  bool
	ReadByClientID uint64
}

type ResetVerdict int

const (
	// ResetInconclusive is the default, so a forgotten branch can never pass.
	ResetInconclusive ResetVerdict = iota
	// ResetEstablished means every oracle held: fresh-handle missing UUID gave
	// -ENOENT with the bystanders untouched, a fresh handle's state-preserving
	// request for the killed client's UUID was refused by the MDS with -EINVAL
	// and left its session and lock standing, a third fresh handle's RESET of
	// that UUID removed exactly its session and lock before any replacement
	// mount, and the replacement read the original bytes.
	ResetEstablished
	// ResetContradicted means the evidence shows behavior the source rules out
	// for a correct fresh-handle RESET.
	ResetContradicted
)

type ResetOutcome struct {
	Verdict ResetVerdict
	Reason  string
}

func resetUnknown(format string, args ...any) ResetOutcome {
	return ResetOutcome{Reason: fmt.Sprintf(format, args...)}
}

func resetBad(format string, args ...any) ResetOutcome {
	return ResetOutcome{Verdict: ResetContradicted, Reason: fmt.Sprintf(format, args...)}
}

const (
	errnoENOENT = -2
	errnoEINVAL = -22
)

// JudgeResetSequence applies the RESET policy.
//
//   - Admission: distinct nonzero client IDs, nonempty distinct UUIDs, a
//     recorded SIGKILL (-9) of A, and ordered times.
//   - Pre-RESET state: observations that started after the kill and ended
//     before the missing-UUID attempt began must show A and U present with both
//     locks held. A "present" claim needs an observation that ended before the
//     attempt started; an "absent" or "free" claim needs one that started after
//     the attempt returned.
//   - Missing UUID: a fresh, initialized handle with no earlier reclaim call,
//     RESET flags, a UUID that is neither A's, U's nor its own, and return -2.
//     A return of 0 on a fresh handle is a contradiction. Afterwards A and U
//     must still be present with locks held.
//   - State-preserving request: another fresh handle, flags 0, A's UUID. It
//     must return -22 with exactly one client_reclaim_reply of -22 from the
//     MDS in its client log, so the refusal is the server's and not one of the
//     client's own -EINVAL checks. A return of 0 is a contradiction (the
//     source refuses every flag but RESET). Afterwards A and U must still be
//     present with locks held: the refused request evicted nothing.
//   - Positive RESET: a different fresh handle, RESET flags, A's UUID, return
//     0. Observations that started after it returned and ended before the
//     replacement mount began must show A's session absent, A's lock free, and
//     U present with its lock held. U lost, or U's lock freed, is a
//     contradiction (collateral damage).
//   - Replacement: a new nonzero client ID distinct from A and U, and an
//     independent read of exactly the seeded size and SHA-256 through a new
//     descriptor by that client.
//
// The judge does not observe blocklist or OSD epoch effects and does not claim
// anything about RESET latency. State-preserving reclaim is judged only as the
// refusal the pinned source makes; no capability or lock is ever adopted.
func JudgeResetSequence(s ResetSequence) ResetOutcome {
	switch {
	case s.A == 0 || s.U == 0 || s.B == 0 || s.A == s.U || s.A == s.B || s.U == s.B:
		return resetUnknown("client identities are not three distinct nonzero IDs (%d, %d, %d)", s.A, s.U, s.B)
	case s.AUUID == "" || s.UUUID == "" || s.AUUID == s.UUUID:
		return resetUnknown("client UUIDs are missing or not distinct")
	case s.AExitStatus == nil || *s.AExitStatus != -9:
		return resetUnknown("A's termination was not recorded as a supervised SIGKILL")
	case s.AKilledAt.IsZero() || s.MountStart.IsZero():
		return resetUnknown("kill or replacement mount time is missing")
	}
	if r := checkResetAttempt(s.Missing, "missing-UUID", CephReclaimReset); r != nil {
		return *r
	}
	if r := checkResetAttempt(s.Preserve, "state-preserving", 0); r != nil {
		return *r
	}
	if r := checkResetAttempt(s.Reset, "RESET", CephReclaimReset); r != nil {
		return *r
	}
	if s.Missing.Handle == s.Reset.Handle || s.Missing.Handle == s.Preserve.Handle || s.Preserve.Handle == s.Reset.Handle {
		return resetUnknown("the missing-UUID, state-preserving and RESET attempts do not each have their own native handle")
	}
	if s.Missing.Target == s.Reset.Target {
		return resetUnknown("the missing-UUID attempt targets the UUID that RESET targets")
	}
	if s.Missing.Target == s.AUUID || s.Missing.Target == s.UUUID || s.Missing.Target == s.Missing.OwnUUID || s.Missing.Target == s.Reset.OwnUUID ||
		s.Missing.Target == s.Preserve.OwnUUID {
		return resetUnknown("the missing-UUID target names a live or own session")
	}
	if s.Preserve.Target != s.AUUID {
		return resetUnknown("the state-preserving request does not target the killed client's UUID")
	}
	if s.Preserve.OwnUUID == s.AUUID || s.Preserve.OwnUUID == s.UUUID {
		return resetUnknown("the state-preserving requester reuses a live client's UUID")
	}
	if s.Reset.Target != s.AUUID {
		return resetUnknown("RESET does not target the killed client's UUID")
	}
	if s.Reset.OwnUUID == s.AUUID || s.Reset.OwnUUID == s.UUUID {
		return resetUnknown("the reclaimer reuses a live client's UUID")
	}
	switch {
	case s.AKilledAt.After(s.Missing.Start) || s.Missing.End.After(s.Preserve.Start) || s.Preserve.End.After(s.Reset.Start) || s.Reset.End.After(s.MountStart):
		return resetUnknown("kill, missing-UUID attempt, state-preserving request, RESET and replacement mount are out of order")
	}

	if r := requirePhase(s.AfterKill, "after the kill", s.AKilledAt, s.Missing.Start, s, bystandersHeld); r != nil {
		return *r
	}
	if *s.Missing.RC == 0 {
		return resetBad("a fresh handle returned 0 for a UUID that no session carries")
	}
	if *s.Missing.RC != errnoENOENT {
		return resetUnknown("missing-UUID attempt returned %d, not -ENOENT", *s.Missing.RC)
	}
	if r := requirePhase(s.AfterMissing, "after the missing-UUID attempt", s.Missing.End, s.Preserve.Start, s, bystandersHeld); r != nil {
		return *r
	}
	if *s.Preserve.RC == 0 {
		return resetBad("the MDS accepted a reclaim without CEPH_RECLAIM_RESET")
	}
	if *s.Preserve.RC != errnoEINVAL {
		return resetUnknown("state-preserving request returned %d, not -EINVAL", *s.Preserve.RC)
	}
	if len(s.Preserve.Replies) != 1 || s.Preserve.Replies[0] != errnoEINVAL {
		return resetUnknown("state-preserving request's client log shows MDS replies %v, want exactly one -22", s.Preserve.Replies)
	}
	if r := requirePhase(s.AfterPreserve, "after the state-preserving request", s.Preserve.End, s.Reset.Start, s, bystandersHeld); r != nil {
		return *r
	}
	if *s.Reset.RC != 0 {
		return resetUnknown("RESET returned %d, not 0", *s.Reset.RC)
	}
	if r := requirePhase(s.AfterReset, "after RESET", s.Reset.End, s.MountStart, s, targetRemoved); r != nil {
		return *r
	}

	switch {
	case s.SeedSize <= 0 || s.ReadSize != s.SeedSize:
		return resetUnknown("read size %d does not match the seeded size %d", s.ReadSize, s.SeedSize)
	case !resetSHA256Hex.MatchString(s.SeedSHA256) || s.ReadSHA256 != s.SeedSHA256:
		return resetUnknown("read digest does not match the seeded digest")
	case !s.ReadFromNewFd:
		return resetUnknown("the data was not read through a new descriptor")
	case s.ReadByClientID != s.B:
		return resetUnknown("the data was read by client.%d, not the replacement client.%d", s.ReadByClientID, s.B)
	}
	return ResetOutcome{Verdict: ResetEstablished, Reason: "fresh-handle missing UUID gave -ENOENT; the MDS refused the state-preserving request with -EINVAL and left the killed client's session and lock; RESET removed only that session and lock before the replacement read the original bytes"}
}

func checkResetAttempt(a ResetAttempt, name string, flags uint) *ResetOutcome {
	var r ResetOutcome
	switch {
	case a.Handle == "" || !a.Initialized:
		r = resetUnknown("%s attempt has no initialized native handle", name)
	case a.PriorReclaimCalls != 0:
		r = resetUnknown("%s attempt reuses a handle that already made %d reclaim call(s)", name, a.PriorReclaimCalls)
	case a.Flags != flags:
		r = resetUnknown("%s attempt used flags %d, not %d", name, a.Flags, flags)
	case a.Target == "" || a.OwnUUID == "" || a.Target == a.OwnUUID:
		r = resetUnknown("%s attempt target or own UUID is missing or identical", name)
	case a.RC == nil:
		r = resetUnknown("%s attempt return value was not recorded", name)
	case a.Start.IsZero() || a.End.Before(a.Start):
		r = resetUnknown("%s attempt times are missing or out of order", name)
	default:
		return nil
	}
	return &r
}

type resetExpect int

const (
	bystandersHeld resetExpect = iota
	targetRemoved
)

// requirePhase scans the observations of one phase. lo and hi bound the phase:
// an observation counts only when it started at or after lo and ended at or
// before hi. At least one counting observation must exist, and all of them must
// agree with the expectation. A definite collateral loss is a contradiction;
// everything else that disagrees is inconclusive.
func requirePhase(obs []ResetObservation, name string, lo, hi time.Time, s ResetSequence, want resetExpect) *ResetOutcome {
	counted := 0
	var prevStart time.Time
	for i, o := range obs {
		if o.Start.IsZero() || o.End.Before(o.Start) || o.Start.Before(prevStart) {
			r := resetUnknown("%s: observation %d has missing or out-of-order times", name, i)
			return &r
		}
		prevStart = o.Start
		if o.Start.Before(lo) || o.End.After(hi) {
			continue
		}
		counted++
		if o.Sessions == nil {
			r := resetUnknown("%s: observation %d has no session listing", name, i)
			return &r
		}
		u := o.Sessions.Status(s.U)
		a := o.Sessions.Status(s.A)
		switch {
		case u == ResetAbsent:
			r := resetBad("%s: the unrelated live client.%d lost its session", name, s.U)
			return &r
		case u != ResetPresent:
			r := resetUnknown("%s: unrelated client.%d is in a transitional or unknown state", name, s.U)
			return &r
		case o.ULock == ResetLockFree:
			r := resetBad("%s: the unrelated client's lock became free", name)
			return &r
		case o.ULock != ResetLockHeld:
			r := resetUnknown("%s: the unrelated client's lock state is unknown", name)
			return &r
		}
		switch want {
		case bystandersHeld:
			if a != ResetPresent || o.ALock != ResetLockHeld {
				r := resetUnknown("%s: A's session or lock is not confirmed present and held (session %d, lock %d)", name, a, o.ALock)
				return &r
			}
		case targetRemoved:
			if a != ResetAbsent || o.ALock != ResetLockFree {
				r := resetUnknown("%s: A's session is not confirmed gone with its lock free (session %d, lock %d)", name, a, o.ALock)
				return &r
			}
		}
	}
	if counted == 0 {
		r := resetUnknown("%s: no observation fits the phase interval", name)
		return &r
	}
	return nil
}
