//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// These controls feed synthetic probe documents, session listings and timed
// observations to the RESET reader and judge. They test parsing and the
// phase policy only. They do not reproduce a native MDS or libcephfs, and the
// synthetic shapes follow the pinned source rather than a captured run.

const (
	rstA = 5101
	rstU = 5102
	rstB = 5103

	rstImage   = "sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"
	rstLibHash = "abababababababababababababababababababababababababababababababab"
	rstExtHash = "2222222222222222222222222222222222222222222222222222222222222222"
	rstSeedSHA = "3333333333333333333333333333333333333333333333333333333333333333"
)

func rstProbeJSON(mutate func(map[string]any)) []byte {
	symbols := map[string]any{}
	for name, c := range resetABIContract {
		symbols[name] = map[string]any{"resolved": true, "restype": c.Restype, "argtypes": c.Argtypes}
	}
	doc := map[string]any{
		"image_config_id":  rstImage,
		"extension_sha256": rstExtHash,
		"library":          map[string]any{"path": "/usr/lib64/libcephfs.so.2", "sha256": rstLibHash, "version": map[string]any{"major": 20, "minor": 2, "patch": 4}},
		"symbols":          symbols,
		"probe_handle":     map[string]any{"create_rc": 0, "release_rc": 0, "initialized": false},
	}
	if mutate != nil {
		mutate(doc)
	}
	out, err := json.Marshal(doc)
	if err != nil {
		panic(err)
	}
	return out
}

func rstSub(doc map[string]any, keys ...string) map[string]any {
	m := doc
	for _, k := range keys {
		m = m[k].(map[string]any)
	}
	return m
}

func TestResetABIProbeReader(t *testing.T) {
	p, err := parseResetABIProbe(rstProbeJSON(nil))
	if err != nil || len(p.Calls) != 11 || p.Calls["ceph_start_reclaim"].Restype != "c_int" {
		t.Fatalf("good probe: %+v %v", p, err)
	}
	if err := checkResetABIProbe(p, 20, 2, 4); err != nil {
		t.Fatalf("good probe rejected: %v", err)
	}
	if len(resetABIContract) != 11 {
		t.Fatalf("contract lists %d symbols, want 11", len(resetABIContract))
	}

	parseRejects := map[string]func(map[string]any){
		"image id not a digest":   func(d map[string]any) { d["image_config_id"] = "latest" },
		"image id missing":        func(d map[string]any) { delete(d, "image_config_id") },
		"image id null":           func(d map[string]any) { d["image_config_id"] = nil },
		"extension hash short":    func(d map[string]any) { d["extension_sha256"] = "abc" },
		"library missing":         func(d map[string]any) { delete(d, "library") },
		"library path empty":      func(d map[string]any) { rstSub(d, "library")["path"] = "" },
		"library hash uppercase":  func(d map[string]any) { rstSub(d, "library")["sha256"] = strings.ToUpper(rstLibHash) },
		"version patch string":    func(d map[string]any) { rstSub(d, "library", "version")["patch"] = "4" },
		"version patch null":      func(d map[string]any) { rstSub(d, "library", "version")["patch"] = nil },
		"version patch fraction":  func(d map[string]any) { rstSub(d, "library", "version")["patch"] = 4.5 },
		"version missing":         func(d map[string]any) { delete(rstSub(d, "library"), "version") },
		"symbol missing":          func(d map[string]any) { delete(rstSub(d, "symbols"), "ceph_start_reclaim") },
		"extra symbol":            func(d map[string]any) { rstSub(d, "symbols")["ceph_other"] = map[string]any{} },
		"symbol unresolved":       func(d map[string]any) { rstSub(d, "symbols", "ceph_create")["resolved"] = false },
		"resolved not boolean":    func(d map[string]any) { rstSub(d, "symbols", "ceph_create")["resolved"] = "true" },
		"resolved missing":        func(d map[string]any) { delete(rstSub(d, "symbols", "ceph_create"), "resolved") },
		"restype missing":         func(d map[string]any) { delete(rstSub(d, "symbols", "ceph_create"), "restype") },
		"restype null":            func(d map[string]any) { rstSub(d, "symbols", "ceph_create")["restype"] = nil },
		"argtypes missing":        func(d map[string]any) { delete(rstSub(d, "symbols", "ceph_create"), "argtypes") },
		"argtypes null":           func(d map[string]any) { rstSub(d, "symbols", "ceph_create")["argtypes"] = nil },
		"argtypes not strings":    func(d map[string]any) { rstSub(d, "symbols", "ceph_create")["argtypes"] = []any{1, 2} },
		"probe handle missing":    func(d map[string]any) { delete(d, "probe_handle") },
		"create rc null":          func(d map[string]any) { rstSub(d, "probe_handle")["create_rc"] = nil },
		"release rc string":       func(d map[string]any) { rstSub(d, "probe_handle")["release_rc"] = "0" },
		"initialized missing":     func(d map[string]any) { delete(rstSub(d, "probe_handle"), "initialized") },
		"symbols not an object":   func(d map[string]any) { d["symbols"] = []any{} },
		"probe handle not object": func(d map[string]any) { d["probe_handle"] = 0 },
	}
	for name, mutate := range parseRejects {
		t.Run("parse rejects "+name, func(t *testing.T) {
			if p, err := parseResetABIProbe(rstProbeJSON(mutate)); err == nil {
				t.Fatalf("accepted: %+v", p)
			}
		})
	}
	if _, err := parseResetABIProbe([]byte(`null`)); err == nil {
		t.Fatal("accepted a null document")
	}
	if _, err := parseResetABIProbe([]byte(`[]`)); err == nil {
		t.Fatal("accepted an array document")
	}

	checkRejects := map[string]func(map[string]any){
		"finish_reclaim declared int": func(d map[string]any) { rstSub(d, "symbols", "ceph_finish_reclaim")["restype"] = "c_int" },
		"set_uuid declared int":       func(d map[string]any) { rstSub(d, "symbols", "ceph_set_uuid")["restype"] = "c_int" },
		"instance id declared int":    func(d map[string]any) { rstSub(d, "symbols", "ceph_get_instance_id")["restype"] = "c_int" },
		"start_reclaim flags signed": func(d map[string]any) {
			rstSub(d, "symbols", "ceph_start_reclaim")["argtypes"] = []string{"c_void_p", "c_char_p", "c_int"}
		},
		"start_reclaim missing flags": func(d map[string]any) {
			rstSub(d, "symbols", "ceph_start_reclaim")["argtypes"] = []string{"c_void_p", "c_char_p"}
		},
		"create takes handle by value": func(d map[string]any) {
			rstSub(d, "symbols", "ceph_create")["argtypes"] = []string{"c_void_p", "c_char_p"}
		},
		"version returns int":          func(d map[string]any) { rstSub(d, "symbols", "ceph_version")["restype"] = "c_int" },
		"wrong release":                func(d map[string]any) { rstSub(d, "library", "version")["minor"] = 1 },
		"probe create failed":          func(d map[string]any) { rstSub(d, "probe_handle")["create_rc"] = -12 },
		"probe release failed":         func(d map[string]any) { rstSub(d, "probe_handle")["release_rc"] = -22 },
		"probe handle was initialized": func(d map[string]any) { rstSub(d, "probe_handle")["initialized"] = true },
		"conf_set has two arguments": func(d map[string]any) {
			rstSub(d, "symbols", "ceph_conf_set")["argtypes"] = []string{"c_void_p", "c_char_p"}
		},
		"select_filesystem returns ptr": func(d map[string]any) { rstSub(d, "symbols", "ceph_select_filesystem")["restype"] = "c_void_p" },
	}
	for name, mutate := range checkRejects {
		t.Run("eligibility rejects "+name, func(t *testing.T) {
			p, err := parseResetABIProbe(rstProbeJSON(mutate))
			if err != nil {
				t.Fatalf("fixture should parse: %v", err)
			}
			if err := checkResetABIProbe(p, 20, 2, 4); err == nil {
				t.Fatal("eligibility passed")
			}
		})
	}
	// Squid is a separate pin; a Tentacle probe must not pass for it.
	if err := checkResetABIProbe(p, 19, 2, 5); err == nil {
		t.Fatal("a 20.2.4 probe passed the 19.2.5 gate")
	}
}

func rstSessionJSON(rows ...string) []byte { return []byte("[" + strings.Join(rows, ",") + "]") }

func rstSessionRow(id uint64, state string) string {
	return fmt.Sprintf(`{"id":%d,"state":%q,"num_caps":2,"entity":{"name":{"type":"client","num":%d}}}`, id, state, id)
}

func TestResetSessionReader(t *testing.T) {
	got, err := parseResetSessions(rstSessionJSON(rstSessionRow(rstA, "open"), rstSessionRow(rstU, "stale"), rstSessionRow(rstB, "closing")))
	if err != nil || len(got) != 3 {
		t.Fatalf("%v %v", got, err)
	}
	if got.status(rstA) != resetPresent || got.status(rstU) != resetPresent || got.status(rstB) != resetTransitional || got.status(9) != resetAbsent {
		t.Fatalf("statuses wrong: %v", got)
	}
	if empty, err := parseResetSessions([]byte(`[]`)); err != nil || empty == nil || empty.status(rstA) != resetAbsent {
		t.Fatalf("empty listing: %v %v", empty, err)
	}
	rejects := map[string][]byte{
		"null document":    []byte(`null`),
		"object document":  []byte(`{}`),
		"row not object":   rstSessionJSON(`7`),
		"id missing":       rstSessionJSON(`{"state":"open"}`),
		"id null":          rstSessionJSON(`{"id":null,"state":"open"}`),
		"id string":        rstSessionJSON(`{"id":"5101","state":"open"}`),
		"id zero":          rstSessionJSON(`{"id":0,"state":"open"}`),
		"id negative":      rstSessionJSON(`{"id":-4,"state":"open"}`),
		"id fraction":      rstSessionJSON(`{"id":1.5,"state":"open"}`),
		"state missing":    rstSessionJSON(`{"id":5101}`),
		"state null":       rstSessionJSON(`{"id":5101,"state":null}`),
		"state number":     rstSessionJSON(`{"id":5101,"state":1}`),
		"duplicate client": rstSessionJSON(rstSessionRow(rstA, "open"), rstSessionRow(rstA, "open")),
	}
	for name, doc := range rejects {
		t.Run("rejects "+name, func(t *testing.T) {
			if got, err := parseResetSessions(doc); err == nil {
				t.Fatalf("accepted: %v", got)
			}
		})
	}
}

var rstT0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func rstAt(sec float64) time.Time { return rstT0.Add(time.Duration(sec * float64(time.Second))) }

func rstInt(v int) *int { return &v }

func rstObs(start, end float64, a, u string, aLock, uLock resetLock) resetObservation {
	s := resetSessions{}
	if a != "" {
		s[rstA] = a
	}
	if u != "" {
		s[rstU] = u
	}
	return resetObservation{Start: rstAt(start), End: rstAt(end), Sessions: s, ALock: aLock, ULock: uLock}
}

// rstGood kills A at t=10, runs the missing-UUID attempt over [20,21], RESET
// over [30,34] and mounts the replacement at t=50.
func rstGood() resetSequence {
	held := func(s, e float64) resetObservation { return rstObs(s, e, "open", "open", resetLockHeld, resetLockHeld) }
	return resetSequence{
		A: rstA, U: rstU, B: rstB, AUUID: "uuid-a", UUUID: "uuid-u",
		AKilledAt: rstAt(10), AExitStatus: rstInt(-9),
		AfterKill: []resetObservation{held(11, 12)},
		Missing: resetAttempt{Handle: "h-missing", Initialized: true, Flags: cephReclaimReset,
			Target: "uuid-nobody", OwnUUID: "uuid-b1", RC: rstInt(-2), Start: rstAt(20), End: rstAt(21)},
		AfterMissing: []resetObservation{held(22, 23)},
		Reset: resetAttempt{Handle: "h-reset", Initialized: true, Flags: cephReclaimReset,
			Target: "uuid-a", OwnUUID: "uuid-b2", RC: rstInt(0), Start: rstAt(30), End: rstAt(34)},
		AfterReset:    []resetObservation{rstObs(35, 36, "", "open", resetLockFree, resetLockHeld)},
		MountStart:    rstAt(50),
		SeedSize:      27,
		SeedSHA256:    rstSeedSHA,
		ReadSize:      27,
		ReadSHA256:    rstSeedSHA,
		ReadFromNewFd: true, ReadByClientID: rstB,
	}
}

func TestResetSequenceJudge(t *testing.T) {
	if got := judgeResetSequence(rstGood()); got.Verdict != resetEstablished {
		t.Fatalf("good sequence: %+v", got)
	}
	cases := []struct {
		name   string
		mutate func(*resetSequence)
		want   resetVerdict
		part   string
	}{
		// Source-grounded contradictions.
		{"fresh handle returns 0 for a missing UUID", func(s *resetSequence) { s.Missing.RC = rstInt(0) }, resetContradicted, "returned 0 for a UUID"},
		{"unrelated session lost after RESET", func(s *resetSequence) { s.AfterReset[0].Sessions = resetSessions{} }, resetContradicted, "lost its session"},
		{"unrelated lock freed after RESET", func(s *resetSequence) { s.AfterReset[0].ULock = resetLockFree }, resetContradicted, "lock became free"},
		{"unrelated lock freed after the missing attempt", func(s *resetSequence) { s.AfterMissing[0].ULock = resetLockFree }, resetContradicted, "lock became free"},
		{"unrelated session lost after the missing attempt", func(s *resetSequence) { s.AfterMissing[0].Sessions = resetSessions{rstA: "open"} }, resetContradicted, "lost its session"},

		// Handle freshness: the stale reclaim_target_addrs hazard.
		{"missing attempt on a handle that already reclaimed", func(s *resetSequence) { s.Missing.PriorReclaimCalls = 1 }, resetInconclusive, "reuses a handle"},
		{"RESET on a reused handle", func(s *resetSequence) { s.Reset.PriorReclaimCalls = 1 }, resetInconclusive, "reuses a handle"},
		{"one handle for both attempts", func(s *resetSequence) { s.Reset.Handle = s.Missing.Handle }, resetInconclusive, "share one native handle"},
		{"uninitialized handle", func(s *resetSequence) { s.Missing.Initialized = false }, resetInconclusive, "no initialized"},
		{"unnamed handle", func(s *resetSequence) { s.Reset.Handle = "" }, resetInconclusive, "no initialized"},

		// Flags, targets, return values.
		{"state-preserving flags 0 on RESET", func(s *resetSequence) { s.Reset.Flags = 0 }, resetInconclusive, "not CEPH_RECLAIM_RESET"},
		{"flags 0 on the missing attempt", func(s *resetSequence) { s.Missing.Flags = 0 }, resetInconclusive, "not CEPH_RECLAIM_RESET"},
		{"missing target is A", func(s *resetSequence) { s.Missing.Target = "uuid-a" }, resetInconclusive, "targets the UUID that RESET"},
		{"missing target is U", func(s *resetSequence) { s.Missing.Target = "uuid-u" }, resetInconclusive, "names a live or own"},
		{"missing target is the reclaimer's own", func(s *resetSequence) { s.Missing.Target = s.Reset.OwnUUID }, resetInconclusive, "names a live or own"},
		{"target equals own UUID", func(s *resetSequence) { s.Reset.OwnUUID = s.Reset.Target }, resetInconclusive, "identical"},
		{"RESET targets U", func(s *resetSequence) { s.Reset.Target = "uuid-u" }, resetInconclusive, "killed client's UUID"},
		{"reclaimer reuses U's UUID", func(s *resetSequence) { s.Reset.OwnUUID = "uuid-u" }, resetInconclusive, "reuses a live"},
		{"missing attempt other errno", func(s *resetSequence) { s.Missing.RC = rstInt(-22) }, resetInconclusive, "not -ENOENT"},
		{"missing attempt return unrecorded", func(s *resetSequence) { s.Missing.RC = nil }, resetInconclusive, "not recorded"},
		{"RESET returns ENOENT", func(s *resetSequence) { s.Reset.RC = rstInt(-2) }, resetInconclusive, "not 0"},
		{"RESET return unrecorded", func(s *resetSequence) { s.Reset.RC = nil }, resetInconclusive, "not recorded"},

		// Admission.
		{"zero A", func(s *resetSequence) { s.A = 0 }, resetInconclusive, "distinct nonzero"},
		{"B equals A", func(s *resetSequence) { s.B = s.A; s.ReadByClientID = s.A }, resetInconclusive, "distinct nonzero"},
		{"A and U equal", func(s *resetSequence) { s.U = s.A }, resetInconclusive, "distinct nonzero"},
		{"empty A uuid", func(s *resetSequence) { s.AUUID = "" }, resetInconclusive, "UUIDs"},
		{"equal uuids", func(s *resetSequence) { s.UUUID = s.AUUID }, resetInconclusive, "UUIDs"},
		{"kill status unrecorded", func(s *resetSequence) { s.AExitStatus = nil }, resetInconclusive, "SIGKILL"},
		{"exit status not SIGKILL", func(s *resetSequence) { s.AExitStatus = rstInt(0) }, resetInconclusive, "SIGKILL"},
		{"kill time unset", func(s *resetSequence) { s.AKilledAt = time.Time{} }, resetInconclusive, "time is missing"},
		{"mount time unset", func(s *resetSequence) { s.MountStart = time.Time{} }, resetInconclusive, "time is missing"},
		{"attempt ends before it starts", func(s *resetSequence) { s.Reset.End = rstAt(29) }, resetInconclusive, "times are missing"},
		{"missing attempt after RESET start", func(s *resetSequence) { s.Missing.End = rstAt(31) }, resetInconclusive, "out of order"},
		{"RESET ends after mount", func(s *resetSequence) { s.MountStart = rstAt(33) }, resetInconclusive, "out of order"},
		{"kill after the first attempt", func(s *resetSequence) { s.AKilledAt = rstAt(25) }, resetInconclusive, "out of order"},

		// Pre-state and phase evidence (daemon-sampled values need the interval).
		{"no observation after the kill", func(s *resetSequence) { s.AfterKill = nil }, resetInconclusive, "no observation fits"},
		{"observation straddles the first attempt", func(s *resetSequence) {
			s.AfterKill = []resetObservation{rstObs(11, 20.5, "open", "open", resetLockHeld, resetLockHeld)}
		}, resetInconclusive, "no observation fits"},
		{"observation began before the kill", func(s *resetSequence) {
			s.AfterKill = []resetObservation{rstObs(9, 12, "open", "open", resetLockHeld, resetLockHeld)}
		}, resetInconclusive, "no observation fits"},
		{"A already gone before the attempts", func(s *resetSequence) { s.AfterKill[0].Sessions = resetSessions{rstU: "open"} }, resetInconclusive, "not confirmed present"},
		{"A lock unknown before the attempts", func(s *resetSequence) { s.AfterKill[0].ALock = resetLockUnknown }, resetInconclusive, "not confirmed present"},
		{"A lock already free before the attempts", func(s *resetSequence) { s.AfterKill[0].ALock = resetLockFree }, resetInconclusive, "not confirmed present"},
		{"A session closing before the attempts", func(s *resetSequence) { s.AfterKill[0].Sessions[rstA] = "closing" }, resetInconclusive, "not confirmed present"},
		{"U lock unknown", func(s *resetSequence) { s.AfterKill[0].ULock = resetLockUnknown }, resetInconclusive, "lock state is unknown"},
		{"U transitional", func(s *resetSequence) { s.AfterKill[0].Sessions[rstU] = "killing" }, resetInconclusive, "transitional"},
		{"observation lacks a listing", func(s *resetSequence) { s.AfterKill[0].Sessions = nil }, resetInconclusive, "no session listing"},
		{"no observation after the missing attempt", func(s *resetSequence) { s.AfterMissing = nil }, resetInconclusive, "no observation fits"},
		{"A lost after the missing attempt", func(s *resetSequence) { s.AfterMissing[0].Sessions = resetSessions{rstU: "open"} }, resetInconclusive, "not confirmed present"},
		{"no observation after RESET", func(s *resetSequence) { s.AfterReset = nil }, resetInconclusive, "no observation fits"},
		{"after-RESET observation began before RESET returned", func(s *resetSequence) {
			s.AfterReset = []resetObservation{rstObs(33, 36, "", "open", resetLockFree, resetLockHeld)}
		}, resetInconclusive, "no observation fits"},
		{"after-RESET observation ends after the mount", func(s *resetSequence) {
			s.AfterReset = []resetObservation{rstObs(35, 51, "", "open", resetLockFree, resetLockHeld)}
		}, resetInconclusive, "no observation fits"},
		{"A still present after RESET", func(s *resetSequence) { s.AfterReset[0].Sessions[rstA] = "open" }, resetInconclusive, "not confirmed gone"},
		{"A session killing after RESET", func(s *resetSequence) { s.AfterReset[0].Sessions[rstA] = "killing" }, resetInconclusive, "not confirmed gone"},
		{"A lock still held after RESET", func(s *resetSequence) { s.AfterReset[0].ALock = resetLockHeld }, resetInconclusive, "not confirmed gone"},
		{"A lock unknown after RESET", func(s *resetSequence) { s.AfterReset[0].ALock = resetLockUnknown }, resetInconclusive, "not confirmed gone"},
		{"observations out of order", func(s *resetSequence) {
			s.AfterReset = append(s.AfterReset, rstObs(34.5, 35.5, "", "open", resetLockFree, resetLockHeld))
		}, resetInconclusive, "out-of-order"},
		{"a later after-RESET observation resurrects A", func(s *resetSequence) {
			s.AfterReset = append(s.AfterReset, rstObs(37, 38, "open", "open", resetLockHeld, resetLockHeld))
		}, resetInconclusive, "not confirmed gone"},

		// Replacement and data oracle.
		{"size differs", func(s *resetSequence) { s.ReadSize = 26 }, resetInconclusive, "read size"},
		{"empty seed", func(s *resetSequence) { s.SeedSize, s.ReadSize = 0, 0 }, resetInconclusive, "read size"},
		{"digest differs", func(s *resetSequence) { s.ReadSHA256 = strings.Repeat("4", 64) }, resetInconclusive, "digest"},
		{"seed digest malformed", func(s *resetSequence) { s.SeedSHA256, s.ReadSHA256 = "abc", "abc" }, resetInconclusive, "digest"},
		{"read through an inherited descriptor", func(s *resetSequence) { s.ReadFromNewFd = false }, resetInconclusive, "new descriptor"},
		{"read by someone else", func(s *resetSequence) { s.ReadByClientID = rstU }, resetInconclusive, "not the replacement"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := rstGood()
			// Deep-copy the observation slices that mutators edit in place.
			s.AfterKill = rstCloneObs(s.AfterKill)
			s.AfterMissing = rstCloneObs(s.AfterMissing)
			s.AfterReset = rstCloneObs(s.AfterReset)
			c.mutate(&s)
			got := judgeResetSequence(s)
			if got.Verdict != c.want || !strings.Contains(got.Reason, c.part) {
				t.Fatalf("verdict %d reason %q, want %d containing %q", got.Verdict, got.Reason, c.want, c.part)
			}
		})
	}

	// The default verdict must not be the passing one.
	if (resetOutcome{}).Verdict != resetInconclusive {
		t.Fatal("zero outcome is not inconclusive")
	}
	names := make([]string, 0, len(cases))
	for _, c := range cases {
		names = append(names, c.name)
	}
	sort.Strings(names)
	for i := 1; i < len(names); i++ {
		if names[i] == names[i-1] {
			t.Fatalf("duplicate case name %q", names[i])
		}
	}
}

func rstCloneObs(in []resetObservation) []resetObservation {
	out := make([]resetObservation, len(in))
	for i, o := range in {
		out[i] = o
		out[i].Sessions = resetSessions{}
		for k, v := range o.Sessions {
			out[i].Sessions[k] = v
		}
	}
	return out
}
