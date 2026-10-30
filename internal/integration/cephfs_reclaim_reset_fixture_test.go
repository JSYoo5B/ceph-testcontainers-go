//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/json"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
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
	for name, c := range harness.ResetABIContract {
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
	p, err := harness.ParseResetABIProbe(rstProbeJSON(nil))
	if err != nil || len(p.Calls) != 11 || p.Calls["ceph_start_reclaim"].Restype != "c_int" {
		t.Fatalf("good probe: %+v %v", p, err)
	}
	if err := harness.CheckResetABIProbe(p, 20, 2, 4); err != nil {
		t.Fatalf("good probe rejected: %v", err)
	}
	if len(harness.ResetABIContract) != 11 {
		t.Fatalf("contract lists %d symbols, want 11", len(harness.ResetABIContract))
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
			if p, err := harness.ParseResetABIProbe(rstProbeJSON(mutate)); err == nil {
				t.Fatalf("accepted: %+v", p)
			}
		})
	}
	if _, err := harness.ParseResetABIProbe([]byte(`null`)); err == nil {
		t.Fatal("accepted a null document")
	}
	if _, err := harness.ParseResetABIProbe([]byte(`[]`)); err == nil {
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
			p, err := harness.ParseResetABIProbe(rstProbeJSON(mutate))
			if err != nil {
				t.Fatalf("fixture should parse: %v", err)
			}
			if err := harness.CheckResetABIProbe(p, 20, 2, 4); err == nil {
				t.Fatal("eligibility passed")
			}
		})
	}
	// Squid is a separate pin; a Tentacle probe must not pass for it.
	if err := harness.CheckResetABIProbe(p, 19, 2, 5); err == nil {
		t.Fatal("a 20.2.4 probe passed the 19.2.5 gate")
	}
}

func rstSessionJSON(rows ...string) []byte { return []byte("[" + strings.Join(rows, ",") + "]") }

func rstSessionRow(id uint64, state string) string {
	return fmt.Sprintf(`{"id":%d,"state":%q,"num_caps":2,"entity":{"name":{"type":"client","num":%d}}}`, id, state, id)
}

func TestResetSessionReader(t *testing.T) {
	got, err := harness.ParseResetSessions(rstSessionJSON(rstSessionRow(rstA, "open"), rstSessionRow(rstU, "stale"), rstSessionRow(rstB, "closing")))
	if err != nil || len(got) != 3 {
		t.Fatalf("%v %v", got, err)
	}
	if got.Status(rstA) != harness.ResetPresent || got.Status(rstU) != harness.ResetPresent || got.Status(rstB) != harness.ResetTransitional || got.Status(9) != harness.ResetAbsent {
		t.Fatalf("statuses wrong: %v", got)
	}
	if empty, err := harness.ParseResetSessions([]byte(`[]`)); err != nil || empty == nil || empty.Status(rstA) != harness.ResetAbsent {
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
			if got, err := harness.ParseResetSessions(doc); err == nil {
				t.Fatalf("accepted: %v", got)
			}
		})
	}
}

var rstT0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func rstAt(sec float64) time.Time { return rstT0.Add(time.Duration(sec * float64(time.Second))) }

func rstInt(v int) *int { return &v }

func rstObs(start, end float64, a, u string, aLock, uLock harness.ResetLock) harness.ResetObservation {
	s := harness.ResetSessions{}
	if a != "" {
		s[rstA] = a
	}
	if u != "" {
		s[rstU] = u
	}
	return harness.ResetObservation{Start: rstAt(start), End: rstAt(end), Sessions: s, ALock: aLock, ULock: uLock}
}

// rstGood kills A at t=10, runs the missing-UUID attempt over [20,21], RESET
// over [30,34] and mounts the replacement at t=50.
func rstGood() harness.ResetSequence {
	held := func(s, e float64) harness.ResetObservation {
		return rstObs(s, e, "open", "open", harness.ResetLockHeld, harness.ResetLockHeld)
	}
	return harness.ResetSequence{
		A: rstA, U: rstU, B: rstB, AUUID: "uuid-a", UUUID: "uuid-u",
		AKilledAt: rstAt(10), AExitStatus: rstInt(-9),
		AfterKill: []harness.ResetObservation{held(11, 12)},
		Missing: harness.ResetAttempt{Handle: "h-missing", Initialized: true, Flags: harness.CephReclaimReset,
			Target: "uuid-nobody", OwnUUID: "uuid-b1", RC: rstInt(-2), Start: rstAt(20), End: rstAt(21)},
		AfterMissing: []harness.ResetObservation{held(22, 23)},
		Reset: harness.ResetAttempt{Handle: "h-reset", Initialized: true, Flags: harness.CephReclaimReset,
			Target: "uuid-a", OwnUUID: "uuid-b2", RC: rstInt(0), Start: rstAt(30), End: rstAt(34)},
		AfterReset:    []harness.ResetObservation{rstObs(35, 36, "", "open", harness.ResetLockFree, harness.ResetLockHeld)},
		MountStart:    rstAt(50),
		SeedSize:      27,
		SeedSHA256:    rstSeedSHA,
		ReadSize:      27,
		ReadSHA256:    rstSeedSHA,
		ReadFromNewFd: true, ReadByClientID: rstB,
	}
}

func TestResetSequenceJudge(t *testing.T) {
	if got := harness.JudgeResetSequence(rstGood()); got.Verdict != harness.ResetEstablished {
		t.Fatalf("good sequence: %+v", got)
	}
	cases := []struct {
		name   string
		mutate func(*harness.ResetSequence)
		want   harness.ResetVerdict
		part   string
	}{
		// Source-grounded contradictions.
		{"fresh handle returns 0 for a missing UUID", func(s *harness.ResetSequence) { s.Missing.RC = rstInt(0) }, harness.ResetContradicted, "returned 0 for a UUID"},
		{"unrelated session lost after RESET", func(s *harness.ResetSequence) { s.AfterReset[0].Sessions = harness.ResetSessions{} }, harness.ResetContradicted, "lost its session"},
		{"unrelated lock freed after RESET", func(s *harness.ResetSequence) { s.AfterReset[0].ULock = harness.ResetLockFree }, harness.ResetContradicted, "lock became free"},
		{"unrelated lock freed after the missing attempt", func(s *harness.ResetSequence) { s.AfterMissing[0].ULock = harness.ResetLockFree }, harness.ResetContradicted, "lock became free"},
		{"unrelated session lost after the missing attempt", func(s *harness.ResetSequence) { s.AfterMissing[0].Sessions = harness.ResetSessions{rstA: "open"} }, harness.ResetContradicted, "lost its session"},

		// Handle freshness: the stale reclaim_target_addrs hazard.
		{"missing attempt on a handle that already reclaimed", func(s *harness.ResetSequence) { s.Missing.PriorReclaimCalls = 1 }, harness.ResetInconclusive, "reuses a handle"},
		{"RESET on a reused handle", func(s *harness.ResetSequence) { s.Reset.PriorReclaimCalls = 1 }, harness.ResetInconclusive, "reuses a handle"},
		{"one handle for both attempts", func(s *harness.ResetSequence) { s.Reset.Handle = s.Missing.Handle }, harness.ResetInconclusive, "share one native handle"},
		{"uninitialized handle", func(s *harness.ResetSequence) { s.Missing.Initialized = false }, harness.ResetInconclusive, "no initialized"},
		{"unnamed handle", func(s *harness.ResetSequence) { s.Reset.Handle = "" }, harness.ResetInconclusive, "no initialized"},

		// Flags, targets, return values.
		{"state-preserving flags 0 on RESET", func(s *harness.ResetSequence) { s.Reset.Flags = 0 }, harness.ResetInconclusive, "not CEPH_RECLAIM_RESET"},
		{"flags 0 on the missing attempt", func(s *harness.ResetSequence) { s.Missing.Flags = 0 }, harness.ResetInconclusive, "not CEPH_RECLAIM_RESET"},
		{"missing target is A", func(s *harness.ResetSequence) { s.Missing.Target = "uuid-a" }, harness.ResetInconclusive, "targets the UUID that RESET"},
		{"missing target is U", func(s *harness.ResetSequence) { s.Missing.Target = "uuid-u" }, harness.ResetInconclusive, "names a live or own"},
		{"missing target is the reclaimer's own", func(s *harness.ResetSequence) { s.Missing.Target = s.Reset.OwnUUID }, harness.ResetInconclusive, "names a live or own"},
		{"target equals own UUID", func(s *harness.ResetSequence) { s.Reset.OwnUUID = s.Reset.Target }, harness.ResetInconclusive, "identical"},
		{"RESET targets U", func(s *harness.ResetSequence) { s.Reset.Target = "uuid-u" }, harness.ResetInconclusive, "killed client's UUID"},
		{"reclaimer reuses U's UUID", func(s *harness.ResetSequence) { s.Reset.OwnUUID = "uuid-u" }, harness.ResetInconclusive, "reuses a live"},
		{"missing attempt other errno", func(s *harness.ResetSequence) { s.Missing.RC = rstInt(-22) }, harness.ResetInconclusive, "not -ENOENT"},
		{"missing attempt return unrecorded", func(s *harness.ResetSequence) { s.Missing.RC = nil }, harness.ResetInconclusive, "not recorded"},
		{"RESET returns ENOENT", func(s *harness.ResetSequence) { s.Reset.RC = rstInt(-2) }, harness.ResetInconclusive, "not 0"},
		{"RESET return unrecorded", func(s *harness.ResetSequence) { s.Reset.RC = nil }, harness.ResetInconclusive, "not recorded"},

		// Admission.
		{"zero A", func(s *harness.ResetSequence) { s.A = 0 }, harness.ResetInconclusive, "distinct nonzero"},
		{"B equals A", func(s *harness.ResetSequence) { s.B = s.A; s.ReadByClientID = s.A }, harness.ResetInconclusive, "distinct nonzero"},
		{"A and U equal", func(s *harness.ResetSequence) { s.U = s.A }, harness.ResetInconclusive, "distinct nonzero"},
		{"empty A uuid", func(s *harness.ResetSequence) { s.AUUID = "" }, harness.ResetInconclusive, "UUIDs"},
		{"equal uuids", func(s *harness.ResetSequence) { s.UUUID = s.AUUID }, harness.ResetInconclusive, "UUIDs"},
		{"kill status unrecorded", func(s *harness.ResetSequence) { s.AExitStatus = nil }, harness.ResetInconclusive, "SIGKILL"},
		{"exit status not SIGKILL", func(s *harness.ResetSequence) { s.AExitStatus = rstInt(0) }, harness.ResetInconclusive, "SIGKILL"},
		{"kill time unset", func(s *harness.ResetSequence) { s.AKilledAt = time.Time{} }, harness.ResetInconclusive, "time is missing"},
		{"mount time unset", func(s *harness.ResetSequence) { s.MountStart = time.Time{} }, harness.ResetInconclusive, "time is missing"},
		{"attempt ends before it starts", func(s *harness.ResetSequence) { s.Reset.End = rstAt(29) }, harness.ResetInconclusive, "times are missing"},
		{"missing attempt after RESET start", func(s *harness.ResetSequence) { s.Missing.End = rstAt(31) }, harness.ResetInconclusive, "out of order"},
		{"RESET ends after mount", func(s *harness.ResetSequence) { s.MountStart = rstAt(33) }, harness.ResetInconclusive, "out of order"},
		{"kill after the first attempt", func(s *harness.ResetSequence) { s.AKilledAt = rstAt(25) }, harness.ResetInconclusive, "out of order"},

		// Pre-state and phase evidence (daemon-sampled values need the interval).
		{"no observation after the kill", func(s *harness.ResetSequence) { s.AfterKill = nil }, harness.ResetInconclusive, "no observation fits"},
		{"observation straddles the first attempt", func(s *harness.ResetSequence) {
			s.AfterKill = []harness.ResetObservation{rstObs(11, 20.5, "open", "open", harness.ResetLockHeld, harness.ResetLockHeld)}
		}, harness.ResetInconclusive, "no observation fits"},
		{"observation began before the kill", func(s *harness.ResetSequence) {
			s.AfterKill = []harness.ResetObservation{rstObs(9, 12, "open", "open", harness.ResetLockHeld, harness.ResetLockHeld)}
		}, harness.ResetInconclusive, "no observation fits"},
		{"A already gone before the attempts", func(s *harness.ResetSequence) { s.AfterKill[0].Sessions = harness.ResetSessions{rstU: "open"} }, harness.ResetInconclusive, "not confirmed present"},
		{"A lock unknown before the attempts", func(s *harness.ResetSequence) { s.AfterKill[0].ALock = harness.ResetLockUnknown }, harness.ResetInconclusive, "not confirmed present"},
		{"A lock already free before the attempts", func(s *harness.ResetSequence) { s.AfterKill[0].ALock = harness.ResetLockFree }, harness.ResetInconclusive, "not confirmed present"},
		{"A session closing before the attempts", func(s *harness.ResetSequence) { s.AfterKill[0].Sessions[rstA] = "closing" }, harness.ResetInconclusive, "not confirmed present"},
		{"U lock unknown", func(s *harness.ResetSequence) { s.AfterKill[0].ULock = harness.ResetLockUnknown }, harness.ResetInconclusive, "lock state is unknown"},
		{"U transitional", func(s *harness.ResetSequence) { s.AfterKill[0].Sessions[rstU] = "killing" }, harness.ResetInconclusive, "transitional"},
		{"observation lacks a listing", func(s *harness.ResetSequence) { s.AfterKill[0].Sessions = nil }, harness.ResetInconclusive, "no session listing"},
		{"no observation after the missing attempt", func(s *harness.ResetSequence) { s.AfterMissing = nil }, harness.ResetInconclusive, "no observation fits"},
		{"A lost after the missing attempt", func(s *harness.ResetSequence) { s.AfterMissing[0].Sessions = harness.ResetSessions{rstU: "open"} }, harness.ResetInconclusive, "not confirmed present"},
		{"no observation after RESET", func(s *harness.ResetSequence) { s.AfterReset = nil }, harness.ResetInconclusive, "no observation fits"},
		{"after-RESET observation began before RESET returned", func(s *harness.ResetSequence) {
			s.AfterReset = []harness.ResetObservation{rstObs(33, 36, "", "open", harness.ResetLockFree, harness.ResetLockHeld)}
		}, harness.ResetInconclusive, "no observation fits"},
		{"after-RESET observation ends after the mount", func(s *harness.ResetSequence) {
			s.AfterReset = []harness.ResetObservation{rstObs(35, 51, "", "open", harness.ResetLockFree, harness.ResetLockHeld)}
		}, harness.ResetInconclusive, "no observation fits"},
		{"A still present after RESET", func(s *harness.ResetSequence) { s.AfterReset[0].Sessions[rstA] = "open" }, harness.ResetInconclusive, "not confirmed gone"},
		{"A session killing after RESET", func(s *harness.ResetSequence) { s.AfterReset[0].Sessions[rstA] = "killing" }, harness.ResetInconclusive, "not confirmed gone"},
		{"A lock still held after RESET", func(s *harness.ResetSequence) { s.AfterReset[0].ALock = harness.ResetLockHeld }, harness.ResetInconclusive, "not confirmed gone"},
		{"A lock unknown after RESET", func(s *harness.ResetSequence) { s.AfterReset[0].ALock = harness.ResetLockUnknown }, harness.ResetInconclusive, "not confirmed gone"},
		{"observations out of order", func(s *harness.ResetSequence) {
			s.AfterReset = append(s.AfterReset, rstObs(34.5, 35.5, "", "open", harness.ResetLockFree, harness.ResetLockHeld))
		}, harness.ResetInconclusive, "out-of-order"},
		{"a later after-RESET observation resurrects A", func(s *harness.ResetSequence) {
			s.AfterReset = append(s.AfterReset, rstObs(37, 38, "open", "open", harness.ResetLockHeld, harness.ResetLockHeld))
		}, harness.ResetInconclusive, "not confirmed gone"},

		// Replacement and data oracle.
		{"size differs", func(s *harness.ResetSequence) { s.ReadSize = 26 }, harness.ResetInconclusive, "read size"},
		{"empty seed", func(s *harness.ResetSequence) { s.SeedSize, s.ReadSize = 0, 0 }, harness.ResetInconclusive, "read size"},
		{"digest differs", func(s *harness.ResetSequence) { s.ReadSHA256 = strings.Repeat("4", 64) }, harness.ResetInconclusive, "digest"},
		{"seed digest malformed", func(s *harness.ResetSequence) { s.SeedSHA256, s.ReadSHA256 = "abc", "abc" }, harness.ResetInconclusive, "digest"},
		{"read through an inherited descriptor", func(s *harness.ResetSequence) { s.ReadFromNewFd = false }, harness.ResetInconclusive, "new descriptor"},
		{"read by someone else", func(s *harness.ResetSequence) { s.ReadByClientID = rstU }, harness.ResetInconclusive, "not the replacement"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := rstGood()
			// Deep-copy the observation slices that mutators edit in place.
			s.AfterKill = rstCloneObs(s.AfterKill)
			s.AfterMissing = rstCloneObs(s.AfterMissing)
			s.AfterReset = rstCloneObs(s.AfterReset)
			c.mutate(&s)
			got := harness.JudgeResetSequence(s)
			if got.Verdict != c.want || !strings.Contains(got.Reason, c.part) {
				t.Fatalf("verdict %d reason %q, want %d containing %q", got.Verdict, got.Reason, c.want, c.part)
			}
		})
	}

	// The default verdict must not be the passing one.
	if (harness.ResetOutcome{}).Verdict != harness.ResetInconclusive {
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

func rstCloneObs(in []harness.ResetObservation) []harness.ResetObservation {
	out := make([]harness.ResetObservation, len(in))
	for i, o := range in {
		out[i] = o
		out[i].Sessions = harness.ResetSessions{}
		for k, v := range o.Sessions {
			out[i].Sessions[k] = v
		}
	}
	return out
}
