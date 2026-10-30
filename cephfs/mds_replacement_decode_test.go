package cephfs

import (
	"context"
	"slices"
	"testing"
)

type replacementDecodeEdit func(n *replacementMDSControl, m map[string]any)

// replacementDecode runs readMDSReplacementScope over the replacement fixture
// after edit changed the native map. withWorker selects the state in which the
// reserved replacement daemon already exists (cephFSMDSReplacement.daemon set).
func replacementDecode(t *testing.T, withWorker bool, edit replacementDecodeEdit) (*cephFSMDSRemovalScope, bool, error) {
	t.Helper()
	r := newReplacementMDSFixture(t)
	if edit != nil {
		// The reader takes the native pool identity from one dump and decodes a
		// second one. The edit lands between them so only the decoded document
		// carries it, as the stopped-MDS characterization does.
		dumps := 0
		r.n.hook = func(call string) {
			if call == "fs dump --format json" {
				if dumps++; dumps == 1 {
					edit(r.n, r.n.target())
				}
			}
		}
	}
	rep := &cephFSMDSReplacement{name: "target-1"}
	if withWorker {
		rep.daemon = &MDS{}
	}
	return r.fs.fleet().readMDSReplacementScope(t.Context(), r.old.identity, rep)
}

func replacementDecodeSibling(info, up map[string]any) replacementDecodeEdit {
	return func(n *replacementMDSControl, _ map[string]any) {
		sibling := coldMDSTestMap("sibling", 5, 6)
		sibling["info"], sibling["up"] = info, up
		n.native["filesystems"] = append(n.native["filesystems"].([]any), map[string]any{"id": 8, "mdsmap": sibling})
	}
}

func replacementDecodeStandby(mod func(row map[string]any)) replacementDecodeEdit {
	return func(n *replacementMDSControl, _ map[string]any) {
		row := coldMDSTestRow("foreign", "up:standby", 5, -1, 8)
		mod(row)
		n.native["standbys"] = []any{row}
	}
}

func replacementDecodeMap(field string, value any) replacementDecodeEdit {
	return func(_ *replacementMDSControl, m map[string]any) { m[field] = value }
}

func replacementDecodeFlag(flag string, value any) replacementDecodeEdit {
	return func(_ *replacementMDSControl, m map[string]any) { m["flags_state"].(map[string]any)[flag] = value }
}

func replacementDecodeWorker(state string, mod func(row map[string]any)) replacementDecodeEdit {
	return func(n *replacementMDSControl, m map[string]any) {
		row := coldMDSTestRow("target-1", state, 33, 0, 7)
		if mod != nil {
			mod(row)
		}
		m["info"] = map[string]any{"gid_33": row}
		m["up"], m["failed"] = map[string]any{"mds_0": uint64(33)}, []int{}
	}
}

// TestMDSReplacementDecodeKeepsEveryGuardAndItsClassification pins, for the
// replacement global-map decoder, the exact refusal text of each guard group.
// It characterizes the decoder as it is, including that up:oneshot-replay is
// refused. It is a Docker-free control, not native evidence. The two guards on
// the native pool identity read for this decode (an attached pool that is
// duplicated, negative or absent) are not reachable through this fixture
// because that identity comes from the same document; they are pinned on the
// decoder directly in TestMDSReplacementDecoderRefusesInvalidPoolIdentity.
func TestMDSReplacementDecodeKeepsEveryGuardAndItsClassification(t *testing.T) {
	const (
		malformed  = "MDS replacement map is incomplete or malformed"
		registered = "original stopped MDS name is still registered"
		contradict = "MDS replacement global daemon map is contradictory"
		fsIncomp   = "MDS replacement global filesystem map is incomplete"
		rankMember = "MDS replacement global rank membership is malformed"
		reservedFS = "reserved MDS replacement name belongs to another filesystem"
		upMap      = "MDS replacement global up map has no matching daemon"
		policy     = "original MDS replacement filesystem policy changed"
		noWorker   = "original MDS replacement requires failed rank zero without a worker"
		exact      = "MDS replacement rank is not the exact new member"
		identity   = "MDS replacement native rank identity or recovery state changed"
		absent     = "original MDS replacement filesystem is absent"
		standbyBad = "MDS replacement global standby map is malformed"
		reservedSb = "reserved MDS replacement name is already registered"
		foreignSb  = "foreign standby can replace the failed original MDS rank"
	)
	sibRow := func(name, state string, gid uint64, rank int) map[string]any {
		return coldMDSTestRow(name, state, gid, rank, 8)
	}
	sibUp := map[string]any{"mds_0": uint64(44)}
	sibInfo := func(row map[string]any) map[string]any { return map[string]any{"gid_44": row} }
	fsEdit := func(mod func(entry map[string]any)) replacementDecodeEdit {
		return func(n *replacementMDSControl, _ map[string]any) {
			mod(n.native["filesystems"].([]any)[0].(map[string]any))
		}
	}
	rawEdit := func(raw string) replacementDecodeEdit {
		return func(n *replacementMDSControl, _ map[string]any) { n.raw = raw }
	}
	cases := []struct {
		name   string
		worker bool
		edit   replacementDecodeEdit
		want   string
	}{
		{"raw-null", false, rawEdit("null"), malformed},
		{"raw-filesystems-null", false, rawEdit(`{"filesystems":null,"standbys":[]}`), malformed},
		{"raw-standbys-missing", false, rawEdit(`{"filesystems":[]}`), malformed},
		{"raw-trailing", false, func(n *replacementMDSControl, _ map[string]any) { n.raw = coldMDSTestJSON(n.native) + ` {}` }, malformed},
		{"raw-duplicate-key", false, rawEdit(`{"standbys":[],"standbys":[],"filesystems":[]}`), malformed},
		{"standbys-null", false, func(n *replacementMDSControl, _ map[string]any) { n.native["standbys"] = nil }, malformed},

		{"standby-old-name", false, replacementDecodeStandby(func(r map[string]any) { r["name"] = "target-0" }), registered},
		{"standby-name-missing", false, replacementDecodeStandby(func(r map[string]any) { delete(r, "name") }), contradict},
		{"standby-name-invalid", false, replacementDecodeStandby(func(r map[string]any) { r["name"] = "bad name" }), contradict},
		{"standby-gid-missing", false, replacementDecodeStandby(func(r map[string]any) { delete(r, "gid") }), contradict},
		{"standby-gid-zero", false, replacementDecodeStandby(func(r map[string]any) { r["gid"] = 0 }), contradict},
		{"standby-rank-missing", false, replacementDecodeStandby(func(r map[string]any) { delete(r, "rank") }), contradict},
		{"standby-join-missing", false, replacementDecodeStandby(func(r map[string]any) { delete(r, "join_fscid") }), contradict},
		{"standby-join-below-minus-one", false, replacementDecodeStandby(func(r map[string]any) { r["join_fscid"] = -2 }), contradict},
		{"standby-state-missing", false, replacementDecodeStandby(func(r map[string]any) { delete(r, "state") }), contradict},
		{"standby-state-oneshot-replay", false, replacementDecodeStandby(func(r map[string]any) { r["state"] = "up:oneshot-replay" }), contradict},
		{"standby-state-down", false, replacementDecodeStandby(func(r map[string]any) { r["state"] = "down:stopped" }), contradict},
		{"standby-state-unknown-name", false, replacementDecodeStandby(func(r map[string]any) { r["state"] = "???" }), contradict},
		{"standby-state-empty", false, replacementDecodeStandby(func(r map[string]any) { r["state"] = "" }), contradict},
		{"standby-duplicate-name", false, func(n *replacementMDSControl, m map[string]any) {
			replacementDecodeSibling(map[string]any{}, map[string]any{})(n, m)
			n.native["standbys"] = []any{coldMDSTestRow("foreign", "up:standby", 5, -1, 8), coldMDSTestRow("foreign", "up:standby", 6, -1, 8)}
		}, contradict},
		{"standby-duplicate-gid", false, func(n *replacementMDSControl, m map[string]any) {
			replacementDecodeSibling(map[string]any{}, map[string]any{})(n, m)
			n.native["standbys"] = []any{coldMDSTestRow("foreign", "up:standby", 5, -1, 8), coldMDSTestRow("other", "up:standby", 5, -1, 8)}
		}, contradict},

		{"filesystem-negative-id", false, func(n *replacementMDSControl, _ map[string]any) {
			n.native["filesystems"] = append(n.native["filesystems"].([]any), map[string]any{"id": -1, "mdsmap": coldMDSTestMap("sibling", 5, 6)})
		}, fsIncomp},
		{"filesystem-id-missing-field", false, func(n *replacementMDSControl, _ map[string]any) {
			n.native["filesystems"] = append(n.native["filesystems"].([]any), map[string]any{"mdsmap": coldMDSTestMap("sibling", 5, 6)})
		}, fsIncomp},
		{"filesystem-duplicate-id", false, func(n *replacementMDSControl, _ map[string]any) {
			n.native["filesystems"] = append(n.native["filesystems"].([]any), map[string]any{"id": 7, "mdsmap": coldMDSTestMap("sibling", 5, 6)})
		}, fsIncomp},
		{"filesystem-mdsmap-null", false, func(n *replacementMDSControl, _ map[string]any) {
			n.native["filesystems"] = append(n.native["filesystems"].([]any), map[string]any{"id": 8, "mdsmap": nil})
		}, fsIncomp},
		{"filesystem-name-empty", false, func(n *replacementMDSControl, _ map[string]any) {
			n.native["filesystems"] = append(n.native["filesystems"].([]any), map[string]any{"id": 8, "mdsmap": coldMDSTestMap("", 5, 6)})
		}, fsIncomp},
		{"filesystem-duplicate-name", false, func(n *replacementMDSControl, _ map[string]any) {
			n.native["filesystems"] = append(n.native["filesystems"].([]any), map[string]any{"id": 8, "mdsmap": coldMDSTestMap("target", 5, 6)})
		}, fsIncomp},
		{"filesystem-info-null", false, func(n *replacementMDSControl, _ map[string]any) {
			sibling := coldMDSTestMap("sibling", 5, 6)
			sibling["info"] = nil
			n.native["filesystems"] = append(n.native["filesystems"].([]any), map[string]any{"id": 8, "mdsmap": sibling})
		}, fsIncomp},
		{"filesystem-up-null", false, func(n *replacementMDSControl, _ map[string]any) {
			sibling := coldMDSTestMap("sibling", 5, 6)
			sibling["up"] = nil
			n.native["filesystems"] = append(n.native["filesystems"].([]any), map[string]any{"id": 8, "mdsmap": sibling})
		}, fsIncomp},

		{"sibling-row-old-name", false, replacementDecodeSibling(sibInfo(sibRow("target-0", "up:active", 44, 0)), sibUp), registered},
		{"sibling-row-state-oneshot-replay", false, replacementDecodeSibling(sibInfo(sibRow("sibling-0", "up:oneshot-replay", 44, 0)), sibUp), contradict},
		{"sibling-row-state-down", false, replacementDecodeSibling(sibInfo(sibRow("sibling-0", "down:stopped", 44, 0)), sibUp), contradict},
		{"sibling-row-duplicate-gid-across-filesystems", false, func(n *replacementMDSControl, _ map[string]any) {
			n.native["standbys"] = []any{coldMDSTestRow("foreign", "up:standby", 44, -1, 8)}
			replacementDecodeSibling(sibInfo(sibRow("sibling-0", "up:active", 44, 0)), sibUp)(n, nil)
		}, contradict},
		{"sibling-row-key-mismatch", false, replacementDecodeSibling(map[string]any{"gid_45": sibRow("sibling-0", "up:active", 44, 0)}, sibUp), rankMember},
		{"sibling-row-negative-rank", false, replacementDecodeSibling(sibInfo(sibRow("sibling-0", "up:active", 44, -1)), sibUp), rankMember},
		{"sibling-row-state-boot", false, replacementDecodeSibling(sibInfo(sibRow("sibling-0", "up:boot", 44, 0)), sibUp), rankMember},
		{"sibling-row-state-standby", false, replacementDecodeSibling(sibInfo(sibRow("sibling-0", "up:standby", 44, 0)), sibUp), rankMember},
		{"sibling-row-assigned-without-up", false, replacementDecodeSibling(sibInfo(sibRow("sibling-0", "up:active", 44, 0)), map[string]any{}), rankMember},
		{"sibling-row-reserved-name", false, replacementDecodeSibling(sibInfo(sibRow("target-1", "up:active", 44, 0)), sibUp), reservedFS},
		{"sibling-up-without-info", false, replacementDecodeSibling(map[string]any{}, sibUp), upMap},
		{"sibling-up-gid-zero", false, replacementDecodeSibling(map[string]any{}, map[string]any{"mds_0": uint64(0)}), upMap},
		{"sibling-up-key-mismatch", false, replacementDecodeSibling(sibInfo(sibRow("sibling-0", "up:standby-replay", 44, 0)), map[string]any{"mds_1": uint64(44)}), upMap},
		{"sibling-up-standby-replay", false, replacementDecodeSibling(sibInfo(sibRow("sibling-0", "up:standby-replay", 44, 0)), sibUp), upMap},

		{"policy-filesystem-id", false, fsEdit(func(e map[string]any) { e["id"] = 99 }), policy},
		{"policy-metadata-pool", false, replacementDecodeMap("metadata_pool", 2), policy},
		{"policy-metadata-pool-null", false, replacementDecodeMap("metadata_pool", nil), policy},
		{"policy-data-pools", false, replacementDecodeMap("data_pools", []int64{2}), policy},
		{"policy-data-pools-null", false, replacementDecodeMap("data_pools", nil), policy},
		{"policy-max-mds", false, replacementDecodeMap("max_mds", 2), policy},
		{"policy-max-mds-null", false, replacementDecodeMap("max_mds", nil), policy},
		{"policy-standby-count", false, replacementDecodeMap("standby_count_wanted", 1), policy},
		{"policy-standby-count-null", false, replacementDecodeMap("standby_count_wanted", nil), policy},
		{"policy-flags-null", false, replacementDecodeMap("flags_state", nil), policy},
		{"policy-not-joinable", false, replacementDecodeFlag("joinable", false), policy},
		{"policy-joinable-null", false, replacementDecodeFlag("joinable", nil), policy},
		{"policy-standby-replay", false, replacementDecodeFlag("allow_standby_replay", true), policy},
		{"policy-standby-replay-null", false, replacementDecodeFlag("allow_standby_replay", nil), policy},
		{"policy-not-protected", false, replacementDecodeFlag("refuse_standby_for_another_fs", false), policy},
		{"policy-protected-null", false, replacementDecodeFlag("refuse_standby_for_another_fs", nil), policy},
		{"policy-in-empty", false, replacementDecodeMap("in", []int{}), policy},
		{"policy-in-other-rank", false, replacementDecodeMap("in", []int{1}), policy},
		{"policy-in-null", false, replacementDecodeMap("in", nil), policy},
		{"policy-failed-null", false, replacementDecodeMap("failed", nil), policy},
		{"policy-damaged-null", false, replacementDecodeMap("damaged", nil), policy},
		{"policy-stopped-null", false, replacementDecodeMap("stopped", nil), policy},
		{"policy-damaged-rank", false, replacementDecodeMap("damaged", []int{0}), policy},
		{"policy-stopped-rank", false, replacementDecodeMap("stopped", []int{0}), policy},

		{"no-worker-failed-empty", false, replacementDecodeMap("failed", []int{}), noWorker},
		{"no-worker-failed-other-rank", false, replacementDecodeMap("failed", []int{1}), noWorker},
		{"no-worker-failed-extra-rank", false, replacementDecodeMap("failed", []int{0, 1}), noWorker},
		{"no-worker-registered-new-name", false, replacementDecodeWorker("up:active", nil), noWorker},

		{"worker-failed-extra-rank", true, replacementDecodeMap("failed", []int{0, 1}), exact},
		{"worker-failed-with-member", true, func(n *replacementMDSControl, m map[string]any) {
			replacementDecodeWorker("up:active", nil)(n, m)
			m["failed"] = []int{0}
		}, exact},
		{"worker-two-members", true, func(n *replacementMDSControl, m map[string]any) {
			replacementDecodeWorker("up:active", nil)(n, m)
			m["info"].(map[string]any)["gid_34"] = coldMDSTestRow("target-2", "up:standby-replay", 34, 0, 7)
		}, exact},
		{"worker-wrong-name", true, replacementDecodeWorker("up:active", func(r map[string]any) { r["name"] = "target-2" }), identity},
		{"worker-wrong-join", true, replacementDecodeWorker("up:active", func(r map[string]any) { r["join_fscid"] = 8 }), identity},
		{"worker-laggy", true, replacementDecodeWorker("up:active", func(r map[string]any) { r["laggy_since"] = "2026-10-08T01:00:00Z" }), identity},
		{"worker-laggy-null", true, replacementDecodeWorker("up:active", func(r map[string]any) { r["laggy_since"] = nil }), identity},
		{"worker-state-oneshot-replay", true, replacementDecodeWorker("up:oneshot-replay", nil), contradict},
		{"worker-state-creating", true, replacementDecodeWorker("up:creating", nil), identity},
		{"worker-state-starting", true, replacementDecodeWorker("up:starting", nil), identity},
		{"worker-state-stopping", true, replacementDecodeWorker("up:stopping", nil), identity},
		{"worker-state-standby", true, replacementDecodeWorker("up:standby", nil), rankMember},
		{"worker-state-boot", true, replacementDecodeWorker("up:boot", nil), rankMember},
		{"worker-state-standby-replay", true, replacementDecodeWorker("up:standby-replay", nil), upMap},

		{"filesystem-absent", false, func(n *replacementMDSControl, _ map[string]any) { n.native["filesystems"] = []any{} }, absent},
		{"only-a-sibling-present", false, func(n *replacementMDSControl, _ map[string]any) {
			n.native["filesystems"] = []any{map[string]any{"id": 8, "mdsmap": coldMDSTestMap("sibling", 5, 6)}}
		}, absent},

		{"global-standby-assigned-rank", false, replacementDecodeStandby(func(r map[string]any) { r["rank"] = 0 }), standbyBad},
		{"global-standby-state-active", false, replacementDecodeStandby(func(r map[string]any) { r["state"] = "up:active" }), standbyBad},
		{"global-standby-state-standby-replay", false, replacementDecodeStandby(func(r map[string]any) { r["state"] = "up:standby-replay" }), standbyBad},
		{"reserved-standby-without-worker", false, replacementDecodeStandby(func(r map[string]any) { r["name"], r["join_fscid"] = "target-1", 7 }), reservedSb},
		{"reserved-standby-foreign-join", true, replacementDecodeStandby(func(r map[string]any) { r["name"], r["join_fscid"] = "target-1", 8 }), reservedSb},
		{"reserved-standby-laggy", true, replacementDecodeStandby(func(r map[string]any) {
			r["name"], r["join_fscid"], r["laggy_since"] = "target-1", 7, "2026-10-08T01:00:00Z"
		}), reservedSb},
		{"foreign-standby-unqualified", false, replacementDecodeStandby(func(r map[string]any) { r["join_fscid"] = -1 }), foreignSb},
		{"foreign-standby-for-target", false, replacementDecodeStandby(func(r map[string]any) { r["join_fscid"] = 7 }), foreignSb},
		{"foreign-standby-for-unknown", false, replacementDecodeStandby(func(r map[string]any) { r["join_fscid"] = 99 }), foreignSb},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scope, ready, err := replacementDecode(t, tc.worker, tc.edit)
			if err == nil || err.Error() != tc.want || scope != nil || ready {
				t.Fatalf("decoder guard changed: want %q, got %+v %v %v", tc.want, scope, ready, err)
			}
		})
	}
}

func TestMDSReplacementDecodeAcceptsTheExactStates(t *testing.T) {
	sibling := func(info, up map[string]any) replacementDecodeEdit { return replacementDecodeSibling(info, up) }
	cases := []struct {
		name   string
		worker bool
		edit   replacementDecodeEdit
		ready  bool
	}{
		{"no-worker-failed-rank", false, nil, false},
		{"worker-not-yet-registered", true, nil, false},
		{"worker-replay", true, replacementDecodeWorker("up:replay", nil), false},
		{"worker-resolve", true, replacementDecodeWorker("up:resolve", nil), false},
		{"worker-reconnect", true, replacementDecodeWorker("up:reconnect", nil), false},
		{"worker-rejoin", true, replacementDecodeWorker("up:rejoin", nil), false},
		{"worker-clientreplay", true, replacementDecodeWorker("up:clientreplay", nil), false},
		{"worker-active", true, replacementDecodeWorker("up:active", nil), true},
		{"worker-standby-for-owned-filesystem", true, replacementDecodeStandby(func(r map[string]any) { r["name"], r["join_fscid"] = "target-1", 7 }), false},
		{"foreign-standby-for-sibling", false, func(n *replacementMDSControl, m map[string]any) {
			replacementDecodeSibling(map[string]any{}, map[string]any{})(n, m)
			replacementDecodeStandby(func(map[string]any) {})(n, m)
		}, false},
		{"sibling-active-member", false, sibling(map[string]any{"gid_44": coldMDSTestRow("sibling-0", "up:active", 44, 0, 8)}, map[string]any{"mds_0": uint64(44)}), false},
		{"sibling-standby-replay-member", false, sibling(map[string]any{"gid_44": coldMDSTestRow("sibling-0", "up:standby-replay", 44, 0, 8)}, map[string]any{}), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scope, ready, err := replacementDecode(t, tc.worker, tc.edit)
			if err != nil || ready != tc.ready || scope == nil || scope.active != 1 || scope.standby != 0 || !scope.joinable || scope.replay || !scope.protected || !slices.Equal(scope.dataPools, []int64{1}) || !slices.Equal(scope.poolNames, []string{"target-data"}) {
				t.Fatalf("accepted state changed: %+v ready=%v %v", scope, ready, err)
			}
		})
	}
}

// TestMDSReplacementDecodeReturnsTheScopeWithAContextError pins the last line
// of the reader: a context that ends after the map was read and accepted is
// reported next to the decoded scope, not instead of a decode refusal.
func TestMDSReplacementDecodeReturnsTheScopeWithAContextError(t *testing.T) {
	r := newReplacementMDSFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dumps := 0
	r.n.hook = func(call string) {
		if call == "fs dump --format json" {
			if dumps++; dumps == 2 {
				cancel()
			}
		}
	}
	scope, ready, err := r.fs.fleet().readMDSReplacementScope(ctx, r.old.identity, &cephFSMDSReplacement{name: "target-1"})
	if scope == nil || ready || err == nil || err.Error() != "context canceled" {
		t.Fatalf("scope and context error changed: %+v ready=%v %v", scope, ready, err)
	}
}

// TestMDSReplacementDecodeNativeReadFailuresKeepTheirContext pins the two
// native queries of the reader and their failure text: the pool read fails
// first, the map read second, and neither reaches the decoder.
func TestMDSReplacementDecodeNativeReadFailuresKeepTheirContext(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		failOn     int
	}{
		{"pool-read", "read original MDS replacement pools", 1},
		{"map-read", "read original MDS replacement map", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newReplacementMDSFixture(t)
			dumps := 0
			r.n.hook = func(call string) {
				if call == "fs dump --format json" {
					if dumps++; dumps == tc.failOn {
						r.n.fail = call
					}
				}
			}
			scope, ready, err := r.fs.fleet().readMDSReplacementScope(t.Context(), r.old.identity, &cephFSMDSReplacement{name: "target-1"})
			if err == nil || err.Error() != tc.want || scope != nil || ready {
				t.Fatalf("native read failure changed: want %q, got %+v %v %v", tc.want, scope, ready, err)
			}
		})
	}
}

// TestMDSReplacementDecoderRefusesInvalidPoolIdentity pins the two guards on the
// native pool identity that the reader fixture cannot reach, because the
// identity is read from the same document: it calls the decoder directly.
func TestMDSReplacementDecoderRefusesInvalidPoolIdentity(t *testing.T) {
	r := newReplacementMDSFixture(t)
	pools, err := r.fs.id.readNativePools(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	data := []byte(coldMDSTestJSON(r.n.native))
	want := func(dataPools []int64) replacementMDSMapExpectation {
		clone := *pools
		clone.dataPools = dataPools
		return replacementMDSMapExpectation{oldName: "target-0", filesystem: "target", reservedName: "target-1", native: r.old.identity.native, pools: &clone}
	}
	// The document attaches pool 1; the identity must agree for the policy
	// guard to pass before the pool guards run.
	if state, err := decodeMDSReplacementMap(data, want([]int64{1})); err != nil || state == nil || state.ready {
		t.Fatalf("baseline decode changed: %+v %v", state, err)
	}
	for _, tc := range []struct {
		name      string
		dataPools []int64
		want      string
	}{
		{"identity-differs-from-document", []int64{2}, "original MDS replacement filesystem policy changed"},
	} {
		if _, err := decodeMDSReplacementMap(data, want(tc.dataPools)); err == nil || err.Error() != tc.want {
			t.Fatalf("%s: %v", tc.name, err)
		}
	}
	// Duplicated and negative attachments need the document to attach them too.
	for name, attached := range map[string][]int64{"duplicate": {1, 1}, "negative": {-1}} {
		m := r.n.target()
		m["data_pools"] = attached
		if _, err := decodeMDSReplacementMap([]byte(coldMDSTestJSON(r.n.native)), want(attached)); err == nil || err.Error() != "MDS replacement data attachments are invalid or duplicated" {
			t.Fatalf("%s attachment: %v", name, err)
		}
	}
	m := r.n.target()
	m["data_pools"] = []int64{9}
	if _, err := decodeMDSReplacementMap([]byte(coldMDSTestJSON(r.n.native)), want([]int64{9})); err == nil || err.Error() != "MDS replacement attached pool is unavailable" {
		t.Fatalf("absent attached pool: %v", err)
	}
}
