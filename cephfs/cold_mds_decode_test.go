package cephfs

import (
	"encoding/json"
	"testing"
)

// coldMDSDecode runs the real cold-start decoder over the constructed
// fixture's native map after edit changed a copy of it.
func coldMDSDecode(t *testing.T, edit func(native map[string]any, m map[string]any)) error {
	t.Helper()
	fs, n := coldMDSTestConstruct(t, false)
	var native map[string]any
	if err := json.Unmarshal([]byte(coldMDSTestJSON(n.native)), &native); err != nil {
		t.Fatal(err)
	}
	if edit != nil {
		edit(native, native["filesystems"].([]any)[0].(map[string]any)["mdsmap"].(map[string]any))
	}
	return decodeColdMDSMap([]byte(coldMDSTestJSON(native)), coldMDSMapExpectation{filesystem: fs.id.config.Name, reservedName: "cold-target-0", native: fs.mds.coldMDS.identity, dataPools: fs.mds.coldMDS.dataPools})
}

func TestColdMDSDecodeAcceptsTheSettledBaseline(t *testing.T) {
	if err := coldMDSDecode(t, nil); err != nil {
		t.Fatalf("baseline cold map refused: %v", err)
	}
}

// TestColdMDSDecodeKeepsEveryGuardAndItsClassification pins, for the cold-start
// native-map decoder, the exact refusal text of each guard group. It
// characterizes the decoder as it is, including that up:oneshot-replay is
// refused. It is a Docker-free control, not native evidence.
func TestColdMDSDecodeKeepsEveryGuardAndItsClassification(t *testing.T) {
	const (
		decode     = "decode cold MDS map"
		entries    = "cold filesystem map entries are incomplete or duplicated"
		foreign    = "foreign MDS map is incomplete or conflicts with initial identity"
		policy     = "original cold filesystem policy or identity changed"
		workers    = "cold filesystem already has MDS rank or worker state"
		absent     = "original cold filesystem map is absent"
		standbyBad = "native standby map is incomplete or conflicting"
		foreignSb  = "foreign standby can join the cold filesystem"
	)
	type edit = func(native map[string]any, m map[string]any)
	field := func(name string, value any) edit { return func(_, m map[string]any) { m[name] = value } }
	missing := func(name string) edit { return func(_, m map[string]any) { delete(m, name) } }
	flag := func(name string, value any) edit {
		return func(_, m map[string]any) { m["flags_state"].(map[string]any)[name] = value }
	}
	add := func(id any, m map[string]any) edit {
		return func(native, _ map[string]any) {
			native["filesystems"] = append(native["filesystems"].([]any), map[string]any{"id": id, "mdsmap": m})
		}
	}
	sibling := func(mod func(m map[string]any)) edit {
		m := coldMDSTestMap("sibling", 5, 6)
		mod(m)
		return add(9, m)
	}
	siblingRow := func(name, state string, gid uint64, rank int) edit {
		return sibling(func(m map[string]any) {
			m["info"] = map[string]any{"gid_44": coldMDSTestRow(name, state, gid, rank, 9)}
		})
	}
	standby := func(row map[string]any) edit {
		return func(native, _ map[string]any) { native["standbys"] = []any{row} }
	}
	withSibling := func(e edit) edit {
		return func(native, m map[string]any) {
			add(9, coldMDSTestMap("sibling", 5, 6))(native, m)
			e(native, m)
		}
	}
	raw := func(f func(native map[string]any, m map[string]any)) edit { return f }
	cases := []struct {
		name string
		edit edit
		want string
	}{
		{"standbys-null", raw(func(native, _ map[string]any) { native["standbys"] = nil }), decode},
		{"standbys-missing", raw(func(native, _ map[string]any) { delete(native, "standbys") }), decode},
		{"filesystems-null", raw(func(native, _ map[string]any) { native["filesystems"] = nil }), decode},
		{"filesystems-missing", raw(func(native, _ map[string]any) { delete(native, "filesystems") }), decode},

		{"filesystem-negative-id", add(-1, coldMDSTestMap("sibling", 5, 6)), entries},
		{"filesystem-id-null", add(nil, coldMDSTestMap("sibling", 5, 6)), entries},
		{"filesystem-duplicate-id", add(coldMDSFixtureTargetID(), coldMDSTestMap("sibling", 5, 6)), entries},
		{"filesystem-mdsmap-null", add(9, nil), entries},
		{"filesystem-name-empty", add(9, coldMDSTestMap("", 5, 6)), entries},
		{"filesystem-duplicate-name", add(9, coldMDSTestMap("cold-target", 5, 6)), entries},
		{"filesystem-info-null", sibling(func(m map[string]any) { m["info"] = nil }), entries},

		{"foreign-row-reserved-name", siblingRow("cold-target-0", "up:active", 44, 0), foreign},
		{"foreign-row-name-invalid", siblingRow("bad name", "up:active", 44, 0), foreign},
		{"foreign-row-gid-zero", siblingRow("sibling-0", "up:active", 0, 0), foreign},
		{"foreign-row-negative-rank", siblingRow("sibling-0", "up:active", 44, -1), foreign},
		{"foreign-row-state-oneshot-replay", siblingRow("sibling-0", "up:oneshot-replay", 44, 0), foreign},
		{"foreign-row-state-down", siblingRow("sibling-0", "down:stopped", 44, 0), foreign},
		{"foreign-row-state-unknown-name", siblingRow("sibling-0", "???", 44, 0), foreign},
		{"foreign-row-state-empty", siblingRow("sibling-0", "", 44, 0), foreign},
		{"foreign-row-state-boot", siblingRow("sibling-0", "up:boot", 44, 0), foreign},
		{"foreign-row-state-standby", siblingRow("sibling-0", "up:standby", 44, 0), foreign},
		{"foreign-row-join-below-minus-one", sibling(func(m map[string]any) {
			m["info"] = map[string]any{"gid_44": coldMDSTestRow("sibling-0", "up:active", 44, 0, -2)}
		}), foreign},
		{"foreign-row-duplicate-name", sibling(func(m map[string]any) {
			m["info"] = map[string]any{"gid_44": coldMDSTestRow("sibling-0", "up:active", 44, 0, 9), "gid_45": coldMDSTestRow("sibling-0", "up:active", 45, 1, 9)}
		}), foreign},
		{"foreign-row-duplicate-gid", sibling(func(m map[string]any) {
			m["info"] = map[string]any{"gid_44": coldMDSTestRow("sibling-0", "up:active", 44, 0, 9), "gid_45": coldMDSTestRow("sibling-1", "up:active", 44, 1, 9)}
		}), foreign},

		{"policy-filesystem-id", raw(func(native, _ map[string]any) {
			native["filesystems"].([]any)[0].(map[string]any)["id"] = 99
		}), policy},
		{"policy-metadata-pool", field("metadata_pool", 99), policy},
		{"policy-metadata-pool-null", field("metadata_pool", nil), policy},
		{"policy-data-pools", field("data_pools", []int64{99}), policy},
		{"policy-data-pools-null", field("data_pools", nil), policy},
		{"policy-max-mds", field("max_mds", 2), policy},
		{"policy-max-mds-missing", missing("max_mds"), policy},
		{"policy-standby-count", field("standby_count_wanted", 1), policy},
		{"policy-standby-count-missing", missing("standby_count_wanted"), policy},
		{"policy-flags-null", field("flags_state", nil), policy},
		{"policy-not-joinable", flag("joinable", false), policy},
		{"policy-joinable-null", flag("joinable", nil), policy},
		{"policy-standby-replay", flag("allow_standby_replay", true), policy},
		{"policy-standby-replay-null", flag("allow_standby_replay", nil), policy},
		{"policy-not-protected", flag("refuse_standby_for_another_fs", false), policy},
		{"policy-protected-null", flag("refuse_standby_for_another_fs", nil), policy},

		{"worker-up-null", field("up", nil), workers},
		{"worker-in-null", field("in", nil), workers},
		{"worker-failed-null", field("failed", nil), workers},
		{"worker-damaged-null", field("damaged", nil), workers},
		{"worker-stopped-null", field("stopped", nil), workers},
		{"worker-info-row", field("info", map[string]any{"gid_5": coldMDSTestRow("cold-target-1", "up:active", 5, 0, 7)}), workers},
		{"worker-up-entry", field("up", map[string]any{"mds_0": uint64(5)}), workers},
		{"worker-in-rank", field("in", []int{0}), workers},
		{"worker-failed-rank", field("failed", []int{0}), workers},
		{"worker-damaged-rank", field("damaged", []int{0}), workers},
		{"worker-stopped-rank", field("stopped", []int{0}), workers},

		{"filesystem-absent", raw(func(native, _ map[string]any) { native["filesystems"] = []any{} }), absent},
		{"only-a-sibling-present", raw(func(native, _ map[string]any) {
			native["filesystems"] = []any{map[string]any{"id": 9, "mdsmap": coldMDSTestMap("sibling", 5, 6)}}
		}), absent},

		{"standby-name-reserved", standby(coldMDSTestRow("cold-target-0", "up:standby", 5, -1, 9)), standbyBad},
		{"standby-state-oneshot-replay", withSibling(standby(coldMDSTestRow("other", "up:oneshot-replay", 5, -1, 9))), standbyBad},
		{"standby-state-down", withSibling(standby(coldMDSTestRow("other", "down:stopped", 5, -1, 9))), standbyBad},
		{"standby-state-empty", withSibling(standby(coldMDSTestRow("other", "", 5, -1, 9))), standbyBad},
		{"standby-state-active", withSibling(standby(coldMDSTestRow("other", "up:active", 5, -1, 9))), standbyBad},
		{"standby-assigned-rank", withSibling(standby(coldMDSTestRow("other", "up:standby", 5, 0, 9))), standbyBad},
		{"standby-gid-zero", withSibling(standby(coldMDSTestRow("other", "up:standby", 0, -1, 9))), standbyBad},
		{"standby-join-below-minus-one", withSibling(standby(coldMDSTestRow("other", "up:standby", 5, -1, -2))), standbyBad},
		{"standby-name-invalid", withSibling(standby(coldMDSTestRow("bad name", "up:standby", 5, -1, 9))), standbyBad},
		{"standby-duplicate-name", withSibling(func(native, _ map[string]any) {
			native["standbys"] = []any{coldMDSTestRow("other", "up:standby", 5, -1, 9), coldMDSTestRow("other", "up:standby", 6, -1, 9)}
		}), standbyBad},
		{"standby-duplicate-gid", withSibling(func(native, _ map[string]any) {
			native["standbys"] = []any{coldMDSTestRow("other", "up:standby", 5, -1, 9), coldMDSTestRow("another", "up:standby", 5, -1, 9)}
		}), standbyBad},
		{"standby-unqualified", standby(coldMDSTestRow("other", "up:standby", 5, -1, -1)), foreignSb},
		{"standby-for-target", standby(coldMDSTestRow("other", "up:standby", 5, -1, coldMDSFixtureTargetID())), foreignSb},
		{"standby-for-unknown", standby(coldMDSTestRow("other", "up:standby", 5, -1, 99)), foreignSb},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := coldMDSDecode(t, tc.edit); err == nil || err.Error() != tc.want {
				t.Fatalf("decoder guard changed: want %q, got %v", tc.want, err)
			}
		})
	}
}

func TestColdMDSDecodeAcceptsIndependentSiblingsAndTheirStandbys(t *testing.T) {
	err := coldMDSDecode(t, func(native, _ map[string]any) {
		sibling := coldMDSTestMap("sibling", 5, 6)
		sibling["info"] = map[string]any{"gid_44": coldMDSTestRow("sibling-0", "up:active", 44, 0, 9), "gid_45": coldMDSTestRow("sibling-1", "up:standby-replay", 45, 0, 9)}
		native["filesystems"] = append(native["filesystems"].([]any), map[string]any{"id": 9, "mdsmap": sibling})
		native["standbys"] = []any{coldMDSTestRow("sibling-2", "up:standby", 46, -1, 9)}
	})
	if err != nil {
		t.Fatalf("independent sibling refused: %v", err)
	}
}

// coldMDSFixtureTargetID is the native filesystem ID of the constructed cold
// fixture.
func coldMDSFixtureTargetID() int64 {
	return 7
}
