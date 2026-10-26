//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// These are Docker-free controls of the strict parsers that turn Ceph command
// output into the upgrade sequence plan's inputs. The documents are synthetic
// but follow the formatter calls of the pinned Ceph source (see the helpers
// file). They executed no Ceph or Docker, so they say nothing about a native
// transition.

const obsFSID = "0b8a9d3c-5e1f-4c7a-9a52-3f6e1d2c4b7a"

// Golden documents in the indentation of Ceph's json-pretty formatter.
const (
	obsGoldenVersions = `{
    "mon": {
        "ceph version 19.2.5 (0123456789abcdef0123456789abcdef01234567) squid (stable)": 2,
        "ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)": 1
    },
    "mgr": {
        "ceph version 19.2.5 (0123456789abcdef0123456789abcdef01234567) squid (stable)": 2
    },
    "osd": {
        "ceph version 19.2.5 (0123456789abcdef0123456789abcdef01234567) squid (stable)": 3
    },
    "overall": {
        "ceph version 19.2.5 (0123456789abcdef0123456789abcdef01234567) squid (stable)": 7,
        "ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)": 1
    }
}`
	obsGoldenMonDump = `{
    "epoch": 3,
    "auth_epoch": 0,
    "fsid": "0b8a9d3c-5e1f-4c7a-9a52-3f6e1d2c4b7a",
    "modified": "2026-10-26T10:00:00.000000Z",
    "created": "2026-10-26T09:00:00.000000Z",
    "min_mon_release": 19,
    "min_mon_release_name": "squid",
    "election_strategy": 1,
    "stretch_mode": false,
    "tiebreaker_mon": "",
    "mons": [
        {"rank": 0, "name": "a", "priority": 0, "weight": 0},
        {"rank": 1, "name": "b", "priority": 0, "weight": 0},
        {"rank": 2, "name": "c", "priority": 0, "weight": 0}
    ],
    "quorum": [0, 2]
}`
	obsGoldenQuorumStatus = `{
    "election_epoch": 8,
    "quorum": [0, 1, 2],
    "quorum_names": ["a", "b", "c"],
    "quorum_leader_name": "a",
    "quorum_age": 120,
    "features": {"quorum_con": "4540138322906710015", "quorum_mon": ["kraken"]},
    "monmap": {
        "epoch": 3,
        "fsid": "0b8a9d3c-5e1f-4c7a-9a52-3f6e1d2c4b7a",
        "min_mon_release": 19,
        "min_mon_release_name": "squid",
        "mons": [
            {"rank": 0, "name": "a"},
            {"rank": 1, "name": "b"},
            {"rank": 2, "name": "c"}
        ]
    }
}`
	obsGoldenOSDStat   = `{"epoch": 21, "num_osds": 3, "num_up_osds": 3, "osd_up_since": 1790000000, "num_in_osds": 3, "osd_in_since": 1790000000, "num_remapped_pgs": 0}`
	obsGoldenMgrStat   = `{"epoch": 9, "available": true, "active_name": "x", "num_standby": 1}`
	obsGoldenMgrDump   = `{"epoch": 9, "flags": 0, "active_gid": 4100, "active_name": "x", "available": true, "standbys": [{"gid": 4200, "name": "y", "mgr_features": 4540138322906710015, "available_modules": []}], "modules": ["iostat"]}`
	obsGoldenPGStat    = `{"num_pg_by_state": [{"name": "active+clean", "num": 31}, {"name": "active+clean+scrubbing", "num": 1}], "num_pgs": 32, "num_bytes": 0, "total_bytes": 300, "total_avail_bytes": 200, "total_used_bytes": 100, "total_used_raw_bytes": 100}`
	obsGoldenPGStuck   = `[]`
	obsGoldenPGStuckPG = `[{"pgid": "1.1f", "state": "active+undersized+degraded"}]`
)

// obsEdit applies f to a decoded copy of an object document.
func obsEdit(t *testing.T, doc string, f func(map[string]any)) string {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(doc), &m); err != nil {
		t.Fatal(err)
	}
	f(m)
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

// obsRaw is a value that is written as is by obsEdit.
func obsRaw(s string) json.RawMessage { return json.RawMessage(s) }

type obsKey struct {
	// container returns the object that holds the key; nil means the document.
	container func(map[string]any) map[string]any
	key, kind string
}

func obsTop(key, kind string) obsKey { return obsKey{key: key, kind: kind} }

func obsIn(path func(map[string]any) map[string]any, key, kind string) obsKey {
	return obsKey{container: path, key: key, kind: kind}
}

func (k obsKey) at(m map[string]any) map[string]any {
	if k.container == nil {
		return m
	}
	return k.container(m)
}

var obsWrongTypes = map[string][]string{
	"uint":   {`"3"`, `1.5`, `true`, `[]`, `{}`, `-1`, `1e2`},
	"string": {`7`, `true`, `[]`, `{}`},
	"bool":   {`"true"`, `1`, `0`, `[]`, `{}`},
	"array":  {`"x"`, `1`, `true`, `{}`},
	"object": {`"x"`, `1`, `true`, `[]`},
}

// obsRequireKeys proves, for every required key, that the document parses
// when intact and that a missing, null, wrong-typed or duplicated key does
// not, each with its own control.
func obsRequireKeys(t *testing.T, parse func([]byte) error, base string, keys []obsKey) {
	t.Helper()
	if err := parse([]byte(base)); err != nil {
		t.Fatalf("intact document rejected: %v", err)
	}
	for _, k := range keys {
		k := k
		t.Run(k.key, func(t *testing.T) {
			check := func(label, doc string) {
				t.Helper()
				if err := parse([]byte(doc)); err == nil {
					t.Errorf("%s: accepted %s", label, doc)
				}
			}
			check("missing", obsEdit(t, base, func(m map[string]any) { delete(k.at(m), k.key) }))
			check("null", obsEdit(t, base, func(m map[string]any) { k.at(m)[k.key] = nil }))
			for _, wrong := range obsWrongTypes[k.kind] {
				check("wrong type "+wrong, obsEdit(t, base, func(m map[string]any) { k.at(m)[k.key] = obsRaw(wrong) }))
			}
			// Duplicate: repeat the key with its own value in front of the
			// first occurrence of that key text.
			var orig json.RawMessage
			obsEdit(t, base, func(m map[string]any) {
				b, _ := json.Marshal(k.at(m)[k.key])
				orig = b
			})
			compact := obsEdit(t, base, func(map[string]any) {})
			needle := fmt.Sprintf("%q:", k.key)
			if !strings.Contains(compact, needle) {
				t.Fatalf("no %s in the document", needle)
			}
			dup := strings.Replace(compact, needle, needle+string(orig)+","+needle, 1)
			if !json.Valid([]byte(dup)) {
				t.Fatalf("duplicate control is not valid JSON: %s", dup)
			}
			check("duplicate", dup)
		})
	}
}

// obsRejectsShapes runs the shape controls common to every document.
func obsRejectsShapes(t *testing.T, parse func([]byte) error, base string) {
	t.Helper()
	for name, doc := range map[string]string{
		"empty":         "",
		"whitespace":    " \n",
		"not json":      "dumped monmap epoch 3",
		"null":          "null",
		"string":        `"x"`,
		"number":        `3`,
		"truncated":     base[:len(base)/2],
		"trailing data": base + "\n" + base,
		"trailing note": base + "\ndumped monmap epoch 3",
	} {
		if err := parse([]byte(doc)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func obsErr[T any](f func([]byte) (T, error)) func([]byte) error {
	return func(b []byte) error { _, err := f(b); return err }
}

func monAt(i int) func(map[string]any) map[string]any {
	return func(m map[string]any) map[string]any { return m["mons"].([]any)[i].(map[string]any) }
}

func TestUpgradeObservationGoldenDocuments(t *testing.T) {
	inv, err := parseUpgradeVersionsStrict([]byte(obsGoldenVersions))
	if err != nil {
		t.Fatal(err)
	}
	want := upgradeInventory{"mon": {"19.2.5": 2, "20.2.4": 1}, "mgr": {"19.2.5": 2}, "osd": {"19.2.5": 3}}
	if !reflect.DeepEqual(inv, want) {
		t.Fatalf("inventory = %v, want %v", inv, want)
	}

	mon, err := parseUpgradeMonDump([]byte(obsGoldenMonDump))
	if err != nil {
		t.Fatal(err)
	}
	if mon.MinMonRelease != 19 || mon.MinMonReleaseName != "squid" || mon.FSID != obsFSID ||
		!reflect.DeepEqual(mon.names(), []string{"a", "b", "c"}) || !reflect.DeepEqual(mon.QuorumNames, []string{"a", "c"}) {
		t.Fatalf("mon dump = %+v", mon)
	}

	q, err := parseUpgradeQuorumStatus([]byte(obsGoldenQuorumStatus))
	if err != nil || !reflect.DeepEqual(q.Names, []string{"a", "b", "c"}) || q.Leader != "a" {
		t.Fatalf("quorum status = %+v, %v", q, err)
	}

	osd, err := parseUpgradeOSDStat([]byte(obsGoldenOSDStat))
	if err != nil || osd.upIn() == nil || *osd.upIn() != 3 {
		t.Fatalf("osd stat = %+v, %v", osd, err)
	}

	stat, err := parseUpgradeMgrStat([]byte(obsGoldenMgrStat))
	if err != nil {
		t.Fatal(err)
	}
	dump, err := parseUpgradeMgrDump([]byte(obsGoldenMgrDump))
	if err != nil {
		t.Fatal(err)
	}
	view, err := upgradeMgrView(stat, dump)
	if err != nil || view.ActiveName != "x" || !reflect.DeepEqual(view.Standbys, []string{"y"}) {
		t.Fatalf("mgr view = %+v, %v", view, err)
	}

	pg, err := parseUpgradePGStat([]byte(obsGoldenPGStat))
	if err != nil || pg.NumPGs != 32 {
		t.Fatalf("pg stat = %+v, %v", pg, err)
	}
	stuck, err := parseUpgradePGStuck([]byte(obsGoldenPGStuck))
	if err != nil || len(stuck) != 0 || stuck == nil {
		t.Fatalf("stuck = %#v, %v", stuck, err)
	}
	if got := upgradePGsSettled(pg, stuck); got != upgradeYes {
		t.Fatalf("settled = %d, want yes", got)
	}
}

func TestUpgradeObservationVersionsControls(t *testing.T) {
	parse := obsErr(parseUpgradeVersionsStrict)
	obsRequireKeys(t, parse, obsGoldenVersions, []obsKey{obsTop("mon", "object"), obsTop("overall", "object")})
	obsRejectsShapes(t, parse, obsGoldenVersions)
	// The same version description repeated in one role keeps only the last
	// count in encoding/json, which would pass the totals check.
	dup := strings.Replace(obsGoldenVersions,
		`"ceph version 19.2.5 (0123456789abcdef0123456789abcdef01234567) squid (stable)": 3`,
		`"ceph version 19.2.5 (0123456789abcdef0123456789abcdef01234567) squid (stable)": 3,
        "ceph version 19.2.5 (0123456789abcdef0123456789abcdef01234567) squid (stable)": 3`, 1)
	if dup == obsGoldenVersions {
		t.Fatal("duplicate control did not change the document")
	}
	if _, err := parseUpgradeVersionsStrict([]byte(dup)); err == nil || !strings.Contains(err.Error(), "duplicate key") {
		t.Fatalf("duplicate description: %v", err)
	}
	// The undecorated decoder, which cannot see duplicates, accepts it.
	if _, err := parseUpgradeInventory([]byte(dup)); err != nil {
		t.Fatalf("control: the plain decoder should accept the duplicate: %v", err)
	}
	for name, doc := range map[string]string{
		"role is an array":     obsEdit(t, obsGoldenVersions, func(m map[string]any) { m["osd"] = []any{} }),
		"empty role":           obsEdit(t, obsGoldenVersions, func(m map[string]any) { m["osd"] = map[string]any{} }),
		"overall disagrees":    strings.Replace(obsGoldenVersions, `squid (stable)": 7`, `squid (stable)": 8`, 1),
		"count is a string":    strings.Replace(obsGoldenVersions, `squid (stable)": 3`, `squid (stable)": "3"`, 1),
		"count is zero":        strings.Replace(obsGoldenVersions, `squid (stable)": 3`, `squid (stable)": 0`, 1),
		"description unusable": strings.ReplaceAll(obsGoldenVersions, "ceph version 19.2.5", "version 19.2.5"),
	} {
		if _, err := parseUpgradeVersionsStrict([]byte(doc)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestUpgradeObservationMonDumpControls(t *testing.T) {
	parse := obsErr(parseUpgradeMonDump)
	obsRequireKeys(t, parse, obsGoldenMonDump, []obsKey{
		obsTop("epoch", "uint"), obsTop("fsid", "string"), obsTop("min_mon_release", "uint"),
		obsTop("min_mon_release_name", "string"), obsTop("mons", "array"), obsTop("quorum", "array"),
		obsIn(monAt(1), "rank", "uint"), obsIn(monAt(1), "name", "string"),
	})
	obsRejectsShapes(t, parse, obsGoldenMonDump)

	edit := func(f func(map[string]any)) string { return obsEdit(t, obsGoldenMonDump, f) }
	for name, doc := range map[string]string{
		"release is the pre-integer name": edit(func(m map[string]any) { m["min_mon_release"] = "squid" }),
		"name does not match the number":  edit(func(m map[string]any) { m["min_mon_release"] = 20 }),
		"epoch zero":                      edit(func(m map[string]any) { m["epoch"] = 0 }),
		"fsid is not a UUID":              edit(func(m map[string]any) { m["fsid"] = "fsid-1" }),
		"no monitors":                     edit(func(m map[string]any) { m["mons"] = []any{} }),
		"quorum rank without a monitor":   edit(func(m map[string]any) { m["quorum"] = []any{0, 7} }),
		"quorum rank twice":               edit(func(m map[string]any) { m["quorum"] = []any{1, 1} }),
		"quorum rank is a name":           edit(func(m map[string]any) { m["quorum"] = []any{"a"} }),
		"quorum rank is negative":         edit(func(m map[string]any) { m["quorum"] = []any{-1} }),
		"monitor name twice":              edit(func(m map[string]any) { monAt(2)(m)["name"] = "a" }),
		"monitor name empty":              edit(func(m map[string]any) { monAt(2)(m)["name"] = "" }),
		"rank gap":                        edit(func(m map[string]any) { monAt(2)(m)["rank"] = 5 }),
		"monitor is not an object":        edit(func(m map[string]any) { m["mons"].([]any)[2] = "c" }),
	} {
		if err := parse([]byte(doc)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// Paired control: an empty quorum is a present value, not a missing one.
	d, err := parseUpgradeMonDump([]byte(edit(func(m map[string]any) { m["quorum"] = []any{} })))
	if err != nil || len(d.QuorumNames) != 0 {
		t.Fatalf("empty quorum = %+v, %v", d, err)
	}
	if err := parse([]byte(edit(func(m map[string]any) { delete(m, "quorum") }))); err == nil {
		t.Fatal("missing quorum accepted")
	}
	// min_mon_release 20 with the matching name is the destination.
	d, err = parseUpgradeMonDump([]byte(edit(func(m map[string]any) {
		m["min_mon_release"], m["min_mon_release_name"] = 20, "tentacle"
	})))
	if err != nil || d.MinMonRelease != 20 {
		t.Fatalf("tentacle = %+v, %v", d, err)
	}
}

func TestUpgradeObservationQuorumStatusControls(t *testing.T) {
	parse := obsErr(parseUpgradeQuorumStatus)
	monmap := func(m map[string]any) map[string]any { return m["monmap"].(map[string]any) }
	obsRequireKeys(t, parse, obsGoldenQuorumStatus, []obsKey{
		obsTop("election_epoch", "uint"), obsTop("quorum", "array"), obsTop("quorum_names", "array"),
		obsTop("quorum_leader_name", "string"), obsTop("quorum_age", "uint"), obsTop("monmap", "object"),
		obsIn(monmap, "fsid", "string"), obsIn(monmap, "min_mon_release", "uint"), obsIn(monmap, "mons", "array"),
	})
	obsRejectsShapes(t, parse, obsGoldenQuorumStatus)

	edit := func(f func(map[string]any)) string { return obsEdit(t, obsGoldenQuorumStatus, f) }
	for name, doc := range map[string]string{
		"names disagree with ranks":    edit(func(m map[string]any) { m["quorum_names"] = []any{"a", "b", "z"} }),
		"fewer names than ranks":       edit(func(m map[string]any) { m["quorum_names"] = []any{"a", "b"} }),
		"name twice":                   edit(func(m map[string]any) { m["quorum_names"] = []any{"a", "a", "b"} }),
		"name is a number":             edit(func(m map[string]any) { m["quorum_names"] = []any{"a", "b", 2} }),
		"rank outside the map":         edit(func(m map[string]any) { m["quorum"] = []any{0, 1, 9} }),
		"leader outside the quorum":    edit(func(m map[string]any) { m["quorum_leader_name"] = "z" }),
		"empty leader with quorum":     edit(func(m map[string]any) { m["quorum_leader_name"] = "" }),
		"leader without a quorum":      edit(func(m map[string]any) { m["quorum"], m["quorum_names"] = []any{}, []any{} }),
		"quorum_age without a quorum":  edit(func(m map[string]any) { m["quorum"], m["quorum_names"], m["quorum_leader_name"] = []any{}, []any{}, "" }),
		"embedded monmap epoch zero":   edit(func(m map[string]any) { monmap(m)["epoch"] = 0 }),
		"embedded monmap without mons": edit(func(m map[string]any) { monmap(m)["mons"] = []any{} }),
	} {
		if err := parse([]byte(doc)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// A null or missing leader name must be rejected where "" is legitimate.
	noQuorum := edit(func(m map[string]any) {
		m["quorum"], m["quorum_names"], m["quorum_leader_name"] = []any{}, []any{}, ""
		delete(m, "quorum_age")
	})
	obsRequireKeys(t, parse, noQuorum, []obsKey{obsTop("quorum_leader_name", "string")})
	// Paired control: no quorum is valid when leader and age are absent too.
	none := edit(func(m map[string]any) {
		m["quorum"], m["quorum_names"], m["quorum_leader_name"] = []any{}, []any{}, ""
		delete(m, "quorum_age")
	})
	q, err := parseUpgradeQuorumStatus([]byte(none))
	if err != nil || len(q.Names) != 0 {
		t.Fatalf("no quorum = %+v, %v", q, err)
	}
}

func TestUpgradeObservationOSDStatControls(t *testing.T) {
	parse := obsErr(parseUpgradeOSDStat)
	var keys []obsKey
	for _, k := range []string{"epoch", "num_osds", "num_up_osds", "osd_up_since", "num_in_osds", "osd_in_since", "num_remapped_pgs"} {
		keys = append(keys, obsTop(k, "uint"))
	}
	obsRequireKeys(t, parse, obsGoldenOSDStat, keys)
	obsRejectsShapes(t, parse, obsGoldenOSDStat)

	edit := func(f func(map[string]any)) string { return obsEdit(t, obsGoldenOSDStat, f) }
	for name, doc := range map[string]string{
		"more up than OSDs": edit(func(m map[string]any) { m["num_up_osds"] = 4 }),
		"more in than OSDs": edit(func(m map[string]any) { m["num_in_osds"] = 4 }),
		"epoch zero":        edit(func(m map[string]any) { m["epoch"] = 0 }),
	} {
		if err := parse([]byte(doc)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// The up-and-in count is known only when every OSD is both.
	for name, tc := range map[string]struct {
		total, up, in int
		want          *int
	}{
		"all up and in":      {3, 3, 3, intFact(3)},
		"one down":           {3, 2, 3, nil},
		"one out":            {3, 3, 2, nil},
		"different one each": {3, 2, 2, nil},
		"no OSDs":            {0, 0, 0, nil},
	} {
		s, err := parseUpgradeOSDStat([]byte(edit(func(m map[string]any) {
			m["num_osds"], m["num_up_osds"], m["num_in_osds"] = tc.total, tc.up, tc.in
		})))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got := s.upIn()
		if (got == nil) != (tc.want == nil) || (got != nil && *got != *tc.want) {
			t.Errorf("%s: upIn = %v, want %v", name, got, tc.want)
		}
	}
}

func TestUpgradeObservationMgrControls(t *testing.T) {
	obsRequireKeys(t, obsErr(parseUpgradeMgrStat), obsGoldenMgrStat, []obsKey{
		obsTop("epoch", "uint"), obsTop("available", "bool"), obsTop("active_name", "string"), obsTop("num_standby", "uint"),
	})
	obsRejectsShapes(t, obsErr(parseUpgradeMgrStat), obsGoldenMgrStat)
	// With no available manager the name is legitimately empty, so a null or
	// missing name must be rejected on its own merit, not through that check.
	noActive := `{"epoch": 9, "available": false, "active_name": "", "num_standby": 1}`
	obsRequireKeys(t, obsErr(parseUpgradeMgrStat), noActive, []obsKey{obsTop("active_name", "string")})
	standby := func(m map[string]any) map[string]any { return m["standbys"].([]any)[0].(map[string]any) }
	obsRequireKeys(t, obsErr(parseUpgradeMgrDump), obsGoldenMgrDump, []obsKey{
		obsTop("epoch", "uint"), obsTop("available", "bool"), obsTop("active_name", "string"), obsTop("standbys", "array"),
		obsIn(standby, "name", "string"), obsIn(standby, "gid", "uint"),
	})
	obsRejectsShapes(t, obsErr(parseUpgradeMgrDump), obsGoldenMgrDump)

	if _, err := parseUpgradeMgrStat([]byte(obsEdit(t, obsGoldenMgrStat, func(m map[string]any) { m["active_name"] = "" }))); err == nil {
		t.Error("available manager without a name accepted")
	}
	dumpEdit := func(f func(map[string]any)) string { return obsEdit(t, obsGoldenMgrDump, f) }
	for name, doc := range map[string]string{
		"standby is the active": dumpEdit(func(m map[string]any) { standby(m)["name"] = "x" }),
		"standby name empty":    dumpEdit(func(m map[string]any) { standby(m)["name"] = "" }),
		"standby twice": dumpEdit(func(m map[string]any) {
			m["standbys"] = []any{standby(m), map[string]any{"name": "y", "gid": 4300}}
		}),
		"gid twice": dumpEdit(func(m map[string]any) {
			m["standbys"] = []any{standby(m), map[string]any{"name": "z", "gid": 4200}}
		}),
		"available without a name": dumpEdit(func(m map[string]any) { m["active_name"] = "" }),
	} {
		if _, err := parseUpgradeMgrDump([]byte(doc)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// Paired control: no standbys is a present empty list.
	d, err := parseUpgradeMgrDump([]byte(dumpEdit(func(m map[string]any) { m["standbys"] = []any{} })))
	if err != nil || len(d.Standbys) != 0 {
		t.Fatalf("no standbys = %+v, %v", d, err)
	}

	// stat and dump must describe the same map.
	stat, _ := parseUpgradeMgrStat([]byte(obsGoldenMgrStat))
	dump, _ := parseUpgradeMgrDump([]byte(obsGoldenMgrDump))
	for name, f := range map[string]func(*upgradeMgrStat, *upgradeMgrDump){
		"epochs differ":       func(s *upgradeMgrStat, _ *upgradeMgrDump) { s.Epoch++ },
		"active differs":      func(s *upgradeMgrStat, _ *upgradeMgrDump) { s.ActiveName = "y" },
		"available differs":   func(s *upgradeMgrStat, _ *upgradeMgrDump) { s.Available = false },
		"standby count wrong": func(s *upgradeMgrStat, _ *upgradeMgrDump) { s.NumStandby = 2 },
	} {
		s, d := stat, dump
		f(&s, &d)
		if _, err := upgradeMgrView(s, d); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestUpgradeObservationPGControls(t *testing.T) {
	stateAt := func(m map[string]any) map[string]any { return m["num_pg_by_state"].([]any)[0].(map[string]any) }
	obsRequireKeys(t, obsErr(parseUpgradePGStat), obsGoldenPGStat, []obsKey{
		obsTop("num_pgs", "uint"), obsTop("num_pg_by_state", "array"),
		obsIn(stateAt, "name", "string"), obsIn(stateAt, "num", "uint"),
	})
	obsRejectsShapes(t, obsErr(parseUpgradePGStat), obsGoldenPGStat)

	edit := func(f func(map[string]any)) string { return obsEdit(t, obsGoldenPGStat, f) }
	for name, doc := range map[string]string{
		"states do not add up": edit(func(m map[string]any) { m["num_pgs"] = 33 }),
		"state twice": edit(func(m map[string]any) {
			m["num_pg_by_state"] = []any{stateAt(m), stateAt(m)}
			m["num_pgs"] = 62
		}),
		"state with no PGs": edit(func(m map[string]any) { stateAt(m)["num"] = 0; m["num_pgs"] = 1 }),
		"empty state name":  edit(func(m map[string]any) { stateAt(m)["name"] = "" }),
	} {
		if _, err := parseUpgradePGStat([]byte(doc)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}

	// Judgement table. Only active+clean (optionally scrubbing) is settled.
	for name, tc := range map[string]struct {
		states map[string]int
		want   upgradeTri
	}{
		"clean":           {map[string]int{"active+clean": 32}, upgradeYes},
		"clean and scrub": {map[string]int{"active+clean": 31, "active+clean+scrubbing+deep": 1}, upgradeYes},
		"peering":         {map[string]int{"active+clean": 31, "peering": 1}, upgradeNo},
		"degraded":        {map[string]int{"active+undersized+degraded": 32}, upgradeNo},
		"remapped":        {map[string]int{"active+clean+remapped": 1, "active+clean": 31}, upgradeNo},
		"recovering":      {map[string]int{"active+recovering+clean": 1, "active+clean": 31}, upgradeNo},
		"inactive":        {map[string]int{"clean": 32}, upgradeNo},
		"not clean":       {map[string]int{"active": 32}, upgradeNo},
		"stale":           {map[string]int{"stale+active+clean": 32}, upgradeNo},
		"unknown state":   {map[string]int{"unknown": 32}, upgradeNo},
		"token not known": {map[string]int{"active+clean+newstate": 32}, upgradeNo},
		"no PGs reported": {map[string]int{}, upgradeUnknown},
	} {
		total := 0
		var states []any
		for n, c := range tc.states {
			states = append(states, map[string]any{"name": n, "num": c})
			total += c
		}
		doc := obsEdit(t, obsGoldenPGStat, func(m map[string]any) {
			m["num_pg_by_state"] = states
			if states == nil {
				m["num_pg_by_state"] = []any{}
			}
			m["num_pgs"] = total
		})
		s, err := parseUpgradePGStat([]byte(doc))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := s.settled(); got != tc.want {
			t.Errorf("%s: settled = %d, want %d", name, got, tc.want)
		}
	}

	// dump_stuck.
	stuck, err := parseUpgradePGStuck([]byte(obsGoldenPGStuckPG))
	if err != nil || !reflect.DeepEqual(stuck, []string{"1.1f"}) {
		t.Fatalf("stuck = %v, %v", stuck, err)
	}
	clean, _ := parseUpgradePGStat([]byte(obsGoldenPGStat))
	if got := upgradePGsSettled(clean, stuck); got != upgradeNo {
		t.Errorf("a stuck PG with a clean pg stat = %d, want no", got)
	}
	empty, _ := parseUpgradePGStat([]byte(edit(func(m map[string]any) { m["num_pg_by_state"], m["num_pgs"] = []any{}, 0 })))
	if got := upgradePGsSettled(empty, []string{}); got != upgradeUnknown {
		t.Errorf("an empty stuck list with no PGs = %d, want unknown", got)
	}
	obsRejectsShapes(t, obsErr(parseUpgradePGStuck), obsGoldenPGStuckPG)
	for name, doc := range map[string]string{
		"object":           `{"stuck_pg_stats": []}`,
		"element not obj":  `["1.1f"]`,
		"no pgid":          `[{"state": "active+undersized"}]`,
		"null pgid":        `[{"pgid": null}]`,
		"pgid not a PG ID": `[{"pgid": "pool.7"}]`,
		"pgid is a number": `[{"pgid": 11}]`,
		"pg twice":         `[{"pgid": "1.1f"}, {"pgid": "1.1f"}]`,
		"duplicate key":    `[{"pgid": "1.1f", "pgid": "1.20"}]`,
	} {
		if _, err := parseUpgradePGStuck([]byte(doc)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// ---- end to end through the sequence plan ----------------------------------

func obsVersion(release string) string {
	if strings.HasPrefix(release, "20.") {
		return "ceph version " + release + " (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)"
	}
	return "ceph version " + release + " (0123456789abcdef0123456789abcdef01234567) squid (stable)"
}

// obsOutputsFor builds the command outputs after the n-th planned transition.
func obsOutputsFor(t *testing.T, n int) upgradeObservationOutputs {
	t.Helper()
	cp, err := upgradeCheckpointAfter(seqPlan(), upgradePair, n)
	if err != nil {
		t.Fatal(err)
	}
	versions := map[string]any{}
	overall := map[string]int{}
	for role, counts := range cp {
		byDesc := map[string]int{}
		for release, c := range counts {
			byDesc[obsVersion(release)] = c
			overall[obsVersion(release)] += c
		}
		versions[role] = byDesc
	}
	versions["overall"] = overall
	marshal := func(v any) []byte {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	release, name := 19, "squid"
	if n >= 3 {
		release, name = 20, "tentacle"
	}
	monDoc := obsEdit(t, obsGoldenMonDump, func(m map[string]any) {
		m["min_mon_release"], m["min_mon_release_name"], m["quorum"] = release, name, []any{0, 1, 2}
	})
	quorumDoc := obsEdit(t, obsGoldenQuorumStatus, func(m map[string]any) {
		m["monmap"].(map[string]any)["min_mon_release"] = release
		m["monmap"].(map[string]any)["min_mon_release_name"] = name
	})
	return upgradeObservationOutputs{
		Versions: marshal(versions), MonDump: []byte(monDoc), QuorumStatus: []byte(quorumDoc),
		OSDStat: []byte(obsGoldenOSDStat), MgrStat: []byte(obsGoldenMgrStat), MgrDump: []byte(obsGoldenMgrDump),
		PGStat: []byte(obsGoldenPGStat), PGStuck: []byte(obsGoldenPGStuck),
	}
}

func TestUpgradeObservationsDriveTheWholePlan(t *testing.T) {
	s := newSeq(t, goodFacts(t))
	plan := seqPlan()
	for i := 1; i <= len(plan); i++ {
		id, d, err := s.admit()
		if err != nil {
			t.Fatal(err)
		}
		ledgerOK(t, s.ledger.fence(id, d))
		ledgerOK(t, s.ledger.oldAbsent(id, false, nil))
		ledgerOK(t, s.ledger.startAttempted(id, "dst-"+id))
		ledgerOK(t, s.ledger.confirm(id, upgradeNativeEvidence{Instance: "dst-" + id, Version: "20.2.4", FSID: d.FSID, StoreID: d.StoreID}))
		ev := upgradeSettledFromOutputs(d.key(), obsOutputsFor(t, i))
		if ev.ObservationErr != nil {
			t.Fatalf("transition %d: %v", i, ev.ObservationErr)
		}
		ledgerOK(t, s.settle(ev))
	}
	ledgerOK(t, s.finalize())
}

func TestUpgradeObservationsMapToPlanInputs(t *testing.T) {
	ev := upgradeSettledFromOutputs("osd.2", obsOutputsFor(t, 8))
	if ev.ObservationErr != nil {
		t.Fatal(ev.ObservationErr)
	}
	if ev.Daemon != "osd.2" || !sameStrings(ev.Quorum, []string{"a", "b", "c"}) || ev.MgrActive != "x" ||
		!reflect.DeepEqual(ev.MgrStandbys, []string{"y"}) || ev.OSDsUpIn == nil || *ev.OSDsUpIn != 3 ||
		ev.PGsSettled != upgradeYes || ev.MinMonRelease == nil || *ev.MinMonRelease != 20 ||
		!reflect.DeepEqual(ev.Inventory["osd"], map[string]int{"20.2.4": 3}) {
		t.Fatalf("evidence = %+v", ev)
	}
}

func TestUpgradeObservationsAnyBrokenOutputIsInconclusive(t *testing.T) {
	good := obsOutputsFor(t, 3)
	fields := map[string]func(*upgradeObservationOutputs) *[]byte{
		"versions":      func(o *upgradeObservationOutputs) *[]byte { return &o.Versions },
		"mon dump":      func(o *upgradeObservationOutputs) *[]byte { return &o.MonDump },
		"quorum_status": func(o *upgradeObservationOutputs) *[]byte { return &o.QuorumStatus },
		"osd stat":      func(o *upgradeObservationOutputs) *[]byte { return &o.OSDStat },
		"mgr stat":      func(o *upgradeObservationOutputs) *[]byte { return &o.MgrStat },
		"mgr dump":      func(o *upgradeObservationOutputs) *[]byte { return &o.MgrDump },
		"pg stat":       func(o *upgradeObservationOutputs) *[]byte { return &o.PGStat },
		"pg dump_stuck": func(o *upgradeObservationOutputs) *[]byte { return &o.PGStuck },
	}
	for name, field := range fields {
		for label, bad := range map[string][]byte{"not collected": nil, "null": []byte("null"), "empty object": []byte("{}")} {
			o := good
			*field(&o) = bad
			ev := upgradeSettledFromOutputs("mon.c", o)
			if ev.ObservationErr == nil {
				t.Errorf("%s %s: no observation error", name, label)
				continue
			}
			if ev.Inventory != nil || ev.Quorum != nil || ev.OSDsUpIn != nil || ev.PGsSettled != upgradeUnknown || ev.MinMonRelease != nil {
				t.Errorf("%s %s: partial evidence leaked: %+v", name, label, ev)
			}
		}
	}

	// The plan refuses such evidence and stays at the same step, so good
	// evidence still settles afterwards.
	s := newSeq(t, goodFacts(t))
	driveTo(t, s, 1)
	id, d, err := s.admit()
	if err != nil {
		t.Fatal(err)
	}
	ledgerOK(t, s.ledger.fence(id, d))
	ledgerOK(t, s.ledger.oldAbsent(id, false, nil))
	ledgerOK(t, s.ledger.startAttempted(id, "dst-"+id))
	ledgerOK(t, s.ledger.confirm(id, upgradeNativeEvidence{Instance: "dst-" + id, Version: "20.2.4", FSID: d.FSID, StoreID: d.StoreID}))
	broken := obsOutputsFor(t, 2)
	broken.PGStat = nil
	wantErr(t, s.settle(upgradeSettledFromOutputs(d.key(), broken)), "settled observation failed")
	ledgerOK(t, s.settle(upgradeSettledFromOutputs(d.key(), obsOutputsFor(t, 2))))
}

func TestUpgradeObservationsDisagreementsAreNotEvidence(t *testing.T) {
	good := obsOutputsFor(t, 3)
	cases := map[string]func(*upgradeObservationOutputs){
		"mon dump of another cluster": func(o *upgradeObservationOutputs) {
			o.MonDump = []byte(obsEdit(t, string(o.MonDump), func(m map[string]any) { m["fsid"] = "11111111-2222-4333-8444-555555555555" }))
		},
		"mon dump lists another monitor set": func(o *upgradeObservationOutputs) {
			o.MonDump = []byte(obsEdit(t, string(o.MonDump), func(m map[string]any) { monAt(2)(m)["name"] = "d" }))
		},
		"mgr stat from another epoch": func(o *upgradeObservationOutputs) {
			o.MgrStat = []byte(obsEdit(t, string(o.MgrStat), func(m map[string]any) { m["epoch"] = 10 }))
		},
		"duplicate key in versions": func(o *upgradeObservationOutputs) {
			o.Versions = []byte(strings.Replace(string(o.Versions), `{`, `{"osd": {},`, 1))
		},
	}
	for name, mutate := range cases {
		o := good
		mutate(&o)
		if ev := upgradeSettledFromOutputs("mon.c", o); ev.ObservationErr == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestUpgradeObservationsFeedPlanRefusals(t *testing.T) {
	type refusal struct {
		name   string
		n      int // transition the plan is at
		mutate func(*upgradeObservationOutputs)
		want   string
	}
	for _, tc := range []refusal{
		{"stale inventory (previous checkpoint)", 4, func(o *upgradeObservationOutputs) { *o = obsOutputsFor(t, 3) }, "version inventory differs"},
		{"quorum lost a monitor", 2, func(o *upgradeObservationOutputs) {
			o.QuorumStatus = []byte(obsEdit(t, string(o.QuorumStatus), func(m map[string]any) {
				m["quorum"], m["quorum_names"] = []any{0, 1}, []any{"a", "b"}
			}))
		}, "quorum"},
		{"min_mon_release still the source after the MON phase", 3, func(o *upgradeObservationOutputs) {
			o.MonDump = []byte(obsEdit(t, string(o.MonDump), func(m map[string]any) {
				m["min_mon_release"], m["min_mon_release_name"] = 19, "squid"
			}))
		}, "min_mon_release 19, want 20"},
		{"standby manager missing", 5, func(o *upgradeObservationOutputs) {
			o.MgrStat = []byte(obsEdit(t, string(o.MgrStat), func(m map[string]any) { m["num_standby"] = 0 }))
			o.MgrDump = []byte(obsEdit(t, string(o.MgrDump), func(m map[string]any) { m["standbys"] = []any{} }))
		}, "MGR active"},
		{"manager not available", 5, func(o *upgradeObservationOutputs) {
			o.MgrStat = []byte(obsEdit(t, string(o.MgrStat), func(m map[string]any) { m["available"] = false }))
			o.MgrDump = []byte(obsEdit(t, string(o.MgrDump), func(m map[string]any) { m["available"] = false }))
		}, "MGR active"},
		{"an OSD is down", 8, func(o *upgradeObservationOutputs) {
			o.OSDStat = []byte(obsEdit(t, string(o.OSDStat), func(m map[string]any) { m["num_up_osds"] = 2 }))
		}, "OSDs up and in not observed"},
		{"a PG is peering", 6, func(o *upgradeObservationOutputs) {
			o.PGStat = []byte(obsEdit(t, string(o.PGStat), func(m map[string]any) {
				m["num_pg_by_state"] = []any{map[string]any{"name": "active+clean", "num": 31}, map[string]any{"name": "peering", "num": 1}}
			}))
		}, "PGs are not settled"},
		{"a PG is stuck", 6, func(o *upgradeObservationOutputs) { o.PGStuck = []byte(obsGoldenPGStuckPG) }, "PGs are not settled"},
	} {
		s := newSeq(t, goodFacts(t))
		driveTo(t, s, tc.n-1)
		id, d, err := s.admit()
		if err != nil {
			t.Fatal(err)
		}
		ledgerOK(t, s.ledger.fence(id, d))
		ledgerOK(t, s.ledger.oldAbsent(id, false, nil))
		ledgerOK(t, s.ledger.startAttempted(id, "dst-"+id))
		ledgerOK(t, s.ledger.confirm(id, upgradeNativeEvidence{Instance: "dst-" + id, Version: "20.2.4", FSID: d.FSID, StoreID: d.StoreID}))
		o := obsOutputsFor(t, tc.n)
		tc.mutate(&o)
		ev := upgradeSettledFromOutputs(d.key(), o)
		if ev.ObservationErr != nil {
			t.Errorf("%s: unexpected observation error %v", tc.name, ev.ObservationErr)
			continue
		}
		if err := s.settle(ev); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: settle = %v, want it to contain %q", tc.name, err, tc.want)
		}
	}
}
