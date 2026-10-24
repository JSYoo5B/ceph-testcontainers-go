//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"fmt"
	"strings"
	"testing"
)

// These fixtures are synthetic: an MDS "dump snaps" table shaped like
// SnapInfo::dump (decimal IDs) and dump_cache entries reused from the
// snaprealm fixtures. They exercise the live scenario's parsing and logging
// helpers without a cluster and say nothing about a running Ceph.

func TestCephFSLiveRealmGateSnapshotID(t *testing.T) {
	table := func(entries ...string) []byte {
		return []byte(`{"last_created":9,"last_destroyed":0,"snaps":[` + strings.Join(entries, ",") + `]}`)
	}
	snap := func(id, ino int, name string) string {
		return fmt.Sprintf(`{"snapid":%d,"ino":%d,"stamp":"2026-01-01T00:00:00.000000+0000","name":%q,"metadata":{}}`, id, ino, name)
	}
	t.Run("found", func(t *testing.T) {
		id, err := mdsSnapshotID(table(snap(4, 1099511627780, "other"), snap(7, 1099511627779, "s")), 0x10000000003, "s")
		if err != nil || id != 7 {
			t.Fatalf("id=%d err=%v", id, err)
		}
	})
	for name, data := range map[string][]byte{
		"absent":            table(snap(4, 1099511627780, "other")),
		"duplicate":         table(snap(7, 1099511627779, "s"), snap(8, 1099511627779, "s")),
		"zero id":           table(snap(0, 1099511627779, "s")),
		"empty table":       table(),
		"no snaps member":   []byte(`{"last_created":9}`),
		"null snaps":        []byte(`{"snaps":null}`),
		"not json":          []byte(`nope`),
		"missing name":      table(`{"snapid":7,"ino":1099511627779}`),
		"null snapid":       table(`{"snapid":null,"ino":1099511627779,"name":"s"}`),
		"string snapid":     table(`{"snapid":"7","ino":1099511627779,"name":"s"}`),
		"missing ino entry": table(`{"snapid":7,"name":"s"}`),
	} {
		t.Run("rejects "+name, func(t *testing.T) {
			if id, err := mdsSnapshotID(data, 0x10000000003, "s"); err == nil {
				t.Fatalf("id=%d, want an error", id)
			}
		})
	}
}

func TestCephFSLiveRealmGateShape(t *testing.T) {
	child := snapFixtureRealm(snapFixtureChildRealm, "96", snapFixtureParentRealm, "1f4", `["63"]`, `["96"]`)
	parent := snapFixtureRealm(snapFixtureParentRealm, "258", "0x0", "1", `[]`, `["257","200","64"]`)
	complete := snapFixtureCache(
		snapFixtureInode("0x1", "head", ""),
		snapFixtureInode(snapFixtureParentRealm, "head", parent),
		snapFixtureInode("0x10000000003", "head", child),
	)
	shape := snapRealmShape(complete, snapFixtureFileInode)
	for _, want := range []string{"inodes=3", "with_snaprealm=2", `"ino":"0x10000000002"`, "selected realm=0x10000000002", "[0x10000000001 seq=258 parent=0x0", "mine=[257 200 64]"} {
		if !strings.Contains(shape, want) {
			t.Errorf("shape lacks %q: %s", want, shape)
		}
	}

	t.Run("classification", func(t *testing.T) {
		missingParent := snapFixtureCache(snapFixtureInode("0x10000000003", "head", child))
		noRealm := snapFixtureCache(snapFixtureInode("0x10000000003", "head", ""))
		badField := snapFixtureCache(snapFixtureInode("0x10000000003", "head", strings.Replace(child, `"seq":"96"`, `"seq":96`, 1)))
		for name, tc := range map[string]struct {
			data []byte
			want string
		}{
			"complete":       {complete, "complete"},
			"missing parent": {missingParent, "incomplete"},
			"no realm":       {noRealm, "incomplete"},
			"no inode":       {snapFixtureCache(), "incomplete"},
			"wrong type":     {badField, "invalid"},
			"not a cache":    {[]byte(`7`), "invalid"},
		} {
			_, _, err := snapContextGate(tc.data, snapFixtureFileInode, 0x200)
			if got := classifySnapRealm(err); got != tc.want {
				t.Errorf("%s: classified %q (%v), want %q", name, got, err, tc.want)
			}
			if tc.want != "complete" {
				if released, _, _ := snapContextGate(tc.data, snapFixtureFileInode, 0x200); released {
					t.Errorf("%s released the gate", name)
				}
			}
		}
	})

	t.Run("shape never fails on partial input", func(t *testing.T) {
		for name, data := range map[string][]byte{
			"missing parent": snapFixtureCache(snapFixtureInode("0x10000000003", "head", child)),
			"no realm":       snapFixtureCache(snapFixtureInode("0x10000000003", "head", "")),
			"garbage":        []byte(`nope`),
			"empty":          nil,
		} {
			if s := snapRealmShape(data, snapFixtureFileInode); !strings.Contains(s, "inodes=") {
				t.Errorf("%s: shape %q", name, s)
			}
		}
	})
}
