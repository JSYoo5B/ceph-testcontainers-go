//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"strings"
	"testing"
)

// These fixtures are synthetic: dump_cache entries built from the snaprealm
// fixtures. They exercise the removal gate without a cluster and say nothing
// about what a running client reports.

func snapRemovalFixture(dirSeq, rootSeq, dirSnaps, rootSnaps string) []byte {
	dir := snapFixtureRealm(snapFixtureParentRealm, dirSeq, "0x1", "1", `[]`, dirSnaps)
	root := snapFixtureRealm("0x1", rootSeq, "0x0", "1", `[]`, rootSnaps)
	return snapFixtureCache(
		snapFixtureInode("0x1", "head", root),
		snapFixtureInode(snapFixtureParentRealm, "head", dir),
		snapFixtureInode("0x10000000003", "head", dir),
	)
}

func TestCephFSLiveRealmRemovalGate(t *testing.T) {
	const snap = 2
	present := snapRemovalFixture("2", "1", `["2"]`, `[]`)
	removed := snapRemovalFixture("3", "1", `[]`, `[]`)
	t.Run("present snapshot is not removal", func(t *testing.T) {
		released, ctx, err := snapRemovalGate(present, snapFixtureFileInode, snap, 2)
		if err != nil || released {
			t.Fatalf("released=%v err=%v ctx=%+v", released, err, ctx)
		}
		if err := snapRemovalPriorOK(ctx, snap); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("absent with a newer sequence releases", func(t *testing.T) {
		released, ctx, err := snapRemovalGate(removed, snapFixtureFileInode, snap, 2)
		if err != nil || !released || ctx.Seq != 3 || len(ctx.Snaps) != 0 {
			t.Fatalf("released=%v err=%v ctx=%+v", released, err, ctx)
		}
	})
	t.Run("absent with the old sequence does not release", func(t *testing.T) {
		stale := snapRemovalFixture("2", "1", `[]`, `[]`)
		if released, _, err := snapRemovalGate(stale, snapFixtureFileInode, snap, 2); err != nil || released {
			t.Fatalf("released=%v err=%v", released, err)
		}
	})
	t.Run("newer sequence with the snapshot still listed does not release", func(t *testing.T) {
		newer := snapRemovalFixture("3", "1", `["2"]`, `[]`)
		if released, _, err := snapRemovalGate(newer, snapFixtureFileInode, snap, 2); err != nil || released {
			t.Fatalf("released=%v err=%v", released, err)
		}
	})
	t.Run("inherited snapshot from the root realm is seen", func(t *testing.T) {
		inherited := snapRemovalFixture("3", "3", `[]`, `["2"]`)
		if released, ctx, err := snapRemovalGate(inherited, snapFixtureFileInode, snap, 2); err != nil || released {
			t.Fatalf("released=%v err=%v ctx=%+v", released, err, ctx)
		}
	})
	t.Run("a newer root sequence alone satisfies the sequence condition", func(t *testing.T) {
		rootRemoved := snapRemovalFixture("2", "5", `[]`, `[]`)
		if released, ctx, err := snapRemovalGate(rootRemoved, snapFixtureFileInode, snap, 2); err != nil || !released || ctx.Seq != 5 {
			t.Fatalf("released=%v err=%v ctx=%+v", released, err, ctx)
		}
	})
	t.Run("never releases on missing, null or wrong-typed evidence", func(t *testing.T) {
		dir := snapFixtureRealm(snapFixtureParentRealm, "3", "0x1", "1", `[]`, `[]`)
		root := snapFixtureRealm("0x1", "1", "0x0", "1", `[]`, `[]`)
		for name, data := range map[string][]byte{
			"missing parent record": snapFixtureCache(snapFixtureInode("0x10000000003", "head", dir)),
			"no realm on inode":     snapFixtureCache(snapFixtureInode("0x10000000003", "head", "")),
			"no inode":              snapFixtureCache(),
			"null realm":            []byte(`[{"path":"p","ino":"0x10000000003","snapid":"head","snaprealm":null}]`),
			"numeric seq":           snapFixtureCache(snapFixtureInode("0x1", "head", root), snapFixtureInode("0x10000000003", "head", strings.Replace(dir, `"seq":"3"`, `"seq":3`, 1))),
			"null my_snaps":         snapFixtureCache(snapFixtureInode("0x1", "head", root), snapFixtureInode("0x10000000003", "head", strings.Replace(dir, `"my_snaps":[]`, `"my_snaps":null`, 1))),
			"absent my_snaps":       snapFixtureCache(snapFixtureInode("0x1", "head", root), snapFixtureInode("0x10000000003", "head", strings.Replace(dir, `"my_snaps":[],`, ``, 1))),
			"not a cache":           []byte(`7`),
			"empty input":           nil,
		} {
			if released, _, err := snapRemovalGate(data, snapFixtureFileInode, snap, 2); released || err == nil {
				t.Errorf("%s: released=%v err=%v, want inconclusive", name, released, err)
			}
		}
	})
	t.Run("rejects an unusable baseline", func(t *testing.T) {
		if released, _, err := snapRemovalGate(removed, snapFixtureFileInode, snap, 0); released || err == nil {
			t.Fatalf("zero prior sequence: released=%v err=%v", released, err)
		}
		if released, _, err := snapRemovalGate(removed, snapFixtureFileInode, 0, 2); released || err == nil {
			t.Fatalf("zero snapshot ID: released=%v err=%v", released, err)
		}
	})
	t.Run("prior context must list the snapshot", func(t *testing.T) {
		if err := snapRemovalPriorOK(clientSnapContext{Seq: 2, Snaps: []uint64{1}}, snap); err == nil {
			t.Fatal("a context without the snapshot passed as prior")
		}
		if err := snapRemovalPriorOK(clientSnapContext{Snaps: []uint64{snap}}, snap); err == nil {
			t.Fatal("a zero-sequence context passed as prior")
		}
	})
}
