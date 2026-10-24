//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

// These fixtures are synthetic. They follow the field representations in the
// pinned Ceph dump code (see cephfs_snaprealm_context_helpers_test.go), not a
// captured native dump. They prove the parser and the derivation arithmetic
// without a cluster; they say nothing about what a running client reports.

const (
	snapFixtureChildRealm  = "0x10000000002"
	snapFixtureParentRealm = "0x10000000001"
	snapFixtureFileInode   = uint64(0x10000000003)
)

// snapFixtureRealm renders one dumped snaprealm object. List arguments are
// the already rendered JSON arrays.
func snapFixtureRealm(ino, seq, parent, since, prior, mine string) string {
	return fmt.Sprintf(`{"ino":%q,"nref":3,"created":"0.000000","seq":%q,"parent_ino":%q,"parent_since":%q,"prior_parent_snaps":%s,"my_snaps":%s,"children":[]}`,
		ino, seq, parent, since, prior, mine)
}

func snapFixtureInode(ino, snapID, realm string) string {
	if realm == "" {
		return fmt.Sprintf(`{"path":"p","ino":%q,"snapid":%q,"size":0}`, ino, snapID)
	}
	return fmt.Sprintf(`{"path":"p","ino":%q,"snapid":%q,"size":0,"snaprealm":%s}`, ino, snapID, realm)
}

func snapFixtureCache(entries ...string) []byte {
	return []byte("[" + strings.Join(entries, ",") + "]")
}

// The issue's arithmetic control: child seq 150 (0x96), own snapshot 150,
// prior-parent snapshot 99 (0x63), parent seq 600 (0x258) with snapshots
// [599, 512, 100] since 500 (0x1f4).
func snapFixtureControl(childSeq, parentSeq, parentSnaps string) []byte {
	child := snapFixtureRealm(snapFixtureChildRealm, childSeq, snapFixtureParentRealm, "1f4", `["63"]`, `["96"]`)
	parent := snapFixtureRealm(snapFixtureParentRealm, parentSeq, "0x0", "1", `[]`, parentSnaps)
	return snapFixtureCache(
		snapFixtureInode("0x1", "head", ""),
		snapFixtureInode(snapFixtureParentRealm, "head", parent),
		snapFixtureInode("0x10000000003", "head", child),
	)
}

func TestCephFSSnapRealmContextDerivation(t *testing.T) {
	data := snapFixtureControl("96", "258", `["257","200","64"]`)
	released, ctx, err := snapContextGate(data, snapFixtureFileInode, 0x200)
	if err != nil || !released {
		t.Fatalf("gate for the inherited snapshot 512: released=%v err=%v", released, err)
	}
	if ctx.Seq != 600 || !slices.Equal(ctx.Snaps, []uint64{599, 512, 150, 99}) {
		t.Fatalf("derived context seq=%d snaps=%v, want 600 [599 512 150 99]", ctx.Seq, ctx.Snaps)
	}
	if !slices.Equal(ctx.Chain, []uint64{0x10000000002, 0x10000000001}) {
		t.Fatalf("chain=%x", ctx.Chain)
	}
	// 100 predates parent_since and is not inherited; looking only at the
	// child's own list would also miss 512.
	if ctx.hasSnap(100) {
		t.Fatal("snapshot 100 is older than parent_since but was inherited")
	}

	t.Run("parent update changes context while local seq stays", func(t *testing.T) {
		before, _, err := snapContextGate(data, snapFixtureFileInode, 0x259)
		if err != nil || before {
			t.Fatalf("pre-trigger gate for 601: released=%v err=%v", before, err)
		}
		after := snapFixtureControl("96", "259", `["259","257","200","64"]`)
		released, ctx, err := snapContextGate(after, snapFixtureFileInode, 0x259)
		if err != nil || !released || ctx.Seq != 0x259 {
			t.Fatalf("post-update gate: released=%v seq=%x err=%v", released, ctx.Seq, err)
		}
		view, _ := parseClientSnapRealms(after, snapFixtureFileInode)
		if view.Records[0x10000000002].Seq != 150 {
			t.Fatal("fixture drifted: the child local seq must stay 150")
		}
	})

	t.Run("newer sequence alone does not release the gate", func(t *testing.T) {
		bumped := snapFixtureControl("2bc", "258", `["257","200","64"]`) // child seq 700
		released, ctx, err := snapContextGate(bumped, snapFixtureFileInode, 0x259)
		if err != nil || released || ctx.Seq != 700 {
			t.Fatalf("released=%v seq=%d err=%v", released, ctx.Seq, err)
		}
	})

	t.Run("wrapped cache object, duplicates and non-selected entries", func(t *testing.T) {
		child := snapFixtureRealm(snapFixtureChildRealm, "96", snapFixtureParentRealm, "1f4", `["63"]`, `["96"]`)
		parent := snapFixtureRealm(snapFixtureParentRealm, "258", "0x0", "1", `[]`, `["257","200","64"]`)
		entries := []string{
			`{"dentry":"name","ino":"0x10000000003"}`, // a dentry has no snapid
			snapFixtureInode(snapFixtureParentRealm, "head", parent),
			snapFixtureInode("0x10000000004", "head", child), // second inode, identical record
			snapFixtureInode("0x10000000003", "5", ""),       // a snapshot inode of the file
			snapFixtureInode("0x10000000003", "head", child),
		}
		wrapped := []byte(`{"cache":[` + strings.Join(entries, ",") + `]}`)
		released, _, err := snapContextGate(wrapped, snapFixtureFileInode, 0x200)
		if err != nil || !released {
			t.Fatalf("released=%v err=%v", released, err)
		}
	})

	t.Run("parent snapshot equal to parent_since is inherited", func(t *testing.T) {
		boundary := snapFixtureControl("96", "258", `["1f4","1f3"]`) // 500 and 499, since 500
		released, ctx, err := snapContextGate(boundary, snapFixtureFileInode, 0x1f4)
		if err != nil || !released || ctx.hasSnap(0x1f3) {
			t.Fatalf("released=%v snaps=%v err=%v", released, ctx.Snaps, err)
		}
	})

	t.Run("no parent means only the realm's own lists", func(t *testing.T) {
		root := snapFixtureRealm("0x1", "5", "0x0", "1", `["2"]`, `["4"]`)
		data := snapFixtureCache(snapFixtureInode("0x1", "head", root))
		released, ctx, err := snapContextGate(data, 1, 4)
		if err != nil || !released || ctx.Seq != 5 || !slices.Equal(ctx.Snaps, []uint64{4, 2}) {
			t.Fatalf("released=%v ctx=%+v err=%v", released, ctx, err)
		}
	})
}

func TestCephFSSnapRealmContextStrictness(t *testing.T) {
	child := snapFixtureRealm(snapFixtureChildRealm, "96", snapFixtureParentRealm, "1f4", `["63"]`, `["96"]`)
	parent := snapFixtureRealm(snapFixtureParentRealm, "258", "0x0", "1", `[]`, `["257"]`)
	selected := func(realm string) string { return snapFixtureInode("0x10000000003", "head", realm) }
	tests := []struct {
		name string
		data []byte
		want error
	}{
		{"parent record absent", snapFixtureCache(selected(child)), errSnapRealmIncomplete},
		{"selected inode absent", snapFixtureCache(snapFixtureInode(snapFixtureParentRealm, "head", parent)), errSnapRealmIncomplete},
		{"selected inode only as snapshot", snapFixtureCache(snapFixtureInode("0x10000000003", "5", child), snapFixtureInode(snapFixtureParentRealm, "head", parent)), errSnapRealmIncomplete},
		{"selected inode without snaprealm", snapFixtureCache(selected(""), snapFixtureInode(snapFixtureParentRealm, "head", parent)), errSnapRealmIncomplete},
		{"selected inode with null snaprealm", snapFixtureCache(`{"ino":"0x10000000003","snapid":"head","snaprealm":null}`), errSnapRealmIncomplete},
		{"empty cache", snapFixtureCache(), errSnapRealmIncomplete},
		{"not json", []byte(`nope`), errSnapRealmInvalid},
		{"neither array nor cache object", []byte(`{"other":[]}`), errSnapRealmInvalid},
		{"cache entry not an object", []byte(`[1]`), errSnapRealmInvalid},
		{"parent cycle", snapFixtureCache(
			selected(child),
			snapFixtureInode(snapFixtureParentRealm, "head", snapFixtureRealm(snapFixtureParentRealm, "258", snapFixtureChildRealm, "1", `[]`, `[]`)),
		), errSnapRealmInvalid},
		{"realm is its own parent", snapFixtureCache(selected(snapFixtureRealm(snapFixtureChildRealm, "96", snapFixtureChildRealm, "1", `[]`, `[]`))), errSnapRealmInvalid},
		{"conflicting duplicate realm record", snapFixtureCache(
			selected(child),
			snapFixtureInode("0x10000000004", "head", strings.Replace(child, `"96"],"children"`, `"97"],"children"`, 1)),
			snapFixtureInode(snapFixtureParentRealm, "head", parent),
		), errSnapRealmInvalid},
		{"one inode in two realms", snapFixtureCache(
			selected(child),
			selected(snapFixtureRealm(snapFixtureParentRealm, "258", "0x0", "1", `[]`, `["257"]`)),
		), errSnapRealmInvalid},
		{"reserved head in my_snaps", snapFixtureCache(selected(strings.Replace(child, `"my_snaps":["96"]`, `"my_snaps":["head"]`, 1)), snapFixtureInode(snapFixtureParentRealm, "head", parent)), errSnapRealmInvalid},
		{"reserved snapdir in prior_parent_snaps", snapFixtureCache(selected(strings.Replace(child, `["63"]`, `["snapdir"]`, 1)), snapFixtureInode(snapFixtureParentRealm, "head", parent)), errSnapRealmInvalid},
		{"uppercase hex", snapFixtureCache(selected(strings.Replace(child, `"96"`, `"9A"`, 1)), snapFixtureInode(snapFixtureParentRealm, "head", parent)), errSnapRealmInvalid},
		{"overflowing hex", snapFixtureCache(selected(strings.Replace(child, `"96"`, `"10000000000000000"`, 1)), snapFixtureInode(snapFixtureParentRealm, "head", parent)), errSnapRealmInvalid},
		{"0x prefix on a snapid", snapFixtureCache(selected(strings.Replace(child, `"96"`, `"0x96"`, 1)), snapFixtureInode(snapFixtureParentRealm, "head", parent)), errSnapRealmInvalid},
		{"realm ino without 0x", snapFixtureCache(selected(strings.Replace(child, snapFixtureParentRealm, "10000000001", 1)), snapFixtureInode(snapFixtureParentRealm, "head", parent)), errSnapRealmInvalid},
		{"snapshot list element null", snapFixtureCache(selected(strings.Replace(child, `["63"]`, `[null]`, 1)), snapFixtureInode(snapFixtureParentRealm, "head", parent)), errSnapRealmInvalid},
		{"snaprealm is an array", snapFixtureCache(selected(`[]`)), errSnapRealmInvalid},
		{"control: complete observation", snapFixtureCache(selected(child), snapFixtureInode(snapFixtureParentRealm, "head", parent)), nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			released, ctx, err := snapContextGate(tc.data, snapFixtureFileInode, 0x257)
			if tc.want == nil {
				if err != nil || !released {
					t.Fatalf("released=%v err=%v", released, err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("err=%v, want %v", err, tc.want)
			}
			if released || ctx.Seq != 0 || ctx.Snaps != nil {
				t.Fatalf("an unusable observation produced evidence: released=%v ctx=%+v", released, ctx)
			}
		})
	}

	// Paired missing, null and wrong-typed controls for every required field.
	for _, field := range []string{"ino", "seq", "parent_ino", "parent_since", "prior_parent_snaps", "my_snaps"} {
		key := fmt.Sprintf("%q:", field)
		for _, tc := range []struct {
			name, with string
			want       error
		}{
			{"missing", "", errSnapRealmIncomplete},
			{"null", key + "null", errSnapRealmIncomplete},
			{"number", key + "7", errSnapRealmInvalid},
			{"object", key + "{}", errSnapRealmInvalid},
		} {
			t.Run(field+" "+tc.name, func(t *testing.T) {
				mutated := dropSnapFixtureField(t, child, field, tc.with)
				data := snapFixtureCache(selected(mutated), snapFixtureInode(snapFixtureParentRealm, "head", parent))
				released, _, err := snapContextGate(data, snapFixtureFileInode, 0x257)
				if !errors.Is(err, tc.want) || released {
					t.Fatalf("released=%v err=%v, want %v", released, err, tc.want)
				}
			})
		}
	}
	t.Run("list fields reject a scalar", func(t *testing.T) {
		for _, field := range []string{"prior_parent_snaps", "my_snaps"} {
			mutated := dropSnapFixtureField(t, child, field, fmt.Sprintf("%q:\"63\"", field))
			data := snapFixtureCache(selected(mutated), snapFixtureInode(snapFixtureParentRealm, "head", parent))
			if _, _, err := snapContextGate(data, snapFixtureFileInode, 0x257); !errors.Is(err, errSnapRealmInvalid) {
				t.Fatalf("%s: err=%v", field, err)
			}
		}
	})
}

// dropSnapFixtureField removes field from the rendered realm and appends
// replacement (when non-empty) as a member in its place.
func dropSnapFixtureField(t *testing.T, realm, field, replacement string) string {
	t.Helper()
	var members []string
	body := strings.TrimSuffix(strings.TrimPrefix(realm, "{"), "}")
	for _, m := range splitTopLevel(body) {
		if !strings.HasPrefix(m, fmt.Sprintf("%q:", field)) {
			members = append(members, m)
		}
	}
	if len(members) != len(splitTopLevel(body))-1 {
		t.Fatalf("fixture field %q not found exactly once", field)
	}
	if replacement != "" {
		members = append(members, replacement)
	}
	return "{" + strings.Join(members, ",") + "}"
}

// splitTopLevel splits a JSON object body on commas outside brackets and
// strings. The fixtures hold no escaped quotes.
func splitTopLevel(body string) []string {
	var parts []string
	depth, inStr, start := 0, false, 0
	for i, c := range body {
		switch {
		case c == '"':
			inStr = !inStr
		case inStr:
		case c == '[' || c == '{':
			depth++
		case c == ']' || c == '}':
			depth--
		case c == ',' && depth == 0:
			parts = append(parts, body[start:i])
			start = i + 1
		}
	}
	return append(parts, body[start:])
}
