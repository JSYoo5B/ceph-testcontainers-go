//go:build all || integration

package cephfs_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Derived inherited snapshot context of a libcephfs client, computed from one
// dump_cache observation.
//
// Source: Ceph Tentacle 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964).
// Inode::dump emits a "snaprealm" object for an inode that holds caps, and
// SnapRealm::dump (src/client/ClientSnapRealm.cc) shows only the realm's own
// record: ino, nref, created, seq, parent_ino, parent_since,
// prior_parent_snaps, my_snaps and children. The context the client writes
// with is built by SnapRealm::build_snap_context and is not in the dump:
//
//	snaps = prior_parent_snaps
//	      + {s in parent context.snaps : s >= parent_since}
//	      + my_snaps
//	seq   = max(seq, parent context.seq)
//
// so a snapshot taken on an ancestor is visible in a descendant realm's
// context without appearing in its own my_snaps, and the local seq can stay
// unchanged while the effective context changes. The result below is
// therefore labelled derived: it is a computation over the dumped records,
// not a direct observation of the cached SnapContext object.
//
// Field representations come from the dump code: inodeno_t streams as
// "0x<hex>", snapid_t as bare lowercase hex ("head" and "snapdir" for the two
// reserved values), and the two snapshot arrays are arrays of such strings
// (the formatter drops the element name inside an array section).

// errSnapRealmIncomplete means the observation lacks something the derivation
// needs: the inode, its snaprealm, a required field, or an ancestor record.
// The result is inconclusive, never an empty context.
var errSnapRealmIncomplete = errors.New("snaprealm observation incomplete")

// errSnapRealmInvalid means the observation contradicts itself or the dump
// format: a wrong-typed or unparsable field, conflicting duplicate records for
// one realm, a parent cycle, or a reserved snapshot ID in a snapshot list.
var errSnapRealmInvalid = errors.New("snaprealm observation invalid")

const (
	snapIDHead    = ^uint64(0)     // CEPH_NOSNAP, printed as "head"
	snapIDSnapDir = ^uint64(0) - 1 // CEPH_SNAPDIR, printed as "snapdir"
)

// snapRealmRecord is one realm's own dumped record.
type snapRealmRecord struct {
	Ino              uint64
	Seq              uint64
	Parent           uint64 // 0 means no parent
	ParentSince      uint64
	PriorParentSnaps []uint64
	MySnaps          []uint64
}

// clientSnapRealmView is the selected HEAD inode's realm plus every realm
// record that one complete dump_cache observation carries.
type clientSnapRealmView struct {
	Inode   uint64
	Realm   uint64
	Records map[uint64]snapRealmRecord
}

// clientSnapContext is the derived inherited snapshot context. Snaps is
// sorted descending without duplicates, as build_snap_context orders it.
// Chain lists the realm IDs used, from the selected realm up to the root.
type clientSnapContext struct {
	Seq   uint64
	Snaps []uint64
	Chain []uint64
}

func (c clientSnapContext) hasSnap(id uint64) bool {
	return slices.Contains(c.Snaps, id)
}

func parseSnapHexInode(raw json.RawMessage, field string) (uint64, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, fmt.Errorf("%w: %s is not a string: %.64s", errSnapRealmInvalid, field, raw)
	}
	digits, ok := strings.CutPrefix(s, "0x")
	if !ok {
		return 0, fmt.Errorf("%w: %s %q lacks the 0x prefix", errSnapRealmInvalid, field, s)
	}
	return parseStrictHex(digits, field, s)
}

func parseStrictHex(digits, field, shown string) (uint64, error) {
	if digits == "" || strings.Trim(digits, "0123456789abcdef") != "" {
		return 0, fmt.Errorf("%w: %s %q is not lowercase hex", errSnapRealmInvalid, field, shown)
	}
	v, err := strconv.ParseUint(digits, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %s %q: %v", errSnapRealmInvalid, field, shown, err)
	}
	return v, nil
}

// parseSnapID reads a snapid_t. The reserved head and snapdir values are
// rejected: they never belong in a realm's sequence or snapshot lists.
func parseSnapID(raw json.RawMessage, field string) (uint64, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, fmt.Errorf("%w: %s is not a string: %.64s", errSnapRealmInvalid, field, raw)
	}
	v, err := parseStrictHex(s, field, s)
	if err != nil {
		return 0, err
	}
	if v == snapIDHead || v == snapIDSnapDir {
		return 0, fmt.Errorf("%w: %s %q is a reserved snapshot ID", errSnapRealmInvalid, field, s)
	}
	return v, nil
}

func parseSnapIDList(raw json.RawMessage, field string) ([]uint64, error) {
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, fmt.Errorf("%w: %s is not an array: %.64s", errSnapRealmInvalid, field, raw)
	}
	ids := make([]uint64, 0, len(items))
	for _, item := range items {
		id, err := parseSnapID(item, field)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// requiredField returns a present, non-null member, else an incomplete error.
func requiredField(obj map[string]json.RawMessage, name string) (json.RawMessage, error) {
	raw, ok := obj[name]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, fmt.Errorf("%w: snaprealm.%s missing or null", errSnapRealmIncomplete, name)
	}
	return raw, nil
}

// parseSnapRealmRecord decodes one dumped "snaprealm" object. Every field the
// derivation reads must be present and well-typed.
func parseSnapRealmRecord(raw json.RawMessage) (snapRealmRecord, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
		return snapRealmRecord{}, fmt.Errorf("%w: snaprealm is not an object: %.64s", errSnapRealmInvalid, raw)
	}
	var rec snapRealmRecord
	var err error
	field := func(name string) json.RawMessage {
		if err != nil {
			return nil
		}
		var f json.RawMessage
		f, err = requiredField(obj, name)
		return f
	}
	if f := field("ino"); err == nil {
		rec.Ino, err = parseSnapHexInode(f, "snaprealm.ino")
	}
	if f := field("seq"); err == nil {
		rec.Seq, err = parseSnapID(f, "snaprealm.seq")
	}
	if f := field("parent_ino"); err == nil {
		rec.Parent, err = parseSnapHexInode(f, "snaprealm.parent_ino")
	}
	if f := field("parent_since"); err == nil {
		rec.ParentSince, err = parseSnapID(f, "snaprealm.parent_since")
	}
	if f := field("prior_parent_snaps"); err == nil {
		rec.PriorParentSnaps, err = parseSnapIDList(f, "snaprealm.prior_parent_snaps")
	}
	if f := field("my_snaps"); err == nil {
		rec.MySnaps, err = parseSnapIDList(f, "snaprealm.my_snaps")
	}
	if err != nil {
		return snapRealmRecord{}, err
	}
	return rec, nil
}

func equalSnapRealmRecord(a, b snapRealmRecord) bool {
	return a.Ino == b.Ino && a.Seq == b.Seq && a.Parent == b.Parent && a.ParentSince == b.ParentSince &&
		slices.Equal(a.PriorParentSnaps, b.PriorParentSnaps) && slices.Equal(a.MySnaps, b.MySnaps)
}

// parseClientSnapRealms reads the selected HEAD inode's realm and every realm
// record from one dump_cache observation, which is a bare array or an object
// with a "cache" array. Dentry entries carry no "snapid" and are skipped. A
// realm record repeated by several inodes must be identical; a conflict is
// invalid. A missing inode or snaprealm is incomplete.
func parseClientSnapRealms(data []byte, inode uint64) (clientSnapRealmView, error) {
	var entries []json.RawMessage
	if err := json.Unmarshal(data, &entries); err != nil {
		var wrapped struct {
			Cache *[]json.RawMessage `json:"cache"`
		}
		if json.Unmarshal(data, &wrapped) != nil || wrapped.Cache == nil {
			return clientSnapRealmView{}, fmt.Errorf("%w: dump_cache is neither an array nor a cache object: %v", errSnapRealmInvalid, err)
		}
		entries = *wrapped.Cache
	}
	view := clientSnapRealmView{Inode: inode, Records: map[uint64]snapRealmRecord{}}
	var selected *uint64
	want := fmt.Sprintf("0x%x", inode)
	for _, entry := range entries {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(entry, &obj); err != nil || obj == nil {
			return clientSnapRealmView{}, fmt.Errorf("%w: dump_cache entry is not an object: %.128s", errSnapRealmInvalid, entry)
		}
		snapRaw, isInode := obj["snapid"]
		if !isInode {
			continue
		}
		var snapID, ino string
		if json.Unmarshal(snapRaw, &snapID) != nil {
			return clientSnapRealmView{}, fmt.Errorf("%w: inode snapid is not a string: %.64s", errSnapRealmInvalid, snapRaw)
		}
		if raw, ok := obj["ino"]; !ok || json.Unmarshal(raw, &ino) != nil {
			return clientSnapRealmView{}, fmt.Errorf("%w: inode entry has no string ino", errSnapRealmInvalid)
		}
		realmRaw, hasRealm := obj["snaprealm"]
		if hasRealm && bytes.Equal(bytes.TrimSpace(realmRaw), []byte("null")) {
			hasRealm = false
		}
		isSelected := ino == want && snapID == "head"
		if !hasRealm {
			if isSelected {
				return clientSnapRealmView{}, fmt.Errorf("%w: inode 0x%x has no snaprealm", errSnapRealmIncomplete, inode)
			}
			continue
		}
		rec, err := parseSnapRealmRecord(realmRaw)
		if err != nil {
			return clientSnapRealmView{}, err
		}
		if prev, ok := view.Records[rec.Ino]; ok && !equalSnapRealmRecord(prev, rec) {
			return clientSnapRealmView{}, fmt.Errorf("%w: conflicting records for realm 0x%x", errSnapRealmInvalid, rec.Ino)
		}
		view.Records[rec.Ino] = rec
		if isSelected {
			if selected != nil && *selected != rec.Ino {
				return clientSnapRealmView{}, fmt.Errorf("%w: inode 0x%x appears in realms 0x%x and 0x%x", errSnapRealmInvalid, inode, *selected, rec.Ino)
			}
			realm := rec.Ino
			selected = &realm
		}
	}
	if selected == nil {
		return clientSnapRealmView{}, fmt.Errorf("%w: dump_cache has no HEAD inode 0x%x", errSnapRealmIncomplete, inode)
	}
	view.Realm = *selected
	return view, nil
}

// derive computes the inherited snapshot context of the selected realm the way
// SnapRealm::build_snap_context does. A parent that is not in the observation
// is incomplete (never an empty parent context), and a cycle is invalid.
func (v clientSnapRealmView) derive() (clientSnapContext, error) {
	var chain []uint64
	seen := map[uint64]bool{}
	for r := v.Realm; ; {
		if seen[r] {
			return clientSnapContext{}, fmt.Errorf("%w: parent cycle through realm 0x%x", errSnapRealmInvalid, r)
		}
		seen[r] = true
		rec, ok := v.Records[r]
		if !ok {
			return clientSnapContext{}, fmt.Errorf("%w: no record for realm 0x%x", errSnapRealmIncomplete, r)
		}
		chain = append(chain, r)
		if rec.Parent == 0 {
			break
		}
		r = rec.Parent
	}
	// Build from the root down so each realm sees its parent's context.
	var parent *clientSnapContext
	var ctx clientSnapContext
	for i := len(chain) - 1; i >= 0; i-- {
		rec := v.Records[chain[i]]
		set := map[uint64]bool{}
		for _, s := range rec.PriorParentSnaps {
			set[s] = true
		}
		seq := rec.Seq
		if parent != nil {
			for _, s := range parent.Snaps {
				if s >= rec.ParentSince {
					set[s] = true
				}
			}
			seq = max(seq, parent.Seq)
		}
		for _, s := range rec.MySnaps {
			set[s] = true
		}
		snaps := make([]uint64, 0, len(set))
		for s := range set {
			snaps = append(snaps, s)
		}
		slices.SortFunc(snaps, func(a, b uint64) int {
			switch {
			case a > b:
				return -1
			case a < b:
				return 1
			}
			return 0
		})
		ctx = clientSnapContext{Seq: seq, Snaps: snaps}
		parent = &ctx
	}
	ctx.Chain = chain
	return ctx, nil
}

// snapContextGate reports whether the derived inherited context of the
// inode's realm lists snapID. Only that membership releases the gate: a newer
// local or derived sequence alone does not, and an incomplete or invalid
// observation never releases it (released is false and err explains why).
func snapContextGate(data []byte, inode, snapID uint64) (released bool, ctx clientSnapContext, err error) {
	view, err := parseClientSnapRealms(data, inode)
	if err != nil {
		return false, clientSnapContext{}, err
	}
	ctx, err = view.derive()
	if err != nil {
		return false, clientSnapContext{}, err
	}
	return ctx.hasSnap(snapID), ctx, nil
}
