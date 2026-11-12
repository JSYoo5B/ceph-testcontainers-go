//go:build all || integration

package cephfs_snapshot_readdir_eviction_test

import (
	"fmt"
	"slices"
	"strings"
)

const (
	// snapshotEntries is the number of files in the snapshot and, with other
	// names, in the live directory afterwards: more than one readdir reply
	// holds.
	snapshotEntries = 3000
	// snapshotOthers is the number of files elsewhere that the listing
	// client looks up between two reads of the snapshot.
	snapshotOthers = 2000
	// snapshotFirst is the number of snapshot entries read before that.
	snapshotFirst = 50
	// snapshotCache is the client_cache_size of the trigger.
	snapshotCache = 100
	// snapshotPrefix and livePrefix start the names of the snapshot's files
	// and of the files that replace them in the live directory.
	snapshotPrefix, livePrefix = "snap-", "live-"
)

// snapshotNames returns n names with a prefix.
func snapshotNames(prefix string, n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("%s%04d", prefix, i)
	}
	return names
}

// snapshotListing is what one client reports of reading the snapshot
// directory through one handle, with lookups elsewhere after the first
// entries, and of listing the live directory afterwards. Requests counts the
// readdir requests the client sent while the handle was open, HeadRequests
// those of them whose path does not name the snapshot, and Trimmed says
// whether the client dropped its dentry of the snapshot between the first
// entries and the rest. Evidence holds a few of the client's log lines for
// the last two.
type snapshotListing struct {
	Client       uint64   `json:"client"`
	Cache        int      `json:"cache"`
	First        int      `json:"first"`
	Stats        int      `json:"stats"`
	Names        []string `json:"names"`
	Live         []string `json:"live"`
	Requests     int      `json:"requests"`
	HeadRequests int      `json:"head_requests"`
	Trimmed      bool     `json:"trimmed"`
	Evidence     []string `json:"evidence"`
}

// snapshotDifference compares a listing with the names it must hold, each
// once.
type snapshotDifference struct {
	Returned, Found int
	Missing         []string
	Duplicate       []string
	Live            []string
	Unknown         []string
}

func (d snapshotDifference) exact() bool {
	return len(d.Missing)+len(d.Duplicate)+len(d.Live)+len(d.Unknown) == 0
}

func (d snapshotDifference) String() string {
	few := func(names []string) string {
		if len(names) > 3 {
			return fmt.Sprintf("%d (%s, ...)", len(names), strings.Join(names[:3], ", "))
		}
		return fmt.Sprintf("%d %v", len(names), names)
	}
	return fmt.Sprintf("%d entries returned, %d of the wanted names found, missing %s, returned twice %s, names of the other directory state %s, unknown %s",
		d.Returned, d.Found, few(d.Missing), few(d.Duplicate), few(d.Live), few(d.Unknown))
}

// snapshotDiffer compares got with want; other holds the names of the other
// state of the directory (the live names for a snapshot listing).
func snapshotDiffer(want, other, got []string) snapshotDifference {
	d := snapshotDifference{Returned: len(got)}
	seen := map[string]int{}
	for _, name := range got {
		seen[name]++
	}
	for _, name := range want {
		switch n := seen[name]; {
		case n == 0:
			d.Missing = append(d.Missing, name)
		case n > 1:
			d.Duplicate = append(d.Duplicate, name)
			d.Found++
		default:
			d.Found++
		}
		delete(seen, name)
	}
	for _, name := range other {
		if seen[name] > 0 {
			d.Live = append(d.Live, name)
			delete(seen, name)
		}
	}
	for name := range seen {
		d.Unknown = append(d.Unknown, name)
	}
	slices.Sort(d.Unknown)
	return d
}

// snapshotFirstFault says what keeps the first entries of a listing, in the
// order the client returned them, from being distinct names of want, or ""
// when they are. A listing keeps the order of its handle, so these are the
// entries read before the lookups elsewhere.
func snapshotFirstFault(want, names []string, first int) string {
	if len(names) < first {
		return fmt.Sprintf("the listing holds %d entries", len(names))
	}
	wanted := make(map[string]bool, len(want))
	for _, name := range want {
		wanted[name] = true
	}
	seen := map[string]bool{}
	for i, name := range names[:first] {
		switch {
		case !wanted[name]:
			return fmt.Sprintf("entry %d is %q", i, name)
		case seen[name]:
			return fmt.Sprintf("entry %d returns %q again", i, name)
		}
		seen[name] = true
	}
	return ""
}

// judgeSnapshotListings compares the listings of a client with the default
// cache (control) and of one with a cache of snapshotCache entries (trigger)
// with the names of the snapshot and of the live directory. Its error
// starts with "condition not established" when a client did not read as the
// case requires or the trigger client kept its dentry of the snapshot, with
// "control failed" when the control client's snapshot listing or either
// live listing is not exact, with "another fault" when the entries the
// trigger client read before it dropped the dentry are not distinct names of
// the snapshot or its snapshot listing is wrong without holding a live name:
// no verdict. Entries read after the dentry was dropped that hold live names
// are the symptom; an exact trigger listing is its absence.
func judgeSnapshotListings(want, live []string, control, trigger snapshotListing) (symptom bool, detail string, err error) {
	for _, listing := range []struct {
		name string
		got  snapshotListing
	}{{"control", control}, {"trigger", trigger}} {
		if listing.got.First != snapshotFirst || listing.got.Stats != snapshotOthers || listing.got.Requests < 2 {
			return false, "", fmt.Errorf("condition not established: the %s client read %d entries, looked up %d files elsewhere and sent %d readdir requests for the snapshot, want %d, %d and at least 2",
				listing.name, listing.got.First, listing.got.Stats, listing.got.Requests, snapshotFirst, snapshotOthers)
		}
	}
	if control.Client == trigger.Client {
		return false, "", fmt.Errorf("condition not established: both listings are of client %d", control.Client)
	}
	if control.Cache < len(want)+snapshotOthers || control.Trimmed {
		return false, "", fmt.Errorf("condition not established: the control client has a cache of %d entries and dropped its dentry of the snapshot: %t; want room for %d entries and the dentry kept",
			control.Cache, control.Trimmed, len(want)+snapshotOthers)
	}
	if trigger.Cache != snapshotCache || !trigger.Trimmed {
		return false, "", fmt.Errorf("condition not established: the trigger client has a cache of %d entries and dropped its dentry of the snapshot between the first entries and the rest: %t; want %d and the dentry dropped",
			trigger.Cache, trigger.Trimmed, snapshotCache)
	}
	if d := snapshotDiffer(want, live, control.Names); !d.exact() {
		return false, "", fmt.Errorf("control failed: the snapshot listing of the client with the default cache: %s", d)
	}
	for _, listing := range []struct {
		name string
		got  snapshotListing
	}{{"control", control}, {"trigger", trigger}} {
		if d := snapshotDiffer(live, want, listing.got.Live); !d.exact() {
			return false, "", fmt.Errorf("control failed: the live listing of the %s client: %s", listing.name, d)
		}
	}
	if fault := snapshotFirstFault(want, trigger.Names, trigger.First); fault != "" {
		return false, "", fmt.Errorf("another fault: the trigger client's first %d entries, read before it dropped its dentry of the snapshot, are not distinct names of the snapshot: %s", trigger.First, fault)
	}
	d := snapshotDiffer(want, live, trigger.Names)
	switch {
	case d.exact():
		return false, fmt.Sprintf("the trigger client dropped its dentry of the snapshot and still listed exactly the snapshot: %s; %d of its %d readdir requests did not name the snapshot", d, trigger.HeadRequests, trigger.Requests), nil
	case len(d.Live) == 0:
		return false, "", fmt.Errorf("another fault: the snapshot listing after the client dropped its dentry of the snapshot holds no live name and is not the snapshot: %s; %d of its %d readdir requests did not name the snapshot", d, trigger.HeadRequests, trigger.Requests)
	}
	return true, fmt.Sprintf("the first %d entries are names of the snapshot, and the entries read after the client dropped its dentry of the snapshot hold names of the live directory: %s; %d of its %d readdir requests did not name the snapshot", trigger.First, d, trigger.HeadRequests, trigger.Requests), nil
}
