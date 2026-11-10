//go:build all || integration

package rbd_trash_purge_timezone_test

import (
	"fmt"
	"slices"
)

// trashPurgeEntry is one image in a pool's trash, with its deadline in
// seconds since the epoch.
type trashPurgeEntry struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Deadline int64  `json:"deadline"`
}

// trashPurgeName names a trash entry by its image name and its ID.
func trashPurgeName(entry trashPurgeEntry) string { return entry.Name + "(" + entry.ID + ")" }

// trashPurgeNames names the entries, in their order.
func trashPurgeNames(entries []trashPurgeEntry) []string {
	var names []string
	for _, entry := range entries {
		names = append(names, trashPurgeName(entry))
	}
	return names
}

// trashPurgeKept names the entries a purge with the cutoff at must keep:
// those whose deferment ends after it.
func trashPurgeKept(entries []trashPurgeEntry, at int64) []string {
	var kept []string
	for _, entry := range entries {
		if entry.Deadline > at {
			kept = append(kept, trashPurgeName(entry))
		}
	}
	return kept
}

// judgeTrashPurge compares what a purge left in a pool's trash with two
// predictions made from the trash before it: the entries the cutoff instant
// keeps, and the entries kept by mktime, the instant that the cutoff's
// fields name when they are read as the client's local time. offset is the
// client's UTC offset in seconds.
//
// A purge that left the first set passes. Any other result from a client in
// UTC, where the two predictions are one, is a failed control. From another
// timezone only the second set is the timezone shift under test; a result
// that matches neither prediction is reported as another fault and not
// attributed to the shift.
func judgeTrashPurge(offset, cutoff, mktime int64, before, after []trashPurgeEntry) error {
	retained, want := trashPurgeNames(after), trashPurgeKept(before, cutoff)
	if slices.Equal(retained, want) {
		return nil
	}
	if offset == 0 {
		return fmt.Errorf("control failed: a client process in UTC purged with the cutoff %d: the trash retains %v, and by that instant it must retain %v", cutoff, retained, want)
	}
	shifted := trashPurgeKept(before, mktime)
	if mktime == cutoff || !slices.Equal(retained, shifted) {
		return fmt.Errorf("another fault: a client process %+d h from UTC purged with the cutoff %d: the trash retains %v, which is neither what that instant keeps (%v) nor what the cutoff's fields read as local time, %+d h from it, keep (%v)",
			offset/3600, cutoff, retained, want, (mktime-cutoff)/3600, shifted)
	}
	return fmt.Errorf("observed defect: a client process %+d h from UTC purged with a cutoff %+d h from the instant it passed: the trash retains %v, which is what the cutoff's fields read as local time keep, and by the instant it must retain %v",
		offset/3600, (mktime-cutoff)/3600, retained, want)
}
