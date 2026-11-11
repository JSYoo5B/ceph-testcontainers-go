//go:build all || integration

package cephfs_readdir_after_snapdiff_test

import (
	"fmt"
	"slices"
	"strings"
)

// dirEntry is one entry of a libcephfs listing: its name and the position
// the client gave as the next one (d_off).
type dirEntry struct {
	Name string `json:"name"`
	Next uint64 `json:"next"`
}

// hashOf is the dentry hash in a hash-ordered directory position: the 24
// bits above the 28 bits that count entries of one hash.
func (e dirEntry) hashOf() uint64 { return e.Next >> 28 & 0xFFFFFF }

// diffEntry is one entry of a snapdiff stream: the name and the snapshot
// the entry's state belongs to.
type diffEntry struct {
	Name string `json:"name"`
	Snap uint64 `json:"snap"`
}

// dirRun is what one mount did with one directory: the listing of its
// second snapshot, the snapdiff between its two snapshots, and the listing
// of the second snapshot again, entry by entry through a new handle.
type dirRun struct {
	Before []dirEntry  `json:"before"`
	Diff   []diffEntry `json:"diff"`
	After  []dirEntry  `json:"after"`
}

func entryNames(entries []dirEntry) []string {
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	return names
}

// sameNames reports whether got holds exactly the names of want, each once,
// in any order.
func sameNames(want, got []string) bool {
	a, b := slices.Clone(want), slices.Clone(got)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

// describe tells membership and multiplicity of got against want.
func describe(want, got []string) string {
	missing, extra := []string{}, []string{}
	count := map[string]int{}
	for _, name := range got {
		count[name]++
	}
	for _, name := range want {
		if count[name] == 0 {
			missing = append(missing, name)
		}
	}
	twice := []string{}
	for name, n := range count {
		if !slices.Contains(want, name) {
			extra = append(extra, name)
		} else if n > 1 {
			twice = append(twice, fmt.Sprintf("%s x%d", name, n))
		}
	}
	slices.Sort(extra)
	slices.Sort(twice)
	return fmt.Sprintf("%d entries [%s], missing %v, more than once %v, not of the snapshot %v", len(got), strings.Join(got, " "), missing, twice, extra)
}

// established checks what must hold before the second listing means
// anything: the first listing on the warm mount and the listing of a fresh
// mount are the snapshot's names, and the snapdiff names exactly the
// changed entry, in the second snapshot's state at least.
func established(label string, want []string, changed string, run dirRun, fresh []dirEntry) string {
	if !sameNames(want, entryNames(run.Before)) {
		return fmt.Sprintf("condition not established: the first listing of the %s snapshot on the warm mount is not the snapshot: %s", label, describe(want, entryNames(run.Before)))
	}
	if !sameNames(want, entryNames(fresh)) {
		return fmt.Sprintf("condition not established: a fresh mount does not list the %s snapshot: %s", label, describe(want, entryNames(fresh)))
	}
	if len(run.Diff) == 0 {
		return fmt.Sprintf("condition not established: the snapdiff of the %s directory names no entry, want %s", label, changed)
	}
	for _, entry := range run.Diff {
		if entry.Name != changed {
			return fmt.Sprintf("condition not established: the snapdiff of the %s directory names %v, want only %s", label, run.Diff, changed)
		}
	}
	return ""
}

// judgeAfterSnapdiff judges the listings of the second snapshot that the
// warm mount read after its snapdiff. trigger is the directory whose two
// names share a dentry hash; control is the one whose names do not. Each
// listing must hold exactly the snapshot's names, each once.
func judgeAfterSnapdiff(triggerNames, controlNames []string, changedTrigger, changedControl string, trigger, control dirRun, freshTrigger, freshControl []dirEntry) string {
	for _, verdict := range []string{
		established("trigger", triggerNames, changedTrigger, trigger, freshTrigger),
		established("control", controlNames, changedControl, control, freshControl),
	} {
		if verdict != "" {
			return verdict
		}
	}
	// The layout that the case is about, read from the positions the
	// client itself gave for the first listings.
	if len(trigger.Before) != 2 || trigger.Before[0].hashOf() != trigger.Before[1].hashOf() {
		return fmt.Sprintf("condition not established: the two names of the trigger directory do not share a dentry hash: positions %s", positions(trigger.Before))
	}
	if trigger.Before[1].Name != changedTrigger {
		return fmt.Sprintf("condition not established: the changed entry %s is not the second of its hash in the listing: %s", changedTrigger, positions(trigger.Before))
	}
	if len(control.Before) != 2 || control.Before[0].hashOf() == control.Before[1].hashOf() {
		return fmt.Sprintf("condition not established: the two names of the control directory share a dentry hash: positions %s", positions(control.Before))
	}
	triggerOK := sameNames(triggerNames, entryNames(trigger.After))
	controlOK := sameNames(controlNames, entryNames(control.After))
	switch {
	case triggerOK && controlOK:
		return ""
	case !controlOK:
		return fmt.Sprintf("another fault: after a snapdiff the warm mount lists the control snapshot, whose names share no dentry hash, as %s (positions %s); the trigger snapshot as %s",
			describe(controlNames, entryNames(control.After)), positions(control.After), describe(triggerNames, entryNames(trigger.After)))
	}
	return fmt.Sprintf("observed defect: after a snapdiff that named only %s, the warm mount lists the snapshot whose two names share a dentry hash as %s (positions %s, before the snapdiff %s); a fresh mount lists both names, and the control snapshot is listed in full",
		changedTrigger, describe(triggerNames, entryNames(trigger.After)), positions(trigger.After), positions(trigger.Before))
}

// positions prints each entry with the hash and the count of its next
// position.
func positions(entries []dirEntry) string {
	parts := make([]string, len(entries))
	for i, e := range entries {
		parts[i] = fmt.Sprintf("%s->%06x+%d", e.Name, e.hashOf(), e.Next&(1<<28-1))
	}
	return "[" + strings.Join(parts, " ") + "]"
}
