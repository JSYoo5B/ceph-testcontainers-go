//go:build all || integration

package cephfs_readdir_after_snapdiff_test

import (
	"errors"
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

// A libcephfs directory position has a count in its low 28 bits and, above
// them, either a dentry hash or a fragment. The client marks a hash-ordered
// position by setting all eight bits above the 24 of the hash, which no
// fragment has; without the marker the same bits name the fragment that
// the entry was read from, and two entries of one fragment carry the same
// value there whatever their names hash to.
const (
	positionCountBits = 28
	positionHashBits  = 24
	positionHashOrder = uint64(0xFF) << (positionCountBits + positionHashBits)
)

// hashOrdered reports whether the position carries the marker of a
// hash-ordered listing. Only then is hashOf a dentry hash.
func (e dirEntry) hashOrdered() bool { return e.Next&positionHashOrder == positionHashOrder }

// hashOf is the 24 bits above the count: the dentry hash of a hash-ordered
// position, and the fragment of any other.
func (e dirEntry) hashOf() uint64 { return e.Next >> positionCountBits & (1<<positionHashBits - 1) }

// allHashOrdered reports whether every entry's position is hash-ordered.
func allHashOrdered(entries []dirEntry) bool {
	for _, e := range entries {
		if !e.hashOrdered() {
			return false
		}
	}
	return true
}

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
// listing must hold exactly the snapshot's names, each once. A trigger
// listing that does not, with the control listed in full, is the symptom;
// both listings in full are its absence. Its error starts with "condition
// not established" when the mounts, the snapdiff or the layout are not what
// the case is about, and with "another fault" when the control listing is
// wrong too: no verdict.
func judgeAfterSnapdiff(triggerNames, controlNames []string, changedTrigger, changedControl string, trigger, control dirRun, freshTrigger, freshControl []dirEntry) (symptom bool, detail string, err error) {
	for _, verdict := range []string{
		established("trigger", triggerNames, changedTrigger, trigger, freshTrigger),
		established("control", controlNames, changedControl, control, freshControl),
	} {
		if verdict != "" {
			return false, "", errors.New(verdict)
		}
	}
	// The layout that the case is about, read from the positions the
	// client itself gave for the first listings. A position says something
	// about a dentry hash only when it is hash-ordered.
	if !allHashOrdered(trigger.Before) || !allHashOrdered(control.Before) {
		return false, "", fmt.Errorf("condition not established: the first listings are not hash-ordered, so their positions name fragments, not dentry hashes: trigger %s, control %s",
			positions(trigger.Before), positions(control.Before))
	}
	if len(trigger.Before) != 2 || trigger.Before[0].hashOf() != trigger.Before[1].hashOf() {
		return false, "", fmt.Errorf("condition not established: the two names of the trigger directory do not share a dentry hash: positions %s", positions(trigger.Before))
	}
	if trigger.Before[1].Name != changedTrigger {
		return false, "", fmt.Errorf("condition not established: the changed entry %s is not the second of its hash in the listing: %s", changedTrigger, positions(trigger.Before))
	}
	if len(control.Before) != 2 || control.Before[0].hashOf() == control.Before[1].hashOf() {
		return false, "", fmt.Errorf("condition not established: the two names of the control directory share a dentry hash: positions %s", positions(control.Before))
	}
	triggerOK := sameNames(triggerNames, entryNames(trigger.After))
	controlOK := sameNames(controlNames, entryNames(control.After))
	switch {
	case triggerOK && controlOK:
		return false, fmt.Sprintf("after the snapdiff the warm mount lists both snapshots in full: trigger %s, control %s", positions(trigger.After), positions(control.After)), nil
	case !controlOK:
		return false, "", fmt.Errorf("another fault: after a snapdiff the warm mount lists the control snapshot, whose names share no dentry hash, as %s (positions %s); the trigger snapshot as %s",
			describe(controlNames, entryNames(control.After)), positions(control.After), describe(triggerNames, entryNames(trigger.After)))
	}
	return true, fmt.Sprintf("after a snapdiff that named only %s, the warm mount lists the snapshot whose two names share a dentry hash as %s (positions %s, before the snapdiff %s); a fresh mount lists both names, and the control snapshot is listed in full",
		changedTrigger, describe(triggerNames, entryNames(trigger.After)), positions(trigger.After), positions(trigger.Before)), nil
}

// positions prints each entry with its next position: the hash (or, in a
// position without the hash-order marker, the fragment, shown as "frag")
// and the count, and the raw value as the client gave it.
func positions(entries []dirEntry) string {
	parts := make([]string, len(entries))
	for i, e := range entries {
		kind := "frag "
		if e.hashOrdered() {
			kind = ""
		}
		parts[i] = fmt.Sprintf("%s->%s%06x+%d (raw %#x)", e.Name, kind, e.hashOf(), e.Next&(1<<positionCountBits-1), e.Next)
	}
	return "[" + strings.Join(parts, " ") + "]"
}
