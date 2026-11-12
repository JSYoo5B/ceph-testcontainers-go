//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_readdir_after_snapdiff_test

import (
	"strings"
	"testing"
)

// at builds a hash-ordered position as the client does: the marker, the
// hash and the count.
func at(name string, hash, count uint64) dirEntry {
	return dirEntry{Name: name, Next: positionHashOrder | hash<<28 | count}
}

// inFragment builds a position of a listing that is not hash-ordered: the
// fragment where a hash would be, no marker.
func inFragment(name string, fragment, count uint64) dirEntry {
	return dirEntry{Name: name, Next: fragment<<28 | count}
}

func TestJudgeAfterSnapdiff(t *testing.T) {
	triggerNames, controlNames := []string{"collision_2379", "collision_2690"}, []string{"plain_first", "plain_second"}
	// The listings of a client without the defect, with the positions
	// that Tentacle 20.2.4 reported.
	triggerBoth := []dirEntry{at("collision_2379", 0x7f4abf, 3), at("collision_2690", 0x7f4abf, 4)}
	controlBoth := []dirEntry{at("plain_first", 0x6769c5, 3), at("plain_second", 0xf51016, 3)}
	good := func() (dirRun, dirRun) {
		return dirRun{Before: triggerBoth, Diff: []diffEntry{{"collision_2690", 3}}, After: triggerBoth},
			dirRun{Before: controlBoth, Diff: []diffEntry{{"plain_second", 5}}, After: controlBoth}
	}
	for name, test := range map[string]struct {
		change                     func(trigger, control *dirRun)
		freshTrigger, freshControl []dirEntry
		want                       string
	}{
		"both listings intact": {want: "absent: after the snapdiff the warm mount lists both snapshots in full"},
		"another order":        {change: func(trigger, _ *dirRun) { trigger.After = []dirEntry{triggerBoth[1], triggerBoth[0]} }, want: "absent: "},
		"diff names both states of the entry": {change: func(trigger, _ *dirRun) {
			trigger.Diff = []diffEntry{{"collision_2690", 2}, {"collision_2690", 3}}
		}, want: "absent: "},
		// What Tentacle 20.2.4 returned: the changed entry is gone from
		// the second listing of the colliding pair.
		"second name missing": {change: func(trigger, _ *dirRun) { trigger.After = triggerBoth[:1] },
			want: "symptom: after a snapdiff that named only collision_2690, the warm mount lists the snapshot whose two names share a dentry hash as 1 entries [collision_2379], missing [collision_2690], more than once [], not of the snapshot [] (positions [collision_2379->7f4abf+3 (raw 0xff7f4abf0000003)], before the snapdiff [collision_2379->7f4abf+3 (raw 0xff7f4abf0000003) collision_2690->7f4abf+4 (raw 0xff7f4abf0000004)])"},
		"first name twice": {change: func(trigger, _ *dirRun) { trigger.After = []dirEntry{triggerBoth[0], triggerBoth[0]} },
			want: "symptom: after a snapdiff that named only collision_2690, the warm mount lists the snapshot whose two names share a dentry hash as 2 entries [collision_2379 collision_2379], missing [collision_2690], more than once [collision_2379 x2]"},
		"both and one again": {change: func(trigger, _ *dirRun) { trigger.After = append(triggerBoth[:2:2], triggerBoth[1]) }, want: "symptom: "},
		"empty":              {change: func(trigger, _ *dirRun) { trigger.After = nil }, want: "symptom: "},
		"a foreign name":     {change: func(trigger, _ *dirRun) { trigger.After = append(triggerBoth[:2:2], at("other", 1, 3)) }, want: "symptom: "},
		// A control listing that is wrong too is not the collision's doing.
		"control damaged as well": {change: func(trigger, control *dirRun) { trigger.After, control.After = triggerBoth[:1], controlBoth[:1] }, want: "another fault: "},
		"control damaged alone":   {change: func(_, control *dirRun) { control.After = controlBoth[:1] }, want: "another fault: "},
		// Nothing is judged before the mounts, the diff and the layout are
		// what the case is about.
		"first listing short":         {change: func(trigger, _ *dirRun) { trigger.Before = triggerBoth[:1] }, want: "condition not established: the first listing of the trigger snapshot"},
		"control first listing short": {change: func(_, control *dirRun) { control.Before = controlBoth[:1] }, want: "condition not established: the first listing of the control snapshot"},
		"fresh mount short":           {freshTrigger: triggerBoth[:1], want: "condition not established: a fresh mount does not list the trigger snapshot"},
		"fresh control short":         {freshControl: controlBoth[:1], want: "condition not established: a fresh mount does not list the control snapshot"},
		"empty diff":                  {change: func(trigger, _ *dirRun) { trigger.Diff = nil }, want: "condition not established: the snapdiff of the trigger directory names no entry"},
		"diff names the unchanged":    {change: func(trigger, _ *dirRun) { trigger.Diff = append(trigger.Diff, diffEntry{"collision_2379", 3}) }, want: "condition not established: the snapdiff of the trigger directory names"},
		"control diff empty":          {change: func(_, control *dirRun) { control.Diff = nil }, want: "condition not established: the snapdiff of the control directory names no entry"},
		// Without the hash-order marker the bits above the count name the
		// fragment. Two entries of one fragment look like a shared hash
		// there, and must not pass as the trigger layout, whatever the
		// second listing holds.
		"trigger listed in fragment order": {change: func(trigger, _ *dirRun) {
			trigger.Before = []dirEntry{inFragment("collision_2379", 0x7f4abf, 3), inFragment("collision_2690", 0x7f4abf, 4)}
			trigger.After = trigger.Before[:1]
		}, want: "condition not established: the first listings are not hash-ordered, so their positions name fragments, not dentry hashes: trigger [collision_2379->frag 7f4abf+3 (raw 0x7f4abf0000003) collision_2690->frag 7f4abf+4 (raw 0x7f4abf0000004)]"},
		"trigger in the leftmost fragment": {change: func(trigger, _ *dirRun) {
			trigger.Before = []dirEntry{inFragment("collision_2379", 0, 3), inFragment("collision_2690", 0, 4)}
			trigger.After = trigger.Before[:1]
		}, want: "condition not established: the first listings are not hash-ordered"},
		"one trigger entry without the marker": {change: func(trigger, _ *dirRun) {
			trigger.Before = []dirEntry{triggerBoth[0], inFragment("collision_2690", 0x7f4abf, 4)}
		}, want: "condition not established: the first listings are not hash-ordered"},
		"control listed in fragment order": {change: func(_, control *dirRun) {
			control.Before = []dirEntry{inFragment("plain_first", 1, 3), inFragment("plain_second", 2, 3)}
		}, want: "condition not established: the first listings are not hash-ordered"},
		"no shared hash": {change: func(trigger, _ *dirRun) {
			trigger.Before = []dirEntry{at("collision_2379", 0x7f4abf, 3), at("collision_2690", 0x7f4ac0, 3)}
		}, want: "condition not established: the two names of the trigger directory do not share a dentry hash"},
		"changed entry first of its hash": {change: func(trigger, _ *dirRun) {
			trigger.Before = []dirEntry{at("collision_2690", 0x7f4abf, 3), at("collision_2379", 0x7f4abf, 4)}
		}, want: "condition not established: the changed entry collision_2690 is not the second of its hash"},
		"control shares a hash": {change: func(_, control *dirRun) {
			control.Before = []dirEntry{at("plain_first", 0x6769c5, 3), at("plain_second", 0x6769c5, 4)}
		}, want: "condition not established: the two names of the control directory share a dentry hash"},
	} {
		trigger, control := good()
		if test.change != nil {
			test.change(&trigger, &control)
		}
		freshTrigger, freshControl := test.freshTrigger, test.freshControl
		if freshTrigger == nil {
			freshTrigger = triggerBoth
		}
		if freshControl == nil {
			freshControl = controlBoth
		}
		symptom, detail, err := judgeAfterSnapdiff(triggerNames, controlNames, "collision_2690", "plain_second", trigger, control, freshTrigger, freshControl)
		got := "absent: " + detail
		switch {
		case err != nil:
			got = err.Error()
		case symptom:
			got = "symptom: " + detail
		}
		if !strings.HasPrefix(got, test.want) {
			t.Errorf("%s: %q, want prefix %q", name, got, test.want)
		}
	}
}

func TestDirEntryHash(t *testing.T) {
	// A hash-ordered position: the marker, the hash, the count.
	hashed := dirEntry{Name: "x", Next: 0xFF<<52 | 0x7f4abf<<28 | 3}
	if !hashed.hashOrdered() || hashed.hashOf() != 0x7f4abf || positions([]dirEntry{hashed}) != "[x->7f4abf+3 (raw 0xff7f4abf0000003)]" {
		t.Fatalf("hash-ordered: %t %x %s", hashed.hashOrdered(), hashed.hashOf(), positions([]dirEntry{hashed}))
	}
	// The same bits without the marker are a fragment, and a marker that
	// is not complete is none.
	for _, next := range []uint64{0x7f4abf<<28 | 3, 0x7F<<52 | 0x7f4abf<<28 | 3, 0xFE<<52 | 3, 3} {
		if e := (dirEntry{Name: "x", Next: next}); e.hashOrdered() || !strings.Contains(positions([]dirEntry{e}), "->frag ") {
			t.Fatalf("position %#x counts as hash-ordered: %s", next, positions([]dirEntry{e}))
		}
	}
}
