//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_readdir_after_snapdiff_test

import (
	"strings"
	"testing"
)

// at builds a hash-ordered position: the hash above 28 bits of count.
func at(name string, hash, count uint64) dirEntry {
	return dirEntry{Name: name, Next: hash<<28 | count}
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
		"both listings intact": {want: ""},
		"another order":        {change: func(trigger, _ *dirRun) { trigger.After = []dirEntry{triggerBoth[1], triggerBoth[0]} }, want: ""},
		"diff names both states of the entry": {change: func(trigger, _ *dirRun) {
			trigger.Diff = []diffEntry{{"collision_2690", 2}, {"collision_2690", 3}}
		}, want: ""},
		// What Tentacle 20.2.4 returned: the changed entry is gone from
		// the second listing of the colliding pair.
		"second name missing": {change: func(trigger, _ *dirRun) { trigger.After = triggerBoth[:1] },
			want: "observed defect: after a snapdiff that named only collision_2690, the warm mount lists the snapshot whose two names share a dentry hash as 1 entries [collision_2379], missing [collision_2690], more than once [], not of the snapshot []"},
		"first name twice": {change: func(trigger, _ *dirRun) { trigger.After = []dirEntry{triggerBoth[0], triggerBoth[0]} },
			want: "observed defect: after a snapdiff that named only collision_2690, the warm mount lists the snapshot whose two names share a dentry hash as 2 entries [collision_2379 collision_2379], missing [collision_2690], more than once [collision_2379 x2]"},
		"both and one again": {change: func(trigger, _ *dirRun) { trigger.After = append(triggerBoth[:2:2], triggerBoth[1]) }, want: "observed defect: "},
		"empty":              {change: func(trigger, _ *dirRun) { trigger.After = nil }, want: "observed defect: "},
		"a foreign name":     {change: func(trigger, _ *dirRun) { trigger.After = append(triggerBoth[:2:2], at("other", 1, 3)) }, want: "observed defect: "},
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
		got := judgeAfterSnapdiff(triggerNames, controlNames, "collision_2690", "plain_second", trigger, control, freshTrigger, freshControl)
		if test.want == "" && got != "" || test.want != "" && !strings.HasPrefix(got, test.want) {
			t.Errorf("%s: %q, want prefix %q", name, got, test.want)
		}
	}
}

func TestDirEntryHash(t *testing.T) {
	// The position 0x7f4abf0000003 with the flag bits of a hash-ordered
	// position above it: the hash is the 24 bits above the count.
	entry := dirEntry{Name: "x", Next: 0xFF<<52 | 0x7f4abf<<28 | 3}
	if entry.hashOf() != 0x7f4abf || positions([]dirEntry{entry}) != "[x->7f4abf+3]" {
		t.Fatalf("hash %x, positions %s", entry.hashOf(), positions([]dirEntry{entry}))
	}
}
