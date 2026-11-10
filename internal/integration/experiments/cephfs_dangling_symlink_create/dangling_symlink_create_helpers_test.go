//go:build all || integration

package cephfs_dangling_symlink_create_test

import (
	"fmt"
	"slices"
	"strings"
	"syscall"
)

// createTypes are the operations of the case and the type of the entry each
// one makes.
var createTypes = map[string]string{"mkdir": "dir", "symlink": "symlink", "link": "file", "mknod": "file"}

// createPayload is the text of the symlinks the symlink operation makes.
const createPayload = "made-by-the-case"

// createCase is one operation with one form of symlink target. Its
// directory holds: "there", an empty directory; "link", a symlink to
// "there/absent", which does not exist; "kept", "occupied" and "source",
// regular files; and "live", a symlink to "kept".
type createCase struct {
	Op         string `json:"op"`
	Form       string `json:"form"`
	Dir        string `json:"dir"`
	LinkTarget string `json:"link_target"`
	LiveTarget string `json:"live_target"`
}

// createCases returns every operation with a relative and an absolute
// symlink target, below root.
func createCases(root string) []createCase {
	var cases []createCase
	for _, op := range slices.Sorted(func(yield func(string) bool) {
		for op := range createTypes {
			if !yield(op) {
				return
			}
		}
	}) {
		dir := op + "-relative"
		cases = append(cases, createCase{Op: op, Form: "relative", Dir: dir, LinkTarget: "there/absent", LiveTarget: "kept"})
		dir = op + "-absolute"
		cases = append(cases, createCase{Op: op, Form: "absolute", Dir: dir, LinkTarget: root + "/" + dir + "/there/absent", LiveTarget: root + "/" + dir + "/kept"})
	}
	return cases
}

// createEntry is what lstat and readlink report of one name. An empty Type
// means that the name does not exist.
type createEntry struct {
	Type   string `json:"type"`
	Inode  uint64 `json:"inode"`
	Target string `json:"target"`
}

// createAct is what the acting client reports of one case: the symlink and
// its target before the operation, and the errno (0 for success) of the
// operation at an unused name, at a regular file, at a symlink to an
// existing file and at the dangling symlink.
type createAct struct {
	Op           string      `json:"op"`
	Form         string      `json:"form"`
	LinkBefore   createEntry `json:"link_before"`
	TargetBefore createEntry `json:"target_before"`
	Fresh        int         `json:"fresh"`
	Occupied     int         `json:"occupied"`
	Live         int         `json:"live"`
	Link         int         `json:"link"`
}

// createSeen is what a client mounted after the operations reports of one
// case's names, with the names in "there".
//
// made reports whether an entry is what the case's operation makes: an entry
// of the operation's type that, for link, has the inode of "source" and,
// for symlink, the text the operation was given. A directory and a node
// carry nothing else of the call that made them.
type createSeen struct {
	Op       string      `json:"op"`
	Form     string      `json:"form"`
	Fresh    createEntry `json:"fresh"`
	Occupied createEntry `json:"occupied"`
	Live     createEntry `json:"live"`
	Kept     createEntry `json:"kept"`
	Source   createEntry `json:"source"`
	Link     createEntry `json:"link"`
	Target   createEntry `json:"target"`
	There    []string    `json:"there"`
}

func (s createSeen) made(entry createEntry) bool {
	switch {
	case entry.Type != createTypes[s.Op]:
		return false
	case s.Op == "link":
		return s.Source.Type == "file" && entry.Inode == s.Source.Inode
	case s.Op == "symlink":
		return entry.Target == createPayload
	}
	return true
}

// judgeCreate compares the reports with the cases. Its error starts with
// "condition not established" when a dangling symlink was not in place
// before its operation, with "control failed" when an operation did not
// create at an unused name or did not report EEXIST at a regular file or at
// a symlink to an existing file, with "another fault" when a result at a
// dangling symlink is neither the contract nor the defect, and with
// "observed defect" when the operation succeeded at the dangling symlink
// and its entry is at the symlink's target. An entry counts as the
// operation's only by createSeen.made: one of the same type that is not a
// link of "source", or a symlink with another text, is not attributed to
// it. The first of these that applies
// to any case is reported, with every case it applies to.
func judgeCreate(cases []createCase, acts []createAct, seen []createSeen) error {
	if len(acts) != len(cases) || len(seen) != len(cases) {
		return fmt.Errorf("condition not established: %d cases, %d acted on and %d observed", len(cases), len(acts), len(seen))
	}
	exists := int(syscall.EEXIST)
	var conditions, controls, faults, defects []string
	for i, c := range cases {
		act, saw, name, made := acts[i], seen[i], c.Op+"/"+c.Form, createTypes[c.Op]
		if act.Op != c.Op || act.Form != c.Form || saw.Op != c.Op || saw.Form != c.Form {
			conditions = append(conditions, fmt.Sprintf("%s: reports of %s/%s and %s/%s", name, act.Op, act.Form, saw.Op, saw.Form))
			continue
		}
		if act.LinkBefore.Type != "symlink" || act.LinkBefore.Target != c.LinkTarget || act.TargetBefore.Type != "" {
			conditions = append(conditions, fmt.Sprintf("%s: before the operation the symlink is %+v and its target %+v", name, act.LinkBefore, act.TargetBefore))
		}
		if act.Fresh != 0 || !saw.made(saw.Fresh) {
			controls = append(controls, fmt.Sprintf("%s at an unused name: errno %d, entry %+v, want its %s (source %+v, symlink text %q)", name, act.Fresh, saw.Fresh, made, saw.Source, createPayload))
		}
		if act.Occupied != exists || saw.Occupied.Type != "file" {
			controls = append(controls, fmt.Sprintf("%s at a regular file: errno %d, entry %+v, want EEXIST and the file", name, act.Occupied, saw.Occupied))
		}
		if act.Live != exists || saw.Live.Type != "symlink" || saw.Live.Target != c.LiveTarget || saw.Kept.Type != "file" {
			controls = append(controls, fmt.Sprintf("%s at a symlink to an existing file: errno %d, symlink %+v, file %+v, want EEXIST and both", name, act.Live, saw.Live, saw.Kept))
		}
		kept := saw.Link == act.LinkBefore
		switch {
		case act.Link == exists && kept && saw.Target.Type == "" && len(saw.There) == 0:
		case act.Link == 0 && kept && saw.made(saw.Target) && slices.Equal(saw.There, []string{"absent"}):
			defects = append(defects, fmt.Sprintf("%s succeeded and made a %s at the symlink's target %s", name, made, c.LinkTarget))
		default:
			faults = append(faults, fmt.Sprintf("%s: errno %d, symlink %+v (before %+v), target %+v (source %+v), names beside the target %v", name, act.Link, saw.Link, act.LinkBefore, saw.Target, saw.Source, saw.There))
		}
	}
	for _, found := range []struct {
		prefix string
		cases  []string
	}{
		{"condition not established", conditions}, {"control failed", controls},
		{"another fault: at a dangling symlink, want EEXIST with the symlink kept and nothing at its target", faults},
		{"observed defect: at a dangling symlink, want EEXIST with nothing at its target", defects},
	} {
		if len(found.cases) > 0 {
			return fmt.Errorf("%s: %s", found.prefix, strings.Join(found.cases, "; "))
		}
	}
	return nil
}
