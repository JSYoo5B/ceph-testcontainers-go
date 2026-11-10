//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_dangling_symlink_create_test

import (
	"strings"
	"syscall"
	"testing"
)

// createReports returns the reports of a client that keeps the contract in
// every case.
func createReports(cases []createCase) ([]createAct, []createSeen) {
	exists := int(syscall.EEXIST)
	var acts []createAct
	var seen []createSeen
	for i, c := range cases {
		link := createEntry{Type: "symlink", Inode: uint64(100 + i), Target: c.LinkTarget}
		acts = append(acts, createAct{Op: c.Op, Form: c.Form, LinkBefore: link, Occupied: exists, Live: exists, Link: exists})
		fresh := createEntry{Type: createTypes[c.Op], Inode: uint64(200 + i)}
		seen = append(seen, createSeen{
			Op: c.Op, Form: c.Form, Fresh: fresh, Occupied: createEntry{Type: "file", Inode: uint64(300 + i)},
			Live: createEntry{Type: "symlink", Inode: uint64(400 + i), Target: c.LiveTarget},
			Kept: createEntry{Type: "file", Inode: uint64(500 + i)}, Link: link,
		})
	}
	return acts, seen
}

// These cases feed synthetic reports to judgeCreate. They are not native
// evidence of what a client does.
func TestJudgeCreate(t *testing.T) {
	cases := createCases("/root")
	if len(cases) != 8 || cases[0].Op != "link" || cases[1].LinkTarget != "/root/link-absolute/there/absent" {
		t.Fatalf("cases %+v", cases)
	}
	through := func(i int) func(acts []createAct, seen []createSeen) {
		return func(acts []createAct, seen []createSeen) {
			acts[i].Link = 0
			seen[i].Target, seen[i].There = createEntry{Type: createTypes[cases[i].Op], Inode: 900}, []string{"absent"}
		}
	}
	for _, tc := range []struct {
		name   string
		change func(acts []createAct, seen []createSeen)
		want   string
	}{
		{name: "contract"},
		{name: "mkdir through a relative symlink", change: through(2), want: "observed defect: at a dangling symlink, want EEXIST with nothing at its target: mkdir/relative succeeded and made a dir at the symlink's target there/absent"},
		{name: "symlink through an absolute symlink", change: through(7), want: "observed defect: at a dangling symlink, want EEXIST with nothing at its target: symlink/absolute succeeded and made a symlink at the symlink's target /root/symlink-absolute/there/absent"},
		{name: "every case through", change: func(acts []createAct, seen []createSeen) {
			for i := range cases {
				through(i)(acts, seen)
			}
		}, want: "observed defect: at a dangling symlink, want EEXIST with nothing at its target: link/relative succeeded"},
		{name: "success without an entry at the target", change: func(acts []createAct, _ []createSeen) { acts[0].Link = 0 }, want: "another fault: "},
		{name: "EEXIST with an entry at the target", change: func(_ []createAct, seen []createSeen) {
			seen[2].Target, seen[2].There = createEntry{Type: "file", Inode: 900}, []string{"absent"}
		}, want: "another fault: "},
		{name: "another errno", change: func(acts []createAct, _ []createSeen) { acts[3].Link = int(syscall.ENOENT) }, want: "another fault: "},
		{name: "an entry of another type at the target", change: func(acts []createAct, seen []createSeen) {
			through(2)(acts, seen)
			seen[2].Target.Type = "file"
		}, want: "another fault: "},
		{name: "the symlink replaced", change: func(_ []createAct, seen []createSeen) { seen[7].Link.Inode = 7 }, want: "another fault: "},
		{name: "another name beside the target", change: func(_ []createAct, seen []createSeen) { seen[1].There = []string{"other"} }, want: "another fault: "},
		{name: "a fault outranks a defect", change: func(acts []createAct, seen []createSeen) {
			through(2)(acts, seen)
			acts[0].Link = int(syscall.EIO)
		}, want: "another fault: "},
		{name: "unused name refused", change: func(acts []createAct, seen []createSeen) {
			through(2)(acts, seen)
			acts[2].Fresh, seen[2].Fresh = int(syscall.EACCES), createEntry{}
		}, want: "control failed: mkdir/relative at an unused name"},
		{name: "regular file not reported", change: func(acts []createAct, _ []createSeen) { acts[4].Occupied = 0 }, want: "control failed: mknod/relative at a regular file"},
		{name: "regular file replaced", change: func(_ []createAct, seen []createSeen) { seen[3].Occupied.Type = "dir" }, want: "control failed: mkdir/absolute at a regular file"},
		{name: "live symlink not reported", change: func(acts []createAct, _ []createSeen) { acts[7].Live = 0 }, want: "control failed: symlink/absolute at a symlink to an existing file"},
		{name: "live symlink's file gone", change: func(_ []createAct, seen []createSeen) { seen[1].Kept = createEntry{} }, want: "control failed: link/absolute at a symlink to an existing file"},
		{name: "symlink not dangling", change: func(acts []createAct, seen []createSeen) {
			through(2)(acts, seen)
			acts[2].TargetBefore = createEntry{Type: "dir", Inode: 9}
		}, want: "condition not established: mkdir/relative: before the operation"},
		{name: "symlink with another target", change: func(acts []createAct, _ []createSeen) { acts[4].LinkBefore.Target = "there" }, want: "condition not established: mknod/relative"},
		{name: "symlink missing", change: func(acts []createAct, _ []createSeen) { acts[0].LinkBefore = createEntry{} }, want: "condition not established: link/relative"},
		{name: "reports of another case", change: func(acts []createAct, _ []createSeen) { acts[1].Op = "mkdir" }, want: "condition not established: link/absolute: reports of"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			acts, seen := createReports(cases)
			if tc.change != nil {
				tc.change(acts, seen)
			}
			err := judgeCreate(cases, acts, seen)
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.HasPrefix(err.Error(), tc.want)) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
		})
	}
	acts, seen := createReports(cases)
	if err := judgeCreate(cases, acts[1:], seen); err == nil || !strings.HasPrefix(err.Error(), "condition not established: 8 cases, 7 acted on and 8 observed") {
		t.Fatalf("error %v", err)
	}
}
