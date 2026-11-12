//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_statfs_mixed_quota_test

import (
	"strings"
	"testing"
)

// These cases feed synthetic reports to judgeStatfs. They are not native
// statfs observations.
func TestJudgeStatfs(t *testing.T) {
	const pool = 2400
	report := func(change func(*statfsReport)) statfsReport {
		r := statfsReport{Client: 4242, Quota: "true", QuotaDF: "true", Converged: true, Polls: []statfsPoll{{}}}
		for i, spec := range statfsLayout {
			dir := statfsDir{Path: spec.Path, Inode: uint64(1099511627776 + i), MaxBytes: spec.MaxBytes, BytesSet: spec.MaxBytes != 0 || spec.MaxFiles != 0,
				MaxFiles: spec.MaxFiles, FilesSet: spec.MaxBytes != 0 || spec.MaxFiles != 0, RBytes: spec.RBytes, RFiles: 1, BSize: statfsBlock,
				Blocks: pool, BFree: pool - 9, BAvail: pool - 9}
			switch spec.ByteRoot {
			case "parent":
				dir.Blocks, dir.BFree, dir.BAvail = 16, 13, 13
			case "":
			default:
				dir.Blocks, dir.BFree, dir.BAvail = 16, 15, 15
			}
			r.Dirs = append(r.Dirs, dir)
		}
		if change != nil {
			change(&r)
		}
		return r
	}
	dir := func(r *statfsReport, path string) *statfsDir {
		for i := range r.Dirs {
			if r.Dirs[i].Path == path {
				return &r.Dirs[i]
			}
		}
		t.Fatalf("no directory %s", path)
		return nil
	}
	poolAt := func(path string) func(*statfsReport) {
		return func(r *statfsReport) { d := dir(r, path); d.Blocks, d.BFree, d.BAvail = pool, pool-9, pool-9 }
	}
	for name, test := range map[string]struct {
		change func(*statfsReport)
		want   string
	}{
		"every directory reports its byte quota":    {want: "absent: every directory below a byte quota"},
		"the child reports the pool":                {change: poolAt("parent/child"), want: "symptom: [parent/child reports 2400 blocks"},
		"the child reports another capacity":        {change: func(r *statfsReport) { dir(r, "parent/child").Blocks = 15 }, want: "symptom: "},
		"the child reports stale free blocks":       {change: func(r *statfsReport) { d := dir(r, "parent/child"); d.BFree, d.BAvail = 16, 16 }, want: "symptom: "},
		"the plain child reports the pool":          {change: poolAt("parent/plain"), want: "control failed"},
		"the parent reports the pool":               {change: poolAt("parent"), want: "control failed"},
		"the same-directory quotas report the pool": {change: poolAt("same"), want: "control failed"},
		"a control and the child report the pool":   {change: func(r *statfsReport) { poolAt("bytes-only")(r); poolAt("parent/child")(r) }, want: "control failed"},
		"client_quota_df is off":                    {change: func(r *statfsReport) { r.QuotaDF = "false" }, want: "condition not established: client"},
		"client_quota is off":                       {change: func(r *statfsReport) { r.Quota = "false" }, want: "condition not established: client"},
		"no client":                                 {change: func(r *statfsReport) { r.Client = 0 }, want: "condition not established: client"},
		"the recursive bytes did not converge":      {change: func(r *statfsReport) { r.Converged = false }, want: "condition not established: the recursive bytes"},
		"a directory is missing":                    {change: func(r *statfsReport) { r.Dirs = r.Dirs[:len(r.Dirs)-1] }, want: "directories reported"},
		"the child has no file quota": {change: func(r *statfsReport) {
			d := dir(r, "parent/child")
			d.MaxFiles, d.FilesSet, d.BytesSet = 0, false, false
		}, want: "condition not established: parent/child has max_bytes"},
		"the child has a byte quota too":    {change: func(r *statfsReport) { d := dir(r, "parent/child"); d.MaxBytes, d.BytesSet = statfsByteQuota, true }, want: "condition not established: parent/child has max_bytes"},
		"the parent lost its byte quota":    {change: func(r *statfsReport) { d := dir(r, "parent"); d.MaxBytes, d.BytesSet, d.FilesSet = 0, false, false }, want: "condition not established: parent has max_bytes"},
		"a set quota reads as absent":       {change: func(r *statfsReport) { dir(r, "bytes-only").BytesSet = false }, want: "condition not established: bytes-only has max_bytes"},
		"a quota reads zero but present":    {change: func(r *statfsReport) { dir(r, "unlimited").BytesSet = true }, want: "condition not established: unlimited has max_bytes"},
		"two paths are one inode":           {change: func(r *statfsReport) { dir(r, "parent/child").Inode = dir(r, "parent").Inode }, want: "an inode of its own"},
		"the child holds other bytes":       {change: func(r *statfsReport) { dir(r, "parent/child").RBytes = 0 }, want: "recursive bytes"},
		"another block size":                {change: func(r *statfsReport) { dir(r, "parent").BSize = 4096 }, want: "block size"},
		"the pool is as small as the quota": {change: func(r *statfsReport) { poolAt("unlimited")(r); dir(r, "unlimited").Blocks = 16 }, want: "not a capacity above"},
	} {
		t.Run(name, func(t *testing.T) {
			symptom, detail, err := judgeStatfs(statfsLayout, report(test.change))
			got := "absent: " + detail
			switch {
			case err != nil:
				got = err.Error()
			case symptom:
				got = "symptom: " + detail
			}
			if !strings.Contains(got, test.want) {
				t.Fatalf("got %q, want it to contain %q", got, test.want)
			}
		})
	}
}
