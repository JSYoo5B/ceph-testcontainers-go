//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_authorization_isolation_test

import (
	"maps"
	"strings"
	"testing"
)

// outcome names what a judge returned, for the prefix comparisons below.
func outcome(symptom bool, detail string, err error) string {
	switch {
	case err != nil:
		return err.Error()
	case symptom:
		return "symptom: " + detail
	}
	return "absent: " + detail
}

// These cases feed synthetic probes to the judges. They are not native
// evidence of what a build does.
func TestJudgeFilesystemCaps(t *testing.T) {
	probes := func(change func([]capsProbe)) []capsProbe {
		var out []capsProbe
		for _, p := range []struct {
			fs       string
			writable bool
		}{{"iso-a", false}, {"iso-b", true}, {"iso-a", true}, {"iso-b", false}} {
			refused := "EACCES"
			if p.writable {
				refused = "ok"
			}
			out = append(out, capsProbe{Caps: "caps", Filesystem: p.fs, Writable: p.writable,
				Result: map[string]any{"mount": "ok", "read": "seed-of-" + p.fs, "create": refused, "mkdir": refused}})
		}
		if change != nil {
			change(out)
		}
		return out
	}
	for name, tc := range map[string]struct {
		change func([]capsProbe)
		want   string
	}{
		"contract":                       {want: "absent: 4 probes"},
		"create on a read-only fs":       {change: func(p []capsProbe) { p[0].Result["create"] = "ok" }, want: `symptom: 1 of 8 probes: caps "caps" on iso-a (read-only): create succeeded`},
		"mkdir on both read-only fs":     {change: func(p []capsProbe) { p[0].Result["mkdir"], p[3].Result["mkdir"] = "ok", "ok" }, want: "symptom: 2 of 8 probes"},
		"another errno on read-only":     {change: func(p []capsProbe) { p[3].Result["create"] = "EIO" }, want: "another fault: "},
		"a fault outranks the symptom":   {change: func(p []capsProbe) { p[0].Result["create"], p[3].Result["create"] = "ok", "EIO" }, want: "another fault: "},
		"writable fs refused":            {change: func(p []capsProbe) { p[1].Result["mkdir"] = "EACCES" }, want: "control failed: "},
		"mount refused":                  {change: func(p []capsProbe) { p[2].Result["mount"] = "EPERM" }, want: "control failed: "},
		"seed not read":                  {change: func(p []capsProbe) { p[0].Result["read"] = "ENOENT" }, want: "control failed: "},
		"a control outranks the symptom": {change: func(p []capsProbe) { p[0].Result["create"], p[1].Result["read"] = "ok", "" }, want: "control failed: "},
	} {
		if got := outcome(judgeFilesystemCaps(probes(tc.change))); !strings.HasPrefix(got, tc.want) {
			t.Errorf("%s: %q, want %q", name, got, tc.want)
		}
	}
	if got := outcome(judgeFilesystemCaps(nil)); !strings.HasPrefix(got, "condition not established: ") {
		t.Errorf("no probes: %q", got)
	}
}

func TestJudgeDeletedDirectorySnapshot(t *testing.T) {
	results := func(change func(before, after map[string]any)) (map[string]any, map[string]any) {
		before := map[string]any{"mount /": "EPERM", "mount /dir2": "EPERM", "live read": isolationSnapshotBytes,
			"snapshot read": isolationSnapshotBytes, "snapshot size": float64(len(isolationSnapshotBytes))}
		after := maps.Clone(before)
		after["deleted"], after["live read"] = "ok", "ENOENT"
		if change != nil {
			change(before, after)
		}
		return before, after
	}
	for name, tc := range map[string]struct {
		change func(before, after map[string]any)
		want   string
	}{
		"contract":                  {want: "absent: after the deletion the snapshot still reads"},
		"snapshot read refused":     {change: func(_, a map[string]any) { a["snapshot read"], a["snapshot size"] = "EPERM", "EPERM" }, want: "symptom: after the deletion the snapshot read of the child is refused with EPERM"},
		"snapshot read EACCES":      {change: func(_, a map[string]any) { a["snapshot read"] = "EACCES" }, want: "symptom: "},
		"snapshot read other bytes": {change: func(_, a map[string]any) { a["snapshot read"] = "other" }, want: "another fault: "},
		"snapshot read ENOENT":      {change: func(_, a map[string]any) { a["snapshot read"] = "ENOENT" }, want: "another fault: "},
		"wrong snapshot size":       {change: func(_, a map[string]any) { a["snapshot size"] = float64(1) }, want: "another fault: "},
		"root mounted before":       {change: func(b, _ map[string]any) { b["mount /"] = "ok" }, want: "control failed: before the deletion"},
		"dir2 mounted after":        {change: func(_, a map[string]any) { a["mount /dir2"] = "ok" }, want: "control failed: after the deletion"},
		"snapshot unreadable first": {change: func(b, _ map[string]any) { b["snapshot read"] = "EPERM" }, want: "condition not established: before the deletion"},
		"live child unreadable":     {change: func(b, _ map[string]any) { b["live read"] = "ENOENT" }, want: "condition not established: before the deletion"},
		"child not deleted":         {change: func(_, a map[string]any) { a["deleted"] = "EACCES" }, want: "condition not established: the live child was not deleted"},
		"child still there":         {change: func(_, a map[string]any) { a["live read"] = isolationSnapshotBytes }, want: "condition not established: the live child was not deleted"},
	} {
		if got := outcome(judgeDeletedDirectorySnapshot(results(tc.change))); !strings.HasPrefix(got, tc.want) {
			t.Errorf("%s: %q, want %q", name, got, tc.want)
		}
	}
}

func TestJudgeSameNamedSubvolumes(t *testing.T) {
	probes := func(change func(a, b *tenantProbe)) (tenantProbe, tenantProbe) {
		probe := func(group, ns, otherNS string) tenantProbe {
			return tenantProbe{Group: group, Namespace: ns, DataPool: "cephfs.iso-a.data", OSDCaps: "allow r pool=cephfs.iso-a.data namespace=" + ns,
				Result: map[string]any{"own file": "bytes-of-" + group, "own object": "bytes-of-" + group, "other object": "EPERM"}}
		}
		a, b := probe("tenant-a", "fsvolumens_a", "fsvolumens_b"), probe("tenant-b", "fsvolumens_b", "fsvolumens_a")
		if change != nil {
			change(&a, &b)
		}
		return a, b
	}
	shared := func(a, b *tenantProbe) {
		b.Namespace = a.Namespace
		a.OSDCaps, b.OSDCaps = "allow r pool=cephfs.iso-a.data namespace="+a.Namespace, "allow r pool=cephfs.iso-a.data namespace="+a.Namespace
		a.Result["other object"], b.Result["other object"] = "bytes-of-tenant-b", "bytes-of-tenant-a"
	}
	for name, tc := range map[string]struct {
		change func(a, b *tenantProbe)
		want   string
	}{
		"contract":                    {want: `absent: namespaces "fsvolumens_a" and "fsvolumens_b"; the reader of tenant-a is refused the object of tenant-b`},
		"one shared namespace":        {change: shared, want: `symptom: tenant-a and tenant-b share the RADOS namespace "fsvolumens_a"; the reader of tenant-a reads the object of tenant-b`},
		"shared namespace, no read":   {change: func(a, b *tenantProbe) { shared(a, b); a.Result["other object"] = "ENOENT" }, want: "symptom: tenant-a and tenant-b share"},
		"other object read":           {change: func(a, _ *tenantProbe) { a.Result["other object"] = "bytes-of-tenant-b" }, want: "symptom: the reader of tenant-a reads the object of tenant-b"},
		"other object another errno":  {change: func(_, b *tenantProbe) { b.Result["other object"] = "ENOENT" }, want: "another fault: the reader of tenant-b got ENOENT"},
		"own file unreadable":         {change: func(a, _ *tenantProbe) { a.Result["own file"] = "EPERM" }, want: "control failed: the reader of tenant-a does not read its own data"},
		"own object unreadable":       {change: func(_, b *tenantProbe) { b.Result["own object"] = "EPERM" }, want: "control failed: the reader of tenant-b"},
		"caps name the other":         {change: func(a, _ *tenantProbe) { a.OSDCaps += " namespace=fsvolumens_b" }, want: "control failed: the reader of tenant-a has OSD caps"},
		"caps miss its own":           {change: func(_, b *tenantProbe) { b.OSDCaps = "allow r pool=cephfs.iso-a.data" }, want: "control failed: the reader of tenant-b has OSD caps"},
		"no namespace":                {change: func(a, _ *tenantProbe) { a.Namespace = "" }, want: "condition not established: "},
		"two data pools":              {change: func(_, b *tenantProbe) { b.DataPool = "other" }, want: "condition not established: "},
		"a control outranks the read": {change: func(a, _ *tenantProbe) { a.Result["other object"], a.Result["own file"] = "bytes-of-tenant-b", "" }, want: "control failed: "},
	} {
		if got := outcome(judgeSameNamedSubvolumes(probes(tc.change))); !strings.HasPrefix(got, tc.want) {
			t.Errorf("%s: %q, want %q", name, got, tc.want)
		}
	}
}
