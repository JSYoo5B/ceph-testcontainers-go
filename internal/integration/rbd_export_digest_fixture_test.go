//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"bytes"
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestRBDExportDigestReportsEveryFailure runs rbdExportDigest in a local
// shell with substituted rbd, md5sum and rm commands: an export failure keeps
// its status without a digest, a failed removal of the exported file fails an
// otherwise successful run with its own diagnostic, and the temporary
// directory is gone whenever its removal succeeds. The commands are
// substitutes; this does not observe a Ceph client container.
func TestRBDExportDigestReportsEveryFailure(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh")
	}
	const digest = "0123456789abcdef0123456789abcdef"
	bin := t.TempDir()
	writeScript := func(name, body string) {
		if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// rbd <args> export --no-progress <image> <path>: write the image, or
	// exit with $EXPORT_STATUS.
	writeScript("rbd", `test -z "$EXPORT_STATUS" || { echo "rbd: export failed" >&2; exit "$EXPORT_STATUS"; }
for last; do :; done
printf image > "$last"`)
	writeScript("md5sum", `cat > /dev/null; echo "`+digest+`  -"`)
	realRM, err := exec.LookPath("rm")
	if err != nil {
		t.Skip("no rm")
	}
	writeScript("rm", `test -z "$RM_STATUS" || { echo "rm: injected failure" >&2; exit "$RM_STATUS"; }
exec `+realRM+` "$@"`)

	for _, tc := range []struct {
		name                  string
		export, remove        string
		code                  int
		stderr                []string
		directoryRemains, sum bool
	}{
		{name: "success", sum: true},
		{name: "removal fails", remove: "73", code: 73, stderr: []string{"remove the temporary export directory exited 73"}, directoryRemains: true},
		{name: "export fails", export: "7", code: 7, stderr: []string{"rbd export exited 7"}},
		{name: "export and removal fail", export: "7", remove: "73", code: 7, stderr: []string{"rbd export exited 7", "remove the temporary export directory exited 73"}, directoryRemains: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			cmd := exec.CommandContext(t.Context(), sh, "-c", rbdExportDigest, "sh", "pool/image", "rbd", "--id", "admin")
			cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "TMPDIR="+tmp, "EXPORT_STATUS="+tc.export, "RM_STATUS="+tc.remove)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			code := 0
			if err := cmd.Run(); err != nil {
				var exit *exec.ExitError
				if !errors.As(err, &exit) {
					t.Fatal(err)
				}
				code = exit.ExitCode()
			}
			got, err := rbdExportResult("pool/image", code, stdout.Bytes(), stderr.Bytes())
			if tc.sum {
				if err != nil || got != digest {
					t.Fatalf("a successful export returned %q, %v", got, err)
				}
			} else if err == nil || code != tc.code {
				t.Fatalf("exit %d with %q, %v; want exit %d and an error", code, got, err, tc.code)
			}
			for _, want := range tc.stderr {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("the error %v lacks %q", err, want)
				}
			}
			left, _ := os.ReadDir(tmp)
			if remains := len(left) > 0; remains != tc.directoryRemains {
				t.Fatalf("temporary export directory remains: %v, want %v", remains, tc.directoryRemains)
			}
		})
	}
}

func TestRBDParentReadHistoryRejectsCacheBypass(t *testing.T) {
	objects := map[string]bool{"rbd_data.12.0000": true}
	read := func(out string) map[string]string {
		t.Helper()
		ops, err := harness.RBDDataReadsInHistory([]byte(out), objects)
		if err != nil {
			t.Fatal(err)
		}
		return ops
	}
	before := read(`{"ops":[
        {"description":"osd_op(client.1 ::rbd_data.12.0000:head [write 0~1])","initiated_at":"1"},
        {"description":"osd_op(client.1 ::rbd_data.12.00000:head [read 0~1])","initiated_at":"2"}
    ]}`)
	after := read(`{"ops":[{"description":"osd_op(client.1 ::rbd_data.12.0000:head [sparse-read 0~1])","initiated_at":"3"}]}`)
	if err := immutableCacheReadDelta(before, before, 0); err != nil {
		t.Fatal(err)
	}
	if err := immutableCacheReadDelta(before, after, 0); err == nil {
		t.Fatal("accepted a parent OSD read during a cache hit")
	}
	if err := immutableCacheReadDelta(before, after, 1); err != nil {
		t.Fatal(err)
	}
	if err := immutableCacheReadDelta(after, before, 0); err == nil {
		t.Fatal("accepted truncated history")
	}
	if _, err := harness.RBDDataReadsInHistory([]byte(`{}`), objects); err == nil {
		t.Fatal("accepted missing ops")
	}
}

// TestRBDReadServersAttributesByHistoryOrCounters covers the attribution of
// TestRBDZoneLocality with recorded perf dump and op history shapes: the
// issue 650 case, where the serving replica's history gained nothing but its
// counters returned the whole object, attributes the read to the replica;
// reads that two OSDs claim, or none, are not attributed to one; and
// contradicting or malformed evidence is refused.
func TestRBDReadServersAttributesByHistoryOrCounters(t *testing.T) {
	const size = 4 << 20
	counters := func(t *testing.T, out string) harness.OSDReadCounters {
		t.Helper()
		c, err := harness.ParseOSDReadCounters([]byte(out))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	// Tentacle osd.0 around a localized read it served, and osd.1 around
	// the same read, which only read the image header.
	replicaBefore := counters(t, `{"osd":{"op":0,"op_r":0,"op_r_out_bytes":0,"replica_read":0,"replica_read_served":0}}`)
	replicaAfter := counters(t, `{"osd":{"op":1,"op_r":1,"op_r_out_bytes":4194328,"replica_read":1,"replica_read_served":1}}`)
	primaryBefore := counters(t, `{"osd":{"op_r":57,"op_r_out_bytes":4195560,"replica_read_served":0}}`)
	primaryAfter := counters(t, `{"osd":{"op_r":68,"op_r_out_bytes":4195799,"replica_read_served":0}}`)
	// A release without the replica counter.
	unreported := counters(t, `{"osd":{"op_r":3,"op_r_out_bytes":12}}`)
	if unreported.ReplicaReadServed != nil {
		t.Fatal("reported an absent replica_read_served")
	}
	entry := []string{"2026-11-09T12:04:34.867152+0000 osd_op(client.4210.0:12 ... [sparse-read 0~4194304 [fadvise_nocache]] ... localize_reads ...)"}

	for _, tc := range []struct {
		name     string
		evidence map[int]harness.RBDReadEvidence
		servers  []int
		err      string
	}{
		{name: "history and counters", servers: []int{0}, evidence: map[int]harness.RBDReadEvidence{
			0: {History: entry, Before: replicaBefore, After: replicaAfter},
			1: {Before: primaryBefore, After: primaryAfter}}},
		{name: "counters without a history entry (issue 650)", servers: []int{0}, evidence: map[int]harness.RBDReadEvidence{
			0: {Before: replicaBefore, After: replicaAfter},
			1: {Before: primaryBefore, After: primaryAfter}}},
		{name: "two OSDs returned the object", servers: []int{0, 1}, evidence: map[int]harness.RBDReadEvidence{
			0: {Before: replicaBefore, After: replicaAfter},
			1: {Before: primaryBefore, After: counters(t, `{"osd":{"op_r":68,"op_r_out_bytes":8390366}}`)}}},
		{name: "no OSD returned the object", evidence: map[int]harness.RBDReadEvidence{
			0: {Before: replicaBefore, After: replicaBefore},
			1: {Before: primaryBefore, After: primaryAfter}}},
		{name: "history on an OSD that returned too little", err: "less than", evidence: map[int]harness.RBDReadEvidence{
			1: {History: entry, Before: primaryBefore, After: primaryAfter}}},
		{name: "counters reset", err: "backwards", evidence: map[int]harness.RBDReadEvidence{
			1: {Before: primaryAfter, After: unreported}}},
		{name: "replica counter reset", err: "replica_read_served went backwards", evidence: map[int]harness.RBDReadEvidence{
			0: {Before: counters(t, `{"osd":{"op_r":0,"op_r_out_bytes":0,"replica_read_served":2}}`), After: replicaAfter}}},
		{name: "unreported replica counter", servers: []int{0}, evidence: map[int]harness.RBDReadEvidence{
			0: {Before: replicaBefore, After: replicaAfter},
			1: {Before: unreported, After: unreported}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			servers, err := harness.RBDReadServers(tc.evidence, size)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("got %v, %v, want an error with %q", servers, err, tc.err)
				}
				return
			}
			if err != nil || !slices.Equal(servers, tc.servers) {
				t.Fatalf("got %v, %v, want %v", servers, err, tc.servers)
			}
		})
	}

	e := harness.RBDReadEvidence{Before: replicaBefore, After: replicaAfter}
	if served, ok := e.ReplicaServed(); !ok || served != 1 || e.OutBytes() != 4194328 {
		t.Fatalf("replica evidence: %d %v, %d bytes", served, ok, e.OutBytes())
	}
	if _, ok := (harness.RBDReadEvidence{Before: unreported, After: replicaAfter}).ReplicaServed(); ok {
		t.Fatal("reported a replica read count from one observation")
	}
	for _, out := range []string{`{}`, `{"osd":{"op_r":1}}`, `{"osd":{"op_r":1,"op_r_out_bytes":-1}}`, `{"osd":{"op_r":1.5,"op_r_out_bytes":1}}`,
		`{"osd":{"op_r":1,"op_r_out_bytes":1,"replica_read_served":"x"}}`, `not json`} {
		if c, err := harness.ParseOSDReadCounters([]byte(out)); err == nil {
			t.Errorf("accepted %s as %+v", out, c)
		}
	}
}
