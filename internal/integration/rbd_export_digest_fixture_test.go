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
