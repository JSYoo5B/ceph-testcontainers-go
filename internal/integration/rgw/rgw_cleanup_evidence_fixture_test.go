//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_test

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestMultisiteDeletionRequiresNoSuchKey(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"key absent", 404, `<Error><Code>NoSuchKey</Code></Error>`, true},
		{"bucket absent", 404, `<Error><Code>NoSuchBucket</Code></Error>`, false},
		{"proxy not found", 404, `not found`, false},
		{"other XML document", 404, `<Response><Code>NoSuchKey</Code></Response>`, false},
		{"server error", 500, `<Error><Code>NoSuchKey</Code></Error>`, false},
		{"present", 200, `payload`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := multisiteObjectMatches(tc.status, []byte(tc.body), http.StatusNotFound, nil); got != tc.want {
				t.Fatalf("deletion observed=%t, want %t", got, tc.want)
			}
		})
	}
	if !multisiteObjectMatches(200, []byte("payload"), 200, []byte("payload")) || multisiteObjectMatches(200, []byte("wrong"), 200, []byte("payload")) {
		t.Fatal("positive replication must retain the payload comparison")
	}
}

func TestMultisiteObjectMatchesExpectedRedirect(t *testing.T) {
	redirect := `<Error><Code>PermanentRedirect</Code></Error>`
	for _, tc := range []struct {
		name               string
		status, wantStatus int
		body               string
		want               bool
	}{
		{"expected redirect", 301, 301, redirect, true},
		{"expected redirect without body", 301, 301, ``, true},
		{"object served where a redirect is expected", 200, 301, `payload`, false},
		{"other redirect status", 307, 301, redirect, false},
		{"redirect where the object is expected", 301, 200, redirect, false},
		{"redirect where absence is expected", 301, 404, redirect, false},
		{"status the matcher does not know", 403, 403, `<Error><Code>AccessDenied</Code></Error>`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := multisiteObjectMatches(tc.status, []byte(tc.body), tc.wantStatus, []byte("payload")); got != tc.want {
				t.Fatalf("HTTP %d observed while waiting for %d matched=%t, want %t", tc.status, tc.wantStatus, got, tc.want)
			}
		})
	}
}

func TestStoppedNotificationLogRetainsPriorEvidence(t *testing.T) {
	for _, tc := range []struct {
		name            string
		prior, observed []string
		want            bool
	}{
		{"retained", []string{"direct"}, []string{"direct"}, true},
		{"truncated", []string{"direct"}, nil, false},
		{"unestablished", nil, nil, false},
		{"queued event delivered", []string{"direct"}, []string{"direct", "queued"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := notificationLogWhileStopped(tc.prior, tc.observed, []string{"queued"}); (err == nil) != tc.want {
				t.Fatalf("log observation error=%v, want success=%t", err, tc.want)
			}
		})
	}
}

func TestRGWAdminExitRequiresObservedProcessState(t *testing.T) {
	for _, tc := range []struct {
		code        int
		state       string
		gone, valid bool
	}{
		{0, "absent", true, true}, {0, "Z", true, true}, {0, "S", false, true},
		{1, "", false, false}, {13, "Permission denied", false, false},
		{0, "", false, false}, {0, "invalid", false, false},
	} {
		gone, err := rgwAdminProcessGone(tc.code, tc.state)
		if gone != tc.gone || (err == nil) != tc.valid {
			t.Errorf("exit=%d state=%q: gone=%t error=%v", tc.code, tc.state, gone, err)
		}
	}
}

func TestRGWAdminProcessProbeRejectsFailedReads(t *testing.T) {
	for _, tc := range []struct {
		name, stat          string
		missingRoot, listed bool
		gone, valid         bool
	}{
		{name: "absent", gone: true, valid: true},
		{name: "zombie", listed: true, stat: "Z", gone: true, valid: true},
		{name: "running", listed: true, stat: "S", valid: true},
		{name: "unavailable proc", missingRoot: true},
		{name: "listed process with unreadable stat", listed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if tc.missingRoot {
				root = filepath.Join(root, "missing")
			}
			if tc.listed {
				if err := os.Mkdir(filepath.Join(root, "123"), 0o700); err != nil {
					t.Fatal(err)
				}
				if tc.stat != "" {
					data := "123 (admin) " + tc.stat + strings.Repeat(" 0", 20)
					if err := os.WriteFile(filepath.Join(root, "123", "stat"), []byte(data), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
			cmd := exec.Command("sh", "-c", rgwAdminProcessState, "process-probe", "123", root)
			out, err := cmd.CombinedOutput()
			code := 0
			if err != nil {
				code = 1
			}
			gone, readErr := rgwAdminProcessGone(code, string(out))
			if gone != tc.gone || (readErr == nil) != tc.valid {
				t.Fatalf("probe output=%q error=%v: gone=%t evidence error=%v", out, err, gone, readErr)
			}
		})
	}
}

func TestRGWAdminProcessProbeConfirmsExitAfterListing(t *testing.T) {
	for _, removeProc := range []bool{false, true} {
		t.Run(fmt.Sprintf("proc-unavailable=%t", removeProc), func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "proc")
			if err := os.MkdirAll(filepath.Join(root, "123"), 0o700); err != nil {
				t.Fatal(err)
			}
			// The probe reaches awk only after listing PID 123. This substitute
			// completes its exit at that exact point, then reports the failed
			// stat read. The second case also makes re-enumeration unavailable.
			bin := t.TempDir()
			awk := "#!/bin/sh\nfor arg do last=$arg; done\nrmdir -- \"${last%/stat}\" || exit\n"
			if removeProc {
				awk += "rmdir -- \"${last%/123/stat}\" || exit\n"
			}
			awk += "exit 1\n"
			if err := os.WriteFile(filepath.Join(bin, "awk"), []byte(awk), 0o700); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("sh", "-c", rgwAdminProcessState, "process-probe", "123", root)
			cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			out, err := cmd.CombinedOutput()
			code := 0
			if err != nil {
				code = 1
			}
			gone, readErr := rgwAdminProcessGone(code, string(out))
			wantGone := !removeProc
			if gone != wantGone || (readErr == nil) != wantGone {
				t.Fatalf("probe output=%q error=%v: gone=%t evidence error=%v", out, err, gone, readErr)
			}
		})
	}
}
