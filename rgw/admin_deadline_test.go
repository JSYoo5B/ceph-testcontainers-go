package rgw

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// runDeadlineScript runs rgwAdminDeadlineScript with the host's sh, as the
// gateway's sh runs it.
func runDeadlineScript(t *testing.T, limit string, command ...string) (int, string, string, time.Duration) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("sh", append([]string{"-c", rgwAdminDeadlineScript, "sh", limit}, command...)...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	started := time.Now()
	err := cmd.Run()
	var exit *exec.ExitError
	code := 0
	if errors.As(err, &exit) {
		code = exit.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	return code, stdout.String(), stderr.String(), time.Since(started)
}

// alive reports whether a process whose command line starts with prefix
// still runs. The script's own sh has the command among its arguments, so
// only the command line's start identifies the command itself.
func alive(t *testing.T, prefix string) bool {
	t.Helper()
	out, _ := exec.Command("ps", "-A", "-o", "args=").Output()
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), prefix) {
			return true
		}
	}
	return false
}

func TestRGWAdminDeadlineScript(t *testing.T) {
	t.Run("a command that ends keeps its output and status", func(t *testing.T) {
		code, stdout, stderr, took := runDeadlineScript(t, "5", "sh", "-c", "echo native; echo detail >&2; exit 3")
		if code != 3 || stdout != "native\n" || stderr != "detail\n" || took > 3*time.Second {
			t.Fatalf("code %d stdout %q stderr %q in %s", code, stdout, stderr, took)
		}
	})
	t.Run("no limit", func(t *testing.T) {
		if code, stdout, _, _ := runDeadlineScript(t, "0", "sh", "-c", "sleep 1; echo done"); code != 0 || stdout != "done\n" {
			t.Fatalf("code %d stdout %q", code, stdout)
		}
	})
	t.Run("a command that runs too long is stopped", func(t *testing.T) {
		code, _, stderr, took := runDeadlineScript(t, "1", "sleep", "31.5")
		if code != 143 || !strings.Contains(stderr, rgwAdminDeadlineMarker) || took > 5*time.Second {
			t.Fatalf("code %d stderr %q in %s", code, stderr, took)
		}
		if alive(t, "sleep 31.5") {
			t.Fatal("the stopped command still runs")
		}
	})
	t.Run("a command that ignores SIGTERM is killed", func(t *testing.T) {
		// One process, like radosgw-admin, that ignores SIGTERM.
		code, _, stderr, took := runDeadlineScript(t, "1", "sh", "-c", `trap "" TERM; while :; do :; done # ignores-term`)
		if code != 137 || !strings.Contains(stderr, rgwAdminDeadlineMarker) || took > 10*time.Second {
			t.Fatalf("code %d stderr %q in %s", code, stderr, took)
		}
		if alive(t, `sh -c trap "" TERM`) {
			t.Fatal("the killed command still runs")
		}
	})
}

func TestRGWAdminReportsAStoppedCommand(t *testing.T) {
	gateway, _ := newRGWAdminTestGateway(Config{}, rgwAdminResponse{code: 143, data: "Error: " + rgwAdminDeadlineMarker + ": still running after 165s, stopping it"})
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	_, err := gateway.Admin(ctx, "user", "stats", "--uid", "u", "--sync-stats")
	if !errors.Is(err, ceph.ErrNativeFailure) || !strings.Contains(err.Error(), "still ran after") {
		t.Fatalf("a stopped radosgw-admin: %v", err)
	}
	// Another failure stays redacted.
	gateway, _ = newRGWAdminTestGateway(Config{}, rgwAdminResponse{code: 22, data: "Error: invalid secret PRIVATE"})
	if _, err := gateway.Admin(ctx, "user", "list"); err == nil || strings.Contains(err.Error(), "still ran after") {
		t.Fatalf("an ordinary failure: %v", err)
	}
}

func TestRGWAdminLimit(t *testing.T) {
	if limit := rgwAdminLimit(context.Background()); limit != 0 {
		t.Fatalf("no deadline: %d", limit)
	}
	for bound, want := range map[time.Duration][2]int{3 * time.Minute: {164, 165}, 20 * time.Second: {9, 10}, time.Second: {1, 1}} {
		ctx, cancel := context.WithTimeout(t.Context(), bound)
		limit := rgwAdminLimit(ctx)
		cancel()
		if limit < want[0] || limit > want[1] {
			t.Errorf("bound %s: limit %d", bound, limit)
		}
	}
}
