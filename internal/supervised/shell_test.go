package supervised

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// runShellScript runs script with the host's sh, as a container's sh runs it.
func runShellScript(t *testing.T, script string, args ...string) (int, string, string, time.Duration) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("POSIX sh is unavailable on Windows")
	}
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("sh", append([]string{"-c", script, "sh"}, args...)...)
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

// recordedCommand returns a command of one process that records its PID in
// a file and then runs body, and a function that returns that PID once it
// was recorded. The supervisor reaps the command, so a PID that signal 0
// does not reach has ended; images such as Debian's have no ps.
func recordedCommand(t *testing.T, body string) ([]string, func() int) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "pid")
	command := []string{"sh", "-c", "echo $$ > " + file + ".tmp && mv " + file + ".tmp " + file + "; " + body}
	return command, func() int {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if data, err := os.ReadFile(file); err == nil {
				if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil {
					return pid
				}
			}
		}
		t.Fatal("the command did not start")
		return 0
	}
}

func processRunning(pid int) bool { return syscall.Kill(pid, 0) == nil }

func TestShellScript(t *testing.T) {
	marker := func(t *testing.T) string { return filepath.Join(t.TempDir(), "markers", "m") }
	t.Run("a command that ends keeps its output and status", func(t *testing.T) {
		m := marker(t)
		code, stdout, stderr, took := runShellScript(t, ShellScript, m, "5", "2", "sh", "-c", "echo native; echo detail >&2; exit 3")
		if code != 3 || stdout != "native\n" || stderr != "detail\n"+ShellEndPrefix+"3\n" || took > 3*time.Second {
			t.Fatalf("code %d stdout %q stderr %q in %s", code, stdout, stderr, took)
		}
		if _, err := os.Stat(m); !os.IsNotExist(err) {
			t.Fatal("the supervisor kept its marker")
		}
	})
	t.Run("no limit", func(t *testing.T) {
		if code, stdout, _, _ := runShellScript(t, ShellScript, marker(t), "0", "2", "sh", "-c", "sleep 1; echo done"); code != 0 || stdout != "done\n" {
			t.Fatalf("code %d stdout %q", code, stdout)
		}
	})
	t.Run("a command that runs too long is stopped", func(t *testing.T) {
		command, pid := recordedCommand(t, "exec sleep 30")
		code, _, stderr, took := runShellScript(t, ShellScript, append([]string{marker(t), "1", "2"}, command...)...)
		if code != 143 || stderr != ShellDeadlineLine+"\n"+ShellEndPrefix+"143\n" || took > 5*time.Second {
			t.Fatalf("code %d stderr %q in %s", code, stderr, took)
		}
		if processRunning(pid()) {
			t.Fatal("the stopped command still runs")
		}
	})
	t.Run("a command that ignores SIGTERM is killed after the grace", func(t *testing.T) {
		command, pid := recordedCommand(t, `trap "" TERM; while :; do :; done`)
		code, _, stderr, took := runShellScript(t, ShellScript, append([]string{marker(t), "1", "1"}, command...)...)
		if code != 137 || !strings.HasPrefix(stderr, ShellDeadlineLine) || took > 6*time.Second {
			t.Fatalf("code %d stderr %q in %s", code, stderr, took)
		}
		if processRunning(pid()) {
			t.Fatal("the killed command still runs")
		}
	})
}

func TestShellCancel(t *testing.T) {
	t.Run("a cancellation before the start runs nothing", func(t *testing.T) {
		m := filepath.Join(t.TempDir(), "markers", "m")
		if code, _, _, _ := runShellScript(t, ShellCancelScript, m, "1"); code != 0 {
			t.Fatalf("canceler exited %d", code)
		}
		ran := filepath.Join(t.TempDir(), "ran")
		code, _, stderr, _ := runShellScript(t, ShellScript, m, "0", "2", "touch", ran)
		if code != 0 || stderr != "" {
			t.Fatalf("a claimed supervisor: code %d stderr %q", code, stderr)
		}
		if _, err := os.Stat(ran); !os.IsNotExist(err) {
			t.Fatal("a claimed supervisor started its command")
		}
		if _, err := os.Stat(m); !os.IsNotExist(err) {
			t.Fatal("a claimed supervisor kept the claim")
		}
	})
	t.Run("a cancellation stops a running command and waits for it", func(t *testing.T) {
		m := filepath.Join(t.TempDir(), "markers", "m")
		command, recorded := recordedCommand(t, `trap "" TERM; exec sleep 30`)
		done := make(chan string, 1)
		go func() {
			_, _, stderr, _ := runShellScript(t, ShellScript, append([]string{m, "0", "1"}, command...)...)
			done <- stderr
		}()
		pid := recorded()
		if code, _, _, took := runShellScript(t, ShellCancelScript, m, "8"); code != 0 || took > 5*time.Second {
			t.Fatalf("canceler exited %d after %s", code, took)
		}
		if processRunning(pid) {
			t.Fatal("the canceler returned while the command still ran")
		}
		if stderr := <-done; stderr != ShellEndPrefix+"137\n" {
			t.Fatalf("a canceled supervisor reported %q", stderr)
		}
		if matches, _ := filepath.Glob(m + "*"); len(matches) != 0 {
			t.Fatalf("the cancellation left %v", matches)
		}
	})
	t.Run("an unacknowledged cancellation is not confirmed", func(t *testing.T) {
		m := filepath.Join(t.TempDir(), "m")
		// A supervisor of this instance that does not acknowledge.
		if err := os.WriteFile(m, []byte("run \n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if code, _, _, _ := runShellScript(t, ShellCancelScript, m, "1"); code != 3 {
			t.Fatalf("canceler exited %d", code)
		}
	})
	t.Run("an invocation of an earlier instance has ended with it", func(t *testing.T) {
		if _, err := os.Stat("/proc/1/stat"); err != nil {
			t.Skip("the container instance needs /proc")
		}
		m := filepath.Join(t.TempDir(), "m")
		if err := os.WriteFile(m, []byte("run 00000000-0000-0000-0000-000000000000/1\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if code, _, _, took := runShellScript(t, ShellCancelScript, m, "8"); code != 0 || took > 2*time.Second {
			t.Fatalf("canceler exited %d after %s", code, took)
		}
	})
}

// endlessShell is a container whose supervisor exec reports code without the
// supervisor's end line, as when a signal killed it, and whose canceler
// fails while cancelFails is set.
type endlessShell struct {
	testcontainers.Container
	code        int
	cancelFails bool
	cancels     int
}

func (ctr *endlessShell) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if len(args) > 2 && args[2] == ShellCancelScript {
		ctr.cancels++
		if ctr.cancelFails {
			return 0, nil, errors.New("engine unavailable")
		}
		return 0, nil, nil
	}
	data := []byte("partial\n")
	header := [8]byte{byte(stdcopy.Stderr)}
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	return ctr.code, io.MultiReader(bytes.NewReader(header[:]), bytes.NewReader(data)), nil
}

func TestRunShellStopsASupervisorThatDidNotReport(t *testing.T) {
	ctr := &endlessShell{code: 137}
	if _, err := RunShell(t.Context(), ctr, "test", 0, "true"); err == nil || ctr.cancels != 1 {
		t.Fatalf("a supervisor without its report: %v after %d cancels", err, ctr.cancels)
	}
	if _, unconfirmed := Unconfirmed(func() error { _, err := RunShell(t.Context(), ctr, "test", 0, "true"); return err }()); unconfirmed {
		t.Fatal("a confirmed stop was reported as unconfirmed")
	}
	ctr.cancelFails = true
	_, err := RunShell(t.Context(), ctr, "test", 0, "true")
	marker, unconfirmed := Unconfirmed(err)
	if !unconfirmed || !strings.HasPrefix(marker, ShellMarkerDir+"/") {
		t.Fatalf("an unconfirmed stop: %v", err)
	}
}

func TestShellEnd(t *testing.T) {
	for stderr, want := range map[string]int{
		"detail\n" + ShellEndPrefix + "3\n": 3, ShellEndPrefix + "0\n": 0, "detail\n": -1, ShellEndPrefix + "3": -1,
		ShellEndPrefix + "x\n": -1, ShellEndPrefix + "-1\n": -1, "": -1,
	} {
		buffer := bytes.NewBufferString(stderr)
		if got := shellEnd(buffer); got != want {
			t.Errorf("%q: %d", stderr, got)
		} else if want >= 0 && strings.Contains(buffer.String(), ShellEndPrefix) {
			t.Errorf("%q kept its end line: %q", stderr, buffer.String())
		}
	}
}
