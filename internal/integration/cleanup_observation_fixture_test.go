//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/containerd/errdefs"
	"os/exec"
	"strings"
	"testing"
)

// The probe source is the same program passed to the native container.
// Substitute only the OS observation so permission and I/O failures can be
// exercised without changing host permissions or relying on PID races.
func TestCleanupObservationRequiresCompleteReads(t *testing.T) {
	for _, test := range []struct {
		name, script, setup string
		args                []string
		pass                bool
		want                string
	}{
		{"process absent", requireProcessAbsent, "os.kill=lambda *args: fail(ProcessLookupError())", []string{"123"}, true, ""},
		{"process present", requireProcessAbsent, "os.kill=lambda *args: None", []string{"123"}, false, ""},
		{"process permission", requireProcessAbsent, "os.kill=lambda *args: fail(PermissionError())", []string{"123"}, false, ""},
		{"process I/O", requireProcessAbsent, "os.kill=lambda *args: fail(OSError('I/O'))", []string{"123"}, false, ""},
		{"markers absent", listSupervisedMarkers, "os.listdir=lambda p: fail(FileNotFoundError())", []string{"/markers"}, true, ""},
		{"markers empty", listSupervisedMarkers, "os.listdir=lambda p: []", []string{"/markers"}, true, ""},
		{"markers retained", listSupervisedMarkers, "os.listdir=lambda p: ['pending']", []string{"/markers"}, true, "pending"},
		{"markers unreadable", listSupervisedMarkers, "os.listdir=lambda p: fail(PermissionError())", []string{"/markers"}, false, ""},
		{"markers I/O", listSupervisedMarkers, "os.listdir=lambda p: fail(OSError('I/O'))", []string{"/markers"}, false, ""},
		{"stat absent", readProcessStat, "builtins.open=lambda p: fail(FileNotFoundError())", []string{"123"}, true, "absent"},
		{"stat empty", readProcessStat, "builtins.open=lambda p: io.StringIO('')", []string{"123"}, false, ""},
		{"stat present", readProcessStat, "builtins.open=lambda p: io.StringIO('123 (worker) S 1')", []string{"123"}, true, "123 (worker) S 1"},
		{"stat unreadable", readProcessStat, "builtins.open=lambda p: fail(PermissionError())", []string{"123"}, false, ""},
		{"stat I/O", readProcessStat, "builtins.open=lambda p: fail(OSError('I/O'))", []string{"123"}, false, ""},
		{"lease parent absent", cacheLeaseFiles, "os.listdir=lambda p: fail(FileNotFoundError())", []string{"/lease/socket"}, true, ""},
		{"lease empty", cacheLeaseFiles, "os.listdir=lambda p: []", []string{"/lease/socket"}, true, ""},
		{"lease unrelated", cacheLeaseFiles, "os.listdir=lambda p: ['other-file']", []string{"/lease/socket"}, true, ""},
		{"lease retained", cacheLeaseFiles, "os.listdir=lambda p: ['socket','immutable-object-cache-7.pid']", []string{"/lease/socket"}, true, "socket\nimmutable-object-cache-7.pid"},
		{"lease unreadable", cacheLeaseFiles, "os.listdir=lambda p: fail(PermissionError())", []string{"/lease/socket"}, false, ""},
		{"lease I/O", cacheLeaseFiles, "os.listdir=lambda p: fail(OSError('I/O'))", []string{"/lease/socket"}, false, ""},
		{"lease pid unreadable", cacheLeaseFiles, "os.listdir=lambda p: ['immutable-object-cache-7.pid']; builtins.open=lambda p: fail(PermissionError())", []string{"/lease/socket", "pid"}, false, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := "import os,io,builtins\ndef fail(error): raise error\n" + test.setup + "\n" + test.script
			args := append([]string{"-c", source}, test.args...)
			output, err := exec.Command("python3", args...).CombinedOutput()
			if (err == nil) != test.pass || (err == nil && strings.TrimSpace(string(output)) != test.want) {
				t.Fatalf("observation output=%q error=%v", output, err)
			}
		})
	}
}

func TestNetworkAbsenceRejectsFailedInspection(t *testing.T) {
	for _, err := range []error{nil, context.Canceled, errdefs.ErrPermissionDenied, errors.New("Docker unavailable")} {
		if message := evidenceRejected(func() { requireNetworkAbsent(evidenceFailure{}, "fixture-network", err) }); !strings.Contains(message, "removal was not confirmed") {
			t.Fatalf("unavailable or present network accepted: %v, %q", err, message)
		}
	}
	requireNetworkAbsent(t, "fixture-network", fmt.Errorf("inspect: %w", errdefs.ErrNotFound))
}
