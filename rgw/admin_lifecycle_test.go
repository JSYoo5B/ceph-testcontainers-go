package rgw

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// hostGateway is a gateway container whose execs run on the test host, as
// Docker runs them in the gateway: an exec whose caller's context ends
// returns at once, and its process keeps running. Its radosgw-admin is a
// controlled program that records its PID and sleeps, so that the test can
// tell whether a call left it running.
type hostGateway struct {
	testcontainers.Container
	dir string
	// ignoreTerm makes radosgw-admin ignore SIGTERM.
	ignoreTerm bool
	// launch, when set, holds the start of each supervisor exec until it is
	// closed, which still starts it after the caller's context ended, as an
	// exec that Docker accepted does.
	launch chan struct{}
	// accepted, when set, runs as each supervisor exec is accepted.
	accepted func()
	mu       sync.Mutex
	// failCancels fails that many cancel execs.
	failCancels int
	started     sync.WaitGroup
}

const hostRadosgwAdmin = `#!/bin/sh
[ -n "$TC_IGNORE_TERM" ] && trap '' TERM
echo $$ > "$TC_PID_FILE.tmp"
mv "$TC_PID_FILE.tmp" "$TC_PID_FILE"
exec sleep 30
`

func newHostGateway(t *testing.T) (*Gateway, *hostGateway) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("the gateway's POSIX shell is unavailable on Windows")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "radosgw-admin"), []byte(hostRadosgwAdmin), 0o755); err != nil {
		t.Fatal(err)
	}
	ctr := &hostGateway{dir: dir}
	t.Cleanup(func() {
		if pid, ok := ctr.pid(); ok {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
		ctr.started.Wait()
	})
	config := Config{Name: "default"}
	owner := testCluster(nil, testhelper.TestConfig{}, map[string]testcontainers.Container{rgwServiceName(config): ctr})
	return &Gateway{Container: ctr, owner: owner, config: config, GatewayName: config.Name}, ctr
}

func (ctr *hostGateway) GetContainerID() string { return "gateway" }

func (ctr *hostGateway) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	args = append([]string(nil), args...)
	for i := range args {
		args[i] = strings.ReplaceAll(args[i], supervised.ShellMarkerDir, filepath.Join(ctr.dir, "markers"))
	}
	canceler := len(args) > 2 && args[2] == supervised.ShellCancelScript
	ctr.mu.Lock()
	if canceler && ctr.failCancels > 0 {
		ctr.failCancels--
		ctr.mu.Unlock()
		return 0, nil, errors.New("engine unavailable")
	}
	ctr.mu.Unlock()
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Env = append(os.Environ(), "PATH="+ctr.dir+":"+os.Getenv("PATH"), "TC_PID_FILE="+filepath.Join(ctr.dir, "pid"))
	if ctr.ignoreTerm {
		cmd.Env = append(cmd.Env, "TC_IGNORE_TERM=1")
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	done := make(chan error, 1)
	ctr.started.Add(1)
	if !canceler && ctr.accepted != nil {
		ctr.accepted()
	}
	go func() {
		defer ctr.started.Done()
		if !canceler && ctr.launch != nil {
			<-ctr.launch
		}
		if err := cmd.Start(); err != nil {
			done <- err
			return
		}
		done <- cmd.Wait()
	}()
	select {
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case err := <-done:
		var exit *exec.ExitError
		if err != nil && !errors.As(err, &exit) {
			return 0, nil, err
		}
		var stream bytes.Buffer
		for _, part := range []struct {
			kind byte
			data []byte
		}{{byte(stdcopy.Stdout), stdout.Bytes()}, {byte(stdcopy.Stderr), stderr.Bytes()}} {
			header := [8]byte{part.kind}
			binary.BigEndian.PutUint32(header[4:], uint32(len(part.data)))
			stream.Write(header[:])
			stream.Write(part.data)
		}
		return cmd.ProcessState.ExitCode(), &stream, nil
	}
}

// pid returns radosgw-admin's PID once it has started.
func (ctr *hostGateway) pid() (int, bool) {
	data, err := os.ReadFile(filepath.Join(ctr.dir, "pid"))
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	return pid, err == nil
}

func (ctr *hostGateway) waitStarted(t *testing.T) int {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if pid, ok := ctr.pid(); ok {
			return pid
		}
	}
	t.Fatal("radosgw-admin did not start")
	return 0
}

// running reports whether pid is a live process; a zombie has ended.
func running(pid int) bool {
	out, err := exec.Command("ps", "-o", "stat=", "-p", strconv.Itoa(pid)).Output()
	state := strings.TrimSpace(string(out))
	return err == nil && state != "" && !strings.HasPrefix(state, "Z")
}

// adminAfterStart runs Admin with ctx and, once radosgw-admin has started,
// calls act; it returns radosgw-admin's PID and Admin's error.
func adminAfterStart(t *testing.T, gateway *Gateway, ctr *hostGateway, ctx context.Context, act func()) (int, error) {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		_, err := gateway.Admin(ctx, "user", "stats", "--uid", "u", "--sync-stats")
		result <- err
	}()
	pid := ctr.waitStarted(t)
	act()
	select {
	case err := <-result:
		return pid, err
	case <-time.After(30 * time.Second):
		t.Fatal("Admin did not return")
		return pid, nil
	}
}

func TestRGWAdminStopsItsCommandWhenTheCallerCancels(t *testing.T) {
	gateway, ctr := newHostGateway(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	pid, err := adminAfterStart(t, gateway, ctr, ctx, cancel)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("a canceled call: %v", err)
	}
	if running(pid) {
		t.Fatal("Admin returned while its radosgw-admin still ran")
	}
}

func TestRGWAdminStopsACommandThatIgnoresSIGTERMWithinAShortDeadline(t *testing.T) {
	gateway, ctr := newHostGateway(t)
	ctr.ignoreTerm = true
	ctx, cancel := context.WithTimeout(t.Context(), 1500*time.Millisecond)
	defer cancel()
	started := time.Now()
	pid, err := adminAfterStart(t, gateway, ctr, ctx, func() {})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a call past its deadline: %v", err)
	}
	if running(pid) {
		t.Fatal("Admin returned while its radosgw-admin still ran")
	}
	// The stop has its own budget beyond the caller's deadline.
	if took := time.Since(started); took > supervised.CancelTimeout+2*time.Second {
		t.Fatalf("the stop took %s", took)
	}
}

func TestRGWAdminCanceledBeforeItsExecStartsRunsNothing(t *testing.T) {
	gateway, ctr := newHostGateway(t)
	// The deadline passes as the supervisor exec is accepted, and the exec
	// starts only after Admin returned, rather than racing a wall-clock
	// deadline against a launch delay that a slow run can overtake.
	ctx := clustertest.NewSignalDeadline(t.Context())
	ctr.accepted = ctx.Expire
	ctr.launch = make(chan struct{})
	var launch sync.Once
	t.Cleanup(func() { launch.Do(func() { close(ctr.launch) }) })
	if _, err := gateway.Admin(ctx, "user", "list"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a call that ended before its exec started: %v", err)
	}
	launch.Do(func() { close(ctr.launch) })
	ctr.started.Wait()
	if _, ok := ctr.pid(); ok {
		t.Fatal("radosgw-admin started after its call was canceled")
	}
}

func TestRGWAdminReturnsARetryableStopWhenItCannotConfirmOne(t *testing.T) {
	gateway, ctr := newHostGateway(t)
	ctr.failCancels = 1
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	pid, err := adminAfterStart(t, gateway, ctr, ctx, cancel)
	var unstopped *ceph.NativeCommandUnstoppedError
	if !errors.As(err, &unstopped) {
		t.Fatalf("an unconfirmed stop: %v", err)
	}
	if !running(pid) {
		t.Fatal("the failed stop stopped the command")
	}
	if err := unstopped.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if running(pid) {
		t.Fatal("Stop returned while radosgw-admin still ran")
	}
}

func TestRGWAdminStopsACommandAtItsLimit(t *testing.T) {
	gateway, ctr := newHostGateway(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	pid, err := adminAfterStart(t, gateway, ctr, ctx, func() {})
	if !errors.Is(err, ceph.ErrNativeFailure) || !strings.Contains(err.Error(), "still ran after 1s and was stopped") {
		t.Fatalf("a command stopped at its limit: %v", err)
	}
	if running(pid) {
		t.Fatal("Admin returned while its radosgw-admin still ran")
	}
}

func (ctr *hostGateway) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	return nil
}

func TestRGWAdminStopsItsCommandWhenTheClusterTerminates(t *testing.T) {
	gateway, ctr := newHostGateway(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	terminated := make(chan error, 1)
	// Admin's own caller stays alive: only Terminate can end the call.
	pid, err := adminAfterStart(t, gateway, ctr, ctx, func() {
		terminateCtx, stop := context.WithTimeout(t.Context(), 20*time.Second)
		defer stop()
		terminated <- gateway.owner.Terminate(terminateCtx)
	})
	if err == nil || !strings.Contains(err.Error(), "terminated") {
		t.Fatalf("an Admin call that Terminate ended: %v", err)
	}
	if running(pid) {
		t.Fatal("Terminate left Admin's radosgw-admin running")
	}
	if err := <-terminated; err != nil {
		t.Fatalf("Terminate: %v", err)
	}
}
