package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	dockercontainer "github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// nativeTarget is a container whose commands run as real processes on the
// test host, with the supervisor's marker directory and the /proc files that
// identify the container instance below root. Like Docker, Exec stops waiting
// when its context ends but leaves the process running, refuses new commands
// while the container is paused, stopped or removed, and may start an
// accepted command late, also in the instance after a restart. Pausing
// freezes the processes. Stopping, restarting or removing the container kills
// its processes, including the published command PIDs and their groups, and a
// restart starts a new instance.
type nativeTarget struct {
	testcontainers.Container
	id, root string
	mu       sync.Mutex
	paused   bool
	stopped  bool
	removed  bool
	noPython bool
	starts   int
	networks []string
	// startGate, when set, holds the start of each supervisor, not of a
	// canceler, until it is closed. A test that needs an accepted exec to
	// start at a given moment closes it then, instead of relying on a delay
	// outlasting the code under test.
	startGate chan struct{}
	// refuseCancel fails every canceler exec, as an unreachable engine does.
	refuseCancel bool
	// refuseAttach fails every exec with its input attached.
	refuseAttach bool
	// attachClosed runs when an attached exec's connection closes.
	attachClosed func()
	// stateHook and networksHook run before State and Networks answer, and
	// an error that they return is the answer.
	stateHook, networksHook func() error
	execs                   int
	procs                   []*os.Process
	wg                      sync.WaitGroup
}

func newNativeTarget(t *testing.T, id string, networks ...string) *nativeTarget {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("POSIX pipes and process groups are unavailable on Windows")
	}
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("local native command regressions require installed python3; native image validation covers the runtime")
	}
	target := &nativeTarget{id: id, root: t.TempDir(), starts: 1, networks: networks}
	for _, dir := range []string{target.pids(), filepath.Join(target.root, "proc", "1"), filepath.Join(target.root, "proc", "sys", "kernel", "random")} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(target.root, "proc", "sys", "kernel", "random", "boot_id"), []byte("boot\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	target.writeInstance()
	t.Cleanup(func() {
		target.mu.Lock()
		target.killAll()
		target.mu.Unlock()
		done := make(chan struct{})
		go func() { target.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("native target processes did not exit")
		}
	})
	return target
}

// requireLinuxGroups skips a test whose command ends on its own unless the
// host is Linux, as inside the containers. The supervisor then kills the
// command's group while its leader is an unreaped zombie, which Linux allows
// and macOS refuses with EPERM.
func requireLinuxGroups(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("the supervisor's group kill after a command exits needs Linux process groups")
	}
}

func (f *nativeTarget) GetContainerID() string { return f.id }

func (f *nativeTarget) pids() string { return filepath.Join(f.root, "pids") }

func (f *nativeTarget) markers() string { return filepath.Join(f.root, "markers") }

func (f *nativeTarget) State(context.Context) (*dockercontainer.State, error) {
	if hook := f.stateHook; hook != nil {
		f.stateHook = nil
		if err := hook(); err != nil {
			return nil, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.removed {
		return nil, fmt.Errorf("container %s: %w", f.id, errdefs.ErrNotFound)
	}
	return &dockercontainer.State{Running: !f.stopped, Paused: f.paused, StartedAt: "start-" + strconv.Itoa(f.starts)}, nil
}

func (f *nativeTarget) Networks(context.Context) ([]string, error) {
	if hook := f.networksHook; hook != nil {
		f.networksHook = nil
		if err := hook(); err != nil {
			return nil, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.removed {
		return nil, fmt.Errorf("container %s: %w", f.id, errdefs.ErrNotFound)
	}
	return append([]string{}, f.networks...), nil
}

func (f *nativeTarget) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	f.mu.Lock()
	f.execs++
	canceler := len(args) > 2 && args[2] == supervised.CancelScript
	var refused error
	switch {
	case f.removed:
		refused = fmt.Errorf("container %s: %w", f.id, errdefs.ErrNotFound)
	case f.stopped:
		refused = fmt.Errorf("container %s is not running", f.id)
	case f.paused:
		refused = fmt.Errorf("container %s is paused, unpause the container before exec", f.id)
	case canceler && f.refuseCancel:
		refused = errors.New("engine unreachable")
	case f.noPython && args[0] == "python3":
		f.mu.Unlock()
		return 127, strings.NewReader(`OCI runtime exec failed: exec: "python3": executable file not found in $PATH`), nil
	}
	gate := f.startGate
	if canceler {
		gate = nil
	}
	f.mu.Unlock()
	if refused != nil {
		return 0, nil, refused
	}
	options := tcexec.NewProcessOptions(args)
	for _, opt := range opts {
		opt.Apply(options)
	}
	rewrite := func(value string) string {
		return strings.ReplaceAll(strings.ReplaceAll(value, supervised.MarkerDir, f.markers()), "/proc/", f.root+"/proc/")
	}
	argv := make([]string, len(args))
	for i, arg := range args {
		argv[i] = rewrite(arg)
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = os.Environ()
	for _, variable := range options.ExecConfig.Env {
		cmd.Env = append(cmd.Env, rewrite(variable))
	}
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	result := make(chan error, 1)
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		if gate != nil {
			<-gate
		}
		f.mu.Lock()
		if f.stopped || f.removed {
			f.mu.Unlock()
			result <- errors.New("exec instance dropped with its container")
			return
		}
		err := cmd.Start()
		if err == nil {
			f.procs = append(f.procs, cmd.Process)
		}
		f.mu.Unlock()
		if err == nil {
			err = cmd.Wait()
		}
		result <- err
	}()
	select {
	case err := <-result:
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			// Docker reports a process that a signal killed as 128 plus
			// the signal.
			if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
				return 128 + int(status.Signal()), &output, nil
			}
			return exitErr.ExitCode(), &output, nil
		}
		if err != nil {
			return 0, nil, err
		}
		return 0, &output, nil
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

// nativeAttached is the attached standard input of a command of a
// nativeTarget, whose output arrives multiplexed as Docker sends it.
type nativeAttached struct {
	stdin  io.WriteCloser
	output *io.PipeReader
	closed func()
	once   sync.Once
}

func (a *nativeAttached) Write(p []byte) (int, error) { return a.stdin.Write(p) }

func (a *nativeAttached) CloseWrite() error { return a.stdin.Close() }

func (a *nativeAttached) Close() error {
	_ = a.stdin.Close()
	err := a.output.Close()
	if a.closed != nil {
		a.once.Do(a.closed)
	}
	return err
}

// dockerFrames frames each write to one stream as Docker does.
type dockerFrames struct {
	w      io.Writer
	stream stdcopy.StdType
}

func (m dockerFrames) Write(p []byte) (int, error) {
	frame := binary.BigEndian.AppendUint32([]byte{byte(m.stream), 0, 0, 0}, uint32(len(p)))
	if _, err := m.w.Write(append(frame, p...)); err != nil {
		return 0, err
	}
	return len(p), nil
}

// AttachExec starts a command with its standard input attached, refused and
// held like Exec. Its output ends when the command has ended.
func (f *nativeTarget) AttachExec(ctx context.Context, args, env []string) (supervised.Attachment, error) {
	f.mu.Lock()
	f.execs++
	var refused error
	switch {
	case f.removed:
		refused = fmt.Errorf("container %s: %w", f.id, errdefs.ErrNotFound)
	case f.stopped:
		refused = fmt.Errorf("container %s is not running", f.id)
	case f.paused:
		refused = fmt.Errorf("container %s is paused, unpause the container before exec", f.id)
	case f.refuseAttach:
		refused = errors.New("engine unreachable")
	}
	gate, closed := f.startGate, f.attachClosed
	f.mu.Unlock()
	if refused != nil {
		return supervised.Attachment{}, refused
	}
	rewrite := func(value string) string {
		return strings.ReplaceAll(strings.ReplaceAll(value, supervised.MarkerDir, f.markers()), "/proc/", f.root+"/proc/")
	}
	argv := make([]string, len(args))
	for i, arg := range args {
		argv[i] = rewrite(arg)
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = os.Environ()
	for _, variable := range env {
		cmd.Env = append(cmd.Env, rewrite(variable))
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return supervised.Attachment{}, err
	}
	reader, writer := io.Pipe()
	cmd.Stdout, cmd.Stderr = dockerFrames{writer, stdcopy.Stdout}, dockerFrames{writer, stdcopy.Stderr}
	exited := make(chan int, 1)
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		if gate != nil {
			<-gate
		}
		f.mu.Lock()
		if f.stopped || f.removed {
			f.mu.Unlock()
			_ = stdin.Close()
			writer.CloseWithError(errors.New("exec instance dropped with its container"))
			exited <- -1
			return
		}
		err := cmd.Start()
		if err == nil {
			f.procs = append(f.procs, cmd.Process)
		}
		f.mu.Unlock()
		code := 126
		if err == nil {
			code = 0
			var exitErr *exec.ExitError
			if errors.As(cmd.Wait(), &exitErr) {
				code = exitErr.ExitCode()
				if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
					code = 128 + int(status.Signal())
				}
			}
		}
		_ = writer.Close()
		exited <- code
	}()
	return supervised.Attachment{
		Input:  &nativeAttached{stdin: stdin, output: reader, closed: closed},
		Output: reader,
		ExitCode: func(ctx context.Context) (int, error) {
			select {
			case code := <-exited:
				exited <- code
				return code, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		},
		Release: func() {},
	}, nil
}

// Terminate removes the container with its processes.
func (f *nativeTarget) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	f.set(func(f *nativeTarget) { f.killAll(); f.removed = true })
	return nil
}

// killAll kills every process of the container. The caller holds f.mu.
func (f *nativeTarget) killAll() {
	for _, proc := range f.procs {
		_ = proc.Kill()
	}
	entries, _ := os.ReadDir(f.pids())
	for _, entry := range entries {
		if data, err := os.ReadFile(filepath.Join(f.pids(), entry.Name())); err == nil {
			if pid, err := strconv.Atoi(string(data)); err == nil && pid > 1 {
				_ = syscall.Kill(-pid, syscall.SIGKILL)
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	}
}

func (f *nativeTarget) set(change func(*nativeTarget)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	change(f)
}

// writeInstance writes the stat file of the instance's PID 1, whose start
// time identifies the instance. The caller holds f.mu or owns f alone.
func (f *nativeTarget) writeInstance() {
	fields := append([]string{"S"}, slices.Repeat([]string{"0"}, 18)...)
	stat := "1 (init) " + strings.Join(append(fields, strconv.Itoa(f.starts)), " ") + " 0 0\n"
	_ = os.WriteFile(filepath.Join(f.root, "proc", "1", "stat"), []byte(stat), 0o600)
}

// start starts a stopped container, or restarts a running one, as a new
// instance. The caller holds f.mu.
func (f *nativeTarget) start() {
	f.killAll()
	f.stopped = false
	f.starts++
	f.writeInstance()
}

func (f *nativeTarget) restart() { f.set((*nativeTarget).start) }

// setPaused freezes or thaws every process of the container. The caller
// holds f.mu.
func (f *nativeTarget) setPaused(paused bool) {
	f.paused = paused
	signal := syscall.SIGCONT
	if paused {
		signal = syscall.SIGSTOP
	}
	for _, proc := range f.procs {
		_ = proc.Signal(signal)
	}
	entries, _ := os.ReadDir(f.pids())
	for _, entry := range entries {
		if data, err := os.ReadFile(filepath.Join(f.pids(), entry.Name())); err == nil {
			if pid, err := strconv.Atoi(string(data)); err == nil && pid > 1 {
				_ = syscall.Kill(-pid, signal)
				_ = syscall.Kill(pid, signal)
			}
		}
	}
}

func (f *nativeTarget) execCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.execs
}

// published waits until the named command published the PID of role.
func (f *nativeTarget) published(t *testing.T, name, role string) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if data, err := os.ReadFile(filepath.Join(f.pids(), name+"."+role)); err == nil {
			pid, err := strconv.Atoi(string(data))
			if err != nil {
				t.Fatal(err)
			}
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not publish its %s PID", name, role)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// leftMarkers lists the marker files that remain in the container.
func (f *nativeTarget) leftMarkers(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(f.markers())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func processAlive(pid int) bool { return syscall.Kill(pid, 0) == nil }

// gone waits until pid has ended and was reaped; an orphaned descendant is
// reaped by the host's init shortly after its group was killed.
func gone(t *testing.T, pid int, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for processAlive(pid) {
		if time.Now().After(deadline) {
			t.Fatalf("%s %d still runs", what, pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// nativeWaiter publishes its PID and a descendant's PID below pids/name and
// waits; only a kill or its 20 second bound ends it.
const nativeWaiter = `import os,subprocess,sys,time
pids,name=sys.argv[1],sys.argv[2]
def publish(role,pid):
 path=os.path.join(pids,name+'.'+role)
 with open(path+'.tmp','w') as f: f.write(str(pid))
 os.rename(path+'.tmp',path)
child=subprocess.Popen([sys.executable,'-c','import time\nd=time.monotonic()+20\nwhile time.monotonic()<d: time.sleep(0.05)'])
publish('descendant',child.pid)
publish('command',os.getpid())
deadline=time.monotonic()+20
while time.monotonic()<deadline: time.sleep(0.01)
`

func waiterCommand(target *nativeTarget, name string, timeout time.Duration) NativeCommand {
	return NativeCommand{Argv: []string{"python3", "-c", nativeWaiter, target.pids(), name}, Timeout: timeout,
		MaxStdout: 4096, MaxStderr: 4096, Label: "waiter " + name}
}

func nativeCluster() *Container {
	return &Container{settings: options{startupTimeout: time.Second},
		network: &testcontainers.DockerNetwork{Name: "tc-public"}, networkRemoved: true,
		clusterNetwork: &testcontainers.DockerNetwork{Name: "tc-cluster"}, clusterNetworkRemoved: true}
}

func registered(c *Container) int {
	c.nativeMu.Lock()
	defer c.nativeMu.Unlock()
	return len(c.nativeInvocations)
}

func TestNativeCommandRefusesMalformedCommandsBeforeRunning(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	valid := NativeCommand{Argv: []string{"true"}, Timeout: time.Second, MaxStdout: 1, MaxStderr: 1}
	for name, change := range map[string]func(*NativeCommand){
		"no argv":         func(c *NativeCommand) { c.Argv = nil },
		"empty program":   func(c *NativeCommand) { c.Argv = []string{""} },
		"NUL argument":    func(c *NativeCommand) { c.Argv = []string{"true", "a\x00b"} },
		"zero timeout":    func(c *NativeCommand) { c.Timeout = 0 },
		"long timeout":    func(c *NativeCommand) { c.Timeout = MaxNativeCommandTimeout + time.Second },
		"no stdout":       func(c *NativeCommand) { c.MaxStdout = 0 },
		"negative stderr": func(c *NativeCommand) { c.MaxStderr = -1 },
		"large stdout":    func(c *NativeCommand) { c.MaxStdout = MaxNativeCommandOutput + 1 },
		"control label":   func(c *NativeCommand) { c.Label = "probe\nsecret" },
		"long label":      func(c *NativeCommand) { c.Label = strings.Repeat("x", 129) },
	} {
		command := valid
		change(&command)
		if _, err := cluster.RunNativeCommand(t.Context(), target, command); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("%s: malformed command returned %v", name, err)
		}
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := cluster.RunNativeCommand(canceled, target, valid); !errors.Is(err, context.Canceled) || classified(err) {
		t.Fatalf("canceled context returned %v", err)
	}
	if _, err := cluster.RunNativeCommand(t.Context(), nil, valid); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("nil target returned %v", err)
	}
	if target.execCount() != 0 || registered(cluster) != 0 {
		t.Fatalf("a refused command reached the container: %d execs", target.execCount())
	}
	// Limits at their maxima are valid.
	valid.Timeout, valid.MaxStdout, valid.MaxStderr = MaxNativeCommandTimeout, MaxNativeCommandOutput, MaxNativeCommandOutput
	if _, err := validateNativeCommand(valid); err != nil {
		t.Fatal(err)
	}
}

func TestNativeCommandAdmitsOnlyClusterAndAttachedContainers(t *testing.T) {
	// The command runs into its deadline, which shows that it ran.
	quick := NativeCommand{Argv: []string{"sleep", "5"}, Timeout: 100 * time.Millisecond, MaxStdout: 64, MaxStderr: 64, Label: "probe"}
	ran := func(result NativeCommandResult, err error) bool {
		return errors.Is(err, ErrNativeCommandIncomplete) && result.TimedOut
	}
	owned := newNativeTarget(t, "osd-container")
	public := newNativeTarget(t, "public-client", "bridge", "tc-public")
	clusterSide := newNativeTarget(t, "cluster-client", "tc-cluster")
	foreign := newNativeTarget(t, "foreign", "bridge", "other")
	cluster := nativeCluster()
	cluster.osds = map[int]*OSDContainer{0: {ID: 0, Container: owned}}
	for _, target := range []*nativeTarget{owned, public, clusterSide} {
		if result, err := cluster.RunNativeCommand(t.Context(), target, quick); !ran(result, err) {
			t.Fatalf("%s was refused: %+v %v", target.id, result, err)
		}
	}
	if _, err := cluster.RunNativeCommand(t.Context(), foreign, quick); !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), "neither part of this cluster nor attached") || foreign.execCount() != 0 {
		t.Fatalf("a foreign container was admitted: %v", err)
	}
	hostCluster := &Container{settings: options{startupTimeout: time.Second, hostNetwork: true}}
	hostClient := newNativeTarget(t, "host-client", "host")
	if result, err := hostCluster.RunNativeCommand(t.Context(), hostClient, quick); !ran(result, err) {
		t.Fatalf("a host-network client of a host-network cluster was refused: %v", err)
	}
	if _, err := hostCluster.RunNativeCommand(t.Context(), public, quick); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("a bridge container of a host-network cluster returned %v", err)
	}
	for name, change := range map[string]func(*nativeTarget){
		"paused":  func(f *nativeTarget) { f.setPaused(true) },
		"stopped": func(f *nativeTarget) { f.stopped = true },
	} {
		target := newNativeTarget(t, name, "tc-public")
		target.set(change)
		if _, err := cluster.RunNativeCommand(t.Context(), target, quick); !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), "running, unpaused") || target.execCount() != 0 {
			t.Fatalf("%s container accepted a command: %v", name, err)
		}
	}
	cluster.closed = true
	before := public.execCount()
	if _, err := cluster.RunNativeCommand(t.Context(), public, quick); !errors.Is(err, errClusterTerminated) || !errors.Is(err, ErrInvalidRequest) || public.execCount() != before {
		t.Fatalf("a terminated cluster ran a command: %v", err)
	}
}

func TestNativeCommandWaitsForTheTopologyGate(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	assertContextLockQueued(t, &cluster.mu, func(ctx context.Context) error {
		_, err := cluster.RunNativeCommand(ctx, target, NativeCommand{Argv: []string{"true"}, Timeout: time.Second, MaxStdout: 1, MaxStderr: 1})
		return err
	})
	if target.execCount() != 0 {
		t.Fatal("a queued native command reached the container")
	}
}

func TestNativeCommandResultKeepsRawSeparateStreams(t *testing.T) {
	requireLinuxGroups(t)
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	script := `import sys
sys.stdout.buffer.write(bytes(range(256)))
sys.stderr.buffer.write(bytes(reversed(range(256))))
sys.exit(3)`
	result, err := cluster.RunNativeCommand(t.Context(), target, NativeCommand{Argv: []string{"python3", "-c", script},
		Timeout: 10 * time.Second, MaxStdout: 256, MaxStderr: 256})
	want := make([]byte, 256)
	for i := range want {
		want[i] = byte(i)
	}
	reversed := make([]byte, 256)
	for i := range reversed {
		reversed[i] = byte(255 - i)
	}
	if err != nil || result.ExitCode != 3 || !result.Complete() || !bytes.Equal(result.Stdout, want) || !bytes.Equal(result.Stderr, reversed) {
		t.Fatalf("result %+v, %v", result, err)
	}
}

func TestNativeCommandReportsIncompleteOutput(t *testing.T) {
	requireLinuxGroups(t)
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	script := `import sys
sys.stdout.write("secret-output-" * 100)
sys.stderr.write("diagnostic")`
	result, err := cluster.RunNativeCommand(t.Context(), target, NativeCommand{Argv: []string{"python3", "-c", script, "secret-argument"},
		Timeout: 10 * time.Second, MaxStdout: 100, MaxStderr: 4096, Label: "overflow"})
	if !errors.Is(err, ErrNativeCommandIncomplete) || classified(err) || result.Complete() || !result.StdoutTruncated || result.StderrTruncated || result.TimedOut {
		t.Fatalf("overflow returned %+v, %v", result, err)
	}
	if string(result.Stdout) != strings.Repeat("secret-output-", 100)[:100] || string(result.Stderr) != "diagnostic" {
		t.Fatalf("overflow kept %q and %q", result.Stdout, result.Stderr)
	}
	if text := err.Error(); !strings.Contains(text, "overflow") || strings.Contains(text, "secret") {
		t.Fatalf("error leaks the command or its output: %s", text)
	}
}

func TestNativeCommandTimeoutKillsTheCommandGroup(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	result, err := cluster.RunNativeCommand(t.Context(), target, waiterCommand(target, "slow", 500*time.Millisecond))
	if !errors.Is(err, ErrNativeCommandIncomplete) || classified(err) || !result.TimedOut {
		t.Fatalf("timeout returned %+v, %v", result, err)
	}
	command, descendant := target.published(t, "slow", "command"), target.published(t, "slow", "descendant")
	if processAlive(command) {
		t.Fatal("the timed-out command was not reaped before the call returned")
	}
	gone(t, descendant, "descendant")
	if markers := target.leftMarkers(t); len(markers) != 0 || registered(cluster) != 0 {
		t.Fatalf("timeout left markers %v or a registration", markers)
	}
}

func TestNativeCommandCancellationStopsOnlyItsCommand(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	canceled := make(chan error, 1)
	go func() {
		_, err := cluster.RunNativeCommand(ctx, target, waiterCommand(target, "canceled", time.Minute))
		canceled <- err
	}()
	kept := make(chan error, 1)
	go func() {
		_, err := cluster.RunNativeCommand(t.Context(), target, waiterCommand(target, "kept", 3*time.Second))
		kept <- err
	}()
	command, descendant := target.published(t, "canceled", "command"), target.published(t, "canceled", "descendant")
	other := target.published(t, "kept", "command")
	cancel()
	if err := <-canceled; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled command returned %v", err)
	}
	if processAlive(command) {
		t.Fatal("the call returned before its command was stopped and reaped")
	}
	gone(t, descendant, "descendant")
	if !processAlive(other) {
		t.Fatal("canceling one command stopped another")
	}
	if err := <-kept; !errors.Is(err, ErrNativeCommandIncomplete) {
		t.Fatalf("the other command returned %v", err)
	}
	if markers := target.leftMarkers(t); len(markers) != 0 || registered(cluster) != 0 {
		t.Fatalf("cancellation left markers %v or a registration", markers)
	}
}

// A command that Docker starts after the caller stopped waiting finds its
// cancellation and starts nothing.
func TestNativeCommandStartedLateRunsNothing(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	// The accepted exec starts once the call has returned, not after a delay
	// that a slow (race-instrumented) run can spend before it cancels.
	gate := make(chan struct{})
	target.startGate = gate
	cluster := nativeCluster()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, err := cluster.RunNativeCommand(ctx, target, waiterCommand(target, "late", time.Minute)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late start returned %v", err)
	}
	close(gate)
	target.wg.Wait()
	if _, err := os.Stat(filepath.Join(target.pids(), "late.command")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a command started after its cancellation ran")
	}
	if markers := target.leftMarkers(t); len(markers) != 0 || registered(cluster) != 0 {
		t.Fatalf("late start left markers %v or a registration", markers)
	}
}

// startUnstopped runs a waiter, pauses its container and cancels the call,
// which then cannot confirm that the command stopped.
func startUnstopped(t *testing.T, cluster *Container, target *nativeTarget, name string, change func(*nativeTarget)) (*NativeCommandUnstoppedError, int) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := cluster.RunNativeCommand(ctx, target, waiterCommand(target, name, time.Minute))
		result <- err
	}()
	command := target.published(t, name, "command")
	target.set(change)
	cancel()
	err := <-result
	var unstopped *NativeCommandUnstoppedError
	if !errors.As(err, &unstopped) || !errors.Is(err, context.Canceled) || classified(err) {
		t.Fatalf("unconfirmed stop returned %v", err)
	}
	if !processAlive(command) || registered(cluster) != 1 {
		t.Fatal("the model lost the running command or its registration")
	}
	return unstopped, command
}

func TestNativeCommandUnstoppedStopRetries(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	unstopped, command := startUnstopped(t, cluster, target, "paused", func(f *nativeTarget) { f.setPaused(true) })
	if !strings.Contains(unstopped.Error(), "waiter paused may still run") {
		t.Fatalf("error does not name the command: %v", unstopped)
	}
	if err := unstopped.Stop(t.Context()); err == nil || registered(cluster) != 1 {
		t.Fatalf("Stop confirmed a command in a paused container: %v", err)
	}
	target.set(func(f *nativeTarget) { f.setPaused(false) })
	if err := unstopped.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if processAlive(command) || registered(cluster) != 0 || len(target.leftMarkers(t)) != 0 {
		t.Fatal("Stop returned before the command was stopped and released")
	}
	if err := unstopped.Stop(t.Context()); err != nil {
		t.Fatalf("a repeated Stop failed: %v", err)
	}
}

// A command ends with its container instance. Stop confirms that once the
// canceler runs in the next instance, or once the container is gone, and
// never from a stopped container that could still start a pending exec.
func TestNativeCommandUnstoppedEndsWithItsContainer(t *testing.T) {
	refuse := func(f *nativeTarget) { f.refuseCancel = true }
	for name, steps := range map[string][]func(*nativeTarget){
		"restarted": {func(f *nativeTarget) { f.start() }, func(f *nativeTarget) { f.refuseCancel = false }},
		"stopped":   {func(f *nativeTarget) { f.killAll(); f.stopped = true; f.refuseCancel = false }, func(f *nativeTarget) { f.start() }},
		"removed":   {func(f *nativeTarget) { f.killAll(); f.removed = true }},
	} {
		t.Run(name, func(t *testing.T) {
			target := newNativeTarget(t, "client", "tc-public")
			cluster := nativeCluster()
			unstopped, command := startUnstopped(t, cluster, target, name, refuse)
			if err := unstopped.Stop(t.Context()); err == nil {
				t.Fatal("Stop confirmed a command whose container still runs")
			}
			for i, step := range steps {
				target.set(step)
				err := unstopped.Stop(t.Context())
				if last := i == len(steps)-1; (err == nil) != last {
					t.Fatalf("after step %d of %d, Stop returned %v", i+1, len(steps), err)
				}
			}
			if registered(cluster) != 0 {
				t.Fatal("the confirmed command is still registered")
			}
			gone(t, command, "command")
		})
	}
}

// Docker can start an exec that it accepted before a restart in the new
// instance. A stop that the canceler cannot confirm then stays unconfirmed,
// although the container has restarted, until the canceler stops the
// command in the new instance.
func TestNativeCommandStartedLateInARestartedContainer(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	// The accepted exec starts only after the restart, however long the
	// refused cancellation takes. The call ends by cancellation, not by a
	// deadline: the supervisor takes its own deadline from the caller's, and
	// a 100 ms one killed the late command before it published its PID.
	gate := make(chan struct{})
	target.startGate = gate
	target.refuseCancel = true
	cluster := nativeCluster()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	time.AfterFunc(100*time.Millisecond, cancel)
	_, err := cluster.RunNativeCommand(ctx, target, waiterCommand(target, "crossing", time.Minute))
	var unstopped *NativeCommandUnstoppedError
	if !errors.As(err, &unstopped) {
		t.Fatalf("a refused cancellation returned %v", err)
	}
	target.restart()
	close(gate)
	command := target.published(t, "crossing", "command")
	if err := unstopped.Stop(t.Context()); err == nil {
		t.Fatal("Stop confirmed a command that runs in the restarted instance")
	}
	target.set(func(f *nativeTarget) { f.refuseCancel = false })
	if err := unstopped.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if processAlive(command) || registered(cluster) != 0 {
		t.Fatal("Stop returned before the command in the new instance was stopped")
	}
}

// A signal that kills the supervisor skips its cleanup, so its command may
// still run: the call reports it as unconfirmed, and the restart that ends
// the command confirms it.
func TestNativeCommandSupervisorKilledBySignal(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	result := make(chan error, 1)
	go func() {
		_, err := cluster.RunNativeCommand(t.Context(), target, waiterCommand(target, "orphaned", time.Minute))
		result <- err
	}()
	command := target.published(t, "orphaned", "command")
	target.set(func(f *nativeTarget) {
		for _, proc := range f.procs {
			_ = proc.Kill()
		}
	})
	err := <-result
	var unstopped *NativeCommandUnstoppedError
	var exit *supervised.ExitError
	if !errors.As(err, &unstopped) || !errors.As(err, &exit) || exit.Code != 128+int(syscall.SIGKILL) {
		t.Fatalf("a killed supervisor returned %v", err)
	}
	if !processAlive(command) || registered(cluster) != 1 {
		t.Fatal("the model lost the orphaned command or its registration")
	}
	target.restart()
	if err := unstopped.Stop(t.Context()); err != nil || registered(cluster) != 0 {
		t.Fatalf("Stop after the restart returned %v", err)
	}
}

func TestTerminateStopsNativeCommands(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	unstopped, paused := startUnstopped(t, cluster, target, "unconfirmed", func(f *nativeTarget) { f.setPaused(true) })
	target.set(func(f *nativeTarget) { f.setPaused(false) })
	running := make(chan error, 1)
	go func() {
		_, err := cluster.RunNativeCommand(t.Context(), target, waiterCommand(target, "running", time.Minute))
		running <- err
	}()
	command, descendant := target.published(t, "running", "command"), target.published(t, "running", "descendant")
	if err := cluster.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-running; !errors.Is(err, errNativeTerminated) || classified(err) || !strings.Contains(err.Error(), "waiter running") {
		t.Fatalf("a running command ended with %v", err)
	}
	if processAlive(command) || processAlive(paused) || registered(cluster) != 0 || len(target.leftMarkers(t)) != 0 {
		t.Fatal("Terminate returned before the native commands were stopped")
	}
	gone(t, descendant, "descendant")
	if err := unstopped.Stop(t.Context()); err != nil {
		t.Fatalf("Stop after Terminate failed: %v", err)
	}
	before := target.execCount()
	if _, err := cluster.RunNativeCommand(t.Context(), target, waiterCommand(target, "after", time.Minute)); !errors.Is(err, errClusterTerminated) || !errors.Is(err, ErrInvalidRequest) || target.execCount() != before {
		t.Fatalf("a command ran after Terminate: %v", err)
	}
}

func TestTerminateKeepsUnconfirmedNativeCommands(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	_, command := startUnstopped(t, cluster, target, "frozen", func(f *nativeTarget) { f.setPaused(true) })
	if err := cluster.Terminate(t.Context()); err == nil || !strings.Contains(err.Error(), "stop waiter frozen") || registered(cluster) != 1 {
		t.Fatalf("Terminate dropped a command it could not stop: %v", err)
	}
	target.set(func(f *nativeTarget) { f.setPaused(false) })
	if err := cluster.Terminate(t.Context()); err != nil || registered(cluster) != 0 || processAlive(command) {
		t.Fatalf("a repeated Terminate did not stop the command: %v", err)
	}
}

// A command admitted while Terminate runs does not start after it.
func TestNativeCommandAdmittedDuringTerminateDoesNotRun(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	target.stateHook = func() error {
		if err := cluster.Terminate(t.Context()); err != nil {
			t.Error(err)
		}
		return nil
	}
	if _, err := cluster.RunNativeCommand(t.Context(), target, waiterCommand(target, "racing", time.Minute)); !errors.Is(err, errClusterTerminated) || !errors.Is(err, ErrInvalidRequest) || target.execCount() != 0 || registered(cluster) != 0 {
		t.Fatalf("a command admitted during Terminate returned %v after %d execs", err, target.execCount())
	}
}

// A Docker call that the caller's cancellation broke fails with an error
// that does not wrap the context's; the call still reports the cancellation.
func TestNativeCommandReportsCancellationBeforeLaunch(t *testing.T) {
	for name, hook := range map[string]func(*nativeTarget, func() error){
		"networks": func(f *nativeTarget, hook func() error) { f.networksHook = hook },
		"state":    func(f *nativeTarget, hook func() error) { f.stateHook = hook },
	} {
		t.Run(name, func(t *testing.T) {
			target := newNativeTarget(t, "client", "tc-public")
			cluster := nativeCluster()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			hook(target, func() error {
				cancel()
				return errors.New("read unix ->/var/run/docker.sock: use of closed network connection")
			})
			if _, err := cluster.RunNativeCommand(ctx, target, waiterCommand(target, name, time.Minute)); !errors.Is(err, context.Canceled) || classified(err) || target.execCount() != 0 {
				t.Fatalf("a call canceled before its launch returned %v after %d execs", err, target.execCount())
			}
		})
	}
}

// An unconfirmed command in one of the cluster's containers ends with the
// container that Terminate removes.
func TestTerminateConfirmsCommandsInRemovedContainers(t *testing.T) {
	owned := newNativeTarget(t, "osd-container")
	cluster := nativeCluster()
	cluster.osds = map[int]*OSDContainer{0: {ID: 0, Container: owned}}
	_, _ = startUnstopped(t, cluster, owned, "owned", func(f *nativeTarget) { f.refuseCancel = true })
	if err := cluster.Terminate(t.Context()); err != nil || registered(cluster) != 0 {
		t.Fatalf("Terminate kept a command whose container it removed: %v", err)
	}
}

// An error from the container implementation, which may quote the command,
// appears only by its kind, and errors.Unwrap still reaches it.
func TestNativeCommandHidesBackendErrorText(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	quoting := &quotingTarget{nativeTarget: target}
	cluster := nativeCluster()
	command := NativeCommand{Argv: []string{"python3", "-c", "key = \"secret-key-value\"\nprint(key)", "pin"}, Timeout: time.Minute, MaxStdout: 64, MaxStderr: 64, Label: "probe"}
	_, err := cluster.RunNativeCommand(t.Context(), quoting, command)
	if err == nil || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "pin") || strings.Contains(err.Error(), "refused") ||
		!strings.Contains(err.Error(), "exec probe supervisor: Docker or the container implementation failed") || !errors.Is(err, errQuotingTarget) ||
		!errors.Is(err, ErrEnvironmentUnavailable) {
		t.Fatalf("a quoted command returned %v", err)
	}
	// A removed container is named by its kind, and refuses the request.
	target.set(func(f *nativeTarget) { f.killAll(); f.removed = true })
	if _, err := cluster.RunNativeCommand(t.Context(), target, command); err == nil || !strings.Contains(err.Error(), "the container does not exist") || !errdefs.IsNotFound(err) ||
		!errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("a removed container returned %v", err)
	}
}

var errQuotingTarget = errors.New("exec refused")

// quotingTarget fails each supervisor exec with an error that quotes it.
type quotingTarget struct{ *nativeTarget }

func (f *quotingTarget) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if len(args) > 2 && args[2] == supervised.Script {
		return 0, nil, fmt.Errorf("%w: %q", errQuotingTarget, args)
	}
	return f.nativeTarget.Exec(ctx, args, opts...)
}

func TestNativeCommandExplainsSupervisorFailures(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	command := NativeCommand{Argv: []string{filepath.Join(target.root, "secret-program")}, Timeout: 10 * time.Second, MaxStdout: 64, MaxStderr: 64, Label: "probe"}
	var missing *OperationError
	if _, err := cluster.RunNativeCommand(t.Context(), target, command); !errors.As(err, &missing) || missing.Kind != ErrImageCapability ||
		!strings.Contains(err.Error(), "could not be started") || strings.Contains(fmt.Sprintf("%v %+v", err, *missing), "secret") {
		t.Fatalf("a missing program returned %v", err)
	}
	target.set(func(f *nativeTarget) { f.noPython = true })
	if _, err := cluster.RunNativeCommand(t.Context(), target, command); !errors.As(err, &missing) || missing.Kind != ErrImageCapability ||
		missing.Capability != "python3" || !strings.Contains(err.Error(), "cannot run python3") {
		t.Fatalf("a container without python3 returned %v", err)
	}
	if registered(cluster) != 0 || len(target.leftMarkers(t)) != 0 {
		t.Fatal("a failed launch left state behind")
	}
	// A launch that failed for want of a process is the environment's.
	target.set(func(f *nativeTarget) { f.noPython = false })
	unavailable := &launchUnavailableTarget{nativeTarget: target}
	if _, err := cluster.RunNativeCommand(t.Context(), unavailable, command); !errors.As(err, &missing) || missing.Kind != ErrEnvironmentUnavailable ||
		errors.Is(err, ErrImageCapability) {
		t.Fatalf("an unavailable launch returned %v", err)
	}
}

// launchUnavailableTarget answers each supervisor exec as a supervisor that
// could not fork its command.
type launchUnavailableTarget struct{ *nativeTarget }

func (f *launchUnavailableTarget) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if len(args) > 2 && args[2] == supervised.Script {
		return supervised.LaunchUnavailableExit, bytes.NewReader(nil), nil
	}
	return f.nativeTarget.Exec(ctx, args, opts...)
}

func TestRedactSecretsUsesTheClusterSecrets(t *testing.T) {
	cluster := &Container{keyring: []byte("[client.admin]\n\tkey = AQBsecretadminkey==\n")}
	cluster.recordClientSecret("AQBreplacedclientkey==")
	data := []byte("admin AQBsecretadminkey== client AQBreplacedclientkey== app opaque-value \x00\xff")
	redacted, err := cluster.RedactSecrets(t.Context(), data, "opaque-value")
	if err != nil {
		t.Fatal(err)
	}
	if want := "admin [REDACTED] client [REDACTED] app [REDACTED] \x00\xff"; string(redacted) != want {
		t.Fatalf("redacted %q, want %q", redacted, want)
	}
	if string(data) != "admin AQBsecretadminkey== client AQBreplacedclientkey== app opaque-value \x00\xff" {
		t.Fatal("RedactSecrets changed its input")
	}
}

// receiptLossNativeTarget runs each supervisor exec to completion and then
// loses its final inspection with cause, as when Docker answers the
// ExecInspect after the output has been drained.
type receiptLossNativeTarget struct {
	*nativeTarget
	cause error
}

func (f *receiptLossNativeTarget) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	code, reader, err := f.nativeTarget.Exec(ctx, args, opts...)
	if err == nil && code == 0 && len(args) > 2 && args[2] == supervised.Script {
		if reader != nil {
			_, _ = io.Copy(io.Discard, reader)
		}
		return 0, nil, fmt.Errorf("lost final exec receipt: %w", f.cause)
	}
	return code, reader, err
}

// A command that ran before its exec lost the final inspection keeps its
// effect. The call is no request refused before any change: it keeps
// Docker's error and matches no category.
func TestNativeCommandLostReceiptAfterTheCommandRan(t *testing.T) {
	requireLinuxGroups(t)
	for name, cause := range map[string]error{"not found": errdefs.ErrNotFound, "conflict": errdefs.ErrConflict} {
		t.Run(name, func(t *testing.T) {
			target := newNativeTarget(t, "client", "tc-public")
			cluster := nativeCluster()
			effect := filepath.Join(target.root, "effect")
			command := NativeCommand{Argv: []string{"sh", "-c", `echo applied > "$1"`, "sh", effect}, Timeout: 20 * time.Second,
				MaxStdout: 4096, MaxStderr: 4096, Label: "mutation"}
			_, err := cluster.RunNativeCommand(t.Context(), &receiptLossNativeTarget{nativeTarget: target, cause: cause}, command)
			var unstopped *NativeCommandUnstoppedError
			if err == nil || errors.Is(err, ErrInvalidRequest) || classified(err) || !errors.Is(err, cause) || errors.As(err, &unstopped) {
				t.Fatalf("a mutation whose receipt was lost returned %v", err)
			}
			if data, readErr := os.ReadFile(effect); readErr != nil || string(data) != "applied\n" {
				t.Fatalf("the mutation's effect is %q, %v", data, readErr)
			}
			// The supervisor's marker stays as the fence of a run that the
			// lost reply cannot tell from one that has not started.
			if registered(cluster) != 0 {
				t.Fatal("the call kept its invocation")
			}
		})
	}
}
