package ceph

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

// inputWorker reads one record per expected SHA-256 from its standard input,
// publishes whether each matched below pids, and prints only the record
// count, whether all matched and whether its input then ended. It publishes
// "ready" before it reads.
const inputWorker = `import hashlib,os,sys
pids,name=sys.argv[1],sys.argv[2]
def publish(role,text):
 path=os.path.join(pids,name+'.'+role)
 with open(path+'.tmp','w') as f: f.write(text)
 os.rename(path+'.tmp',path)
publish('ready','1')
matched=[]
for index,spec in enumerate(sys.argv[3:]):
 size,digest=spec.split(':')
 data=b''
 while len(data)<int(size):
  chunk=os.read(0,int(size)-len(data))
  if not chunk: break
  data+=chunk
 matched.append(hashlib.sha256(data).hexdigest()==digest)
 publish('record%d'%index,str(matched[-1]))
print(len(matched),all(matched),os.read(0,1)==b'')
`

func recordSpec(record []byte) string {
	sum := sha256.Sum256(record)
	return strconv.Itoa(len(record)) + ":" + hex.EncodeToString(sum[:])
}

// awaitFile waits until the named command published role below pids.
func awaitFile(t *testing.T, target *nativeTarget, name, role string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if data, err := os.ReadFile(filepath.Join(target.pids(), name+"."+role)); err == nil {
			return string(data)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not publish %s", name, role)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type nativeRun struct {
	result NativeCommandResult
	err    error
}

func startNative(ctx context.Context, cluster *Container, target *nativeTarget, command NativeCommand) chan nativeRun {
	done := make(chan nativeRun, 1)
	go func() {
		result, err := cluster.RunNativeCommand(ctx, target, command)
		done <- nativeRun{result, err}
	}()
	return done
}

// Two binary records reach a worker in separate phases, after it published
// that it is ready, and then the end of input. The worker reports only
// whether they matched.
func TestNativeCommandInputDeliversRecordsInPhases(t *testing.T) {
	requireLinuxGroups(t)
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	first := []byte{0x00, 'r', 'e', 'c', 0xff, 0x00, '\n', 0x80}
	second := append(bytes.Repeat([]byte{0xfe, 0x00}, 40000), 0x7f)
	input := NewNativeInput()
	done := startNative(t.Context(), cluster, target, NativeCommand{
		Argv:    []string{"python3", "-c", inputWorker, target.pids(), "phases", recordSpec(first), recordSpec(second)},
		Timeout: 20 * time.Second, MaxStdout: 4096, MaxStderr: 4096, Input: input, MaxInput: len(first) + len(second), Label: "phases"})
	awaitFile(t, target, "phases", "ready")
	if err := input.Write(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if matched := awaitFile(t, target, "phases", "record0"); matched != "True" {
		t.Fatalf("first record matched %s", matched)
	}
	if err := input.Write(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	awaitFile(t, target, "phases", "record1")
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
	run := <-done
	total := int64(len(first) + len(second))
	if run.err != nil || run.result.ExitCode != 0 || !run.result.Complete() || string(run.result.Stdout) != "2 True True\n" ||
		run.result.InputWritten != total || run.result.InputDelivered != total || !run.result.InputEOF {
		t.Fatalf("result %+v, %v", run.result, run.err)
	}
	if err := input.Write(t.Context(), []byte("late")); !errors.Is(err, ErrNativeInputClosed) {
		t.Fatalf("a write after the command returned %v", err)
	}
	if markers := target.leftMarkers(t); len(markers) != 0 || registered(cluster) != 0 {
		t.Fatalf("left markers %v or a registration", markers)
	}
}

func TestNativeCommandInputEndsEmpty(t *testing.T) {
	requireLinuxGroups(t)
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	input := NewNativeInput()
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := cluster.RunNativeCommand(t.Context(), target, NativeCommand{
		Argv:    []string{"python3", "-c", "import sys; print(len(sys.stdin.buffer.read()))"},
		Timeout: 10 * time.Second, MaxStdout: 64, MaxStderr: 4096, Input: input, MaxInput: 1})
	if err != nil || string(result.Stdout) != "0\n" || result.InputWritten != 0 || result.InputDelivered != 0 || !result.InputEOF {
		t.Fatalf("result %+v, %v", result, err)
	}
}

// A write that reaches MaxInput exactly is taken; one more byte is refused
// whole, and the input stays open.
func TestNativeCommandInputLimit(t *testing.T) {
	requireLinuxGroups(t)
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	input := NewNativeInput()
	done := startNative(t.Context(), cluster, target, NativeCommand{
		Argv:    []string{"python3", "-c", "import sys; print(sys.stdin.buffer.read().hex())"},
		Timeout: 10 * time.Second, MaxStdout: 64, MaxStderr: 4096, Input: input, MaxInput: 4})
	if err := input.Write(t.Context(), []byte{0x00, 0x01, 0x02}); err != nil {
		t.Fatal(err)
	}
	if err := input.Write(t.Context(), []byte{0x03, 0x04}); !errors.Is(err, ErrInvalidRequest) || strings.Contains(err.Error(), "\x03") {
		t.Fatalf("a write past the limit returned %v", err)
	}
	if err := input.Write(t.Context(), []byte{0xff}); err != nil {
		t.Fatalf("the write that reaches the limit returned %v", err)
	}
	_ = input.Close()
	run := <-done
	if run.err != nil || string(run.result.Stdout) != "000102ff\n" || run.result.InputWritten != 4 || !run.result.InputEOF {
		t.Fatalf("result %+v, %v", run.result, run.err)
	}
}

func TestNativeCommandInputRefusesMalformedLimitsBeforeRunning(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	used := NewNativeInput()
	used.claimed.Store(true)
	for name, command := range map[string]NativeCommand{
		"no limit":              {Input: NewNativeInput()},
		"negative limit":        {Input: NewNativeInput(), MaxInput: -1},
		"limit too large":       {Input: NewNativeInput(), MaxInput: MaxNativeCommandInput + 1},
		"limit without input":   {MaxInput: 8},
		"input not constructed": {Input: &NativeInput{}, MaxInput: 8},
		"input already used":    {Input: used, MaxInput: 8},
	} {
		command.Argv, command.Timeout, command.MaxStdout, command.MaxStderr = []string{"true"}, time.Second, 64, 64
		if _, err := cluster.RunNativeCommand(t.Context(), target, command); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("%s returned %v", name, err)
		}
	}
	if target.execCount() != 0 {
		t.Fatalf("a malformed command started %d execs", target.execCount())
	}
}

// A write waits for its command; its context bounds the wait.
func TestNativeCommandInputWriteBeforeTheCommandStarts(t *testing.T) {
	input := NewNativeInput()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := input.Write(ctx, []byte{1}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a write without a command returned %v", err)
	}
	_ = input.Close()
	if err := input.Write(t.Context(), []byte{1}); !errors.Is(err, ErrNativeInputClosed) {
		t.Fatalf("a write after Close returned %v", err)
	}
}

// A command that exits without reading leaves the input it did not take
// undelivered, without the end of input, and later writes fail.
func TestNativeCommandInputCommandExitsEarly(t *testing.T) {
	requireLinuxGroups(t)
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	input := NewNativeInput()
	done := startNative(t.Context(), cluster, target, NativeCommand{
		Argv:    []string{"python3", "-c", "import os,sys\nos.read(0,4)\nos.close(0)\nprint('closed')"},
		Timeout: 10 * time.Second, MaxStdout: 64, MaxStderr: 4096, Input: input, MaxInput: 1 << 20})
	if err := input.Write(t.Context(), []byte("four")); err != nil {
		t.Fatal(err)
	}
	// The supervisor takes this at once, although the command never reads it.
	if err := input.Write(t.Context(), bytes.Repeat([]byte{0xaa}, 256<<10)); err != nil && !errors.Is(err, ErrNativeInputClosed) {
		t.Fatal(err)
	}
	run := <-done
	if run.err != nil || string(run.result.Stdout) != "closed\n" || run.result.InputEOF ||
		run.result.InputDelivered < 4 || run.result.InputDelivered >= run.result.InputWritten {
		t.Fatalf("result %+v, %v", run.result, run.err)
	}
	if err := input.Write(t.Context(), []byte{1}); !errors.Is(err, ErrNativeInputClosed) {
		t.Fatalf("a write after the command ended returned %v", err)
	}
}

// Large input, output and error streams move at once: the command copies its
// input to stdout and writes to stderr past its limit.
func TestNativeCommandInputWithBothOutputStreams(t *testing.T) {
	requireLinuxGroups(t)
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	data := make([]byte, 512<<10)
	for i := range data {
		data[i] = byte(i * 7)
	}
	input := NewNativeInput()
	done := startNative(t.Context(), cluster, target, NativeCommand{
		Argv: []string{"python3", "-c", `import os,sys
while True:
 chunk=os.read(0,4096)
 if not chunk: break
 sys.stdout.buffer.write(chunk); sys.stderr.buffer.write(b'e'*len(chunk))`},
		Timeout: 20 * time.Second, MaxStdout: len(data), MaxStderr: 64 << 10, Input: input, MaxInput: len(data)})
	for chunk := range slices.Chunk(data, 50000) {
		if err := input.Write(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
	}
	_ = input.Close()
	run := <-done
	if !errors.Is(run.err, ErrNativeCommandIncomplete) || !bytes.Equal(run.result.Stdout, data) || !run.result.StderrTruncated ||
		run.result.StdoutTruncated || !run.result.InputEOF || run.result.InputDelivered != int64(len(data)) {
		t.Fatalf("result stdout=%d stderr=%d %+v, %v", len(run.result.Stdout), len(run.result.Stderr), run.result.InputEOF, run.err)
	}
}

// Canceling the call while a write waits for a command that does not read
// stops the command, fails the write and joins the input work; no marker or
// registration remains.
func TestNativeCommandInputCancellationDuringTransfer(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	input := NewNativeInput()
	command := waiterCommand(target, "input", time.Minute)
	command.Input, command.MaxInput = input, MaxNativeCommandInput
	done := startNative(ctx, cluster, target, command)
	pid := target.published(t, "input", "command")
	writes := make(chan error, 1)
	go func() {
		chunk := bytes.Repeat([]byte{0x55}, 1<<20)
		for {
			if err := input.Write(t.Context(), chunk); err != nil {
				writes <- err
				return
			}
		}
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()
	run := <-done
	if !errors.Is(run.err, context.Canceled) {
		t.Fatalf("canceled command returned %+v, %v", run.result, run.err)
	}
	select {
	case err := <-writes:
		if !errors.Is(err, ErrNativeInputClosed) && !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("the writer ended with %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the writer did not return after the command was canceled")
	}
	gone(t, pid, "command")
	if markers := target.leftMarkers(t); len(markers) != 0 || registered(cluster) != 0 {
		t.Fatalf("cancellation left markers %v or a registration", markers)
	}
}

// Terminate stops a command that waits for input and ends its input.
func TestTerminateStopsACommandWithInput(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	input := NewNativeInput()
	command := waiterCommand(target, "terminated", time.Minute)
	command.Input, command.MaxInput = input, 1024
	done := startNative(t.Context(), cluster, target, command)
	pid := target.published(t, "terminated", "command")
	if errs := cluster.stopNativeInvocations(t.Context()); len(errs) != 0 {
		t.Fatal(errs)
	}
	if run := <-done; !errors.Is(run.err, errNativeTerminated) {
		t.Fatalf("terminated command returned %v", run.err)
	}
	gone(t, pid, "command")
	if err := input.Write(t.Context(), []byte{1}); !errors.Is(err, ErrNativeInputClosed) {
		t.Fatalf("a write after Terminate returned %v", err)
	}
}

// A container that refuses the exec fails the call before the command runs,
// and the input then takes no writes.
func TestNativeCommandInputRefusedExec(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	input := NewNativeInput()
	target.refuseAttach = true
	_, err := cluster.RunNativeCommand(t.Context(), target, NativeCommand{Argv: []string{"true"}, Timeout: time.Second,
		MaxStdout: 64, MaxStderr: 64, Input: input, MaxInput: 8})
	if err == nil {
		t.Fatal("a refused exec succeeded")
	}
	if err := input.Write(t.Context(), []byte{1}); !errors.Is(err, ErrNativeInputClosed) {
		t.Fatalf("a write after the refused exec returned %v", err)
	}
	if registered(cluster) != 0 {
		t.Fatal("a refused exec stayed registered")
	}
}

// The supervisor takes what is written while the command does not read, so
// Write returns before the command reads anything.
func TestNativeCommandInputDoesNotWaitForACommandThatDoesNotRead(t *testing.T) {
	requireLinuxGroups(t)
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	gate := filepath.Join(target.pids(), "gate")
	input := NewNativeInput()
	data := bytes.Repeat([]byte{0x00, 0xff}, 512<<10)
	done := startNative(t.Context(), cluster, target, NativeCommand{
		Argv: []string{"python3", "-c", `import os,sys,time
while not os.path.exists(sys.argv[1]): time.sleep(0.01)
print(len(sys.stdin.buffer.read()))`, gate},
		Timeout: 20 * time.Second, MaxStdout: 64, MaxStderr: 4096, Input: input, MaxInput: len(data)})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := input.Write(ctx, data); err != nil {
		t.Fatalf("a write to a command that does not read returned %v", err)
	}
	if err := os.WriteFile(gate, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	_ = input.Close()
	run := <-done
	if run.err != nil || string(run.result.Stdout) != strconv.Itoa(len(data))+"\n" || run.result.InputDelivered != int64(len(data)) || !run.result.InputEOF {
		t.Fatalf("result %+v, %v", run.result, run.err)
	}
}

// A call canceled before its supervisor starts runs nothing, also with input,
// and the input then takes no writes.
func TestNativeCommandInputCanceledBeforeLaunch(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	target.startDelay = 400 * time.Millisecond
	cluster := nativeCluster()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	input := NewNativeInput()
	command := waiterCommand(target, "late", time.Minute)
	command.Input, command.MaxInput = input, 8
	if _, err := cluster.RunNativeCommand(ctx, target, command); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late start returned %v", err)
	}
	target.wg.Wait()
	if _, err := os.Stat(filepath.Join(target.pids(), "late.command")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a command started after its cancellation ran")
	}
	if markers := target.leftMarkers(t); len(markers) != 0 || registered(cluster) != 0 {
		t.Fatalf("late start left markers %v or a registration", markers)
	}
	if err := input.Write(t.Context(), []byte{1}); !errors.Is(err, ErrNativeInputClosed) {
		t.Fatalf("a write after the canceled call returned %v", err)
	}
}

// A supervisor killed by a signal leaves its command unconfirmed, as without
// input, and Stop confirms it after the container restarted.
func TestNativeCommandInputSupervisorKilledBySignal(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	input := NewNativeInput()
	command := waiterCommand(target, "orphaned", time.Minute)
	command.Input, command.MaxInput = input, 8
	done := startNative(t.Context(), cluster, target, command)
	pid := target.published(t, "orphaned", "command")
	target.set(func(f *nativeTarget) {
		for _, proc := range f.procs {
			_ = proc.Kill()
		}
	})
	run := <-done
	var unstopped *NativeCommandUnstoppedError
	var exit *supervised.ExitError
	if !errors.As(run.err, &unstopped) || !errors.As(run.err, &exit) || exit.Code != 128+int(syscall.SIGKILL) {
		t.Fatalf("a killed supervisor returned %v", run.err)
	}
	if !processAlive(pid) || registered(cluster) != 1 {
		t.Fatal("the model lost the orphaned command or its registration")
	}
	if err := input.Write(t.Context(), []byte{1}); !errors.Is(err, ErrNativeInputClosed) {
		t.Fatalf("a write after the killed supervisor returned %v", err)
	}
	target.restart()
	if err := unstopped.Stop(t.Context()); err != nil || registered(cluster) != 0 {
		t.Fatalf("Stop after the restart returned %v", err)
	}
}

// A supervisor killed by a signal just as cancellation arrives leaves its
// command unconfirmed all the same: the call keeps the registration and a
// Stop handle until the container restarted.
func TestNativeCommandInputCancellationAtSupervisorDeath(t *testing.T) {
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	target.attachClosed = cancel
	input := NewNativeInput()
	command := waiterCommand(target, "orphaned", time.Minute)
	command.Input, command.MaxInput = input, 8
	done := startNative(ctx, cluster, target, command)
	pid := target.published(t, "orphaned", "command")
	target.set(func(f *nativeTarget) {
		for _, proc := range f.procs {
			_ = proc.Kill()
		}
	})
	run := <-done
	var unstopped *NativeCommandUnstoppedError
	var exit *supervised.ExitError
	if !errors.As(run.err, &unstopped) || !errors.As(run.err, &exit) || exit.Code != 128+int(syscall.SIGKILL) || !errors.Is(run.err, context.Canceled) {
		t.Fatalf("a killed supervisor under cancellation returned %v", run.err)
	}
	if !processAlive(pid) || registered(cluster) != 1 {
		t.Fatal("the model lost the orphaned command or its registration")
	}
	target.restart()
	if err := unstopped.Stop(t.Context()); err != nil || registered(cluster) != 0 {
		t.Fatalf("Stop after the restart returned %v", err)
	}
}

// A cancellation that arrives just as the command's output ended returns the
// call, which keeps no registration and claims no marker, and Terminate
// finds nothing left to wait for.
func TestNativeCommandInputCancellationAtCompletion(t *testing.T) {
	requireLinuxGroups(t)
	target := newNativeTarget(t, "client", "tc-public")
	cluster := nativeCluster()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	target.attachClosed = cancel
	input := NewNativeInput()
	_ = input.Close()
	done := startNative(ctx, cluster, target, NativeCommand{Argv: []string{"python3", "-c", "print('done')"},
		Timeout: 10 * time.Second, MaxStdout: 64, MaxStderr: 4096, Input: input, MaxInput: 1})
	select {
	case run := <-done:
		if !errors.Is(run.err, context.Canceled) {
			t.Fatalf("result %+v, %v", run.result, run.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("RunNativeCommand stuck after its output completion coincided with cancellation")
	}
	if markers := target.leftMarkers(t); len(markers) != 0 || registered(cluster) != 0 {
		t.Fatalf("left markers %v or a registration", markers)
	}
	terminated := make(chan []error, 1)
	go func() { terminated <- cluster.stopNativeInvocations(t.Context()) }()
	select {
	case errs := <-terminated:
		if len(errs) != 0 {
			t.Fatal(errs)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Terminate waited for the finished call")
	}
}
