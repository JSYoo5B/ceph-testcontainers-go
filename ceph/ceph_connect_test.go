package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// monStallTarget models the ceph CLI of a cluster whose MONs do not answer
// for a while, as when the runner's storage stalls: each connection during
// the stall times out before the command is sent, and afterwards the command
// applies once.
type monStallTarget struct {
	testcontainers.Container
	stalled           int
	attempts, applied int
	onStall           func()
	// stderr is the output of a stalled connection; it defaults to the
	// expiry of --connect-timeout.
	stderr string
}

func (f *monStallTarget) Exec(_ context.Context, _ []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	f.attempts++
	if f.stalled > 0 {
		f.stalled--
		if f.onStall != nil {
			f.onStall()
		}
		stderr := f.stderr
		if stderr == "" {
			stderr = "timed out\n"
		}
		return 1, multiplexed(stdcopy.Stderr, stderr), nil
	}
	f.applied++
	return 0, multiplexed(stdcopy.Stdout, "ok"), nil
}

func multiplexed(stream stdcopy.StdType, data string) io.Reader {
	var buffer bytes.Buffer
	header := make([]byte, 8)
	header[0] = byte(stream)
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	buffer.Write(header)
	buffer.WriteString(data)
	return &buffer
}

func shortCephReconnect(t *testing.T) {
	t.Helper()
	wait, pause := cephConnectWait, cephConnectPause
	cephConnectWait, cephConnectPause = time.Minute, time.Millisecond
	t.Cleanup(func() { cephConnectWait, cephConnectPause = wait, pause })
}

// A command whose connection times out was not sent: it runs once the MONs
// answer, and exactly once.
func TestCephConnectsAgainAfterAMonitorStall(t *testing.T) {
	shortCephReconnect(t)
	target := &monStallTarget{stalled: 3}
	out, err := command(t.Context(), target, "ceph", "--connect-timeout", "5", "osd", "pool", "create", "data")
	if err != nil || string(out) != "ok" || target.attempts != 4 || target.applied != 1 {
		t.Fatalf("returned %q, %v after %d attempts and %d changes", out, err, target.attempts, target.applied)
	}
}

func TestCephConnectsAgainOnlyWhenNothingWasSent(t *testing.T) {
	shortCephReconnect(t)
	for name, args := range map[string][]string{
		"no connection bound": {"ceph", "osd", "pool", "create", "data"},
		"help":                {"ceph", "--connect-timeout", "5", "osd", "-h"},
		"another program":     {"rbd", "--connect-timeout", "5", "ls"},
	} {
		target := &monStallTarget{stalled: 3}
		if _, err := command(t.Context(), target, args...); !errors.Is(err, ErrNativeFailure) || target.attempts != 1 {
			t.Fatalf("%s: returned %v after %d attempts", name, err, target.attempts)
		}
	}
	// A failure that Ceph reported is no connection timeout.
	refusal := &commandTarget{code: 2, stderr: "Error ENOENT: unrecognized pool 'x'\n"}
	if _, err := command(t.Context(), refusal, "ceph", "--connect-timeout", "5", "osd", "pool", "get", "x", "size"); !errors.Is(err, ErrNativeFailure) {
		t.Fatalf("a refusal returned %v", err)
	}
}

// replyTarget models a ceph CLI that sends its command, which changes state,
// and then exits with output that only mentions a connection: Ceph's own
// reply ("Error EPERM: ...") or a connection error that waiting cannot fix.
type replyTarget struct {
	testcontainers.Container
	stderr            string
	attempts, applied int
}

func (f *replyTarget) Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
	f.attempts++
	f.applied++
	return 1, multiplexed(stdcopy.Stderr, f.stderr), nil
}

// Only the exact outputs of a connection that timed out are retried: a reply
// to a command that ran is never sent again.
func TestCephDoesNotResendACommandThatRan(t *testing.T) {
	shortCephReconnect(t)
	for name, stderr := range map[string]string{
		"errno reply":     "Error EPERM: error connecting to the cluster\n",
		"timeout reply":   "Error ETIMEDOUT: timed out\n",
		"missing keyring": "[errno 2] RADOS object not found (error connecting to the cluster)\n",
		"partial output":  "timed out waiting for the PG\n",
	} {
		target := &replyTarget{stderr: stderr}
		_, err := command(t.Context(), target, "ceph", "--connect-timeout", "5", "osd", "pool", "create", "data")
		var failure *NativeFailure
		if !errors.As(err, &failure) || target.attempts != 1 || target.applied != 1 {
			t.Fatalf("%s: returned %v after %d attempts and %d changes", name, err, target.attempts, target.applied)
		}
	}
	// librados's own connection timeout sent nothing either.
	target := &monStallTarget{stalled: 1}
	target.stderr = "[errno 110] RADOS timed out (error connecting to the cluster)\n"
	if _, err := command(t.Context(), target, "ceph", "--connect-timeout", "5", "fsid"); err != nil || target.attempts != 2 || target.applied != 1 {
		t.Fatalf("a librados timeout returned %v after %d attempts", err, target.attempts)
	}
}

// The attempts end with the caller's context, which the call then matches
// without a category, and after cephConnectWait with the last failure.
func TestCephConnectionAttemptsEnd(t *testing.T) {
	shortCephReconnect(t)
	ctx, cancel := context.WithCancel(t.Context())
	target := &monStallTarget{stalled: 100}
	target.onStall = func() {
		if target.attempts == 2 {
			cancel()
		}
	}
	if _, err := command(ctx, target, "ceph", "--connect-timeout", "5", "fsid"); !errors.Is(err, context.Canceled) || classified(err) || target.applied != 0 {
		t.Fatalf("a canceled wait returned %v after %d changes", err, target.applied)
	}
	cephConnectWait = 0
	target = &monStallTarget{stalled: 100}
	var failure *NativeFailure
	if _, err := command(t.Context(), target, "ceph", "--connect-timeout", "5", "fsid"); !errors.As(err, &failure) || failure.Reason != "timed out" ||
		target.attempts != 1 || target.applied != 0 {
		t.Fatalf("an exhausted wait returned %v after %d attempts", err, target.attempts)
	}
}
