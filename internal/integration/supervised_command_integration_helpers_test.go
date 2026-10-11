//go:build all || integration

package integration_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// testSupervisedCommandCancellation runs the supervisor and canceler that
// AdminSocket and the quorum reads use, on Linux in the control image and
// without a cluster, and covers the orderings that a live cluster cannot
// produce on demand.
func testSupervisedCommandCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	image, _ := integrationImages(t)
	ctr, err := testcontainers.Run(ctx, image, ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("exec started after its cancellation", func(t *testing.T) {
		// Docker may start an exec that it accepted after the caller gave up,
		// and after the canceler ran. Replay that order: cancel first, then
		// start the exact supervisor command for the same invocation.
		marker := supervised.MarkerDir + "/" + uuid.NewString()
		if err := supervised.Cancel(ctx, ctr, "late", marker); err != nil {
			t.Fatal(err)
		}
		command := []string{"python3", "-c", supervised.Script, "4096", "4096", "30",
			"sh", "-c", "touch /tmp/late-started; exec sleep 30"}
		started := time.Now()
		code, output := supervisedTestExec(t, ctx, ctr, command, supervised.MarkerVariable+"="+marker)
		if code != 0 || output != "" || time.Since(started) > 10*time.Second {
			t.Fatalf("late supervisor exited %d after %s: %q", code, time.Since(started), output)
		}
		if code, output := supervisedTestExec(t, ctx, ctr, []string{"sh", "-c", "test ! -e /tmp/late-started && test ! -e " + marker}); code != 0 {
			t.Fatalf("a supervisor started after its cancellation ran its command or kept its marker: %q", output)
		}
	})

	t.Run("cancel stops only its own invocation", func(t *testing.T) {
		first := startSupervisedTestCommand(t, ctx, ctr, "first")
		second := startSupervisedTestCommand(t, ctx, ctr, "second")
		first.cancelAndCheck(t, ctx, ctr)
		if !supervisedTestRunning(t, ctx, ctr, second.pid) {
			t.Fatal("canceling one invocation stopped another")
		}
		second.cancelAndCheck(t, ctx, ctr)
	})

	// Every invocation above, including the one canceled before it started,
	// removed its markers.
	if code, output := supervisedTestExec(t, ctx, ctr, []string{"ls", "-A", supervised.MarkerDir}); code != 0 || strings.TrimSpace(output) != "" {
		t.Fatalf("markers remain: exit %d, %q", code, output)
	}
}

// supervisedTestCommand is one supervised.Command call whose command records
// its PID and waits until it is canceled.
type supervisedTestCommand struct {
	name   string
	pid    string
	cancel context.CancelFunc
	done   chan error
}

func startSupervisedTestCommand(t *testing.T, ctx context.Context, ctr testcontainers.Container, name string) *supervisedTestCommand {
	t.Helper()
	callCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	command := &supervisedTestCommand{name: name, cancel: cancel, done: make(chan error, 1)}
	pidPath := "/tmp/" + name + ".pid"
	go func() {
		_, err := supervised.Command(callCtx, ctr, name, time.Minute, 4096, 4096, "sh", "-c", "echo $$ > "+pidPath+".tmp && mv "+pidPath+".tmp "+pidPath+" && exec sleep 60")
		command.done <- err
	}()
	deadline := time.Now().Add(time.Minute)
	for {
		if code, output := supervisedTestExec(t, ctx, ctr, []string{"cat", pidPath}); code == 0 {
			command.pid = strings.TrimSpace(output)
			return command
		}
		select {
		case err := <-command.done:
			t.Fatalf("%s returned before cancellation: %v", name, err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not start", name)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// cancelAndCheck cancels the call and checks that its command was already
// reaped when supervised.Command returned.
func (command *supervisedTestCommand) cancelAndCheck(t *testing.T, ctx context.Context, ctr testcontainers.Container) {
	t.Helper()
	command.cancel()
	select {
	case err := <-command.done:
		if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "cancel "+command.name+" command") {
			t.Fatalf("%s returned %v", command.name, err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("%s did not return after cancellation", command.name)
	}
	if supervisedTestRunning(t, ctx, ctr, command.pid) {
		t.Fatalf("%s returned while its command %s still exists", command.name, command.pid)
	}
}

// supervisedTestRunning reports whether pid exists in ctr, zombies included.
func supervisedTestRunning(t *testing.T, ctx context.Context, ctr testcontainers.Container, pid string) bool {
	t.Helper()
	code, _ := supervisedTestExec(t, ctx, ctr, []string{"test", "-e", "/proc/" + pid})
	return code == 0
}

func supervisedTestExec(t *testing.T, ctx context.Context, ctr testcontainers.Container, command []string, env ...string) (int, string) {
	t.Helper()
	code, reader, err := ctr.Exec(ctx, command, tcexec.Multiplexed(), tcexec.WithEnv(env))
	if err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return code, string(output)
}
