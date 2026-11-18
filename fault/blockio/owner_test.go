package blockio

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestOwnerBuildLaunchRefusalSettlesWithoutBacking(t *testing.T) {
	dir := t.TempDir()
	instance := "test-keeper"
	// Keep the owner protocol and manifest paths intact within a private
	// directory. Substitute only the Linux generation read and Popen.
	script := strings.Replace(ownerScript, `work = "/tc-blk"`, "work = "+strconv.Quote(dir), 1)
	script = strings.Replace(script, "generation=instance()", "generation="+strconv.Quote(instance), 1)
	const prelude = `
import errno, subprocess, sys
def refuse(*args, **kwargs):
    raise OSError(errno.EAGAIN, "injected creation launch refusal")
subprocess.Popen = refuse
`
	program := filepath.Join(dir, "owner.py")
	if err := os.WriteFile(program, []byte(prelude+script), 0o600); err != nil {
		t.Fatal(err)
	}
	keeper := &cleanupKeeper{failures: 1}
	b := &Container{Container: keeper, name: "owned", instance: instance, size: 64 << 20}
	var actions []string
	b.nativeCommand = func(ctx context.Context, args ...string) ([]byte, error) {
		action := args[3]
		actions = append(actions, action)
		switch action {
		case "start", "inspect", "build-started":
			cmd := exec.CommandContext(ctx, "python3", append([]string{"-I", program}, args[3:]...)...)
			return cmd.Output()
		default:
			t.Fatalf("a launch refusal cannot require a %s operation", action)
			return nil, nil
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	// The detached launch failed before any child or backing artifact existed.
	// It still claimed the exclusive dispatch marker and must not be replayed.
	dispatchErr := b.startCreation(ctx)
	if dispatchErr != nil {
		t.Logf("dispatch result: %v", dispatchErr)
	}
	if _, err := os.Stat(filepath.Join(dir, "build.started")); err != nil {
		t.Fatal("build was never claimed:", err)
	}
	checkRefusal := func(err error) {
		t.Helper()
		if !errors.Is(err, ceph.ErrNativeFailure) || !strings.Contains(err.Error(), "injected creation launch refusal") {
			t.Fatalf("known refusal was not retained: %v", err)
		}
	}
	_, err := b.Status(ctx)
	checkRefusal(err)
	checkRefusal(b.Restore(ctx))
	if err := b.Terminate(ctx); err == nil || !strings.Contains(err.Error(), "helper termination failed") {
		t.Fatalf("keeper did not reach its first termination attempt: %v", err)
	}
	if err := b.Terminate(ctx); err != nil || !b.released || keeper.calls != 2 {
		t.Fatalf("keeper was not released on retry: released=%t calls=%d err=%v", b.released, keeper.calls, err)
	}
	if got := strings.Join(actions, ","); got != "start,inspect,inspect,inspect" {
		t.Fatalf("release should not dispatch a backing mutation or replay creation: %s", got)
	}
}
