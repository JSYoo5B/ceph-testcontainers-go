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

func TestOwnerWithdrawalRequiresStartedEvidence(t *testing.T) {
	for _, action := range []string{"build-started", "detach-started"} {
		t.Run(action, func(t *testing.T) {
			for _, tc := range []struct {
				name, field    string
				valid, started bool
			}{
				{name: "absent"},
				{name: "null", field: `,"started":null`},
				{name: "not started", field: `,"started":false`, valid: true},
				{name: "started", field: `,"started":true`, valid: true, started: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					b := &Container{name: "owned", instance: "test-keeper", detachID: randomID()}
					sent, stopped := &b.creationSent, &b.creationStopped
					if action == "detach-started" {
						sent, stopped = &b.detachSent, &b.detachStopped
					}
					*sent, *stopped = true, true
					b.nativeCommand = func(_ context.Context, argv ...string) ([]byte, error) {
						if argv[3] != action {
							t.Fatalf("unexpected owner action %q", argv[3])
						}
						return []byte(`{"lease":"owned","instance":"test-keeper","attempt":` + strconv.Quote(b.detachID) + tc.field + `}`), nil
					}
					err := b.withdrawUnstarted(t.Context())
					if tc.valid {
						if err != nil || *sent != tc.started || *stopped {
							t.Fatalf("explicit receipt: sent=%t stopped=%t err=%v", *sent, *stopped, err)
						}
					} else if !errors.Is(err, ceph.ErrNativeFailure) || !*sent || !*stopped {
						t.Fatalf("incomplete receipt altered pending dispatch: sent=%t stopped=%t err=%v", *sent, *stopped, err)
					}
				})
			}
		})
	}
}

// Run the keeper's actual detach, poll and unlink code on a temporary file.
// The subprocess substitute models just losetup, including one post-detach
// query failure. No kernel mapping or host loop is changed by this test.
func TestTerminateRechecksAssociationAfterSuccessfulDetach(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("Python 3 is required for the keeper readback test")
	}
	for _, replaced := range []bool{false, true} {
		t.Run(map[bool]string{false: "original backing", true: "replacement backing"}[replaced], func(t *testing.T) {
			b, model := newLifetimeFixture(t)
			dir := t.TempDir()
			backing := filepath.Join(dir, "data-"+b.name)
			detachMarker := filepath.Join(dir, "detached")
			queryMarker := filepath.Join(dir, "query-failed")
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			setup := exec.CommandContext(ctx, python, "-I", "-c", `
import json, os, pathlib, sys
root, name, generation, loop = sys.argv[1:]
path = pathlib.Path(root) / ("data-" + name)
path.write_text("original backing")
s = path.stat()
(pathlib.Path(root) / "create.json").write_text(json.dumps(dict(
    lease=name, instance=generation, file=str(path), loop=loop, Inode=s.st_ino, FileDev=s.st_dev, done=True)))
`, dir, b.name, b.instance, b.loop.Loop)
			if out, err := setup.CombinedOutput(); err != nil {
				t.Fatalf("prepare keeper manifest: %v: %s", err, out)
			}
			prelude := "root = " + strconv.Quote(dir) + "\nloop = " + strconv.Quote(b.loop.Loop) + "\n" + `
import errno, os, pathlib, subprocess
native_listdir = os.listdir
def listdir(path):
    if str(path).startswith("/sys/block/") and str(path).endswith("/holders"):
        return []
    return native_listdir(path)
os.listdir = listdir
detached = pathlib.Path(root) / "detached"
failed = pathlib.Path(root) / "query-failed"
def run(args, **kwargs):
    if args == ("losetup", "--detach", loop):
        with detached.open("x"):
            pass
        return subprocess.CompletedProcess(args, 0, "", "")
    if args[:3] == ("losetup", "--list", "--associated"):
        if not detached.exists():
            return subprocess.CompletedProcess(args, 0, loop, "")
        if not failed.exists():
            failed.touch()
            raise OSError(errno.EIO, "injected association read failure")
        return subprocess.CompletedProcess(args, 0, "", "")
    raise AssertionError(args)
subprocess.run = run
`
			script := strings.Replace(ownerScript, `work = "/tc-blk"`, "work = "+strconv.Quote(dir), 1)
			script = strings.Replace(script, "generation=instance()", "generation="+strconv.Quote(b.instance), 1)
			program := filepath.Join(dir, "owner.py")
			if err := os.WriteFile(program, []byte(prelude+script), 0o600); err != nil {
				t.Fatal(err)
			}
			var loopChecks int
			b.nativeCommand = func(ctx context.Context, argv ...string) ([]byte, error) {
				switch argv[3] {
				case "start-detach":
					reply, err := model.command(ctx, argv...)
					if err == nil {
						// Drive the detached child to completion before polling.
						childArgs := append([]string{"-I", program, "detach"}, argv[4:]...)
						_, err = exec.CommandContext(ctx, python, childArgs...).Output()
					}
					return reply, err
				case "detached", "unlink":
					return exec.CommandContext(ctx, python, append([]string{"-I", program}, argv[3:]...)...).Output()
				case "exec":
					if len(argv) > 12 && argv[11] == "loopid" {
						loopChecks++
						if _, err := os.Stat(detachMarker); err == nil {
							return nil, errors.New("the old loop is absent")
						}
					}
				}
				return model.command(ctx, argv...)
			}
			if err := b.Terminate(ctx); err == nil || !b.mappingRemoved || !b.detachSent || b.loopReleased || model.keeper.calls != 0 {
				t.Fatalf("unavailable association did not retain the attempt: sent=%t released=%t err=%v", b.detachSent, b.loopReleased, err)
			}
			if _, err := os.Stat(queryMarker); err != nil {
				t.Fatalf("first association read did not fail: %v", err)
			}
			if replaced {
				if err := os.Rename(backing, backing+".original"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(backing, []byte("replacement"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			err := b.Terminate(ctx)
			if model.detachCalls != 1 || loopChecks != 1 {
				t.Fatalf("recovery acted on the old loop again: detaches=%d loop lookups=%d err=%v", model.detachCalls, loopChecks, err)
			}
			if replaced {
				if data, readErr := os.ReadFile(backing); err == nil || b.released || model.keeper.calls != 0 ||
					readErr != nil || string(data) != "replacement" {
					t.Fatalf("replacement was not retained: release=%t err=%v read=%v", b.released, err, readErr)
				}
			} else if _, readErr := os.Stat(backing); err != nil || !b.released || model.keeper.calls != 1 || !errors.Is(readErr, os.ErrNotExist) {
				t.Fatalf("positive absence did not release the original backing: release=%t err=%v stat=%v", b.released, err, readErr)
			}
		})
	}
}

// The keeper's probe reads the image tools through the probe it shares with
// ceph/preflight and keeps the classification Run reports: a missing tool is
// an image capability, before the private tmpfs or host gates are read.
func TestOwnerProbeClassifiesMissingToolsAsImageCapability(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is not available")
	}
	shell, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("sh is not available")
	}
	dir := t.TempDir()
	for _, name := range []string{"sh", "sleep", "losetup"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!"+shell+"\nexit 0\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	script := strings.Replace(ownerScript, "generation=instance()", `generation="test-keeper"`, 1)
	program := filepath.Join(dir, "owner.py")
	if err := os.WriteFile(program, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	b := &Container{name: "owned", size: 64 << 20}
	b.nativeCommand = func(ctx context.Context, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, python, append([]string{"-I", program}, args[3:]...)...)
		cmd.Env = []string{"PATH=" + dir}
		return cmd.Output()
	}
	_, err = b.owner(t.Context(), "probe")
	if !errors.Is(err, ceph.ErrImageCapability) || !strings.Contains(err.Error(), "missing executables: dmsetup, blockdev") {
		t.Fatalf("missing tools were not an image capability: %v", err)
	}
}
