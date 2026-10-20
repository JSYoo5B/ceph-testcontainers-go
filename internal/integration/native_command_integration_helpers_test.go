//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/containerd/errdefs"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// testNativeCommands runs native commands under RunNativeCommand in the
// cluster's own containers and in caller-owned containers, and checks the
// processes and supervisor markers inside each container rather than only
// the returned errors.
func testNativeCommands(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t, append([]testcontainers.ContainerCustomizer{ceph.WithOSDCount(1), ceph.WithPoolDefaults(1, 1)}, opts...)...)
	control := cluster.ControlContainer()
	logNativeRuntime(t, ctx, cluster, map[string]testcontainers.Container{
		"control": control, "mgr": cluster.ManagerContainer(), "osd": cluster.OSDs()[0], "client": client,
	})

	// The reproduction of the issue: a read-only MGR query that hangs on a
	// frozen MGR. A raw Ceph call leaves its CLI running; this call does not.
	t.Run("paused_mgr_read", func(t *testing.T) {
		mustCeph(t, ctx, cluster, "tell", "mgr.a", "version")
		pause, err := fault.Pause(ctx, cluster, cluster.ManagerContainer())
		if err != nil {
			if pause != nil {
				_ = pause.Resume(ctx)
			}
			t.Fatal(err)
		}
		before := scanNative(t, ctx, control).Markers
		query := ceph.NativeCommand{Argv: []string{"ceph", "tell", "mgr.a", "version"}, Timeout: 2 * time.Minute,
			MaxStdout: 1 << 20, MaxStderr: 1 << 20, Label: "mgr version"}
		call := startNative(ctx, cluster, control, query)
		// The supervisor and the CLI, whose interpreter runs with -s.
		running := waitNative(t, ctx, control, 2, "tell", "mgr.a", "version")
		call.cancel()
		outcome := <-call.done
		var unstopped *ceph.NativeCommandUnstoppedError
		if !errors.Is(outcome.err, context.Canceled) || errors.As(outcome.err, &unstopped) {
			t.Fatalf("canceled MGR query returned %v", outcome.err)
		}
		if left := scanNative(t, ctx, control, "tell", "mgr.a", "version"); len(left.Processes) != 0 || !slices.Equal(left.Markers, before) {
			t.Fatalf("the canceled query left %+v", left)
		}
		if err := pause.Resume(ctx); err != nil {
			t.Fatal(err)
		}
		result, err := cluster.RunNativeCommand(ctx, control, query)
		if err != nil || result.ExitCode != 0 || !bytes.Contains(result.Stdout, []byte(cluster.CephVersion())) {
			t.Fatalf("MGR query after resume: %+v %v", result, err)
		}
		t.Logf("canceled the query to the frozen MGR while processes %s ran; none remained", nativePIDs(running))
	})

	t.Run("binding_with_descendants", func(t *testing.T) {
		const token = "native-binding"
		before := scanNative(t, ctx, client).Markers
		call := startNative(ctx, cluster, client, ceph.NativeCommand{Argv: []string{"python3", "-c", nativeBindingTreeScript, token},
			Timeout: 15 * time.Second, MaxStdout: 4096, MaxStderr: 64 << 10, Label: "binding tree"})
		// The supervisor, the binding client, its child and grandchild.
		running := waitNative(t, ctx, client, 4, token)
		outcome := <-call.done
		fsid := strings.TrimSpace(string(mustCeph(t, ctx, cluster, "fsid")))
		if !errors.Is(outcome.err, ceph.ErrNativeCommandIncomplete) || !outcome.result.TimedOut || string(outcome.result.Stdout) != "fsid "+fsid+"\n" {
			t.Fatalf("binding tree returned %q, %+v, %v", outcome.result.Stdout, outcome.result, outcome.err)
		}
		if left := scanNative(t, ctx, client, token); len(left.Processes) != 0 || !slices.Equal(left.Markers, before) {
			t.Fatalf("the timed-out tree left %+v", left)
		}
		t.Logf("timeout killed the binding client and its descendants: %s", nativePIDs(running))
	})

	t.Run("output_limits", func(t *testing.T) {
		before := scanNative(t, ctx, client).Markers
		defer func() {
			if after := scanNative(t, ctx, client).Markers; !slices.Equal(after, before) {
				t.Errorf("output checks left markers %v", after)
			}
		}()
		stdout := bytes.Repeat(func() []byte {
			b := make([]byte, 256)
			for i := range b {
				b[i] = byte(i)
			}
			return b
		}(), 4096)
		stderr := bytes.Repeat([]byte("\x00secret-output\xff"), 1000)
		command := ceph.NativeCommand{Argv: []string{"python3", "-c", nativeOutputScript}, Timeout: time.Minute,
			MaxStdout: len(stdout), MaxStderr: len(stderr), Label: "output"}
		result, err := cluster.RunNativeCommand(ctx, client, command)
		if err != nil || !result.Complete() || result.ExitCode != 7 || !bytes.Equal(result.Stdout, stdout) || !bytes.Equal(result.Stderr, stderr) {
			t.Fatalf("complete output: exit %d, %d/%d bytes, %v", result.ExitCode, len(result.Stdout), len(result.Stderr), err)
		}
		command.MaxStdout, command.MaxStderr = 1000, 10
		result, err = cluster.RunNativeCommand(ctx, client, command)
		if !errors.Is(err, ceph.ErrNativeCommandIncomplete) || !result.StdoutTruncated || !result.StderrTruncated || result.TimedOut ||
			!bytes.Equal(result.Stdout, stdout[:1000]) || !bytes.Equal(result.Stderr, stderr[:10]) {
			t.Fatalf("clipped output: %d/%d bytes, %v", len(result.Stdout), len(result.Stderr), err)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatalf("error contains output: %v", err)
		}
	})

	// The MON container's PID 1 is the daemon, which reaps no orphans; the
	// supervisor reaps the killed group's descendants itself.
	t.Run("descendants_in_daemon_container", func(t *testing.T) {
		const token = "native-daemon-tree"
		before := scanNative(t, ctx, control).Markers
		call := startNative(ctx, cluster, control, nativeTree(token, 5*time.Second))
		running := waitNative(t, ctx, control, 4, token)
		outcome := <-call.done
		if !errors.Is(outcome.err, ceph.ErrNativeCommandIncomplete) || !outcome.result.TimedOut {
			t.Fatalf("tree in the control container returned %+v, %v", outcome.result, outcome.err)
		}
		pids := make([]int, 0, len(running))
		for _, process := range running {
			pids = append(pids, process.PID)
		}
		if left := nativeExisting(t, ctx, control, pids); len(left) != 0 {
			t.Fatalf("processes %v of the timed-out tree remain, zombies included", left)
		}
		if after := scanNative(t, ctx, control).Markers; !slices.Equal(after, before) {
			t.Fatalf("the tree left markers %v", after)
		}
		t.Logf("no process of %s remains, zombies included", nativePIDs(running))
	})

	// Docker can start an exec long after it was created. A cancellation
	// before the start claims the invocation, so the late supervisor runs
	// nothing; the same holds when the container restarted in between.
	t.Run("delayed_exec_start", func(t *testing.T) {
		helper := startNativeHelper(t, ctx, cluster)
		docker, err := testcontainers.NewDockerClientWithOpts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = docker.Close() }()
		create := func(token string) (string, string) {
			marker := supervised.MarkerDir + "/" + token
			created, err := docker.ExecCreate(ctx, helper.GetContainerID(), mobycl.ExecCreateOptions{
				Cmd: append([]string{"python3", "-c", supervised.Script, "4096", "4096", "60"}, nativeTree(token, time.Minute).Argv...),
				Env: []string{supervised.MarkerVariable + "=" + marker}, AttachStdout: true, AttachStderr: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			return created.ID, marker
		}

		id, marker := create("native-delayed")
		if err := supervised.Cancel(ctx, helper, "delayed", marker); err != nil {
			t.Fatal(err)
		}
		if _, err := docker.ExecStart(ctx, id, mobycl.ExecStartOptions{Detach: true}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(time.Minute)
		for {
			inspection, err := docker.ExecInspect(ctx, id, mobycl.ExecInspectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !inspection.Running && inspection.PID != 0 {
				if inspection.ExitCode != 0 {
					t.Fatalf("the late supervisor exited %d", inspection.ExitCode)
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the late supervisor did not finish")
			}
			time.Sleep(50 * time.Millisecond)
		}
		if nativeStarted(t, ctx, helper, "native-delayed") || len(scanNative(t, ctx, helper, "native-delayed").Processes) != 0 || len(scanNative(t, ctx, helper).Markers) != 0 {
			t.Fatal("a supervisor started after its cancellation ran its command or left its marker")
		}

		id, marker = create("native-delayed-restart")
		if err := supervised.Cancel(ctx, helper, "delayed", marker); err != nil {
			t.Fatal(err)
		}
		timeout := 5 * time.Second
		if err := helper.Stop(ctx, &timeout); err != nil {
			t.Fatal(err)
		}
		if err := helper.Start(ctx); err != nil {
			t.Fatal(err)
		}
		_, startErr := docker.ExecStart(ctx, id, mobycl.ExecStartOptions{Detach: true})
		time.Sleep(2 * time.Second)
		if nativeStarted(t, ctx, helper, "native-delayed-restart") || len(scanNative(t, ctx, helper, "native-delayed-restart").Processes) != 0 {
			t.Fatal("an exec created before a restart ran its command")
		}
		t.Logf("starting the exec created before the restart returned %v", startErr)
	})

	t.Run("cancel_before_and_during_launch", func(t *testing.T) {
		before := scanNative(t, ctx, client).Markers
		canceled, cancelNow := context.WithCancel(ctx)
		cancelNow()
		if _, err := cluster.RunNativeCommand(canceled, client, nativeTree("native-before", time.Minute)); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled context returned %v", err)
		}
		started := 0
		for i, delay := range []time.Duration{time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond,
			50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond} {
			token := fmt.Sprintf("native-launch-%d", i)
			// A cancellation, unlike a deadline, does not shorten the
			// supervisor's own timeout.
			callCtx, cancelCall := context.WithCancel(ctx)
			timer := time.AfterFunc(delay, cancelCall)
			_, err := cluster.RunNativeCommand(callCtx, client, nativeTree(token, time.Minute))
			timer.Stop()
			cancelCall()
			var unstopped *ceph.NativeCommandUnstoppedError
			if !errors.Is(err, context.Canceled) || errors.As(err, &unstopped) {
				t.Fatalf("launch canceled after %v returned %v", delay, err)
			}
			if left := scanNative(t, ctx, client, token); len(left.Processes) != 0 {
				t.Fatalf("launch canceled after %v left %+v", delay, left.Processes)
			}
			// A start that Docker delivers late must find its cancellation.
			time.Sleep(time.Second)
			if left := scanNative(t, ctx, client, token); len(left.Processes) != 0 {
				t.Fatalf("launch canceled after %v started later: %+v", delay, left.Processes)
			}
			if nativeStarted(t, ctx, client, token) {
				started++
			}
		}
		claims := slices.DeleteFunc(scanNative(t, ctx, client).Markers, func(name string) bool { return slices.Contains(before, name) })
		if slices.ContainsFunc(claims, func(name string) bool { return strings.HasSuffix(name, ".cancel") }) {
			t.Fatalf("cancellations left cancel markers: %v", claims)
		}
		if left := scanNative(t, ctx, client, "native-before"); len(left.Processes) != 0 || nativeStarted(t, ctx, client, "native-before") {
			t.Fatal("a call with a canceled context ran its command")
		}
		t.Logf("the command had started in %d of 9 canceled launches; %d claims of invocations that Docker never started remain", started, len(claims))
	})

	t.Run("only_the_owned_invocation", func(t *testing.T) {
		execOutput(t, ctx, client, "sh", "-c", "python3 -c 'import time; time.sleep(300)' native-unrelated >/dev/null 2>&1 &")
		defer killNative(t, ctx, client, "native-unrelated")
		first := startNative(ctx, cluster, client, nativeTree("native-first", time.Minute))
		second := startNative(ctx, cluster, client, nativeTree("native-second", time.Minute))
		waitNative(t, ctx, client, 4, "native-first")
		waitNative(t, ctx, client, 4, "native-second")
		waitNative(t, ctx, client, 1, "native-unrelated")
		first.cancel()
		if outcome := <-first.done; !errors.Is(outcome.err, context.Canceled) {
			t.Fatalf("first call returned %v", outcome.err)
		}
		if left := scanNative(t, ctx, client, "native-first"); len(left.Processes) != 0 {
			t.Fatalf("the canceled call left %+v", left.Processes)
		}
		if len(scanNative(t, ctx, client, "native-second").Processes) != 4 || len(scanNative(t, ctx, client, "native-unrelated").Processes) != 1 {
			t.Fatal("canceling one call stopped another call or an unrelated process")
		}
		second.cancel()
		<-second.done
		if left := scanNative(t, ctx, client, "native-second"); len(left.Processes) != 0 {
			t.Fatalf("the second call left %+v", left.Processes)
		}
	})

	t.Run("unconfirmed_stop", func(t *testing.T) {
		helper := startNativeHelper(t, ctx, cluster)
		unstopped, pause := startFrozenNative(t, ctx, cluster, helper, "native-frozen")
		short, cancelShort := context.WithTimeout(ctx, 3*time.Second)
		if err := unstopped.Stop(short); err == nil {
			t.Fatal("Stop confirmed a command in a frozen container")
		}
		cancelShort()
		if err := pause.Resume(ctx); err != nil {
			t.Fatal(err)
		}
		if err := unstopped.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		if left := scanNative(t, ctx, helper, "native-frozen"); len(left.Processes) != 0 || len(left.Markers) != 0 {
			t.Fatalf("Stop returned before the command and its markers were gone: %+v", left)
		}
		if err := unstopped.Stop(ctx); err != nil {
			t.Fatalf("a repeated Stop failed: %v", err)
		}

		// A restart ends the command; the next Stop confirms that through
		// the new start time, without touching the new instance's processes.
		unstopped, pause = startFrozenNative(t, ctx, cluster, helper, "native-restarted")
		timeout := 5 * time.Second
		if err := helper.Stop(ctx, &timeout); err != nil {
			t.Fatal(err)
		}
		if err := helper.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if err := pause.Resume(ctx); err != nil {
			t.Fatal(err)
		}
		execOutput(t, ctx, helper, "sh", "-c", "python3 -c 'import time; time.sleep(300)' native-restarted >/dev/null 2>&1 &")
		if err := unstopped.Stop(ctx); err != nil {
			t.Fatalf("Stop after a restart: %v", err)
		}
		if left := scanNative(t, ctx, helper, "native-restarted"); len(left.Processes) != 1 {
			t.Fatalf("Stop touched the restarted container's processes: %+v", left.Processes)
		}
		killNative(t, ctx, helper, "native-restarted")

		removed := startNativeHelper(t, ctx, cluster)
		unstopped, pause = startFrozenNative(t, ctx, cluster, removed, "native-removed")
		if err := removed.Terminate(ctx); err != nil {
			t.Fatal(err)
		}
		if err := pause.Resume(ctx); err != nil {
			t.Fatal(err)
		}
		if err := unstopped.Stop(ctx); err != nil {
			t.Fatalf("Stop after removal: %v", err)
		}
	})

	// A signal that kills the supervisor skips its cleanup: its command
	// keeps running, the call says so, and a restart ends the command, which
	// Stop then confirms inside the new instance.
	t.Run("killed_supervisor", func(t *testing.T) {
		helper := startNativeHelper(t, ctx, cluster)
		call := startNative(ctx, cluster, helper, nativeTree("native-killed", time.Minute))
		running := waitNative(t, ctx, helper, 4, "native-killed")
		supervisor := slices.IndexFunc(running, func(process nativeProcess) bool {
			return len(process.Argv) > 2 && process.Argv[2] == supervised.Script
		})
		if supervisor < 0 {
			t.Fatalf("no supervisor among %s", nativePIDs(running))
		}
		execOutput(t, ctx, helper, "python3", "-c", "import os,sys; os.kill(int(sys.argv[1]),9)", strconv.Itoa(running[supervisor].PID))
		outcome := <-call.done
		var unstopped *ceph.NativeCommandUnstoppedError
		if !errors.As(outcome.err, &unstopped) {
			t.Fatalf("a killed supervisor returned %v", outcome.err)
		}
		if left := scanNative(t, ctx, helper, "native-killed"); len(left.Processes) != 3 {
			t.Fatalf("the orphaned command does not run as expected: %+v", left.Processes)
		}
		timeout := 5 * time.Second
		if err := helper.Stop(ctx, &timeout); err != nil {
			t.Fatal(err)
		}
		if err := helper.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if err := unstopped.Stop(ctx); err != nil {
			t.Fatalf("Stop after the restart: %v", err)
		}
		if left := scanNative(t, ctx, helper, "native-killed"); len(left.Processes) != 0 {
			t.Fatalf("processes remain after the restart: %+v", left.Processes)
		}
	})

	t.Run("admission", func(t *testing.T) {
		// OSD images need not have python3.
		osd := cluster.OSDs()[0]
		result, err := cluster.RunNativeCommand(ctx, osd, ceph.NativeCommand{Argv: []string{"ceph-osd", "--version"},
			Timeout: time.Minute, MaxStdout: 4096, MaxStderr: 4096})
		switch {
		case errors.Is(err, ceph.ErrImageCapability) && strings.Contains(err.Error(), "cannot run python3"):
			t.Logf("the OSD image has no python3: %v", err)
		case err != nil || !bytes.Contains(result.Stdout, []byte(cluster.CephVersion())):
			t.Fatalf("OSD container: %+v %v", result, err)
		}
		foreign, err := testcontainers.Run(ctx, "busybox:1.37.0", ceph.WithIdleEntrypoint())
		testcontainers.CleanupContainer(t, foreign)
		if err != nil {
			t.Fatal(err)
		}
		quick := ceph.NativeCommand{Argv: []string{"true"}, Timeout: time.Minute, MaxStdout: 64, MaxStderr: 64, Label: "probe"}
		if _, err := cluster.RunNativeCommand(ctx, foreign, quick); !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "neither part of this cluster nor attached") {
			t.Fatalf("an unattached container was admitted: %v", err)
		}
		attached, err := testcontainers.Run(ctx, "busybox:1.37.0", cluster.WithClient(), ceph.WithIdleEntrypoint())
		testcontainers.CleanupContainer(t, attached)
		if err != nil {
			t.Fatal(err)
		}
		var missing *ceph.OperationError
		if _, err := cluster.RunNativeCommand(ctx, attached, quick); !errors.As(err, &missing) || missing.Kind != ceph.ErrImageCapability ||
			missing.Capability != "python3" || !strings.Contains(err.Error(), "cannot run python3") {
			t.Fatalf("a container without python3 returned %v", err)
		}
		if state, err := attached.State(ctx); err != nil || !state.Running {
			t.Fatalf("the caller's container did not keep running: %+v %v", state, err)
		}
	})

	// The recipe of docs/ceph.md: a bounded rados_stat2 and read through
	// librados, and ceph-dencoder on the current OSDMap.
	t.Run("oracle_recipe", func(t *testing.T) {
		const pool, object = "tc-native", "oracle"
		if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rados"}); err != nil {
			t.Fatal(err)
		}
		data := bytes.Repeat([]byte("\x00oracle\xff"), 2048)
		write := ceph.NativeCommand{Argv: []string{"python3", "-c", nativeWriteScript, pool, object, base64.StdEncoding.EncodeToString(data)},
			Timeout: time.Minute, MaxStdout: 4096, MaxStderr: 64 << 10, Label: "write object"}
		if _, err := cluster.RunNativeCommand(ctx, client, write); err != nil {
			t.Fatal(err)
		}
		read, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"python3", "-c", nativeStatReadScript, pool, object},
			Timeout: time.Minute, MaxStdout: 1 << 20, MaxStderr: 64 << 10, Label: "stat and read object"})
		var stat struct {
			Size  int     `json:"size"`
			MTime [2]int  `json:"mtime"`
			Data  []byte  `json:"data"`
			Error *string `json:"error"`
		}
		if err != nil || read.ExitCode != 0 || json.Unmarshal(read.Stdout, &stat) != nil || stat.Size != len(data) || !bytes.Equal(stat.Data, data) || stat.MTime[0] == 0 {
			t.Fatalf("stat and read: exit %d, %d bytes, size %d, %v", read.ExitCode, len(stat.Data), stat.Size, err)
		}

		// The official images, and so the recipe, have ceph-dencoder.
		dencoder, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"ceph-dencoder", "version"},
			Timeout: time.Minute, MaxStdout: 4096, MaxStderr: 4096})
		if err != nil || !strings.HasPrefix(string(dencoder.Stdout), cluster.CephVersion()) {
			t.Fatalf("ceph-dencoder version: %q %v", dencoder.Stdout, err)
		}
		getmap, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"ceph", "osd", "getmap", "-o", "/tmp/tc-osdmap"},
			Timeout: time.Minute, MaxStdout: 4096, MaxStderr: 4096, Label: "get OSDMap"})
		if err != nil || getmap.ExitCode != 0 {
			t.Fatalf("get OSDMap: exit %d, %v", getmap.ExitCode, err)
		}
		decoded, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
			Argv:    []string{"ceph-dencoder", "type", "OSDMap", "import", "/tmp/tc-osdmap", "decode", "dump_json"},
			Timeout: time.Minute, MaxStdout: 4 << 20, MaxStderr: 64 << 10, Label: "decode OSDMap"})
		type decodedPool struct {
			Name string `json:"pool_name"`
		}
		var osdmap struct {
			Epoch int           `json:"epoch"`
			FSID  string        `json:"fsid"`
			Pools []decodedPool `json:"pools"`
		}
		fsid := strings.TrimSpace(string(mustCeph(t, ctx, cluster, "fsid")))
		if err != nil || decoded.ExitCode != 0 || json.Unmarshal(decoded.Stdout, &osdmap) != nil || osdmap.FSID != fsid || osdmap.Epoch == 0 ||
			!slices.ContainsFunc(osdmap.Pools, func(p decodedPool) bool { return p.Name == pool }) {
			t.Fatalf("decoded OSDMap epoch %d fsid %q: exit %d, %v", osdmap.Epoch, osdmap.FSID, decoded.ExitCode, err)
		}
		t.Logf("rados_stat2 and read returned %d bytes; ceph-dencoder decoded OSDMap epoch %d (%d bytes of JSON)", stat.Size, osdmap.Epoch, len(decoded.Stdout))
	})

	// Terminate cancels a running command, also in a caller's container
	// that outlives the cluster.
	t.Run("classified_failures", func(t *testing.T) {
		// The errno that Ceph's tools print, from the ceph CLI's "Error ENOENT:"
		// line and from the "(2) No such file or directory" form of rados.
		_, err := cluster.Ceph(ctx, "osd", "pool", "get", "tc-missing-pool", "size")
		var failure *ceph.NativeFailure
		if !errors.Is(err, fs.ErrNotExist) || !errors.As(err, &failure) || failure.Program != "ceph" || failure.Errno != "ENOENT" ||
			!strings.Contains(failure.Reason, "tc-missing-pool") {
			t.Fatalf("a missing pool returned %v", err)
		}
		control := cluster.ControlContainer()
		if _, err := servicehook.Command(ctx, control, "rados", "--pool", "tc-missing-pool", "ls"); !errors.Is(err, fs.ErrNotExist) ||
			!errors.As(err, &failure) || failure.Program != "rados" || failure.Errno != "ENOENT" {
			t.Fatalf("rados on a missing pool returned %v", err)
		}
		// A tool that prints no errno fails without one, whatever its exit code.
		if _, err := servicehook.Command(ctx, control, "ls", "/tc-missing-path"); !errors.Is(err, ceph.ErrNativeFailure) || !errors.As(err, &failure) ||
			failure.Errno != "" || errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("ls of a missing path returned %v", err)
		}
		// A program that the image lacks.
		var missing *ceph.OperationError
		if _, err := servicehook.Command(ctx, control, "tc-no-such-program"); !errors.As(err, &missing) || missing.Kind != ceph.ErrImageCapability ||
			missing.Capability != "tc-no-such-program" || errors.Is(err, ceph.ErrNativeFailure) {
			t.Fatalf("a missing program returned %v", err)
		}
		t.Logf("Ceph %s: %v", cluster.CephVersion(), failure)
	})

	t.Run("lost_receipt", func(t *testing.T) {
		// A mutation that ran keeps its effect when its exec loses the final
		// inspection; the call is no request refused before any change.
		control := cluster.ControlContainer()
		lossy := &nativeReceiptLoss{Container: control, cause: errdefs.ErrNotFound}
		_, err := cluster.RunNativeCommand(ctx, lossy, ceph.NativeCommand{Argv: []string{"ceph", "--connect-timeout", "5", "config", "set", "mon.a", "debug_mon", "3/6"},
			Timeout: 20 * time.Second, MaxStdout: 4096, MaxStderr: 4096, Label: "set debug_mon"})
		t.Cleanup(func() { _, _ = cluster.Ceph(context.Background(), "config", "rm", "mon.a", "debug_mon") })
		var unstopped *ceph.NativeCommandUnstoppedError
		if !lossy.lost || err == nil || errors.Is(err, ceph.ErrInvalidRequest) || errors.Is(err, ceph.ErrEnvironmentUnavailable) ||
			!errdefs.IsNotFound(err) || errors.As(err, &unstopped) {
			t.Fatalf("a mutation whose receipt was lost returned %v (receipt lost %t)", err, lossy.lost)
		}
		value, err := cluster.Ceph(ctx, "config", "get", "mon.a", "debug_mon")
		if err != nil || strings.TrimSpace(string(value)) != "3/6" {
			t.Fatalf("the mutation's effect is %q, %v", value, err)
		}
	})

	t.Run("terminate", func(t *testing.T) {
		before := scanNative(t, ctx, client).Markers
		call := startNative(ctx, cluster, client, nativeTree("native-terminate", time.Minute))
		waitNative(t, ctx, client, 4, "native-terminate")
		// The caller's attached containers still block the network removal.
		if err := cluster.Terminate(ctx); err != nil {
			t.Logf("Terminate with attached caller containers: %v", err)
		}
		if outcome := <-call.done; outcome.err == nil || !strings.Contains(outcome.err.Error(), "ceph cluster is terminated") {
			t.Fatalf("a running command ended with %v", outcome.err)
		}
		if left := scanNative(t, ctx, client, "native-terminate"); len(left.Processes) != 0 || !slices.Equal(left.Markers, before) {
			t.Fatalf("Terminate left %+v", left)
		}
		if _, err := cluster.RunNativeCommand(ctx, client, nativeTree("native-after", time.Minute)); err == nil {
			t.Fatal("a terminated cluster ran a command")
		}
	})
}

// logNativeRuntime records the platform, the Ceph release and the immutable
// image IDs that the native checks ran on.
func logNativeRuntime(t *testing.T, ctx context.Context, cluster *ceph.Container, containers map[string]testcontainers.Container) {
	t.Helper()
	for _, name := range []string{"control", "mgr", "osd", "client"} {
		inspection, err := containers[name].Inspect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s container image %s id %s", name, inspection.Config.Image, inspection.Image)
	}
	result, err := cluster.RunNativeCommand(ctx, containers["client"], ceph.NativeCommand{Argv: []string{"uname", "-srm"},
		Timeout: time.Minute, MaxStdout: 4096, MaxStderr: 4096, Label: "platform"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Ceph %s on %s", cluster.CephVersion(), strings.TrimSpace(string(result.Stdout)))
}

type nativeOutcome struct {
	result ceph.NativeCommandResult
	err    error
}

type nativeCall struct {
	cancel context.CancelFunc
	done   chan nativeOutcome
}

func startNative(ctx context.Context, cluster *ceph.Container, target testcontainers.Container, command ceph.NativeCommand) *nativeCall {
	callCtx, cancel := context.WithCancel(ctx)
	call := &nativeCall{cancel: cancel, done: make(chan nativeOutcome, 1)}
	go func() {
		defer cancel()
		result, err := cluster.RunNativeCommand(callCtx, target, command)
		call.done <- nativeOutcome{result, err}
	}()
	return call
}

// startNativeHelper starts a caller-owned container attached to the cluster.
func startNativeHelper(t *testing.T, ctx context.Context, cluster *ceph.Container) testcontainers.Container {
	t.Helper()
	helper, err := testcontainers.Run(ctx, cluster.ControlImage(), cluster.WithClient(), ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, helper)
	if err != nil {
		t.Fatal(err)
	}
	return helper
}

// startFrozenNative runs a command in helper, freezes helper and cancels the
// call, which then cannot confirm that the command stopped.
func startFrozenNative(t *testing.T, ctx context.Context, cluster *ceph.Container, helper testcontainers.Container, token string) (*ceph.NativeCommandUnstoppedError, *fault.PauseLease) {
	t.Helper()
	call := startNative(ctx, cluster, helper, nativeTree(token, time.Minute))
	waitNative(t, ctx, helper, 4, token)
	pause, err := fault.Pause(ctx, cluster, helper)
	if err != nil {
		if pause != nil {
			_ = pause.Resume(ctx)
		}
		t.Fatal(err)
	}
	call.cancel()
	outcome := <-call.done
	var unstopped *ceph.NativeCommandUnstoppedError
	if !errors.As(outcome.err, &unstopped) || !errors.Is(outcome.err, context.Canceled) {
		_ = pause.Resume(ctx)
		t.Fatalf("a call canceled in a frozen container returned %v", outcome.err)
	}
	return unstopped, pause
}

func nativeTree(token string, timeout time.Duration) ceph.NativeCommand {
	return ceph.NativeCommand{Argv: []string{"python3", "-c", nativeTreeScript, token}, Timeout: timeout,
		MaxStdout: 4096, MaxStderr: 4096, Label: "tree " + token}
}

// nativeTreeScript records that it started, then waits with a child and a
// grandchild; each process names the token in its arguments.
const nativeTreeScript = `import subprocess,sys,time
open("/tmp/tc-native-"+sys.argv[1],"w").close()
child="import subprocess,sys,time; subprocess.Popen([sys.executable,'-c','import time; time.sleep(600)',sys.argv[1]]); time.sleep(600)"
subprocess.Popen([sys.executable,"-c",child,sys.argv[1]])
time.sleep(600)
`

// nativeBindingTreeScript holds a librados session, prints the cluster's
// fsid and waits with a child and a grandchild.
const nativeBindingTreeScript = `import rados,subprocess,sys,time
cluster=rados.Rados(conffile="/etc/ceph/ceph.conf")
cluster.connect()
print("fsid",cluster.get_fsid(),flush=True)
child="import subprocess,sys,time; subprocess.Popen([sys.executable,'-c','import time; time.sleep(600)',sys.argv[1]]); time.sleep(600)"
subprocess.Popen([sys.executable,"-c",child,sys.argv[1]])
time.sleep(600)
`

const nativeOutputScript = `import sys
sys.stdout.buffer.write(bytes(range(256))*4096)
sys.stderr.buffer.write(b"\x00secret-output\xff"*1000)
sys.exit(7)
`

const nativeWriteScript = `import base64,rados,sys
with rados.Rados(conffile="/etc/ceph/ceph.conf") as cluster:
    with cluster.open_ioctx(sys.argv[1]) as ioctx:
        ioctx.write_full(sys.argv[2],base64.b64decode(sys.argv[3]))
`

// nativeStatReadScript is the stat2 and read recipe of docs/ceph.md.
const nativeStatReadScript = `import base64,ctypes,json,sys
pool,name=sys.argv[1].encode(),sys.argv[2].encode()
lib=ctypes.CDLL("librados.so.2")
class timespec(ctypes.Structure):
    _fields_=[("tv_sec",ctypes.c_int64),("tv_nsec",ctypes.c_long)]
def check(ret,call):
    if ret<0:
        raise OSError(-ret,call)
cluster,ioctx=ctypes.c_void_p(),ctypes.c_void_p()
check(lib.rados_create(ctypes.byref(cluster),None),"rados_create")
try:
    check(lib.rados_conf_read_file(cluster,b"/etc/ceph/ceph.conf"),"rados_conf_read_file")
    check(lib.rados_connect(cluster),"rados_connect")
    check(lib.rados_ioctx_create(cluster,pool,ctypes.byref(ioctx)),"rados_ioctx_create")
    size,mtime=ctypes.c_uint64(),timespec()
    check(lib.rados_stat2(ioctx,name,ctypes.byref(size),ctypes.byref(mtime)),"rados_stat2")
    buf=ctypes.create_string_buffer(max(size.value,1))
    read=lib.rados_read(ioctx,name,buf,ctypes.c_size_t(size.value),ctypes.c_uint64(0))
    check(read,"rados_read")
    print(json.dumps({"size":size.value,"mtime":[mtime.tv_sec,mtime.tv_nsec],"data":base64.b64encode(buf.raw[:read]).decode()}))
finally:
    if ioctx.value:
        lib.rados_ioctx_destroy(ioctx)
    lib.rados_shutdown(cluster)
`

type nativeProcess struct {
	PID  int      `json:"pid"`
	PGID int      `json:"pgid"`
	Argv []string `json:"argv"`
}

type nativeScan struct {
	// Processes are the live processes whose arguments include every token.
	Processes []nativeProcess `json:"processes"`
	// Markers are the supervisor's marker files.
	Markers []string `json:"markers"`
}

const nativeScanScript = `import json,os,sys
tokens=[t.encode() for t in json.loads(sys.argv[1])]; kill=sys.argv[2]=="kill"
found=[]
for name in os.listdir("/proc"):
 if not name.isdigit() or int(name)==os.getpid(): continue
 pid=int(name)
 try:
  with open("/proc/%d/cmdline"%pid,"rb") as f: argv=f.read().split(b"\0")
  with open("/proc/%d/stat"%pid,"rb") as f: stat=f.read()
  pgid=os.getpgid(pid)
 except OSError: continue
 if not tokens or stat[stat.rindex(b")")+2:][:1]==b"Z" or not all(t in argv for t in tokens): continue
 found.append({"pid":pid,"pgid":pgid,"argv":[a.decode(errors="replace") for a in argv if a]})
 if kill: os.kill(pid,9)
d="/tmp/tc-supervised"
print(json.dumps({"processes":found,"markers":sorted(os.listdir(d)) if os.path.isdir(d) else []}))
`

func runNativeScan(t *testing.T, ctx context.Context, ctr testcontainers.Container, action string, tokens ...string) nativeScan {
	t.Helper()
	encoded, _ := json.Marshal(append([]string{}, tokens...))
	execCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	code, reader, err := ctr.Exec(execCtx, []string{"python3", "-c", nativeScanScript, string(encoded), action}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(reader)
	var scan nativeScan
	if err != nil || code != 0 || json.Unmarshal(out, &scan) != nil {
		t.Fatalf("scan processes: exit %d, %q, %v", code, out, err)
	}
	return scan
}

// scanNative lists the live processes whose arguments include every token,
// and the supervisor markers, in ctr.
func scanNative(t *testing.T, ctx context.Context, ctr testcontainers.Container, tokens ...string) nativeScan {
	t.Helper()
	return runNativeScan(t, ctx, ctr, "list", tokens...)
}

func killNative(t *testing.T, ctx context.Context, ctr testcontainers.Container, tokens ...string) {
	t.Helper()
	runNativeScan(t, ctx, ctr, "kill", tokens...)
}

// waitNative waits until at least count processes name every token.
func waitNative(t *testing.T, ctx context.Context, ctr testcontainers.Container, count int, tokens ...string) []nativeProcess {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		if found := scanNative(t, ctx, ctr, tokens...).Processes; len(found) >= count {
			return found
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d processes naming %v did not start", count, tokens)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// nativeExisting returns the PIDs that still exist in ctr, zombies included.
func nativeExisting(t *testing.T, ctx context.Context, ctr testcontainers.Container, pids []int) []int {
	t.Helper()
	encoded, _ := json.Marshal(pids)
	out := execOutput(t, ctx, ctr, "python3", "-c", `import json,os,sys
print(json.dumps([p for p in json.loads(sys.argv[1]) if os.path.exists("/proc/%d"%p)]))`, string(encoded))
	var left []int
	if err := json.Unmarshal([]byte(lastLine(out)), &left); err != nil {
		t.Fatalf("existing processes %q: %v", out, err)
	}
	return left
}

// nativePIDs formats processes as PID/PGID pairs.
func nativePIDs(processes []nativeProcess) string {
	pairs := make([]string, 0, len(processes))
	for _, process := range processes {
		pairs = append(pairs, fmt.Sprintf("%d/%d", process.PID, process.PGID))
	}
	return strings.Join(pairs, " ")
}

// nativeStarted reports whether the tree command for token has started.
func nativeStarted(t *testing.T, ctx context.Context, ctr testcontainers.Container, token string) bool {
	t.Helper()
	code, _, err := ctr.Exec(ctx, []string{"test", "-e", "/tmp/tc-native-" + token})
	if err != nil {
		t.Fatal(err)
	}
	return code == 0
}

// nativeReceiptLoss forwards each supervisor exec to completion and then
// loses its final inspection with cause.
type nativeReceiptLoss struct {
	testcontainers.Container
	cause error
	lost  bool
}

func (f *nativeReceiptLoss) Exec(ctx context.Context, argv []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	code, reader, err := f.Container.Exec(ctx, argv, opts...)
	if err == nil && code == 0 && len(argv) > 2 && argv[2] == supervised.Script {
		if reader != nil {
			_, _ = io.Copy(io.Discard, reader)
		}
		f.lost = true
		return 0, nil, fmt.Errorf("lost final exec receipt: %w", f.cause)
	}
	return code, reader, err
}
