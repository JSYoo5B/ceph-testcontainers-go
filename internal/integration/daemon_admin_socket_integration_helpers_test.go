//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// testDaemonAdminSocket reads each daemon's own view through its admin socket
// and checks it against the cluster maps and the daemon's lifecycle.
func testDaemonAdminSocket(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, _ := newServiceCluster(t, append([]testcontainers.ContainerCustomizer{ceph.WithOSDCount(1), ceph.WithPoolDefaults(1, 1)}, opts...)...)

	for _, daemon := range []string{"mon.a", "mgr.a", "osd.0"} {
		out, err := cluster.AdminSocket(ctx, daemon, "version")
		var version struct {
			Version string `json:"version"`
		}
		if err != nil || json.Unmarshal(out, &version) != nil || !strings.HasPrefix(version.Version, cluster.CephVersion()) {
			t.Fatalf("%s admin socket version %q, err=%v, cluster release %s", daemon, out, err, cluster.CephVersion())
		}
	}
	out, err := cluster.AdminSocket(ctx, "mon.a", "mon_status")
	var monStatus struct {
		Name  string `json:"name"`
		State string `json:"state"`
	}
	if err != nil || json.Unmarshal(out, &monStatus) != nil || monStatus.Name != "a" || monStatus.State != "leader" {
		t.Fatalf("MON a admin socket status %q, err=%v", out, err)
	}

	// A new pool advances the OSDMap; the OSD's own status reports when it has
	// applied that epoch, which the cluster maps alone cannot show.
	before := adminSocketOSDMapEpoch(t, ctx, cluster)
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-admin-socket"}); err != nil {
		t.Fatal(err)
	}
	after := adminSocketOSDMapEpoch(t, ctx, cluster)
	if after <= before {
		t.Fatalf("pool creation did not advance the OSDMap: %d -> %d", before, after)
	}
	applied := waitAdminSocketOSDEpoch(t, ctx, cluster, after)
	t.Logf("osd.0 applied OSDMap epoch %d (cluster epoch %d, before pool %d)", applied, after, before)

	// A frozen OSD accepts the connection but never replies. Canceling the
	// call must also end its CLI process group in the control container, so
	// repeated canceled calls do not accumulate.
	osd := cluster.OSDs()[0]
	pause, err := cluster.PauseContainer(ctx, osd)
	if err != nil {
		if pause != nil {
			_ = pause.Resume(ctx)
		}
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		cancelAdminSocketCall(t, ctx, cluster, attempt)
	}
	if err := pause.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	waitAdminSocketOSDEpoch(t, ctx, cluster, after)

	// The socket belongs to the running process: a stopped OSD has none, and
	// the restarted process answers again.
	stop := 10 * time.Second
	if err := osd.Stop(ctx, &stop); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.AdminSocket(ctx, "osd.0", "status"); err == nil {
		t.Fatal("a stopped OSD answered its admin socket")
	}
	if err := osd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitAdminSocketOSDEpoch(t, ctx, cluster, after)

	if _, err := cluster.AdminSocket(ctx, "osd.9", "status"); err == nil {
		t.Fatal("an OSD that the cluster does not own was reachable")
	}
	t.Log("MON, MGR and OSD admin sockets: versions match the cluster release, MON a is leader, osd.0 applied the new OSDMap epoch before and after a restart, canceled calls to a frozen osd.0 left no process behind")
}

func adminSocketOSDMapEpoch(t *testing.T, ctx context.Context, cluster *ceph.Container) int {
	t.Helper()
	out, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	var dump struct {
		Epoch int `json:"epoch"`
	}
	if err != nil || json.Unmarshal(out, &dump) != nil || dump.Epoch <= 0 {
		t.Fatalf("osd dump: %q, %v", out, err)
	}
	return dump.Epoch
}

// waitAdminSocketOSDEpoch waits until osd.0 reports an applied OSDMap epoch
// of at least want. A starting OSD may answer before it registers "status".
func waitAdminSocketOSDEpoch(t *testing.T, ctx context.Context, cluster *ceph.Container, want int) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var last string
	for {
		out, err := cluster.AdminSocket(ctx, "osd.0", "status")
		var status struct {
			State     string `json:"state"`
			NewestMap string `json:"newest_map"`
		}
		if err == nil && json.Unmarshal(out, &status) == nil {
			// OSD::status prints epochs as JSON strings.
			if newest, convErr := strconv.Atoi(status.NewestMap); convErr == nil && newest >= want && status.State == "active" {
				return newest
			}
		}
		last = string(out)
		if err != nil {
			last = err.Error()
		}
		select {
		case <-ctx.Done():
			t.Fatalf("osd.0 did not report OSDMap epoch %d: last %s", want, last)
		case <-time.After(time.Second):
		}
	}
}

// adminSocketOSDPath is the socket that osd.0's admin socket calls name.
const adminSocketOSDPath = "/var/run/ceph/ceph-osd.0.asok"

// cancelAdminSocketCall starts a call to the frozen osd.0, waits until its CLI
// runs in the control container, cancels it, and checks that the CLI's process
// group and every process naming the socket are gone.
func cancelAdminSocketCall(t *testing.T, ctx context.Context, cluster *ceph.Container, attempt int) {
	t.Helper()
	callCtx, cancelCall := context.WithCancel(ctx)
	defer cancelCall()
	done := make(chan error, 1)
	go func() {
		_, err := cluster.AdminSocket(callCtx, "osd.0", "status")
		done <- err
	}()
	var cli adminSocketProcess
	waitCtx, cancelWait := context.WithTimeout(ctx, time.Minute)
	defer cancelWait()
	for found := false; !found; {
		for _, process := range adminSocketProcesses(t, ctx, cluster, 0).Matches {
			// The CLI leads its own group; its supervisor runs python3 -c.
			if process.PID == process.PGID && (len(process.Argv) < 2 || process.Argv[1] != "-c") {
				cli, found = process, true
			}
		}
		select {
		case err := <-done:
			t.Fatalf("attempt %d: call to a frozen OSD returned before cancellation: %v", attempt, err)
		case <-waitCtx.Done():
			t.Fatalf("attempt %d: the admin socket CLI did not start", attempt)
		case <-time.After(100 * time.Millisecond):
		}
	}
	cancelCall()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "clean up") {
			t.Fatalf("attempt %d: canceled call returned %v", attempt, err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("attempt %d: canceled call did not return", attempt)
	}
	// Cleanup completes before the call returns; allow only for the reaping
	// of an already killed process.
	deadline := time.Now().Add(5 * time.Second)
	for {
		left := adminSocketProcesses(t, ctx, cluster, cli.PGID)
		if len(left.Matches) == 0 && len(left.Group) == 0 {
			t.Logf("attempt %d: canceled CLI pid %d and its process group are gone", attempt, cli.PID)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("attempt %d: canceled CLI group %d remains: %+v", attempt, cli.PGID, left)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

type adminSocketProcess struct {
	PID  int      `json:"pid"`
	PGID int      `json:"pgid"`
	Argv []string `json:"argv"`
}

type adminSocketProcessScan struct {
	// Matches are live processes whose command line names the socket.
	Matches []adminSocketProcess `json:"matches"`
	// Group lists every process, zombies included, in the requested group.
	Group []int `json:"group"`
}

const adminSocketProcessScript = `import json,os,sys
path=sys.argv[1].encode(); group=int(sys.argv[2]); found={"matches":[],"group":[]}
for name in os.listdir("/proc"):
 if not name.isdigit() or int(name)==os.getpid(): continue
 pid=int(name)
 try:
  with open("/proc/"+name+"/cmdline","rb") as f: argv=f.read().split(b"\0")
  pgid=os.getpgid(pid)
 except OSError: continue
 if group and pgid==group: found["group"].append(pid)
 if path in argv: found["matches"].append({"pid":pid,"pgid":pgid,"argv":[a.decode(errors="replace") for a in argv if a]})
print(json.dumps(found))
`

// adminSocketProcesses lists the control container's processes that name
// osd.0's socket and, when group is not zero, the members of that group.
func adminSocketProcesses(t *testing.T, ctx context.Context, cluster *ceph.Container, group int) adminSocketProcessScan {
	t.Helper()
	code, reader, err := cluster.ControlContainer().Exec(ctx, []string{"python3", "-c", adminSocketProcessScript, adminSocketOSDPath, strconv.Itoa(group)}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(reader)
	var scan adminSocketProcessScan
	if err != nil || code != 0 || json.Unmarshal(out, &scan) != nil {
		t.Fatalf("scan control processes: exit %d, %q, %v", code, out, err)
	}
	return scan
}
