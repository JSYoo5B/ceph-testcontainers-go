//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// testDaemonKeyRotation rotates the keys of a running OSD and MGR and checks
// that the MONs, the daemons' keyring files and restarts agree on the new
// keys. With 10-second service tickets it then reads, from the OSD's own
// debug log, the rotating service secrets that it holds and the secret that
// decrypted a client's ticket. Last, a handoff to a stopped OSD process
// leaves both keys valid, and the OSD gets no other key until it restarts.
func testDaemonKeyRotation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	tick := filepath.Join(t.TempDir(), "tick.conf")
	if err := os.WriteFile(tick, []byte("[mon]\nmon_tick_interval = 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cluster, _ := newServiceCluster(t, ceph.WithOSDCount(1), ceph.WithPoolDefaults(1, 1),
		ceph.WithAuth(ceph.AuthConfig{ServiceTicketTTL: 10 * time.Second}), ceph.WithConfigFile(tick))
	osd, mgr := clusteraccess.OSDs(t, cluster)[0], cluster.ManagerContainer()

	// A pending key left by an interrupted rotation is the one a later call
	// finishes with: get-or-create-pending returns it instead of a new key.
	if _, err := cluster.Ceph(ctx, "auth", "get-or-create-pending", "osd.0"); err != nil {
		t.Fatal(err)
	}
	leftover := pendingKey(t, ctx, cluster, "osd.0")
	for _, daemon := range []struct {
		name     string
		ctr      testcontainers.Container
		keyrings []string
	}{
		{"osd.0", osd, []string{"/etc/ceph/osd.keyring", "/var/lib/ceph/osd/ceph-0/keyring"}},
		{"mgr.a", mgr, []string{"/etc/ceph/mgr.keyring", "/var/lib/ceph/mgr/ceph-a/keyring"}},
	} {
		before := authKey(t, ctx, cluster, daemon.name)
		if err := cluster.RotateDaemonKey(ctx, daemon.name); err != nil {
			t.Fatal(err)
		}
		after := authKey(t, ctx, cluster, daemon.name)
		if after == before {
			t.Fatalf("%s kept its key", daemon.name)
		}
		if daemon.name == "osd.0" && after != leftover {
			t.Fatal("the rotation did not resume with the pending key of the interrupted one")
		}
		if pending := pendingKey(t, ctx, cluster, daemon.name); pending != "" {
			t.Fatalf("%s still has a pending key", daemon.name)
		}
		for _, path := range daemon.keyrings {
			if keyring := containerFile(t, ctx, daemon.ctr, path); !strings.Contains(keyring, "key = "+after) {
				t.Fatalf("%s keyring %s does not hold the new key: %s", daemon.name, path, keyring)
			}
		}
	}
	if _, err := cluster.Ceph(ctx, "tell", "osd.0", "version"); err != nil {
		t.Fatalf("the OSD stopped answering after the rotation: %v", err)
	}
	// A restart authenticates with the keyring files only.
	stop := 10 * time.Second
	for _, ctr := range []testcontainers.Container{osd, mgr} {
		if err := ctr.Stop(ctx, &stop); err != nil {
			t.Fatal(err)
		}
		if err := ctr.Start(ctx); err != nil {
			t.Fatalf("restart with the rotated key: %v", err)
		}
	}
	waitRotatedDaemonsUp(t, ctx, cluster)

	// Refusals change nothing.
	for _, daemon := range []string{"mon.a", "osd.9", "mds.a"} {
		if err := cluster.RotateDaemonKey(ctx, daemon); err == nil {
			t.Fatalf("%s was rotated", daemon)
		}
	}
	if err := osd.Stop(ctx, &stop); err != nil {
		t.Fatal(err)
	}
	if err := cluster.RotateDaemonKey(ctx, "osd.0"); err == nil || !strings.Contains(err.Error(), "running") {
		t.Fatalf("a stopped OSD was rotated: %v", err)
	}
	if pending := pendingKey(t, ctx, cluster, "osd.0"); pending != "" {
		t.Fatal("a refused rotation left a pending key")
	}
	if err := osd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitRotatedDaemonsUp(t, ctx, cluster)
	t.Log("OSD and MGR keys rotated without a restart; the MONs, keyring files and restarts agree")

	// Service secrets: the OSD's debug log lists the rotating secrets it holds
	// and the secret that decrypted each client ticket.
	if _, err := cluster.AdminSocket(ctx, "osd.0", "config", "set", "debug_auth", "10/10"); err != nil {
		t.Fatal(err)
	}
	first := waitRotatingSecretIDs(t, ctx, osd, 0)
	if _, err := cluster.Ceph(ctx, "tell", "osd.0", "version"); err != nil {
		t.Fatal(err)
	}
	used := decryptedSecretIDs(t, ctx, osd)
	if len(used) == 0 || !slices.Contains(first, used[len(used)-1]) {
		t.Fatalf("no client ticket was decrypted with a held secret: used %v, held %v", used, first)
	}
	earlier := used[len(used)-1]
	later := waitRotatingSecretIDs(t, ctx, osd, slices.Max(first)+2)
	// A new client gets a ticket for the current secret.
	if _, err := cluster.Ceph(ctx, "tell", "osd.0", "version"); err != nil {
		t.Fatal(err)
	}
	used = decryptedSecretIDs(t, ctx, osd)
	if renewed := used[len(used)-1]; renewed <= earlier || !slices.Contains(later, renewed) {
		t.Fatalf("a later client ticket used secret %d after %d; held %v", renewed, earlier, later)
	}
	if _, err := cluster.AdminSocket(ctx, "osd.0", "config", "set", "debug_auth", "1/5"); err != nil {
		t.Fatal(err)
	}
	t.Logf("osd.0 held rotating secrets %v, then %v; client tickets were decrypted with secret %d, then %d", first, later, earlier, used[len(used)-1])

	// The OSD process stops, so rotate-key hangs until the call's deadline
	// and may reach the OSD once it continues.
	current := authKey(t, ctx, cluster, "osd.0")
	signalContainer(t, ctx, osd, "STOP")
	callCtx, callCancel := context.WithTimeout(ctx, 30*time.Second)
	err := cluster.RotateDaemonKey(callCtx, "osd.0")
	callCancel()
	signalContainer(t, ctx, osd, "CONT")
	if err == nil || !strings.Contains(err.Error(), "RotateDaemonKey again") {
		t.Fatalf("a handoff to a stopped OSD returned %v", err)
	}
	handed := pendingKey(t, ctx, cluster, "osd.0")
	if handed == "" || authKey(t, ctx, cluster, "osd.0") != current {
		t.Fatal("an uncertain handoff did not keep both keys")
	}
	if running := rotateKeyProcesses(t, ctx, clusteraccess.ControlContainer(t, cluster)); running != "" {
		t.Fatalf("a rotate-key CLI is still running in the control container:\n%s", running)
	}
	if err := cluster.RotateDaemonKey(ctx, "osd.0"); err != nil {
		t.Fatal(err)
	}
	if authKey(t, ctx, cluster, "osd.0") != handed || pendingKey(t, ctx, cluster, "osd.0") != "" {
		t.Fatal("the retry did not finish with the handed key")
	}
	if err := cluster.RotateDaemonKey(ctx, "osd.0"); err == nil || !strings.Contains(err.Error(), "until its container restarts") {
		t.Fatalf("a new key while the earlier handoff may still arrive: %v", err)
	}
	if pendingKey(t, ctx, cluster, "osd.0") != "" {
		t.Fatal("a refused rotation left a pending key")
	}
	if err := osd.Stop(ctx, &stop); err != nil {
		t.Fatal(err)
	}
	if err := osd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitRotatedDaemonsUp(t, ctx, cluster)
	if err := cluster.RotateDaemonKey(ctx, "osd.0"); err != nil {
		t.Fatalf("rotation after the restart: %v", err)
	}
	if authKey(t, ctx, cluster, "osd.0") == handed {
		t.Fatal("the rotation after the restart kept the handed key")
	}
	if _, err := cluster.Ceph(ctx, "tell", "osd.0", "version"); err != nil {
		t.Fatalf("the OSD stopped answering: %v", err)
	}
	t.Log("an uncertain handoff kept both keys, the retry finished with the same key, and a new key waited for a restart")
}

// signalContainer sends signal to the main process of ctr from the host, which
// can stop and continue it, unlike a signal sent from inside the container.
func signalContainer(t *testing.T, ctx context.Context, ctr testcontainers.Container, signal string) {
	t.Helper()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = docker.Close() }()
	if _, err := docker.ContainerKill(ctx, ctr.GetContainerID(), mobycl.ContainerKillOptions{Signal: signal}); err != nil {
		t.Fatal(err)
	}
}

// rotateKeyProcessScript prints every other process whose arguments include
// rotate-key, which matches both the supervisor and its ceph child. It reads
// /proc with the Python that the control image already provides, and fails
// when it could read no process at all.
const rotateKeyProcessScript = `import os,sys
seen=0
for pid in os.listdir("/proc"):
 if not pid.isdigit() or int(pid)==os.getpid(): continue
 try:
  with open("/proc/"+pid+"/cmdline","rb") as f: argv=f.read().split(b"\0")
 except OSError: continue
 seen+=1
 if b"rotate-key" in argv: print(pid,b" ".join(argv).decode(errors="replace"))
sys.exit(0 if seen else 2)
`

// rotateKeyProcesses returns the rotate-key processes running in ctr, one
// per line.
func rotateKeyProcesses(t *testing.T, ctx context.Context, ctr testcontainers.Container) string {
	t.Helper()
	code, reader, err := ctr.Exec(ctx, []string{"python3", "-c", rotateKeyProcessScript}, tcexec.Multiplexed())
	if err != nil || code != 0 {
		t.Fatalf("list the control container's processes: exit %d, %v", code, err)
	}
	data, _ := io.ReadAll(reader)
	return strings.TrimSpace(string(data))
}

func authKey(t *testing.T, ctx context.Context, cluster *ceph.Container, entity string) string {
	t.Helper()
	out, err := cluster.Ceph(ctx, "auth", "get-key", entity)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(out))
}

func pendingKey(t *testing.T, ctx context.Context, cluster *ceph.Container, entity string) string {
	t.Helper()
	out, err := cluster.Ceph(ctx, "auth", "get", entity, "--format", "json")
	var entries []struct {
		PendingKey string `json:"pending_key"`
	}
	if err != nil || json.Unmarshal(out, &entries) != nil || len(entries) != 1 {
		t.Fatalf("auth get %s: %s %v", entity, out, err)
	}
	return entries[0].PendingKey
}

func containerFile(t *testing.T, ctx context.Context, ctr testcontainers.Container, path string) string {
	t.Helper()
	code, reader, err := ctr.Exec(ctx, []string{"cat", path}, tcexec.Multiplexed())
	if err != nil || code != 0 {
		t.Fatalf("read %s: exit %d, %v", path, code, err)
	}
	data, _ := io.ReadAll(reader)
	return string(data)
}

func waitRotatedDaemonsUp(t *testing.T, ctx context.Context, cluster *ceph.Container) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		osdStat, osdErr := cluster.Ceph(ctx, "osd", "stat", "--format", "json")
		mgrStat, mgrErr := cluster.Ceph(ctx, "mgr", "stat", "--format", "json")
		var osds struct {
			NumUpOSDs int `json:"num_up_osds"`
		}
		var mgrs struct {
			Available bool `json:"available"`
		}
		if osdErr == nil && mgrErr == nil && json.Unmarshal(osdStat, &osds) == nil && json.Unmarshal(mgrStat, &mgrs) == nil && osds.NumUpOSDs == 1 && mgrs.Available {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("daemons did not come back: %s %v, %s %v", osdStat, osdErr, mgrStat, mgrErr)
		}
		time.Sleep(time.Second)
	}
}

// Squid prints "auth:  id N <key> expires T" after "auth: dump_rotating:"
// and "cephx: verify_authorizer decrypted service osd secret_id=N" for a
// client ticket. Tentacle prints "RKR(...).dump_rotating  id N <key> expires
// T" and "cephx: cephx_verify_authorizer: decoded service osd secret_id=N".
var (
	rotatingSecretLine  = regexp.MustCompile(`\sid (\d+) \S+ expires \S+`)
	decryptedSecretLine = regexp.MustCompile(`(?:decoded|decrypted) service osd secret_id=(\d+)`)
)

// waitRotatingSecretIDs returns the secret IDs of the last rotating secret
// dump in the OSD's log once its highest ID reaches at least atLeast.
func waitRotatingSecretIDs(t *testing.T, ctx context.Context, osd testcontainers.Container, atLeast int) []int {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		lines := containerLogLines(t, ctx, osd)
		var ids []int
		for i := len(lines) - 1; i >= 0; i-- {
			match := rotatingSecretLine.FindStringSubmatch(lines[i])
			if match == nil {
				if len(ids) > 0 {
					break
				}
				continue
			}
			id, _ := strconv.Atoi(match[1])
			ids = append(ids, id)
		}
		slices.Sort(ids)
		if len(ids) > 0 && slices.Max(ids) >= atLeast {
			return ids
		}
		if time.Now().After(deadline) {
			t.Fatalf("the OSD logged no rotating secret with ID %d or higher: %v", atLeast, ids)
		}
		time.Sleep(time.Second)
	}
}

func decryptedSecretIDs(t *testing.T, ctx context.Context, osd testcontainers.Container) []int {
	t.Helper()
	var ids []int
	for _, line := range containerLogLines(t, ctx, osd) {
		if match := decryptedSecretLine.FindStringSubmatch(line); match != nil {
			id, _ := strconv.Atoi(match[1])
			ids = append(ids, id)
		}
	}
	return ids
}

func containerLogLines(t *testing.T, ctx context.Context, ctr testcontainers.Container) []string {
	t.Helper()
	reader, err := ctr.Logs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(string(data), "\n")
}
