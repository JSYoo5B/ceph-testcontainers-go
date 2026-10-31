//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_address_families))))

//ci: timeout=50m job-timeout=60 image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestTargetedCommandRecipe sends commands to one named OSD, one PG and one
// MDS and records which daemon answered, with its map epoch, and how each
// kind of failure reaches the caller:
//
//   - The OSD's "status" names the OSD id, OSD UUID and cluster FSID that the
//     OSDMap lists for the target; a PG's "query" names the PG and the acting
//     set the OSDMap maps it to; the MDS's "status" names the rank, GID and
//     filesystem that the FSMap lists for the daemon.
//   - An identity without "allow *" on OSDs or MDSs is refused by the daemon
//     with EACCES: librados returns -13, the CLI exits 13.
//   - An OSD id or a pool that the client's OSDMap does not have is refused by
//     librados before anything is sent (ENOENT), a stopped OSD that the map
//     marks down likewise (ENXIO).
//   - The OSD refuses an unknown command prefix with EINVAL.
//   - A paused OSD that the map keeps up receives the command and does not
//     answer: the client's timeout ends it with ETIMEDOUT, an unknown outcome.
//
// librados reports the signed result of the reply; the CLI exit status is the
// errno's absolute value. Neither shows the wire message itself, so for
// commands with binary input and output buffers the test raises debug_ms on
// the client and the daemons and binds each buffer's length to the data
// segment of the message that carried it (TARGETED_BINARY_IO_RECEIPT):
//
//   - An OSD receives 1 MiB of input with "status" and ignores it; the MON
//     session keeps exactly its config-key entry limit of arbitrary bytes and
//     returns them; the MGR session's crash module returns a 96 KiB report
//     with one newline appended. "osd getcrushmap" and "osd getmap" are
//     decoded with crushtool and osdmaptool, a compiled CRUSH map is set, a
//     broken one refused, and the original bytes are set back and read back
//     exactly.
//   - A command targeted at a MON or a MGR reaches only its admin socket
//     commands, which, like the OSD's, return no binary output.
//   - The paused OSD's command was sent with 256 KiB of input; the client
//     cancels it at its timeout, the OSD answers it once it runs again, and
//     the client drops that reply.
func TestTargetedCommandRecipe(t *testing.T) {
	recordEvidence := registerMessengerEvidence(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	image, roles := harness.IntegrationImages(t)
	const filesystem, pool, object = "tc-targets", "tc-targets", "targeted"
	cluster, _ := harness.NewServiceClusterWithOptions(t, image, append(roles, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(2)),
		cephfs.WithFilesystems(cephfs.Config{Name: filesystem}))...)
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 8, Replicas: 2, MinSize: 1}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	fsid := targetedJSON[struct {
		FSID string `json:"fsid"`
	}](t, ctx, cluster, "fsid").FSID
	osdMap := targetedJSON[struct {
		Epoch int `json:"epoch"`
		OSDs  []struct {
			OSD  int    `json:"osd"`
			UUID string `json:"uuid"`
			Up   int    `json:"up"`
		} `json:"osds"`
	}](t, ctx, cluster, "osd", "dump")
	mapping := targetedJSON[struct {
		Epoch  int    `json:"epoch"`
		PGID   string `json:"pgid"`
		Acting []int  `json:"acting"`
	}](t, ctx, cluster, "osd", "map", pool, object)
	receipt := map[string]any{"fsid": fsid, "osdmap_epoch": osdMap.Epoch, "pgid": mapping.PGID, "acting": mapping.Acting}

	// Identity: each OSD, the PG, and refusals the client decides alone.
	toolkit := harness.ToolkitContainer(t, cluster)
	admin := runTargetedProbe(t, ctx, toolkit, "/etc/ceph/ceph.client.admin.keyring", "client.admin", 30,
		"osd0=osd:0:status", "osd1=osd:1:status", "unknown=osd:0:tc-no-such-command", "missing=osd:99:status",
		"pg=pg:"+mapping.PGID+":query", "missingpg=pg:99.0:query")
	for _, osd := range osdMap.OSDs {
		reply := admin[fmt.Sprintf("osd%d", osd.OSD)]
		identity, err := parseOSDStatusIdentity(reply.JSON)
		if reply.Ret != 0 || err != nil {
			t.Fatalf("osd.%d status: %+v %v", osd.OSD, reply, err)
		}
		if identity.WhoAmI != osd.OSD || identity.OSDFSID != osd.UUID || identity.ClusterFSID != fsid || identity.NewestMap < 1 {
			t.Fatalf("osd.%d answered as %+v; the OSDMap lists uuid %s in cluster %s", osd.OSD, identity, osd.UUID, fsid)
		}
		receipt[fmt.Sprintf("osd.%d", osd.OSD)] = identity
	}
	pg, err := parsePGQueryIdentity(admin["pg"].JSON)
	if admin["pg"].Ret != 0 || err != nil || pg.PGID != mapping.PGID || !slices.Equal(pg.Acting, mapping.Acting) || pg.Epoch < 1 {
		t.Fatalf("pg %s query: %+v %v; the OSDMap maps it to %v", mapping.PGID, admin["pg"], err, mapping.Acting)
	}
	receipt["pg"] = pg
	for label, want := range map[string]int{"missing": -2, "missingpg": -2} {
		if admin[label].Ret != want {
			t.Fatalf("%s: ret %d, want %d: %+v", label, admin[label].Ret, want, admin[label])
		}
	}
	if admin["unknown"].Ret != -22 {
		t.Fatalf("an unknown OSD command returned %+v, want -22", admin["unknown"])
	}
	receipt["client_refusals"] = map[string]int{"osd.99": admin["missing"].Ret, "pg 99.0": admin["missingpg"].Ret}
	receipt["unknown_command"] = admin["unknown"]

	// The MDS: its own status against the FSMap.
	mds := targetedActiveMDS(t, ctx, cluster, filesystem)
	status := targetedJSON[struct {
		ClusterFSID string `json:"cluster_fsid"`
		WhoAmI      int    `json:"whoami"`
		ID          uint64 `json:"id"`
		State       string `json:"state"`
		FSName      string `json:"fs_name"`
		MDSMapEpoch int    `json:"mdsmap_epoch"`
		OSDMapEpoch int    `json:"osdmap_epoch"`
	}](t, ctx, cluster, "tell", "mds."+mds.Name, "status")
	if status.ClusterFSID != fsid || status.WhoAmI != mds.Rank || status.ID != mds.GID || status.FSName != filesystem ||
		status.State != "up:active" || status.MDSMapEpoch < 1 {
		t.Fatalf("mds.%s answered as %+v; the FSMap lists rank %d gid %d", mds.Name, status, mds.Rank, mds.GID)
	}
	receipt["mds"] = status

	// A daemon refuses an identity without "allow *".
	limited, err := cluster.CreateClient(ctx, "targets-limited", ceph.ClientCaps{Mon: "allow r", OSD: "allow r", MDS: "allow r", MGR: "allow r"})
	if err != nil {
		t.Fatal(err)
	}
	client, err := testcontainers.Run(ctx, harness.NativeClientImage(t, image), limited.WithIdentity(), ceph.WithIdleEntrypoint(),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"python3", "-c", "import rados"})))
	if client != nil {
		testcontainers.CleanupContainer(t, client)
	}
	if err != nil {
		t.Fatal(err)
	}
	refused := runTargetedProbe(t, ctx, client, limited.KeyringPath(), limited.Name(), 30,
		"osd0=osd:0:status", "pg=pg:"+mapping.PGID+":query")
	for label, reply := range refused {
		if reply.Ret != -13 {
			t.Fatalf("limited %s: %+v, want -13", label, reply)
		}
	}
	cli := runTargetedCLI(t, ctx, client,
		[]string{"ceph", "--connect-timeout", "10", "--name", limited.Name(), "tell", "osd.0", "status"},
		[]string{"ceph", "--connect-timeout", "10", "--name", limited.Name(), "tell", "mds." + mds.Name, "status"})
	for _, run := range cli {
		if run.Exit != 13 || !strings.Contains(run.Stderr, "EACCES") && !strings.Contains(run.Stderr, "Permission denied") {
			t.Fatalf("limited CLI %v: exit %d, %q; want 13 and EACCES", run.Argv, run.Exit, run.Stderr)
		}
	}
	receipt["server_refusals"] = map[string]any{"librados": refused, "cli": cli}

	// Binary buffers, with the OSDs' and MONs' messengers logging what they
	// receive: an OSD, the MGR and the MON session carry them byte for byte.
	monMap := targetedJSON[struct {
		Mons []struct {
			Rank int    `json:"rank"`
			Name string `json:"name"`
		} `json:"mons"`
	}](t, ctx, cluster, "mon", "dump")
	mons := map[int]string{}
	debugged := []string{"osd.0", "osd.1"}
	for _, mon := range monMap.Mons {
		mons[mon.Rank] = mon.Name
		debugged = append(debugged, "mon."+mon.Name)
	}
	mgrMap := targetedJSON[struct {
		ActiveGID  uint64 `json:"active_gid"`
		ActiveName string `json:"active_name"`
	}](t, ctx, cluster, "mgr", "dump")
	restoreDebug := targetedDebugMS(t, ctx, cluster, debugged...)
	binary := targetedBinaryIO(t, ctx, cluster, fsid, mons, mgrMap.ActiveGID, mgrMap.ActiveName)

	// A paused OSD that the map keeps up: the command is sent and never
	// answered, so only the client's timeout ends it. The client cancels it
	// and drops the reply that the OSD sends once it runs again.
	binary["cancellation"] = targetedCancellation(t, ctx, cluster)
	restoreDebug()
	receipt["paused"] = binary["cancellation"].(map[string]any)["calls"].([]binaryCall)[1]
	data, err := json.Marshal(binary)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("TARGETED_BINARY_IO_RECEIPT %s", data)
	osds := clusteraccess.OSDs(t, cluster)

	// A stopped OSD that the map marks down: librados refuses it with ENXIO.
	stop := 20 * time.Second
	if err := osds[1].Stop(ctx, &stop); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(2 * time.Minute); ; {
		down := targetedJSON[struct {
			OSDs []struct {
				OSD int `json:"osd"`
				Up  int `json:"up"`
			} `json:"osds"`
		}](t, ctx, cluster, "osd", "dump")
		if slices.ContainsFunc(down.OSDs, func(o struct {
			OSD int `json:"osd"`
			Up  int `json:"up"`
		}) bool {
			return o.OSD == 1 && o.Up == 0
		}) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("osd.1 is still up in the OSDMap after it stopped")
		}
		time.Sleep(time.Second)
	}
	stopped := runTargetedProbe(t, ctx, toolkit, "/etc/ceph/ceph.client.admin.keyring", "client.admin", 10,
		"stopped=osd:1:status")["stopped"]
	if stopped.Ret != -6 {
		t.Fatalf("stopped osd.1 status: %+v, want -6", stopped)
	}
	receipt["stopped"] = stopped
	if err := osds[1].Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("TARGETED_COMMAND_RECEIPT %s", data)
	recordEvidence(targetedMessengerEvidence(t, ctx, cluster, data, fsid, toolkit, client))
}

func runTargetedProbe(t *testing.T, ctx context.Context, ctr testcontainers.Container, keyring, name string, timeout int, requests ...string) map[string]targetedReply {
	t.Helper()
	args := append([]string{"python3", "-c", targetedCommandProbe, keyring, name, fmt.Sprint(timeout)}, requests...)
	out := harness.ExecOutput(t, ctx, ctr, args...)
	var replies map[string]targetedReply
	if err := json.Unmarshal([]byte(out), &replies); err != nil {
		t.Fatalf("targeted probe: %v: %s", err, out)
	}
	return replies
}

func runTargetedCLI(t *testing.T, ctx context.Context, ctr testcontainers.Container, argvs ...[]string) []targetedCLI {
	t.Helper()
	data, err := json.Marshal(argvs)
	if err != nil {
		t.Fatal(err)
	}
	out := harness.ExecOutput(t, ctx, ctr, "python3", "-c", targetedCLIProbe, string(data))
	var runs []targetedCLI
	if err := json.Unmarshal([]byte(out), &runs); err != nil {
		t.Fatalf("targeted CLI: %v: %s", err, out)
	}
	return runs
}

type targetedMDS struct {
	Name string
	Rank int
	GID  uint64
}

// targetedActiveMDS returns the active rank 0 MDS of filesystem from the
// FSMap.
func targetedActiveMDS(t *testing.T, ctx context.Context, cluster *ceph.Container, filesystem string) targetedMDS {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Minute); ; {
		dump := targetedJSON[struct {
			Filesystems []struct {
				MDSMap struct {
					FSName string `json:"fs_name"`
					Info   map[string]struct {
						GID   uint64 `json:"gid"`
						Name  string `json:"name"`
						Rank  int    `json:"rank"`
						State string `json:"state"`
					} `json:"info"`
				} `json:"mdsmap"`
			} `json:"filesystems"`
		}](t, ctx, cluster, "fs", "dump")
		for _, fs := range dump.Filesystems {
			if fs.MDSMap.FSName != filesystem {
				continue
			}
			for _, info := range fs.MDSMap.Info {
				if info.Rank == 0 && info.State == "up:active" {
					return targetedMDS{Name: info.Name, Rank: info.Rank, GID: info.GID}
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s has no active rank 0", filesystem)
		}
		time.Sleep(time.Second)
	}
}
