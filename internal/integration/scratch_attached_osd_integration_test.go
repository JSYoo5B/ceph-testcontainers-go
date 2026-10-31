//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_scratch_tmpfs))))

//ci: timeout=20m job-timeout=30 image-roles=essential

package integration_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/moby/moby/api/types/mount"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

const (
	// The owned tmpfs ceiling and the OSD's logical block length: the block
	// file is sparse and longer than the filesystem can ever hold.
	attachedCeiling   = int64(512 << 20)
	attachedBlockSize = int64(1 << 30)
	attachedKeeperDir = "/tc/osd-memory"
	attachedOSDDir    = "/var/lib/ceph/osd"
	attachedPool      = "attached-capacity"
)

// attachedWrites writes COUNT objects of SIZE bytes named PREFIX-N through one
// client with bounded operations and prints the mutation ledger: "acked" for a
// write the OSD acknowledged, "unknown" for one whose completion was not
// observed (a timeout), "failed" for an explicit refusal. Each entry carries
// the digest of the bytes the client sent. The errno of each write that did
// not complete is printed first, on a line of its own, so the ledger line keeps
// exactly the fields that parseAttachLedger reads. Arguments: pool, prefix,
// count, size, the operation timeout in seconds.
const attachedWrites = `import errno, hashlib, json, rados, sys
pool, prefix, count, size, timeout = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
ledger, errnos = [], {}
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"client_mount_timeout": "30", "rados_mon_op_timeout": "30",
                                                       "rados_osd_op_timeout": timeout}) as c:
    with c.open_ioctx(pool) as io:
        for n in range(count):
            key = "%s-%d" % (prefix, n)
            data = hashlib.sha256(key.encode()).digest() * (size // 32)
            entry = {"key": key, "sha256": hashlib.sha256(data).hexdigest()}
            try:
                io.write_full(key, data)
                entry["outcome"] = "acked"
            except rados.Error as e:
                code = abs(getattr(e, "errno", 0) or 0)
                entry["outcome"] = "unknown" if code in (errno.ETIMEDOUT, errno.EINTR) or isinstance(e, rados.TimedOut) else "failed"
                errnos[key] = code
            ledger.append(entry)
print(json.dumps({"errnos": errnos}))
print(json.dumps(ledger))`

// attachedReadback reads every key given and prints one record each:
// "present" with the digest, "absent" when the object does not exist, and
// "error" when the read itself failed. Arguments: pool, the timeout, keys.
const attachedReadback = `import hashlib, json, rados, sys
pool, timeout, keys = sys.argv[1], sys.argv[2], sys.argv[3:]
out = []
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"client_mount_timeout": "30", "rados_mon_op_timeout": "30",
                                                       "rados_osd_op_timeout": timeout}) as c:
    with c.open_ioctx(pool) as io:
        for key in keys:
            try:
                size = io.stat(key)[0]
                out.append({"key": key, "status": "present", "sha256": hashlib.sha256(io.read(key, size, 0)).hexdigest()})
            except rados.ObjectNotFound:
                out.append({"key": key, "status": "absent"})
            except rados.Error:
                out.append({"key": key, "status": "error"})
print(json.dumps(out))`

// attachedDockerExec runs argv in a container known only by its ID and
// returns its standard output, failing on a nonzero exit.
func attachedDockerExec(t *testing.T, parent context.Context, docker *mobycl.Client, cid string, argv ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 4*time.Minute)
	defer cancel()
	created, err := docker.ExecCreate(ctx, cid, mobycl.ExecCreateOptions{AttachStdout: true, AttachStderr: true, TTY: true, Cmd: argv})
	if err != nil {
		t.Fatal(err)
	}
	attached, err := docker.ExecAttach(ctx, created.ID, mobycl.ExecAttachOptions{TTY: true})
	if err != nil {
		t.Fatal(err)
	}
	defer attached.Close()
	data, err := io.ReadAll(io.LimitReader(attached.Reader, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	result, err := docker.ExecInspect(ctx, created.ID, mobycl.ExecInspectOptions{})
	if err != nil || result.Running || result.ExitCode != 0 {
		t.Fatalf("exec %v in %s: running=%v exit=%d %v: %s", argv[:2], cid, result.Running, result.ExitCode, err, data)
	}
	return []byte(strings.ReplaceAll(string(data), "\r\n", "\n"))
}

// attachedState is the container's inspection as JSON, for the judge's
// State parser.
func attachedState(t *testing.T, ctx context.Context, docker *mobycl.Client, cid string) []byte {
	t.Helper()
	inspection, err := docker.ContainerInspect(ctx, cid, mobycl.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]any{"State": inspection.Container.State})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// attachedLogTail keeps the lines of the OSD's log that say how it reacted to
// the full volume: space, allocation, aborts and the exit.
func attachedLogTail(ctx context.Context, docker *mobycl.Client, cid string) []string {
	reader, err := docker.ContainerLogs(ctx, cid, mobycl.ContainerLogsOptions{ShowStdout: true, ShowStderr: true, Tail: "400"})
	if err != nil {
		return []string{"logs unavailable: " + err.Error()}
	}
	defer reader.Close()
	data, _ := io.ReadAll(io.LimitReader(reader, 4<<20))
	var kept []string
	for _, line := range strings.Split(string(data), "\n") {
		lower := strings.ToLower(line)
		for _, word := range []string{"no space", "enospc", "abort", "allocat", "bluefs", "error", "fail", "exit", "(28)", "(5)"} {
			if strings.Contains(lower, word) {
				if len(line) > 400 {
					line = line[:400]
				}
				kept = append(kept, line)
				break
			}
		}
	}
	if len(kept) > 60 {
		kept = kept[len(kept)-60:]
	}
	return kept
}

// attachedKeeper finds the one keeper that holds the OSD's tmpfs volume.
func attachedKeeper(t *testing.T, ctx context.Context, docker *mobycl.Client, osd *ceph.OSDContainer) string {
	t.Helper()
	inspection, err := docker.ContainerInspect(ctx, osd.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	name := ""
	for _, m := range inspection.Container.Mounts {
		if m.Destination == attachedOSDDir && m.Type == mount.TypeVolume {
			name = m.Name
		}
	}
	owner, found := strings.CutPrefix(name, "tc-ceph-osd-memory-")
	if !found {
		t.Fatalf("the OSD's %s is not an owned memory volume: %q", attachedOSDDir, name)
	}
	listed, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: make(mobycl.Filters).Add("label", osdMemoryLabel+"="+owner)})
	if err != nil || len(listed.Items) != 1 {
		t.Fatalf("keeper inventory for %s: %d, %v", owner, len(listed.Items), err)
	}
	return listed.Items[0].ID
}

func attachedKeys(ledger []byte, outcome string) []string {
	var entries []struct{ Key, Outcome string }
	_ = json.Unmarshal(ledger, &entries)
	var keys []string
	for _, e := range entries {
		if outcome == "" || e.Outcome == outcome {
			keys = append(keys, e.Key)
		}
	}
	return keys
}

// attachedArrayLine returns the last line of out that holds a JSON array, so
// library noise on the same stream cannot corrupt the ledger.
func attachedArrayLine(out string) string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	for index := len(lines) - 1; index >= 0; index-- {
		if line := strings.TrimSpace(lines[index]); strings.HasPrefix(line, "[") {
			return line
		}
	}
	return ""
}

// joinLedgers concatenates two JSON ledgers into one document.
func joinLedgers(t *testing.T, parts ...[]byte) []byte {
	t.Helper()
	var all []json.RawMessage
	for _, part := range parts {
		var entries []json.RawMessage
		if err := json.Unmarshal(part, &entries); err != nil {
			t.Fatalf("ledger %s: %v", part, err)
		}
		all = append(all, entries...)
	}
	data, _ := json.Marshal(all)
	return data
}

// The attached capacity run of issue 63. One BlueStore OSD keeps its store on
// the cluster's owned tmpfs volume (OSDStorage.WithInMemory) with a sparse
// block file longer than the volume. The run ties the OSD's store view to the
// keeper's view of the volume, shows that the block file is sparse on that
// tmpfs, records the fullness policy, a seed and acknowledged writes, then
// fills the volume from the keeper with the bounded filler until the byte
// ceiling refuses it. With the volume full the client writes again; then the
// identified filler is released and the OSD's state, the keeper's generation,
// the policy and, while the OSD is up, a readback of the acknowledged writes
// are observed. Every input goes through the existing producers into
// buildAttachRun, and judgeScratchAttachment decides. The test requires the
// attachment to be established and a verdict without error; the verdict itself
// (recovered, aborted, daemon unknown, ...) is the native result it records.
func TestScratchTmpfsAttachedOSD(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	image, opts := harness.IntegrationImages(t)
	opts = append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs().WithPoolDefaults(1, 1)),
		ceph.WithOSDStorage(ceph.OSDStorage{}.WithInMemory(attachedCeiling).WithBlockSize(attachedBlockSize)))
	cluster, err := ceph.Run(ctx, image, opts...)
	if cluster != nil {
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
			defer done()
			if err := cluster.Terminate(cleanup); err != nil {
				t.Error("attached capacity cluster cleanup", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true"}); err != nil {
		t.Fatal(err)
	}
	osd, err := cluster.AddOSD(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if osds := clusteraccess.OSDs(t, cluster); len(osds) != 1 {
		t.Fatalf("%d OSDs, want 1", len(osds))
	}
	keeper := attachedKeeper(t, ctx, docker.Client, osd)
	client := harness.ToolkitContainer(t, cluster)
	q, err := harness.ObserveCluster(cluster).QuorumStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fsid := q.MonMap.FSID
	in := &attachRawInputs{CeilingBytes: attachedCeiling, FSID: fsid}
	receipt := map[string]any{"fsid": fsid, "ceiling": attachedCeiling, "block_size": attachedBlockSize, "osd": osd.ID}
	defer func() {
		data, _ := json.Marshal(receipt)
		t.Logf("SCRATCH_ATTACHED_OSD_RECEIPT %s", data)
	}()

	// The OSD's store and the keeper's volume are one tmpfs instance.
	raw := make([]byte, 8)
	_, _ = rand.Read(raw)
	in.Token = "attached-" + hex.EncodeToString(raw)
	marker := "attach-marker-" + hex.EncodeToString(raw)
	planted := attachedDockerExec(t, ctx, docker.Client, keeper, "python3", "-I", "-c", scratchStoreMarkerScript, attachedKeeperDir+"/"+marker, in.Token)
	if m, err := parseStoreMarker(planted); err != nil || *m.Errno != 0 {
		t.Fatalf("marker: %s, %v", planted, err)
	}
	in.KeeperView = attachedDockerExec(t, ctx, docker.Client, keeper, "python3", "-I", "-c", scratchStoreViewScript, attachedKeeperDir+"/"+marker)
	in.OSDView = scratchContainerExec(t, ctx, osd, "python3", "-I", "-c", scratchStoreViewScript, attachedOSDDir+"/"+marker)
	block := attachedOSDDir + "/ceph-" + strconv.Itoa(osd.ID) + "/block"
	in.BlockBacking = scratchContainerExec(t, ctx, osd, "python3", "-I", "-c", scratchBlockBackingScript, block, attachedOSDDir)
	receipt["block_backing"] = json.RawMessage(in.BlockBacking)

	if in.PolicyBefore, err = cluster.Ceph(ctx, "osd", "dump", "--format", "json"); err != nil {
		t.Fatal(err)
	}
	seed := attachedDockerExec(t, ctx, docker.Client, keeper, "python3", "-I", "-c", `import hashlib,os,sys
data=bytes((i*7+3)%256 for i in range(int(sys.argv[2])))
fd=os.open(sys.argv[1],os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
os.write(fd,data); os.fsync(fd); os.close(fd)
print(hashlib.sha256(data).hexdigest())`, attachedKeeperDir+"/seed", strconv.Itoa(scratchSeedLen))
	in.SeedSHA = strings.TrimSpace(string(seed))

	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: attachedPool, Application: "rados", Replicas: 1, MinSize: 1, PGNum: 8}); err != nil {
		t.Fatal(err)
	}
	before := harness.ExecOutput(t, ctx, client, "python3", "-c", attachedWrites, attachedPool, "before", "4", strconv.Itoa(64<<10), "30")
	receipt["before_writes"] = json.RawMessage(attachedArrayLine(before))
	in.KeeperAtAttach = attachedState(t, ctx, docker.Client, keeper)
	in.DaemonAtAttach = attachedState(t, ctx, docker.Client, osd.GetContainerID())

	// The keeper fills the volume until the byte ceiling refuses the filler;
	// the exec returning is the writer's confirmed exit.
	in.Fill = attachedDockerExec(t, ctx, docker.Client, keeper, "python3", "-I", "-c", scratchFillerScript, "write",
		attachedKeeperDir, "filler", strconv.Itoa(64<<10), strconv.FormatInt(2*attachedCeiling, 10), "180", "0")
	in.WriterJoined = true
	fill, err := parseScratchFillReceipt(in.Fill)
	if err != nil {
		t.Fatalf("fill: %v: %s", err, in.Fill)
	}
	// The receipt keeps the filler's identity and its last operation, not
	// every chunk.
	receipt["fill"] = map[string]any{"file": fill.File, "ops": len(fill.Ops), "last": fill.Ops[len(fill.Ops)-1],
		"vfs_before": fill.VFSBefore}

	// With the volume full, the client writes new objects: BlueStore must
	// allocate in the sparse block file's holes.
	during := harness.ExecOutput(t, ctx, client, "python3", "-c", attachedWrites, attachedPool, "during", "8", strconv.Itoa(1<<20), "20")
	receipt["during_writes"] = json.RawMessage(attachedArrayLine(during))
	receipt["during_errnos"] = json.RawMessage(harness.LastJSONLine(during))
	in.Ledger = joinLedgers(t, []byte(attachedArrayLine(before)), []byte(attachedArrayLine(during)))

	in.Release = attachedDockerExec(t, ctx, docker.Client, keeper, "python3", "-I", "-c", scratchFillerScript, "release",
		attachedKeeperDir, "filler", strconv.FormatUint(*fill.File.Ino, 10), strconv.FormatUint(*fill.File.Dev, 10), "seed")
	receipt["release"] = json.RawMessage(strings.TrimSpace(string(in.Release)))

	// Let the OSD react to the released space before it is judged.
	time.Sleep(20 * time.Second)
	in.KeeperAtEval = attachedState(t, ctx, docker.Client, keeper)
	in.DaemonAtEval = attachedState(t, ctx, docker.Client, osd.GetContainerID())
	receipt["daemon_at_eval"] = json.RawMessage(in.DaemonAtEval)
	receipt["osd_log"] = attachedLogTail(ctx, docker.Client, osd.GetContainerID())
	if in.PolicyAtEval, err = cluster.Ceph(ctx, "osd", "dump", "--format", "json"); err != nil {
		t.Fatal(err)
	}
	in.Readback = []byte("[]")
	if acked := attachedKeys(in.Ledger, mutAcked); len(acked) > 0 {
		var state struct{ State struct{ Running bool } }
		if json.Unmarshal(in.DaemonAtEval, &state) == nil && state.State.Running {
			out := harness.ExecOutput(t, ctx, client, append([]string{"python3", "-c", attachedReadback, attachedPool, "30"}, acked...)...)
			in.Readback = []byte(attachedArrayLine(out))
			receipt["readback"] = json.RawMessage(in.Readback)
		}
	}

	run, err := buildAttachRun(in)
	if err != nil {
		t.Fatal("attachment inputs:", err)
	}
	receipt["link"], receipt["backing"], receipt["daemon"] = run.Link, run.Backing, run.Daemon
	verdict, err := judgeScratchAttachment(run)
	receipt["verdict"] = verdict
	if err != nil {
		receipt["judge_error"] = err.Error()
		t.Fatal("attachment judge:", err)
	}
	switch verdict {
	case attachNotAttached, attachAllocUnproved, attachUnreached:
		t.Fatalf("the attachment or the fill was not established: %s", verdict)
	}
	t.Logf("SCRATCH_ATTACHED_OSD verdict=%s link=%s backing=%s daemon=%s", verdict, run.Link, run.Backing, run.Daemon)

	// Outside the judge: an OSD that aborted is started again in the same
	// container with the space released, and every acknowledged write must
	// read back intact. The writes that did not complete are read too and
	// reported as they are found, never promoted.
	if verdict != attachDaemonAborted {
		return
	}
	cid := osd.GetContainerID()
	if err := osd.Start(ctx); err != nil {
		receipt["restart"] = map[string]any{"error": err.Error()}
		t.Fatal("restart the aborted OSD:", err)
	}
	if osd.GetContainerID() != cid {
		t.Fatal("the restart replaced the OSD container")
	}
	attachedWaitUp(t, ctx, cluster, osd)
	keys := attachedKeys(in.Ledger, "")
	out := harness.ExecOutput(t, ctx, client, append([]string{"python3", "-c", attachedReadback, attachedPool, "30"}, keys...)...)
	restarted := []byte(attachedArrayLine(out))
	receipt["restart"] = map[string]any{"readback": json.RawMessage(restarted), "keeper": json.RawMessage(attachedState(t, ctx, docker.Client, keeper))}
	recovered, err := attachRecoveredOf(run.Ledger, restarted)
	if err != nil {
		t.Fatal("readback after the restart:", err)
	}
	for _, m := range run.Ledger {
		if m.Outcome == mutAcked && recovered[m.Key] != m.SHA256 {
			t.Fatalf("acknowledged %s is missing or different after the restart", m.Key)
		}
	}
	t.Logf("SCRATCH_ATTACHED_OSD_RESTART acked_intact=%d", len(attachedKeys(in.Ledger, mutAcked)))
}

// attachedWaitUp waits until the OSD runs in its container and the OSDMap
// marks it up and in.
func attachedWaitUp(t *testing.T, parent context.Context, cluster *ceph.Container, osd *ceph.OSDContainer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	for {
		state, err := osd.State(ctx)
		if err != nil || state == nil || !state.Running {
			t.Fatalf("the restarted OSD exited: %+v %v", state, err)
		}
		states, err := harness.ObserveCluster(cluster).OSDStates(ctx)
		if err == nil && len(states) == 1 && states[0].Up && states[0].In {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("the restarted OSD did not come up", ctx.Err())
		case <-time.After(time.Second):
		}
	}
}
