//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_scratch_tmpfs))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// The owned tmpfs is four times the OSD's logical block length, so the
	// filesystem keeps room when BlueStore runs out.
	logicalCeiling   = int64(1 << 30)
	logicalBlockSize = int64(256 << 20)
	logicalPool      = "logical-capacity"
)

// logicalWrites writes up to COUNT objects of SIZE bytes named PREFIX-N, sleeps
// PAUSE seconds after each, and stops after STOP consecutive writes that were
// not acknowledged. It prints the errnos and then the mutation ledger, as
// attachedWrites does. Arguments: pool, prefix, count, size, the operation
// timeout in seconds, stop, pause.
const logicalWrites = `import errno, hashlib, json, rados, sys, time
pool, prefix, count, size, timeout, stop, pause = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5], int(sys.argv[6]), float(sys.argv[7])
ledger, errnos, misses = [], {}, 0
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
                misses = 0
            except rados.Error as e:
                code = abs(getattr(e, "errno", 0) or 0)
                entry["outcome"] = "unknown" if code in (errno.ETIMEDOUT, errno.EINTR) or isinstance(e, rados.TimedOut) else "failed"
                errnos[key] = code
                misses += 1
            ledger.append(entry)
            if misses >= stop:
                break
            if pause:
                time.sleep(pause)
print(json.dumps({"errnos": errnos}))
print(json.dumps(ledger))`

// logicalRemove removes the given objects with FULL_TRY, which a full OSDMap
// admits for an operation that frees space, and prints one outcome each.
// Python's rados module does not name the flag; 64 is
// LIBRADOS_OPERATION_FULL_TRY in librados.h. Arguments: pool, keys.
const logicalRemove = `import json, rados, sys
out = {}
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"client_mount_timeout": "30", "rados_mon_op_timeout": "30",
                                                       "rados_osd_op_timeout": "30"}) as c:
    with c.open_ioctx(sys.argv[1]) as io:
        for key in sys.argv[2:]:
            try:
                with rados.WriteOpCtx() as op:
                    op.remove()
                    io.operate_write_op(op, key, flags=64)
                out[key] = "removed"
            except rados.Error as e:
                out[key] = "error %s" % getattr(e, "errno", "")
print(json.dumps(out))`

// logicalGrow lengthens the block file by EXTRA bytes from the keeper; the
// owned filesystem still has room for them. Growing the file alone does not
// give BlueFS the space: blueStoreExpand must follow. Arguments: the OSD data
// directory, extra.
const logicalGrow = `import json, os, subprocess, sys
path = os.path.join(sys.argv[1], "block")
before = os.stat(path).st_size
os.truncate(path, before + int(sys.argv[2]))
print(json.dumps({"before": before, "after": os.stat(path).st_size, "rc": 0}))`

const logicalStatVFS = `import json, os, sys
v = os.statvfs(sys.argv[1])
print(json.dumps({"total": v.f_blocks * v.f_frsize, "avail": v.f_bavail * v.f_frsize}))`

// logicalEnv is one cluster with one OSD whose sparse block file stands on the
// cluster's owned tmpfs.
type logicalEnv struct {
	cluster *ceph.Container
	osd     *ceph.OSDContainer
	docker  *mobycl.Client
	keeper  string
	client  testcontainers.Container
	acked   []attachMutation
}

func newLogicalEnv(t *testing.T, ctx context.Context) *logicalEnv {
	t.Helper()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	image, opts := harness.IntegrationImages(t)
	opts = append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs().WithPoolDefaults(1, 1)),
		ceph.WithOSDStorage(ceph.OSDStorage{}.WithInMemory(logicalCeiling).WithBlockSize(logicalBlockSize)))
	cluster, err := ceph.Run(ctx, image, opts...)
	if cluster != nil {
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
			defer done()
			if err := cluster.Terminate(cleanup); err != nil {
				t.Error("logical capacity cluster cleanup", err)
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
	env := &logicalEnv{cluster: cluster, osd: osd, docker: docker.Client, keeper: attachedKeeper(t, ctx, docker.Client, osd), client: harness.ToolkitContainer(t, cluster)}
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: logicalPool, Application: "rados", Replicas: 1, MinSize: 1, PGNum: 8}); err != nil {
		t.Fatal(err)
	}
	return env
}

// write runs one write phase and keeps its acknowledged writes for the later
// readbacks.
func (e *logicalEnv) write(t *testing.T, ctx context.Context, prefix string, count int, size int64, pause string) []attachMutation {
	t.Helper()
	out := harness.ExecOutput(t, ctx, e.client, "python3", "-c", logicalWrites, logicalPool, prefix, strconv.Itoa(count), strconv.FormatInt(size, 10), "15", "2", pause)
	ledger, err := parseAttachLedger([]byte(attachedArrayLine(out)))
	if err != nil {
		t.Fatalf("%s writes: %v: %s", prefix, err, out)
	}
	for _, m := range ledger {
		if m.Outcome == mutAcked {
			e.acked = append(e.acked, m)
		}
	}
	return ledger
}

// observe gathers the phase receipt after the writes stopped. The block entry
// and the filesystem are seen from the keeper, which holds the volume whether
// the OSD runs or not.
func (e *logicalEnv) observe(t *testing.T, ctx context.Context, ledger []attachMutation) (*logicalPhase, map[string]any) {
	t.Helper()
	// Let the OSD report its state, or exit, before it is judged.
	time.Sleep(10 * time.Second)
	block := attachedKeeperDir + "/ceph-" + strconv.Itoa(e.osd.ID) + "/block"
	raw := attachedDockerExec(t, ctx, e.docker, e.keeper, "python3", "-I", "-c", scratchBlockBackingScript, block, attachedKeeperDir)
	backing, err := parseBlockBacking(raw)
	if err != nil {
		t.Fatalf("%v: %s", err, raw)
	}
	var vfs struct{ Total, Avail int64 }
	if err := json.Unmarshal(attachedDockerExec(t, ctx, e.docker, e.keeper, "python3", "-I", "-c", logicalStatVFS, attachedKeeperDir), &vfs); err != nil || vfs.Total != logicalCeiling {
		t.Fatalf("owned filesystem %+v: %v", vfs, err)
	}
	health, err := e.cluster.Ceph(ctx, "health", "detail", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	checks, err := parseHealthChecks(health)
	if err != nil {
		t.Fatal(err)
	}
	state, err := e.osd.State(ctx)
	if err != nil || state == nil {
		t.Fatal("OSD state", err)
	}
	phase := &logicalPhase{Ledger: ledger, Checks: checks, Running: state.Running, Log: logicalLogTail(ctx, e.docker, e.osd.GetContainerID(), ""),
		FSAvail: vfs.Avail, BlockSize: *backing.Block.Size, BlockAllocated: *backing.Block.Allocated}
	df, _ := e.cluster.Ceph(ctx, "osd", "df", "--format", "json")
	return phase, map[string]any{"ledger": logicalLedgerSummary(ledger), "checks": checks, "running": state.Running, "exit_code": state.ExitCode,
		"fs_avail": vfs.Avail, "block_allocated": phase.BlockAllocated, "osd_df": logicalDFSummary(df), "log": phase.Log}
}

// readback requires every acknowledged write so far to read back intact.
func (e *logicalEnv) readback(t *testing.T, ctx context.Context) int {
	t.Helper()
	keys := make([]string, 0, len(e.acked))
	for _, m := range e.acked {
		keys = append(keys, m.Key)
	}
	out := harness.ExecOutput(t, ctx, e.client, append([]string{"python3", "-c", attachedReadback, logicalPool, "30"}, keys...)...)
	recovered, err := attachRecoveredOf(e.acked, []byte(attachedArrayLine(out)))
	if err != nil {
		t.Fatal("readback:", err)
	}
	for _, m := range e.acked {
		if recovered[m.Key] != m.SHA256 {
			t.Fatalf("acknowledged %s is missing or different", m.Key)
		}
	}
	return len(keys)
}

// Outcomes of starting an aborted OSD again in the same container.
const (
	restartBooted  = "booted"  // a new OSDMap boot and still running after logicalSettle
	restartAborted = "aborted" // the container exited again
)

const logicalSettle = 20 * time.Second

// upFrom is the OSDMap epoch from which the OSD is up, or 0 while it is down.
func (e *logicalEnv) upFrom(t *testing.T, ctx context.Context) int {
	t.Helper()
	data, err := e.cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var dump struct {
		OSDs []struct {
			OSD    int `json:"osd"`
			Up     int `json:"up"`
			UpFrom int `json:"up_from"`
		} `json:"osds"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		t.Fatal(err)
	}
	for _, o := range dump.OSDs {
		if o.OSD == e.osd.ID && o.Up == 1 {
			return o.UpFrom
		}
	}
	return 0
}

// restart starts the aborted OSD again in the same container. The OSDMap can
// still show the aborted daemon up, so a boot counts only with a later up_from
// and a process that keeps running for logicalSettle; an exit is the other
// outcome, with the log lines that name it.
func (e *logicalEnv) restart(t *testing.T, parent context.Context) (string, []string) {
	t.Helper()
	before := e.upFrom(t, parent)
	cid := e.osd.GetContainerID()
	// A few seconds of slack for the engine's clock; the earlier abort is at
	// least the observation's settling time older.
	started := time.Now().Add(-3 * time.Second).UTC().Format(time.RFC3339Nano)
	if err := e.osd.Start(parent); err != nil {
		t.Fatal("restart the aborted OSD:", err)
	}
	if e.osd.GetContainerID() != cid {
		t.Fatal("the restart replaced the OSD container")
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	var booted time.Time
	for {
		state, err := e.osd.State(ctx)
		if err != nil || state == nil {
			t.Fatal("OSD state", err)
		}
		if !state.Running {
			return restartAborted, logicalLogTail(ctx, e.docker, cid, started)
		}
		if booted.IsZero() && e.upFrom(t, ctx) > before {
			booted = time.Now()
		}
		if !booted.IsZero() && time.Since(booted) >= logicalSettle {
			return restartBooted, nil
		}
		select {
		case <-ctx.Done():
			t.Fatal("the restarted OSD neither booted nor exited", ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

// blueStoreExpand runs `ceph-bluestore-tool bluefs-bdev-expand` on the
// stopped OSD's store, which lets BlueFS and the allocator take a grown block
// file. The essential image does not ship the tool, so it runs in the official
// Ceph image of the cluster's release with the owned volume mounted, no
// network, and nothing else of the cluster, until the essential role ships it
// (JSYoo5B/ceph-testcontainers-images#24).
func (e *logicalEnv) expand(t *testing.T, ctx context.Context) map[string]any {
	t.Helper()
	version, err := e.cluster.Ceph(ctx, "version")
	if err != nil {
		t.Fatal(err)
	}
	release := logicalReleaseRE.FindSubmatch(version)
	if release == nil {
		t.Fatalf("no release in %q", version)
	}
	image := "quay.io/ceph/ceph:v" + string(release[1])
	inspection, err := e.docker.ContainerInspect(ctx, e.keeper, mobycl.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	volume := ""
	for _, m := range inspection.Container.Mounts {
		if m.Destination == attachedKeeperDir && m.Type == mount.TypeVolume {
			volume = m.Name
		}
	}
	if volume == "" {
		t.Fatal("the keeper holds no owned volume")
	}
	tool, err := testcontainers.Run(ctx, image,
		testcontainers.WithEntrypoint("ceph-bluestore-tool"),
		testcontainers.WithCmd("bluefs-bdev-expand", "--path", attachedOSDDir+"/ceph-"+strconv.Itoa(e.osd.ID)),
		testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
			host.NetworkMode = "none"
			host.Mounts = append(host.Mounts, mount.Mount{Type: mount.TypeVolume, Source: volume, Target: attachedOSDDir})
		}),
		testcontainers.WithWaitStrategy(wait.ForExit().WithExitTimeout(3*time.Minute)))
	if tool != nil {
		t.Cleanup(func() { _ = tool.Terminate(context.Background()) })
	}
	if err != nil {
		t.Fatal("run ceph-bluestore-tool:", err)
	}
	state, err := tool.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := tool.Logs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	data, _ := io.ReadAll(io.LimitReader(reader, 1<<20))
	var kept []string
	for _, line := range strings.Split(string(data), "\n") {
		if strings.Contains(line, "size") || strings.Contains(line, "Expanding") {
			kept = append(kept, strings.TrimSpace(line))
		}
	}
	if state.ExitCode != 0 || !slices.ContainsFunc(kept, func(line string) bool { return strings.Contains(line, "size updated to") }) {
		t.Fatalf("ceph-bluestore-tool exited %d: %s", state.ExitCode, data)
	}
	return map[string]any{"image": image, "exit_code": state.ExitCode, "output": kept}
}

var logicalReleaseRE = regexp.MustCompile(`ceph version (\d+\.\d+\.\d+)`)

// recover restarts the aborted OSD. If it aborts again for lack of BlueFS
// space, the block file is grown on the owned filesystem and expanded, and the
// OSD must then boot. Every acknowledged write must read back.
func (e *logicalEnv) recover(t *testing.T, ctx context.Context) map[string]any {
	t.Helper()
	outcome, log := e.restart(t, ctx)
	record := map[string]any{"restart": outcome, "restart_log": log}
	if outcome == restartAborted {
		// Expand only a store whose RocksDB could not allocate BlueFS space.
		if !slices.ContainsFunc(log, func(line string) bool { return strings.Contains(line, "BlueRocksEnv") }) {
			t.Fatalf("the restarted OSD aborted for another reason: %v", log)
		}
		dir := attachedKeeperDir + "/ceph-" + strconv.Itoa(e.osd.ID)
		out := attachedDockerExec(t, ctx, e.docker, e.keeper, "python3", "-I", "-c", logicalGrow, dir, strconv.FormatInt(logicalBlockSize/2, 10))
		var grow struct{ RC int }
		if err := json.Unmarshal([]byte(harness.LastJSONLine(string(out))), &grow); err != nil || grow.RC != 0 {
			t.Fatalf("grow the block: %v: %s", err, out)
		}
		record["grow"] = json.RawMessage(harness.LastJSONLine(string(out)))
		record["expand"] = e.expand(t, ctx)
		if outcome, log = e.restart(t, ctx); outcome != restartBooted {
			record["expanded_restart_log"] = log
			t.Fatalf("the OSD did not boot after the expansion: %v", log)
		}
		record["expanded_restart"] = outcome
	}
	record["readback"] = e.readback(t, ctx)
	return record
}

// waitChecks waits until the health check code is present or absent.
func (e *logicalEnv) waitChecks(t *testing.T, parent context.Context, code string, present bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	for {
		health, err := e.cluster.Ceph(ctx, "health", "detail", "--format", "json")
		if err == nil {
			if checks, err := parseHealthChecks(health); err == nil && slices.Contains(checks, code) == present {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s present=%t was not reached: %v", code, present, ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

// logicalLogTail keeps the OSD log lines that name a refusal: the allocator
// markers, the full and failsafe states, and the abort. A nonempty since
// limits it to the output after that instant (RFC 3339).
func logicalLogTail(ctx context.Context, docker *mobycl.Client, cid, since string) []string {
	reader, err := docker.ContainerLogs(ctx, cid, mobycl.ContainerLogsOptions{ShowStdout: true, ShowStderr: true, Tail: "4000", Since: since})
	if err != nil {
		return []string{"logs unavailable: " + err.Error()}
	}
	defer func() { _ = reader.Close() }()
	data, _ := io.ReadAll(io.LimitReader(reader, 16<<20))
	words := append(slices.Clone(logicalAllocatorMarkers), "Caught signal", "failsafe", "is full", "ceph_abort_msg", "BlueRocksEnv")
	var kept []string
	for _, line := range strings.Split(string(data), "\n") {
		// Drop the stream multiplexing header and the event index of a
		// crash dump, so the same line from the dump and the live log match.
		if i := strings.Index(line, "20"); i > 0 && i < 24 {
			line = line[i:]
		}
		for _, word := range words {
			if strings.Contains(line, word) {
				if len(line) > 300 {
					line = line[:300]
				}
				if !slices.Contains(kept, line) {
					kept = append(kept, line)
				}
				break
			}
		}
	}
	if len(kept) > 30 {
		kept = kept[len(kept)-30:]
	}
	return kept
}

func logicalLedgerSummary(ledger []attachMutation) map[string]any {
	counts := map[string]int{}
	var misses []string
	for _, m := range ledger {
		counts[m.Outcome]++
		if m.Outcome != mutAcked {
			misses = append(misses, m.Key+":"+m.Outcome)
		}
	}
	return map[string]any{"counts": counts, "misses": misses}
}

func logicalDFSummary(df []byte) any {
	var parsed struct {
		Nodes []struct {
			KB          int64   `json:"kb"`
			KBUsed      int64   `json:"kb_used"`
			KBUsedData  int64   `json:"kb_used_data"`
			KBUsedMeta  int64   `json:"kb_used_meta"`
			Utilization float64 `json:"utilization"`
		} `json:"nodes"`
	}
	if json.Unmarshal(df, &parsed) != nil {
		return nil
	}
	return parsed.Nodes
}

// The BlueStore logical-capacity runs of issue 63. One OSD's sparse 256 MiB
// block file stands on the cluster's owned 1 GiB tmpfs, so the filesystem
// keeps room and the refusal must come from a policy or from BlueStore's own
// allocator. Each phase's receipt goes through judgeLogicalPhase.
//
// paced: with the default policy, writes paced at 1 MiB per second reach the
// OSDMap full ratio, which holds every later write while the OSD stays up.
// Removing objects with FULL_TRY returns below the ratio and writes are
// admitted again. Then the full ratio and the OSD failsafe are lifted to 1 and
// a burst runs into the allocator, which aborts the OSD.
//
// burst: with the default policy, an unpaced burst reaches the allocator
// before the OSD's utilization report lets the full ratio act, and the OSD
// aborts the same way.
//
// After each abort, with the policy back, recover starts the OSD again. It
// aborts at mount because RocksDB cannot allocate BlueFS space; the block
// file is grown and expanded with ceph-bluestore-tool, the OSD boots, and
// every acknowledged write reads back.
func TestBlueStoreLogicalCapacity(t *testing.T) {
	receipt := map[string]any{"ceiling": logicalCeiling, "block_size": logicalBlockSize}
	defer func() {
		data, _ := json.Marshal(receipt)
		t.Logf("BLUESTORE_LOGICAL_CAPACITY_RECEIPT %s", data)
	}()
	judge := func(t *testing.T, name string, phase *logicalPhase, record map[string]any, want string) {
		t.Helper()
		verdict, err := judgeLogicalPhase(phase)
		record["verdict"] = verdict
		receipt[name] = record
		if err != nil {
			record["judge_error"] = err.Error()
			t.Fatalf("%s: %v", name, err)
		}
		if verdict != want {
			t.Fatalf("%s: verdict %s, want %s", name, verdict, want)
		}
		t.Logf("BLUESTORE_LOGICAL_CAPACITY phase=%s verdict=%s fs_avail=%d block_allocated=%d", name, verdict, phase.FSAvail, phase.BlockAllocated)
	}

	t.Run("paced", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
		defer cancel()
		env := newLogicalEnv(t, ctx)
		for _, m := range env.write(t, ctx, "pre", 36, 4<<20, "0") {
			if m.Outcome != mutAcked {
				t.Fatalf("prefill %s: %s", m.Key, m.Outcome)
			}
		}
		paced := env.write(t, ctx, "paced", 150, 1<<20, "1")
		phase, record := env.observe(t, ctx, paced)
		judge(t, "paced_policy", phase, record, logicalPolicyBlocked)

		// Below the ratio again, writes are admitted.
		var removed []string
		for _, m := range env.acked[:16] {
			removed = append(removed, m.Key)
		}
		out := harness.ExecOutput(t, ctx, env.client, append([]string{"python3", "-c", logicalRemove, logicalPool}, removed...)...)
		var outcomes map[string]string
		if err := json.Unmarshal([]byte(harness.LastJSONLine(out)), &outcomes); err != nil || len(outcomes) != len(removed) {
			t.Fatalf("remove: %v: %s", err, out)
		}
		for key, outcome := range outcomes {
			if outcome != "removed" {
				t.Fatalf("remove %s while full: %s", key, outcome)
			}
		}
		env.acked = env.acked[16:]
		env.waitChecks(t, ctx, "OSD_FULL", false)
		after := env.write(t, ctx, "after", 2, 1<<20, "0")
		if logicalLedgerSummary(after)["counts"].(map[string]int)[mutAcked] != 2 {
			t.Fatalf("writes after the removal were not admitted: %+v", after)
		}
		receipt["paced_release"] = map[string]any{"removed": len(removed), "readback": env.readback(t, ctx)}

		// Lift the policies; the allocator is the next limit.
		ratios, err := override.SetFullRatios(ctx, env.cluster, ceph.FullRatios{NearFull: .98, BackfillFull: .99, Full: 1})
		t.Cleanup(func() { _ = ratios.Restore(context.Background()) })
		if err != nil {
			t.Fatal(err)
		}
		failsafe, err := override.SetConfig(ctx, env.cluster, ceph.ConfigSetting{Section: "osd", Name: "osd_failsafe_full_ratio", Value: "1"})
		t.Cleanup(func() { _ = failsafe.Restore(context.Background()) })
		if err != nil {
			t.Fatal(err)
		}
		lifted := env.write(t, ctx, "lifted", 60, 4<<20, "0")
		phase, record = env.observe(t, ctx, lifted)
		judge(t, "lifted_allocator", phase, record, logicalAllocatorAborted)

		if err := failsafe.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		if err := ratios.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		receipt["lifted_recovery"] = env.recover(t, ctx)
	})

	t.Run("burst", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
		defer cancel()
		env := newLogicalEnv(t, ctx)
		burst := env.write(t, ctx, "burst", 120, 4<<20, "0")
		phase, record := env.observe(t, ctx, burst)
		judge(t, "burst_allocator", phase, record, logicalAllocatorAborted)
		receipt["burst_recovery"] = env.recover(t, ctx)
	})
}
