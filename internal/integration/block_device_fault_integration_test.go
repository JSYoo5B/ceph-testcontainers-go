//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_scratch_tmpfs))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const (
	blockDeviceBytes = int64(512 << 20)
	blockKeeperDir   = "/tc-blk"
	blockPool        = "block-device-fault"
	blockDelayMS     = 200
	blockObjectSize  = int64(64 << 10)
)

// blockEnv is one cluster whose only OSD runs on the keeper's mapping, and an
// unused control mapping of the same kind.
type blockEnv struct {
	*logicalEnv
	keeper      testcontainers.Container
	chain, idle blockChainState
	device      faulthook.BlockDevice
	sectors     int64
}

func (b *blockEnv) keeperDo(t *testing.T, ctx context.Context, action string, args ...string) blockKeeperReply {
	t.Helper()
	raw := scratchContainerExec(t, ctx, b.keeper, append([]string{"python3", "-I", "-c", blockDeviceKeeperScript, action}, args...)...)
	reply, err := parseBlockKeeperReply(raw, action)
	if err != nil {
		t.Fatalf("%v: %s", err, raw)
	}
	return reply
}

func (b *blockEnv) stat(t *testing.T, ctx context.Context, chain blockChainState) blockKeeperReply {
	t.Helper()
	return b.keeperDo(t, ctx, "stat", chain.Name)
}

// load swaps the mapping's table. A table the kernel refuses (a target the
// kernel does not have) leaves the old table and is reported, not fatal.
func (b *blockEnv) load(t *testing.T, ctx context.Context, kind string, ms int) blockKeeperReply {
	t.Helper()
	r := b.keeperDo(t, ctx, "table", b.chain.Name, b.chain.Loop, strconv.FormatInt(b.sectors, 10), kind, strconv.Itoa(ms))
	if r.Loaded && r.Error != "" {
		t.Fatalf("%s table swap left the mapping uncertain: %+v", kind, r)
	}
	return r
}

// timed runs one timed write phase and keeps its acknowledged writes for the
// later readbacks.
func (b *blockEnv) timed(t *testing.T, ctx context.Context, prefix string, count int, timeout string) blockTimedLedger {
	t.Helper()
	out := harness.ExecOutput(t, ctx, b.client, "python3", "-c", blockTimedWrites, b.pool, prefix, strconv.Itoa(count), strconv.FormatInt(blockObjectSize, 10), timeout)
	l, err := parseBlockTimedLedger(out)
	if err != nil {
		t.Fatalf("%s writes: %v: %s", prefix, err, out)
	}
	for _, m := range l.Ledger {
		if m.Outcome == mutAcked {
			b.acked = append(b.acked, m)
		}
	}
	return l
}

// createChain makes one backing file, loop and linear mapping in the keeper.
// The returned state names whatever was created, also on failure, so cleanup
// releases exactly that.
func (b *blockEnv) createChain(t *testing.T, ctx context.Context, name string) (blockChainState, blockKeeperReply) {
	t.Helper()
	raw, err := blockKeeperExec(ctx, b.keeper, "create", name, blockKeeperDir+"/"+name, strconv.FormatInt(blockDeviceBytes, 10))
	r, perr := parseBlockKeeperReply(raw, "create")
	if err != nil || perr != nil {
		// Nothing tells what was made: keep the name so that the receipt
		// names the chain that may be left.
		t.Errorf("create chain %s: %v %v: %s", name, err, perr, raw)
	}
	return blockChainState{Name: name, File: r.File, Loop: r.Loop, Mapped: r.Mapped}, r
}

// releaseChain removes one chain the test created. The OSD's chain is released
// only when the cluster's claim table and the keeper's evidence both raise no
// objection; otherwise the whole chain is kept and reported.
func (b *blockEnv) releaseChain(t *testing.T, ctx context.Context, chain blockChainState, cluster *ceph.Container, osdID string) map[string]any {
	out := map[string]any{"name": chain.Name}
	if chain.Mapped && cluster != nil && chain == b.chain {
		outcome, detail := faulthook.JudgeDeviceChain(ctx, cluster, b.device, chain.Name, func(ctx context.Context) (faulthook.ChainEvidence, error) {
			return b.chainEvidence(ctx, chain, osdID)
		})
		out["verdict"], out["detail"] = outcome, detail
		if outcome != faulthook.ChainClear {
			t.Errorf("the OSD's device chain %s is retained: %s: %s", chain.Name, outcome, detail)
			return out
		}
	}
	if chain.Loop == "" {
		return out
	}
	raw, err := blockKeeperExec(ctx, b.keeper, "remove", chain.Name, chain.Loop, chain.File)
	reply, perr := parseBlockKeeperReply(raw, "remove")
	if err != nil || perr != nil || !reply.MappingGone || !reply.LoopReleased || !reply.FileRemoved {
		t.Errorf("release of chain %s: %v %v %s", chain.Name, err, perr, raw)
	}
	out["removed"] = reply
	return out
}

// chainEvidence is the keeper's side of the release gate: the OSD container
// is gone or stopped, the observer borrowed no descriptor (it only stats the
// descriptor links), and the kernel's open count of the mapping is the scan.
func (b *blockEnv) chainEvidence(ctx context.Context, chain blockChainState, osdID string) (faulthook.ChainEvidence, error) {
	ev := faulthook.ChainEvidence{Lease: chain.Name, BorrowsClosed: faulthook.FactYes}
	if osdID == "" {
		ev.DaemonStopped = faulthook.FactYes
	} else if inspect, err := b.docker.ContainerInspect(ctx, osdID, mobycl.ContainerInspectOptions{}); err != nil {
		if !cerrdefs.IsNotFound(err) {
			return ev, err
		}
		ev.DaemonStopped = faulthook.FactYes
	} else if inspect.Container.State != nil && !inspect.Container.State.Running {
		ev.DaemonStopped = faulthook.FactYes
	} else {
		ev.DaemonStopped = faulthook.FactNo
	}
	raw, err := blockKeeperExec(ctx, b.keeper, "stat", chain.Name)
	if err != nil {
		return ev, err
	}
	r, err := parseBlockKeeperReply(raw, "stat")
	if err != nil {
		return ev, err
	}
	ev.Device = b.device
	if r.Open != nil {
		ev.ScanComplete, ev.Openers = faulthook.FactYes, *r.Open
	}
	return ev, nil
}

// blockKeeperExec is keeperDo for cleanup, where a failure is reported rather
// than fatal.
func blockKeeperExec(ctx context.Context, keeper testcontainers.Container, action string, args ...string) ([]byte, error) {
	code, reader, err := keeper.Exec(ctx, append([]string{"python3", "-I", "-c", blockDeviceKeeperScript, action}, args...), tcexec.Multiplexed())
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(reader)
	if err == nil && code != 0 {
		err = fmt.Errorf("keeper %s exited %d", action, code)
	}
	return data, err
}

func randomName(prefix string) string {
	raw := make([]byte, 4)
	_, _ = rand.Read(raw)
	return prefix + hex.EncodeToString(raw)
}

// TestBlockDeviceFault runs one BlueStore OSD on a device-mapper node that a
// test-owned keeper made from a backing file on its own tmpfs and a loop
// device. It first shows that the OSD daemon opened that node read-write and
// that the workload's writes went through it, with an unused mapping of the
// same kind as the control. It then swaps the mapping's table: a delay table,
// when the kernel has the delay target, slows every write by the configured
// delay and the restored table makes them fast again; an error table fails
// every sector, and the OSD either aborts with a logged I/O error or returns
// EIO to the client. After the linear table is back, the OSD starts again if
// it aborted, every acknowledged write reads back, and new writes are
// acknowledged. The fault spans the whole main device, which also carries
// BlueFS, so it is a shared BlueStore/BlueFS fault, never a data-only one.
func TestBlockDeviceFault(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	image, opts := harness.IntegrationImages(t)
	receipt := map[string]any{"bytes": blockDeviceBytes}
	defer func() {
		data, _ := json.Marshal(receipt)
		t.Logf("BLOCK_DEVICE_FAULT_RECEIPT %s", data)
	}()

	// The keeper sees the engine host's /dev, so the mapping it creates is a
	// /dev/mapper node there; its backing file is on the keeper's own tmpfs.
	keeper, err := testcontainers.Run(ctx, image, testcontainers.WithEntrypoint("sleep", "infinity"),
		testcontainers.WithHostConfigModifier(func(h *container.HostConfig) {
			h.Privileged = true
			h.Binds = append(h.Binds, "/dev:/dev")
			h.Tmpfs = map[string]string{blockKeeperDir: "size=" + strconv.FormatInt(2*blockDeviceBytes+(64<<20), 10)}
		}))
	if keeper != nil {
		t.Cleanup(func() {
			if err := keeper.Terminate(context.Background()); err != nil {
				t.Error("block device keeper cleanup", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	b := &blockEnv{logicalEnv: &logicalEnv{docker: docker.Client, pool: blockPool}, keeper: keeper, sectors: blockDeviceBytes / 512}
	var cluster *ceph.Container
	var osdID string
	// Registered before the cluster's Terminate, so it runs after it.
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
		defer done()
		// The test's receipt is logged before cleanup runs; the release has
		// its own.
		data, _ := json.Marshal([]any{b.releaseChain(t, cleanup, b.chain, cluster, osdID), b.releaseChain(t, cleanup, b.idle, cluster, osdID)})
		t.Logf("BLOCK_DEVICE_RELEASE_RECEIPT %s", data)
	})
	var created, idle blockKeeperReply
	b.chain, created = b.createChain(t, ctx, randomName("tcblk-"))
	b.idle, idle = b.createChain(t, ctx, b.chain.Name+"-idle")
	receipt["chain"], receipt["control"] = created, idle
	if t.Failed() || created.Error != "" || idle.Error != "" || created.Bytes == nil || *created.Bytes != blockDeviceBytes {
		t.Fatalf("device chain: %+v control: %+v", created, idle)
	}
	b.device = faulthook.BlockDevice{HostPath: created.Path, Major: created.Major, Minor: created.Minor, SizeBytes: *created.Bytes}
	number := strconv.FormatUint(uint64(created.Major), 10) + ":" + strconv.FormatUint(uint64(created.Minor), 10)

	opts = append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs().WithPoolDefaults(1, 1)))
	cluster, err = ceph.Run(ctx, image, opts...)
	if cluster != nil {
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
			defer done()
			if err := cluster.Terminate(cleanup); err != nil {
				t.Error("block device cluster cleanup", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	b.cluster = cluster
	if _, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true"}); err != nil {
		t.Fatal(err)
	}
	osd, err := faulthook.AddBlockDeviceOSD(ctx, cluster, b.device, b.chain.Name)
	if osd != nil {
		osdID = osd.GetContainerID()
	}
	if err != nil {
		t.Fatal("device-backed OSD:", err)
	}
	b.osd = osd
	b.client = harness.ToolkitContainer(t, cluster)
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: blockPool, Application: "rados", Replicas: 1, MinSize: 1, PGNum: 8}); err != nil {
		t.Fatal(err)
	}

	// Attachment: the daemon holds a read-write descriptor of the node, and
	// its store is the OSD's own.
	var view blockAttachView
	raw := scratchContainerExec(t, ctx, osd, "python3", "-I", "-c", blockDeviceObserverScript, "/var/lib/ceph/osd/ceph-"+strconv.Itoa(osd.ID))
	if err := json.Unmarshal(raw, &view); err != nil {
		t.Fatalf("observer: %v: %s", err, raw)
	}
	verdict, detail := judgeBlockAttachment(view, number, b.osdUUID(t, ctx))
	receipt["attachment"] = map[string]any{"verdict": verdict, "detail": detail, "view": view}
	if verdict != blockAttached {
		t.Fatalf("attachment: %s: %s", verdict, detail)
	}

	// Exposure: the baseline writes go through the mapping, and the control
	// mapping sees nothing.
	before, idleBefore := b.stat(t, ctx, b.chain), b.stat(t, ctx, b.idle)
	base := b.timed(t, ctx, "base", 16, "30")
	after, idleAfter := b.stat(t, ctx, b.chain), b.stat(t, ctx, b.idle)
	exposure := judgeBlockExposure(before, after, idleBefore, idleAfter)
	receipt["exposure"] = map[string]any{"verdict": exposure, "before": before, "after": after, "control_before": idleBefore, "control_after": idleAfter,
		"base": logicalLedgerSummary(base.Ledger), "base_median_s": median(base.Seconds)}
	if exposure != blockExposed || len(b.acked) != 16 {
		t.Fatalf("exposure: %s, %d of 16 baseline writes acknowledged", exposure, len(b.acked))
	}
	b.readback(t, ctx)

	// Delay window, when the kernel has the delay target. Loading a table
	// whose target is not registered makes the kernel load its module on
	// demand, a change to the host; as in the block-fault probe, that is
	// allowed only on a GitHub-hosted runner, whose machine is discarded
	// after the job. The measurement ends before the restore swaps the table:
	// suspending a delay mapping flushes its queue early.
	targets := b.keeperDo(t, ctx, "targets", "-")
	autoload := os.Getenv("RUNNER_ENVIRONMENT") == "github-hosted"
	delayInfo := map[string]any{"targets_before": targets.Targets, "autoload_allowed": autoload}
	receipt["delay"] = delayInfo
	if !slices.Contains(targets.Targets, "delay") && !autoload {
		delayInfo["eligible"], delayInfo["reason"] = false, "target-not-registered"
		t.Logf("BLOCK_DEVICE_DELAY not eligible: the delay target is not registered and this is not a disposable runner")
	} else if loaded := b.load(t, ctx, "delay", blockDelayMS); !loaded.Loaded {
		delayInfo["eligible"], delayInfo["reason"], delayInfo["refused"] = false, "table-refused", loaded.Error
		t.Logf("BLOCK_DEVICE_DELAY not eligible: the kernel refused the delay table: %s", loaded.Error)
	} else {
		delayed := b.timed(t, ctx, "delayed", 8, "60")
		restoredTable := b.load(t, ctx, "linear", 0)
		restored := b.timed(t, ctx, "restored", 8, "30")
		v := judgeBlockDelay(base, delayed, restored, blockDelayMS)
		for k, value := range map[string]any{"eligible": true, "verdict": v, "table": loaded.Table, "restored_table": restoredTable.Table,
			"delayed_median_s": median(delayed.Seconds), "restored_median_s": median(restored.Seconds), "delayed": logicalLedgerSummary(delayed.Ledger)} {
			delayInfo[k] = value
		}
		if v != blockDelayed {
			t.Fatalf("delay window: %s: base %.3fs delayed %.3fs restored %.3fs", v, median(base.Seconds), median(delayed.Seconds), median(restored.Seconds))
		}
		b.readback(t, ctx)
	}

	// Error window: every sector of the main device fails.
	since := time.Now().Add(-3 * time.Second).UTC().Format(time.RFC3339Nano)
	errTable := b.load(t, ctx, "error", 0)
	if !errTable.Loaded {
		t.Fatalf("error table refused: %+v", errTable)
	}
	failed := b.timed(t, ctx, "fault", 3, "15")
	time.Sleep(10 * time.Second)
	state, err := osd.State(ctx)
	if err != nil || state == nil {
		t.Fatal("OSD state", err)
	}
	phase := blockErrorPhase{Writes: failed, Running: state.Running, Log: logicalLogTail(ctx, docker.Client, osd.GetContainerID(), since, blockIOErrorMarkers...)}
	errVerdict := judgeBlockError(phase)
	receipt["error"] = map[string]any{"verdict": errVerdict, "table": errTable.Table, "running": state.Running, "exit_code": state.ExitCode,
		"writes": logicalLedgerSummary(failed.Ledger), "errnos": failed.Errnos, "log": phase.Log}
	if errVerdict != blockOSDAborted && errVerdict != blockClientEIO {
		t.Fatalf("error window: %s: running=%t errnos=%v log=%v", errVerdict, state.Running, failed.Errnos, phase.Log)
	}

	// Recovery: the linear table is back; an aborted OSD starts again in the
	// same container on the same store.
	if r := b.load(t, ctx, "linear", 0); !r.Loaded {
		t.Fatalf("restore table refused: %+v", r)
	}
	recovery := map[string]any{}
	if !state.Running {
		outcome, log := b.restart(t, ctx)
		recovery["restart"], recovery["log"] = outcome, log
		if outcome != restartBooted {
			receipt["recovery"] = recovery
			t.Fatalf("the OSD did not start again on the restored device: %s %v", outcome, log)
		}
	} else {
		attachedWaitUp(t, ctx, cluster, osd)
	}
	recovery["readback"] = b.readback(t, ctx)
	post := b.timed(t, ctx, "post", 4, "30")
	recovery["post"] = logicalLedgerSummary(post.Ledger)
	receipt["recovery"] = recovery
	for _, m := range post.Ledger {
		if m.Outcome != mutAcked {
			t.Fatalf("a write after recovery was not acknowledged: %+v", post.Ledger)
		}
	}
	recovery["readback_post"] = b.readback(t, ctx)
}

// osdUUID is the OSD's UUID from the OSDMap.
func (b *blockEnv) osdUUID(t *testing.T, ctx context.Context) string {
	t.Helper()
	data, err := b.cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var dump struct {
		OSDs []struct {
			OSD  int    `json:"osd"`
			UUID string `json:"uuid"`
		} `json:"osds"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		t.Fatal(err)
	}
	for _, o := range dump.OSDs {
		if o.OSD == b.osd.ID {
			return o.UUID
		}
	}
	t.Fatalf("osd.%d is not in the OSDMap", b.osd.ID)
	return ""
}
