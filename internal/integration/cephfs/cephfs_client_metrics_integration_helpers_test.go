//go:build all || integration

package cephfs_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

// cephFSMetricsActor mounts with client metrics enabled, opens the seeded
// file and publishes receipt 1 with its native client ID. The writer waits at
// the "act" gate, writes the payload at offset 0, fsyncs and publishes receipt
// 2 with the acknowledged size; the idle actor never writes. Both close after
// the "finish" gate and publish receipt 3. Arguments: filesystem, work
// directory, token, path, mode ("writer" or "idle"), the payload size. The
// payload is generated here as byte i%251 at offset i, because a 128 KiB
// payload passed as hex would exceed Linux's per-argument limit.
const cephFSMetricsActor harness.CephFSActorScript = harness.CephFSActorHelpers + `
path, mode = sys.argv[4].encode(), sys.argv[5]
payload = bytes(i % 251 for i in range(int(sys.argv[6])))
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.conf_set("client_collect_and_send_global_metrics", "true")
fs.mount(filesystem_name=name.encode())
fd = fs.open(path, os.O_RDWR, 0)
publish(1, client=fs.get_instance_id())
if mode == "writer":
    gate("act")
    written = fs.write(fd, payload, 0)
    fs.fsync(fd, 0)
    publish(2, client=fs.get_instance_id(), size=written, count=1)
gate("finish")
fs.close(fd)
fs.unmount()
fs.shutdown()
publish(3, closed=True)
`

// cephFSMetricsSeed creates the empty file that both actors open and prints
// the seeding client's ID. Arguments: filesystem, path.
const cephFSMetricsSeed = `
import cephfs, json, os, sys
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=sys.argv[1].encode())
fd = fs.open(sys.argv[2].encode(), os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
fs.fsync(fd, 0)
print(json.dumps(dict(client=fs.get_instance_id(), size=fs.fstat(fd).st_size)))
fs.close(fd)
fs.unmount()
fs.shutdown()
`

// cephFSMetricsReader reads the file through a client of its own. Arguments:
// filesystem, path, the largest size to read.
const cephFSMetricsReader = `
import cephfs, hashlib, json, os, sys
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=sys.argv[1].encode())
fd = fs.open(sys.argv[2].encode(), os.O_RDONLY, 0)
data = fs.read(fd, 0, int(sys.argv[3]) + 1)
print(json.dumps(dict(client=fs.get_instance_id(), size=len(data), sha256=hashlib.sha256(data).hexdigest())))
fs.close(fd)
fs.unmount()
fs.shutdown()
`

type cephFSMetricsFile struct {
	Client uint64 `json:"client"`
	Size   int    `json:"size"`
	SHA256 string `json:"sha256"`
}

// cephFSMetricsExecutor is the actual image config ID of a container that
// ran an actor, recorded per run rather than borrowed from another recipe.
func cephFSMetricsExecutor(t *testing.T, ctx context.Context, c testcontainers.Container) string {
	t.Helper()
	inspect, err := c.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return inspect.Image
}

// testCephFSClientMetrics runs the client-metrics recipe on one active MDS:
//
//  1. A seed client creates an empty file; writer A and idle B, two further
//     clients with client metrics enabled, open it.
//  2. Counter-dump samples of the active MDS run until both clients' rank
//     rows appear; every such baseline sample must show zero writes.
//  3. The work gate opens and only A writes and fsyncs the payload.
//  4. Samples after A's acknowledgement run until A's row covers the
//     acknowledged bytes or the deadline passes; B's row must stay at zero.
//  5. The MDS generation is checked again and a third client reads the bytes.
//
// judgeClientMetricsWindow decides from the timed samples; the receipt is
// logged on every outcome.
func testCephFSClientMetrics(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	m := harness.StartCephFSSingleMDS(t, ctx)
	helper := harness.ToolkitContainer(t, m.Cluster)
	const path = "/client-metrics"
	payload := make([]byte, 131072)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	wantSum := sha256.Sum256(payload)
	executors := map[string]string{"client": cephFSMetricsExecutor(t, ctx, m.Client), "toolkit": cephFSMetricsExecutor(t, ctx, helper)}

	var seed cephFSMetricsFile
	if err := json.Unmarshal([]byte(harness.LastJSONLine(harness.ExecOutput(t, ctx, helper, "python3", "-c", cephFSMetricsSeed, m.FS.FilesystemName, path))), &seed); err != nil || seed.Client == 0 || seed.Size != 0 {
		t.Fatalf("seed: %+v, %v", seed, err)
	}
	writer := harness.StartCephFSActor(t, ctx, m.Cluster, m.Client, cephFSMetricsActor, m.FS.FilesystemName, "/tmp/client-metrics-writer", path, "writer", strconv.Itoa(len(payload)))
	idle := harness.StartCephFSActor(t, ctx, m.Cluster, helper, cephFSMetricsActor, m.FS.FilesystemName, "/tmp/client-metrics-idle", path, "idle", "0")
	a, b := writer.Receipt(t, ctx, 1), idle.Receipt(t, ctx, 1)
	if a.Client == 0 || b.Client == 0 || a.Client == b.Client || a.Client == seed.Client || b.Client == seed.Client {
		t.Fatalf("client identities: seed %d, writer %+v, idle %+v", seed.Client, a, b)
	}

	window := clientMetricsWindow{Writer: a.Client, Idle: b.Client, MDSGIDStart: m.MDS.GID}
	var outcome clientMetricsOutcome
	var read cephFSMetricsFile
	defer func() {
		// The receipt is emitted before the cluster is torn down, whatever the
		// outcome, so a failed run keeps the ledger.
		data, err := json.Marshal(map[string]any{"filesystem": m.FS.FilesystemName, "mds": m.MDS, "native_version": m.Version,
			"executors": executors, "writer": a, "idle": b, "acked_bytes": window.AckedBytes,
			"gate_opened": window.GateOpened, "acked": window.Acked, "deadline": window.Deadline,
			"mds_gid_start": window.MDSGIDStart, "mds_gid_end": window.MDSGIDEnd, "samples": len(window.Samples),
			"verdict": outcome.Verdict, "reason": outcome.Reason, "writer_totals": outcome.Writer, "idle_totals": outcome.Idle,
			"independent_read": read,
			"scope":            "cumulative total_write_ops/total_write_size of one writer and one idle client on rank 0; not message counts, other metrics, reporting latency or MDS/rank changes"})
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("CEPHFS_CLIENT_METRICS_RECEIPT %s", data)
	}()
	sample := func() clientMetricsSample {
		t.Helper()
		s := clientMetricsSample{Start: time.Now()}
		dump, err := m.Cluster.Ceph(ctx, "tell", "mds."+m.MDS.Name, "counter", "dump")
		s.End = time.Now()
		if err != nil {
			t.Fatalf("counter dump: %v", err)
		}
		if s.Section, err = parseClientMetricsSection(dump, m.FS.FilesystemName, uint64(m.MDS.Rank), a.Client, b.Client); err != nil {
			t.Fatalf("counter dump: %v", err)
		}
		window.Samples = append(window.Samples, s)
		return s
	}
	both := func(s clientMetricsSample) bool {
		_, wok := s.Section.Rows[a.Client]
		_, iok := s.Section.Rows[b.Client]
		return wok && iok
	}

	// The rows appear once each client's first report arrives.
	baselineEnd := time.Now().Add(90 * time.Second)
	for s := sample(); !both(s); s = sample() {
		if time.Now().After(baselineEnd) {
			t.Fatal("the rank rows of both clients did not appear before the gate")
		}
		time.Sleep(time.Second)
	}
	window.GateOpened = time.Now()
	writer.Open(t, ctx, "act")
	written := writer.Receipt(t, ctx, 2)
	window.Acked = time.Now()
	if written.Client != a.Client || written.PID != a.PID || written.Size != int64(len(payload)) {
		t.Fatalf("writer changed or wrote short: %+v", written)
	}
	window.AckedBytes = uint64(written.Size)
	window.Deadline = window.Acked.Add(90 * time.Second)
	for time.Now().Before(window.Deadline) {
		s := sample()
		if wt, ok := s.Section.Rows[a.Client]; ok && wt.Ops >= 1 && wt.Bytes >= window.AckedBytes && both(s) {
			break
		}
		time.Sleep(time.Second)
	}
	status, err := m.FS.MDSStatus(ctx)
	if err != nil || len(status.Active) != 1 {
		t.Fatalf("MDS status after the observation: %+v, %v", status, err)
	}
	window.MDSGIDEnd = status.Active[0].GID
	outcome = judgeClientMetricsWindow(window)

	out := harness.ExecOutput(t, ctx, helper, "python3", "-c", cephFSMetricsReader, m.FS.FilesystemName, path, strconv.Itoa(len(payload)))
	if err := json.Unmarshal([]byte(harness.LastJSONLine(out)), &read); err != nil || read.Client == 0 || read.Client == a.Client || read.Client == b.Client ||
		read.Size != len(payload) || read.SHA256 != hex.EncodeToString(wantSum[:]) {
		t.Fatalf("independent readback: %s, %v", out, err)
	}
	for _, run := range []*harness.CephFSActorRun{writer, idle} {
		run.Open(t, ctx, "finish")
		if closed := run.Receipt(t, ctx, 3); !closed.Closed {
			t.Fatalf("actor did not close: %+v", closed)
		}
		run.Wait(t, ctx)
	}
	switch outcome.Verdict {
	case clientMetricsAttributed:
	case clientMetricsContradicted:
		t.Fatalf("client metrics contradicted: %s", outcome.Reason)
	default:
		t.Fatalf("client metrics inconclusive: %s", outcome.Reason)
	}
}
