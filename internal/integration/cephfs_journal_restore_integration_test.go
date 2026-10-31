//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_cephfs_journal_restore))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs

package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const (
	journalFS      = "tc-journal-fs"
	journalBackup  = "/tmp/tc-journal-backup.bin"
	journalHeader  = "200.00000000"
	journalFileCnt = 12
)

// journalFiles writes (seed) or reads back the known files through libcephfs
// and prints {"name": sha256} for every file it could read. Arguments: mode,
// filesystem, prefix, count.
const journalFiles = `import cephfs, hashlib, json, sys
mode, name, prefix, count = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.conf_set("client_fs", name)
fs.mount()
out = {}
try:
    for n in range(count):
        path = "/%s-%d" % (prefix, n)
        data = hashlib.sha256(path.encode()).digest() * 512
        if mode == "seed":
            fd = fs.open(path, "w", 0o600)
            fs.write(fd, data, 0)
            fs.fsync(fd, False)
            fs.close(fd)
        try:
            fd = fs.open(path, "r")
            got = fs.read(fd, 0, len(data) + 1)
            fs.close(fd)
            out[path] = hashlib.sha256(got).hexdigest()
        except cephfs.Error:
            out[path] = ""
finally:
    fs.unmount()
    fs.shutdown()
print(json.dumps(out))`

// journalHeaderObject reads (read) or overwrites (damage) the rank-0 journal
// header object in the metadata pool and prints its size and digest.
// Arguments: mode, pool.
const journalHeaderObject = `import hashlib, json, rados, sys
mode, pool = sys.argv[1], sys.argv[2]
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"rados_osd_op_timeout": "30"}) as c:
    with c.open_ioctx(pool) as io:
        if mode == "damage":
            io.write_full("200.00000000", b"tc-journal-damage" * 4)
        data = io.read("200.00000000", 1 << 20)
print(json.dumps({"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}))`

// journalExport runs the export in the journal-tool helper and prints the
// observation jeObserve reads. Argument: rank, path.
const journalExport = `import hashlib, json, os, subprocess, sys
rank, path = sys.argv[1], sys.argv[2]
r = subprocess.run(["cephfs-journal-tool", "--rank=" + rank, "journal", "export", path], capture_output=True, text=True, timeout=120)
out = {"exit_code": r.returncode, "stdout": r.stdout[-2000:], "stderr": r.stderr[-2000:]}
if os.path.exists(path):
    with open(path, "rb") as f:
        data = f.read()
    out["export_size"], out["export_sha256"] = len(data), hashlib.sha256(data).hexdigest()
print(json.dumps(out))`

// journalExec runs argv in ctr and returns the exit code and combined output.
func journalExec(t *testing.T, ctx context.Context, ctr testcontainers.Container, argv ...string) (int, string) {
	t.Helper()
	code, reader, err := ctr.Exec(ctx, argv, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(reader)
	return code, string(data)
}

// journalRankState reads rank 0 of the filesystem from the MDS map: whether it
// is damaged, failed, or held by an active daemon.
func journalRankState(t *testing.T, ctx context.Context, cluster *ceph.Container) (damaged, failed bool, up int) {
	t.Helper()
	raw, err := cluster.Ceph(ctx, "fs", "get", journalFS, "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var fs struct {
		MDSMap struct {
			Damaged []int          `json:"damaged"`
			Failed  []int          `json:"failed"`
			Up      map[string]int `json:"up"`
		} `json:"mdsmap"`
	}
	if err := json.Unmarshal(raw, &fs); err != nil {
		t.Fatalf("fs get: %v: %s", err, raw)
	}
	return slices.Contains(fs.MDSMap.Damaged, 0), slices.Contains(fs.MDSMap.Failed, 0), len(fs.MDSMap.Up)
}

func journalWait(t *testing.T, ctx context.Context, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("%s was not reached", what)
		}
		time.Sleep(3 * time.Second)
	}
}

// TestCephFSJournalHeaderRestore is the first offline metadata recovery
// scenario of issue 67: the rank-0 journal header of a one-MDS filesystem is
// destroyed and restored from the filesystem's own untouched journal export.
//
//  1. Known files are written through libcephfs.
//  2. The MDS flushes its journal, the filesystem is failed, so no MDS holds
//     rank 0, and the journal is exported with cephfs-journal-tool from the
//     cephfs role image; jeObserve and jbAdmit must admit the export (one
//     stripe, equal ends, hashes).
//  3. The header object 200.00000000 is overwritten. journal inspect must no
//     longer report the journal intact, and rank 0, made joinable again, must
//     be marked damaged by the MDS that tried to replay it.
//  4. With the filesystem failed again, the backup is imported; journal
//     inspect must report it intact, the journal payload read from RADOS must
//     equal the backup's, and no object after the restored end may remain.
//  5. Rank 0 is marked repaired and made joinable; it must become active, every
//     known file must read back with its digest, and a new file is written.
//
// This recovers from a known backup of the same journal. It does not
// reconstruct lost events, salvage arbitrary metadata or establish general
// CephFS disaster recovery.
func TestCephFSJournalHeaderRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	opts = append(opts, cephfs.WithFilesystems(cephfs.Config{Name: journalFS, ActiveMDS: 1}))
	cluster, client := harness.NewServiceClusterRun(t, harness.CEPHFSRun, image, opts...)
	filesystems := cephfs.Filesystems(cluster)
	if len(filesystems) != 1 {
		t.Fatalf("%d filesystems", len(filesystems))
	}
	fs := filesystems[0]
	facts, err := ceph.FactsOf(cluster)
	if err != nil {
		t.Fatal(err)
	}
	tool, err := testcontainers.Run(ctx, facts.Images.CephFS, cluster.WithClient(), ceph.WithIdleEntrypoint())
	if tool != nil {
		t.Cleanup(func() { _ = tool.Terminate(context.Background()) })
	}
	if err != nil {
		t.Fatal(err)
	}
	receipt := map[string]any{"fs": journalFS, "metadata_pool": fs.MetadataPool}
	defer func() { t.Logf("CEPHFS_JOURNAL_RESTORE_RECEIPT %s", mustMarshal(receipt)) }()
	files := func(mode string) map[string]string {
		t.Helper()
		out := harness.ExecOutput(t, ctx, client, "python3", "-c", journalFiles, mode, journalFS, "known", "12")
		var got map[string]string
		if err := json.Unmarshal([]byte(harness.LastJSONLine(out)), &got); err != nil || len(got) != journalFileCnt {
			t.Fatalf("%s files: %v: %s", mode, err, out)
		}
		return got
	}
	header := func(mode string) map[string]any {
		t.Helper()
		out := harness.ExecOutput(t, ctx, client, "python3", "-c", journalHeaderObject, mode, fs.MetadataPool)
		var got map[string]any
		if err := json.Unmarshal([]byte(harness.LastJSONLine(out)), &got); err != nil {
			t.Fatalf("%s header: %v: %s", mode, err, out)
		}
		return got
	}
	inspect := func() (int, string) {
		t.Helper()
		return journalExec(t, ctx, tool, "cephfs-journal-tool", "--rank="+journalFS+":0", "journal", "inspect")
	}
	offline := func(step string) {
		t.Helper()
		if _, err := cluster.Ceph(ctx, "fs", "fail", journalFS); err != nil {
			t.Fatal(step, err)
		}
		journalWait(t, ctx, step+": rank 0 without an active MDS", func() bool {
			_, _, up := journalRankState(t, ctx, cluster)
			return up == 0
		})
	}

	// 1. Known files.
	seeded := files("seed")
	receipt["seeded"] = len(seeded)

	// 2. Offline export of the untouched journal. Without a flush the saved
	// header lags the events the MDS already wrote (persisted write_pos at
	// the start, payload beyond it), which the first experiment refuses as a
	// divergent end; flushing makes the MDS write the header first.
	mds := fs.MDSs()
	if len(mds) != 1 {
		t.Fatalf("%d MDS daemons", len(mds))
	}
	if _, err := cluster.Ceph(ctx, "tell", "mds."+mds[0].ID, "flush", "journal"); err != nil {
		t.Fatal("flush journal:", err)
	}
	offline("export")
	original := header("read")
	receipt["header_original"] = original
	obs := harness.ExecOutput(t, ctx, tool, "python3", "-I", "-c", journalExport, journalFS+":0", journalBackup)
	reader, err := tool.CopyFileFromContainer(ctx, journalBackup)
	if err != nil {
		t.Fatalf("copy the export: %v: %s", err, obs)
	}
	backup, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	outcome, detail, rec := jeObserve([]byte(harness.LastJSONLine(obs)), backup)
	receipt["export"] = map[string]any{"outcome": outcome, "detail": detail, "class": rec.Class, "start": rec.Start, "length": rec.Length,
		"persisted_write_pos": rec.PersistedWritePos, "payload_end": rec.PayloadEnd, "stripe_unit": rec.StripeUnit, "stripe_count": rec.StripeCount,
		"object_size": rec.ObjectSize, "file_size": rec.FileSize}
	if outcome != jeObsAdmission || rec.Class != jbAdmitted {
		t.Fatalf("the export was not admitted: %s %s %s", outcome, detail, rec.Class)
	}
	code, out := inspect()
	receipt["inspect_before"] = map[string]any{"code": code, "out": strings.TrimSpace(out)}
	if code != 0 || !strings.Contains(out, "Overall journal integrity: OK") {
		t.Fatalf("the untouched journal is not intact: %d %s", code, out)
	}

	// 3. Damage the header and show that the MDS cannot replay it.
	damaged := header("damage")
	receipt["header_damaged"] = damaged
	code, out = inspect()
	receipt["inspect_damaged"] = map[string]any{"code": code, "out": strings.TrimSpace(out)}
	if code == 0 && strings.Contains(out, "Overall journal integrity: OK") {
		t.Fatalf("the damaged journal still inspects intact: %s", out)
	}
	if _, err := cluster.Ceph(ctx, "fs", "set", journalFS, "joinable", "true"); err != nil {
		t.Fatal(err)
	}
	journalWait(t, ctx, "rank 0 damaged by the replay", func() bool {
		d, _, _ := journalRankState(t, ctx, cluster)
		return d
	})
	receipt["rank_damaged"] = true
	offline("import")

	// 4. Import the backup.
	code, out = journalExec(t, ctx, tool, "cephfs-journal-tool", "--rank="+journalFS+":0", "journal", "import", journalBackup)
	receipt["import"] = map[string]any{"code": code, "out": strings.TrimSpace(out)}
	if code != 0 {
		t.Fatalf("journal import: %d %s", code, out)
	}
	// The payload and the purge, read from RADOS, independently of the tool's
	// exit status and of inspect.
	pout := harness.ExecOutput(t, ctx, client, "python3", "-c", journalPayload, fs.MetadataPool,
		strconv.FormatUint(rec.Start, 10), strconv.FormatUint(rec.PayloadEnd, 10), strconv.FormatUint(rec.ObjectSize, 10))
	var payload journalPayloadView
	if err := json.Unmarshal([]byte(harness.LastJSONLine(pout)), &payload); err != nil {
		t.Fatalf("payload: %v: %s", err, pout)
	}
	receipt["payload_restored"] = payload
	if err := checkJournalPayload(payload, rec); err != nil {
		t.Fatal(err)
	}
	restored := header("read")
	receipt["header_restored"] = restored
	receipt["header_restored_equal"] = restored["sha256"] == original["sha256"]
	code, out = inspect()
	receipt["inspect_restored"] = map[string]any{"code": code, "out": strings.TrimSpace(out)}
	if code != 0 || !strings.Contains(out, "Overall journal integrity: OK") {
		t.Fatalf("the restored journal is not intact: %d %s", code, out)
	}

	// 5. Bring rank 0 back and read the files.
	if _, err := cluster.Ceph(ctx, "mds", "repaired", journalFS+":0"); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.Ceph(ctx, "fs", "set", journalFS, "joinable", "true"); err != nil {
		t.Fatal(err)
	}
	journalWait(t, ctx, "rank 0 active again", func() bool {
		d, _, up := journalRankState(t, ctx, cluster)
		return !d && up == 1
	})
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	back := files("read")
	for path, digest := range seeded {
		if digest == "" || back[path] != digest {
			t.Fatalf("%s did not read back after the restore", path)
		}
	}
	receipt["readback"] = len(back)
	out = harness.ExecOutput(t, ctx, client, "python3", "-c", journalFiles, "seed", journalFS, "after", "2")
	receipt["after_restore_writes"] = strings.TrimSpace(harness.LastJSONLine(out)) != ""
}
