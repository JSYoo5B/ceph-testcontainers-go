//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_scratch_tmpfs))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

const (
	dbDeviceSize  = int64(96 << 20)
	walDeviceSize = int64(64 << 20)
	dbPool        = "db-capacity"
	dbObjects     = 40
	dbKeys        = 500
	dbValueSize   = 1024
	dbRounds      = 6
)

// dbOmapWrites writes COUNT objects named PREFIX-N, each with KEYS omap keys
// of SIZE bytes derived from the object and key names, and prints how many
// were acknowledged. Omap lives in RocksDB, so this fills BlueFS, not the
// object data. Arguments: pool, prefix, count, keys, size.
const dbOmapWrites = `import hashlib, json, rados, sys
pool, prefix, count, keys, size = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
acked = 0
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"client_mount_timeout": "30", "rados_osd_op_timeout": "30"}) as c:
    with c.open_ioctx(pool) as io:
        for n in range(count):
            obj = "%s-%d" % (prefix, n)
            names = tuple("k%06d" % i for i in range(keys))
            values = tuple(hashlib.sha256(("%s/%s" % (obj, k)).encode()).digest() * (size // 32) for k in names)
            try:
                with rados.WriteOpCtx() as op:
                    io.set_omap(op, names, values)
                    io.operate_write_op(op, obj)
                acked += 1
            except rados.Error:
                pass
print(json.dumps({"acked": acked}))`

// dbOmapReadback reads every key of every object the rounds wrote and prints
// how many objects had all their keys with the expected values. Arguments:
// pool, rounds, count, keys, size.
const dbOmapReadback = `import hashlib, json, rados, sys
pool, rounds, count, keys, size = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
intact = 0
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"client_mount_timeout": "30", "rados_osd_op_timeout": "30"}) as c:
    with c.open_ioctx(pool) as io:
        for r in range(rounds):
            for n in range(count):
                obj = "omap%d-%d" % (r, n)
                with rados.ReadOpCtx() as op:
                    it, _ = io.get_omap_vals(op, "", "", keys + 1)
                    io.operate_read_op(op, obj)
                    got = dict(it)
                want = {"k%06d" % i: hashlib.sha256(("%s/k%06d" % (obj, i)).encode()).digest() * (size // 32) for i in range(keys)}
                intact += got == want
print(json.dumps({"intact": intact}))`

// The BlueFS DB/WAL run of issue 63. The OSD gets its own DB and WAL devices,
// files beside the main block on the same owned tmpfs, through BlueStore's
// bluestore_block_db_* and bluestore_block_wal_* options before mkfs, and
// fit_to_fast placement so that BlueFS moves files to the main device only for
// lack of room. Omap writes fill RocksDB until the DB device has no room; the
// run then requires spillover: slow-device use that grew, a DB device within
// dbSpillSlack of its size and BLUEFS_SPILLOVER, with every write
// acknowledged, the OSD up and every omap value read back.
func TestBlueFSDBCapacity(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	opts = append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs().WithPoolDefaults(1, 1)),
		ceph.WithOSDStorage(ceph.OSDStorage{}.WithInMemory(logicalCeiling).WithBlockSize(512<<20)))
	cluster, err := ceph.Run(ctx, image, opts...)
	if cluster != nil {
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
			defer done()
			if err := cluster.Terminate(cleanup); err != nil {
				t.Error("db capacity cluster cleanup", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	// The device paths must differ from block.db and block.wal: BlueStore
	// links those names to the configured paths.
	for _, s := range []ceph.ConfigSetting{
		{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true"},
		{Section: "osd", Name: "bluestore_block_db_path", Value: "/var/lib/ceph/osd/ceph-$id/db.file"},
		{Section: "osd", Name: "bluestore_block_db_size", Value: strconv.FormatInt(dbDeviceSize, 10)},
		{Section: "osd", Name: "bluestore_block_db_create", Value: "true"},
		{Section: "osd", Name: "bluestore_block_wal_path", Value: "/var/lib/ceph/osd/ceph-$id/wal.file"},
		{Section: "osd", Name: "bluestore_block_wal_size", Value: strconv.FormatInt(walDeviceSize, 10)},
		{Section: "osd", Name: "bluestore_block_wal_create", Value: "true"},
		{Section: "osd", Name: "bluestore_volume_selection_policy", Value: "fit_to_fast"},
	} {
		if _, err := override.SetConfig(ctx, cluster, s); err != nil {
			t.Fatal(s.Name, err)
		}
	}
	osd, err := cluster.AddOSD(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client := harness.ToolkitContainer(t, cluster)
	usage := func() bluefsUsage {
		t.Helper()
		data, err := cluster.Ceph(ctx, "tell", "osd."+strconv.Itoa(osd.ID), "perf", "dump", "bluefs")
		if err != nil {
			t.Fatal(err)
		}
		u, err := parseBlueFSUsage(data)
		if err != nil {
			t.Fatalf("%v: %s", err, data)
		}
		return u
	}
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: dbPool, Application: "rados", Replicas: 1, MinSize: 1, PGNum: 8}); err != nil {
		t.Fatal(err)
	}
	before := usage()
	receipt := map[string]any{"db_size": dbDeviceSize, "wal_size": walDeviceSize, "before": before}
	defer func() {
		data, _ := json.Marshal(receipt)
		t.Logf("BLUEFS_DB_CAPACITY_RECEIPT %s", data)
	}()
	acked, rounds := 0, []bluefsUsage{}
	for round := range dbRounds {
		out := harness.ExecOutput(t, ctx, client, "python3", "-c", dbOmapWrites, dbPool, "omap"+strconv.Itoa(round),
			strconv.Itoa(dbObjects), strconv.Itoa(dbKeys), strconv.Itoa(dbValueSize))
		var result struct{ Acked int }
		if err := json.Unmarshal([]byte(harness.LastJSONLine(out)), &result); err != nil {
			t.Fatalf("round %d: %v: %s", round, err, out)
		}
		acked += result.Acked
		rounds = append(rounds, usage())
	}
	receipt["rounds"] = rounds
	// The health check follows the OSD's next report.
	var checks []string
	deadline := time.Now().Add(2 * time.Minute)
	for {
		health, err := cluster.Ceph(ctx, "health", "detail", "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		if checks, err = parseHealthChecks(health); err != nil {
			t.Fatal(err)
		}
		if slices.Contains(checks, "BLUEFS_SPILLOVER") || time.Now().After(deadline) {
			break
		}
		time.Sleep(3 * time.Second)
	}
	after := usage()
	state, err := osd.State(ctx)
	if err != nil || state == nil {
		t.Fatal("OSD state", err)
	}
	receipt["after"], receipt["checks"], receipt["acked"] = after, checks, acked
	verdict, err := judgeDBCapacity(dbDeviceSize, walDeviceSize, before, after, checks, acked, dbRounds*dbObjects, state.Running)
	receipt["verdict"] = verdict
	if err != nil {
		t.Fatal("db capacity judge:", err)
	}
	if verdict != dbSpilled {
		t.Fatalf("verdict %s, want %s", verdict, dbSpilled)
	}
	out := harness.ExecOutput(t, ctx, client, "python3", "-c", dbOmapReadback, dbPool, strconv.Itoa(dbRounds),
		strconv.Itoa(dbObjects), strconv.Itoa(dbKeys), strconv.Itoa(dbValueSize))
	var readback struct{ Intact int }
	if err := json.Unmarshal([]byte(harness.LastJSONLine(out)), &readback); err != nil || readback.Intact != dbRounds*dbObjects {
		t.Fatalf("omap readback %+v: %v: %s", readback, err, out)
	}
	receipt["intact"] = readback.Intact
	t.Logf("BLUEFS_DB_CAPACITY verdict=%s db_used=%d/%d slow_used=%d→%d", verdict, after.DBUsed, after.DBTotal, before.SlowUsed, after.SlowUsed)
}
