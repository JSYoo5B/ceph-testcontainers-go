//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// cephFSRecoveryWriter keeps one mount with an admin socket and one O_RDWR
// fd. It writes and fsyncs the base bytes and publishes receipt 1. For each
// chunk i it waits at gate "append-<i>", appends the chunk through the same
// fd and fsyncs once, without a retry, and publishes receipt 2+i with the
// size, the errno and how long the write and fsync took. At "finish" it reads
// the whole file through the fd, closes it and unmounts, and publishes the
// last receipt. Arguments after the common ones: the admin socket, the file,
// the hex of the base and of each chunk.
const cephFSRecoveryWriter cephFSActorScript = cephFSActorHelpers + `
asok, path = sys.argv[4], sys.argv[5].encode()
base, chunks = bytes.fromhex(sys.argv[6]), [bytes.fromhex(arg) for arg in sys.argv[7:]]
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.conf_set("admin_socket", asok)
fs.mount(filesystem_name=name.encode())
fd = fs.open(path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
fs.write(fd, base, 0)
fs.fsync(fd, 0)
stat = fs.fstat(fd)
publish(1, client=fs.get_instance_id(), inode=stat.st_ino, size=stat.st_size)
offset = len(base)
for index, chunk in enumerate(chunks):
    gate("append-%d" % index)
    started = time.monotonic()
    result = {}
    try:
        fs.write(fd, chunk, offset)
        offset += len(chunk)
        fs.fsync(fd, 0)
        result["size"] = fs.fstat(fd).st_size
    except cephfs.Error as error:
        result.update(errno=getattr(error, "errno", None) or -1, error=str(error)[:200])
    result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
    publish(2 + index, **result)
gate("finish")
closing = {}
try:
    stat = fs.fstat(fd)
    closing.update(inode=stat.st_ino, size=stat.st_size, full=fs.read(fd, 0, stat.st_size + 1).hex())
    fs.close(fd)
    fs.unmount()
except cephfs.Error as error:
    closing.update(close_errno=getattr(error, "errno", None) or -1)
fs.shutdown()
publish(2 + len(chunks), closed=True, **closing)
`

// holdOSDFlag sets a global OSD flag through override.SetOSDFlag and registers
// its restoration before it returns.
func holdOSDFlag(t *testing.T, ctx context.Context, cluster *ceph.Container, flag ceph.OSDFlag) *override.OSDFlagHandle {
	t.Helper()
	hold, err := override.SetOSDFlag(ctx, cluster, flag, true)
	t.Cleanup(func() {
		restoreCtx, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if err := hold.Restore(restoreCtx); err != nil {
			t.Errorf("restore %s: %v", flag, err)
		}
	})
	if err != nil {
		t.Fatalf("hold %s: %v", flag, err)
	}
	return hold
}

// recoveryPG is one look at the PG of the file's first object and the state
// of the stopped OSD.
type recoveryPG struct {
	pg       ceph.PGState
	found    bool
	osdUp    bool
	osdIn    bool
	poolSize int
	minSize  int
}

func (r recoveryPG) String() string {
	return fmt.Sprintf("pg %s %s up %v acting %v primary %d degraded %d misplaced %d (osd up=%t in=%t)",
		r.pg.PGID, r.pg.State, r.pg.Up, r.pg.Acting, r.pg.ActingPrimary, r.pg.Stats.ObjectsDegraded, r.pg.Stats.ObjectsMisplaced, r.osdUp, r.osdIn)
}

// lookRecoveryPG reads the PG pgid of pool and the OSDMap state of osd.
func lookRecoveryPG(ctx context.Context, cluster *ceph.Container, pool, pgid string, osd int) (recoveryPG, error) {
	var look recoveryPG
	states, err := observeCluster(cluster).OSDStates(ctx)
	if err != nil {
		return look, err
	}
	for _, state := range states {
		if state.ID == osd {
			look.osdUp, look.osdIn = state.Up, state.In
		}
	}
	handle, err := cluster.Pool(ctx, pool)
	if err != nil {
		return look, err
	}
	snapshot, err := handle.PGs(ctx)
	if err != nil {
		return look, err
	}
	look.poolSize, look.minSize = snapshot.PoolAfter.Size, snapshot.PoolAfter.MinSize
	for _, pg := range snapshot.PGs {
		if pg.PGID.String() == pgid {
			look.pg, look.found = pg, true
		}
	}
	return look, nil
}

// testCephFSOSDRecovery keeps a live client writing to one file while the
// acting primary of the file's first object stops and comes back. On the
// two-OSD cluster, with replica 2 and min_size 1, noout keeps the stopped OSD
// in, and norecover and nobackfill keep its return from repairing anything
// until the test lets it. With the OSD down the object's PG is active,
// undersized and degraded on the other OSD alone, and an append with fsync
// through the client's original fd returns 0. With the OSD up again the PG
// is back on both OSDs and waits for recovery (recovery_wait or
// backfill_wait) while still degraded, its degraded objects counted, and
// another append with fsync returns 0. A write to a missing object starts
// that object's recovery even under norecover, so the PG can be counted
// clean of degraded objects before the flags are released. Without norecover and nobackfill the
// metadata and data pools recover to active+clean with no degraded object;
// the recovery states that pg ls reports on the way are kept. The client
// then reads every byte through its fd, a second client reads them too, the
// session stayed open under the same address, nobody is blocklisted and the
// MDS keeps its GID.
func testCephFSOSDRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	m := startCephFSSingleMDS(t, ctx)
	osds := clusteraccess.OSDs(t, m.cluster)
	if len(osds) != 2 {
		t.Fatalf("want two owned OSDs, have %d", len(osds))
	}
	const path, workdir = "/recovering", "/tmp/osd-recovery-writer"
	asok := workdir + "/client.asok"
	// The bytes travel as hex arguments, each well below Linux's 128 KiB.
	base := make([]byte, 16<<10)
	for i := range base {
		base[i] = byte(i*13 + i>>9)
	}
	degraded := append([]byte("\x00written while the OSD was down\xff"), bytes.Repeat([]byte{0x3a, 0xa3}, 8<<10)...)
	recovering := append([]byte("\x00written while recovery waited\xff"), bytes.Repeat([]byte{0x6b, 0xb6}, 8<<10)...)
	whole := slices.Concat(base, degraded, recovering)

	writer := startCephFSActor(t, ctx, m.cluster, m.client, cephFSRecoveryWriter, m.fs.FilesystemName, workdir, asok, path,
		hex.EncodeToString(base), hex.EncodeToString(degraded), hex.EncodeToString(recovering))
	opened := writer.receipt(t, ctx, 1)
	if opened.Client == 0 || opened.Inode == 0 || opened.Size != int64(len(base)) {
		t.Fatalf("writer receipt 1 = %+v", opened)
	}
	session := cephFSRetainedSession(t, ctx, m.fs, opened.Client, 0)

	object := fmt.Sprintf("%x.00000000", opened.Inode)
	placed := cephOSDMapObject(t, ctx, m.cluster, m.fs.DataPool, object)
	if pool := cephFSDataPool(t, ctx, m, opened.Inode); pool != placed.PoolID {
		t.Fatalf("inode %d lives in pool %d, osd map names %d", opened.Inode, pool, placed.PoolID)
	}
	var victim *ceph.OSDContainer
	survivor := -1
	for _, osd := range osds {
		if osd.ID == placed.ActingPrimary {
			victim = osd
		} else {
			survivor = osd.ID
		}
	}
	if victim == nil || survivor < 0 || len(placed.Acting) != 2 {
		t.Fatalf("%s in pg %s (acting %v, primary %d) is not on both owned OSDs", object, placed.PGID, placed.Acting, placed.ActingPrimary)
	}
	clean, err := lookRecoveryPG(ctx, m.cluster, m.fs.DataPool, placed.PGID, victim.ID)
	if err != nil || !clean.found || clean.pg.State != "active+clean" || clean.poolSize != 2 || clean.minSize != 1 {
		t.Fatalf("before the stop: %s (pool size %d, min_size %d), %v", clean, clean.poolSize, clean.minSize, err)
	}

	noout := holdOSDFlag(t, ctx, m.cluster, ceph.OSDFlagNoOut)
	norecover := holdOSDFlag(t, ctx, m.cluster, ceph.OSDFlagNoRecover)
	nobackfill := holdOSDFlag(t, ctx, m.cluster, ceph.OSDFlagNoBackfill)
	grace := 20 * time.Second
	if err := victim.Stop(ctx, &grace); err != nil {
		t.Fatalf("stop osd.%d: %v", victim.ID, err)
	}
	stoppedAt := time.Now()
	// A stopped OSD is started again even when the test fails before that.
	t.Cleanup(func() {
		startCtx, stop := context.WithTimeout(context.Background(), 3*time.Minute)
		defer stop()
		if state, err := victim.State(startCtx); err == nil && !state.Running {
			if err := victim.Start(startCtx); err != nil {
				t.Errorf("start osd.%d: %v", victim.ID, err)
			}
		}
	})
	down, err := observe(ctx, 2*time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) (recoveryPG, bool, error) {
		look, err := lookRecoveryPG(probeCtx, m.cluster, m.fs.DataPool, placed.PGID, victim.ID)
		return look, err == nil && look.found && !look.osdUp && look.osdIn && strings.HasPrefix(look.pg.State, "active+") &&
			strings.Contains(look.pg.State, "undersized") && strings.Contains(look.pg.State, "degraded") &&
			slices.Equal(look.pg.Acting, []int{survivor}) && look.pg.ActingPrimary == survivor, err
	})
	if err != nil {
		t.Fatalf("pg %s did not go active on osd.%d alone: %s: %v", placed.PGID, survivor, down.Value, err)
	}
	t.Logf("osd.%d down: %s", victim.ID, down.Value)

	writer.open(t, ctx, "append-0")
	whileDown := writer.receipt(t, ctx, 2)
	if whileDown.Errno != 0 || whileDown.Error != "" || whileDown.Size != int64(len(base)+len(degraded)) {
		t.Fatalf("append while osd.%d is down: %+v", victim.ID, whileDown)
	}
	// Reports reach the MGR on their own schedule: the object written
	// without its second copy is counted degraded.
	counted, err := observe(ctx, time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) (recoveryPG, bool, error) {
		look, err := lookRecoveryPG(probeCtx, m.cluster, m.fs.DataPool, placed.PGID, victim.ID)
		return look, err == nil && look.found && !look.osdUp && look.pg.Stats.ObjectsDegraded > 0, err
	})
	if err != nil {
		t.Fatalf("pg %s reports no degraded object while osd.%d is down: %s: %v", placed.PGID, victim.ID, counted.Value, err)
	}

	if err := victim.Start(ctx); err != nil {
		t.Fatalf("start osd.%d: %v", victim.ID, err)
	}
	// Back on both OSDs, the PG still misses the copies written while the
	// OSD was down, and norecover and nobackfill hold their repair.
	returned, err := observe(ctx, 3*time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) (recoveryPG, bool, error) {
		look, err := lookRecoveryPG(probeCtx, m.cluster, m.fs.DataPool, placed.PGID, victim.ID)
		return look, err == nil && look.found && look.osdUp && look.osdIn && strings.HasPrefix(look.pg.State, "active+") &&
			!strings.Contains(look.pg.State, "undersized") && strings.Contains(look.pg.State, "degraded") &&
			(strings.Contains(look.pg.State, "recovery_wait") || strings.Contains(look.pg.State, "backfill_wait")) &&
			slices.Contains(look.pg.Up, victim.ID) && look.pg.Stats.ObjectsDegraded > 0, err
	})
	if err != nil {
		t.Fatalf("pg %s is not back on osd.%d with its repair held: %s: %v", placed.PGID, victim.ID, returned.Value, err)
	}
	t.Logf("osd.%d up with recovery held: %s", victim.ID, returned.Value)

	writer.open(t, ctx, "append-1")
	whileHeld := writer.receipt(t, ctx, 3)
	if whileHeld.Errno != 0 || whileHeld.Error != "" || whileHeld.Size != int64(len(whole)) {
		t.Fatalf("append while recovery is held: %+v", whileHeld)
	}
	afterAppend, err := lookRecoveryPG(ctx, m.cluster, m.fs.DataPool, placed.PGID, victim.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("after the append with recovery held: %s", afterAppend)

	// Recovery runs: every state that pg ls reports for the two pools on
	// the way to active+clean is kept.
	if err := norecover.Restore(ctx); err != nil {
		t.Fatal("restore norecover:", err)
	}
	if err := nobackfill.Restore(ctx); err != nil {
		t.Fatal("restore nobackfill:", err)
	}
	released := time.Now()
	var states []string
	recovered, err := observe(ctx, 5*time.Minute, 30*time.Second, 500*time.Millisecond, func(probeCtx context.Context) (string, bool, error) {
		cleanPGs, total := 0, 0
		var seen []string
		for _, pool := range []string{m.fs.MetadataPool, m.fs.DataPool} {
			handle, err := findPool(probeCtx, m.cluster, pool)
			if err != nil {
				return "", false, err
			}
			snapshot, err := handle.PGs(probeCtx)
			if err != nil {
				return "", false, err
			}
			for _, pg := range snapshot.PGs {
				total++
				if pg.State == "active+clean" && pg.Stats.ObjectsDegraded == 0 {
					cleanPGs++
				} else {
					seen = append(seen, fmt.Sprintf("%s %s degraded=%d", pg.PGID, pg.State, pg.Stats.ObjectsDegraded))
				}
				if !slices.Contains(states, pg.State) {
					states = append(states, pg.State)
				}
			}
		}
		return strings.Join(seen, "; "), total > 0 && cleanPGs == total, nil
	})
	if err != nil {
		t.Fatalf("the pools did not recover: %s: %v", recovered.Value, err)
	}
	recoveredIn := time.Since(released).Round(time.Millisecond)
	if err := noout.Restore(ctx); err != nil {
		t.Fatal("restore noout:", err)
	}
	if err := m.cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	final, err := lookRecoveryPG(ctx, m.cluster, m.fs.DataPool, placed.PGID, victim.ID)
	if err != nil || final.pg.State != "active+clean" || !slices.Equal(final.pg.Acting, placed.Acting) {
		t.Fatalf("pg %s after recovery: %s, want acting %v: %v", placed.PGID, final, placed.Acting, err)
	}

	if after := cephFSRetainedSession(t, ctx, m.fs, opened.Client, 0); after.Address != session.Address {
		t.Fatalf("client.%d session after recovery %+v, before %+v", opened.Client, after, session)
	}
	writer.open(t, ctx, "finish")
	closed := writer.receipt(t, ctx, 4)
	if !closed.Closed || closed.CloseErrno != 0 || closed.Inode != opened.Inode || closed.Full != hex.EncodeToString(whole) {
		t.Fatalf("writer's closing receipt: closed=%t errno=%d inode=%d size=%d, want inode %d with %d bytes", closed.Closed, closed.CloseErrno, closed.Inode, closed.Size, opened.Inode, len(whole))
	}
	writer.wait(t, ctx)
	independent := readCephFSIndependently(t, ctx, m, path)
	if independent.Inode != opened.Inode || independent.Data != hex.EncodeToString(whole) || independent.Client == opened.Client {
		t.Fatalf("independent reader: client=%d inode=%d size=%d, want inode %d with %d bytes", independent.Client, independent.Inode, independent.Size, opened.Inode, len(whole))
	}
	status, err := m.fs.MDSStatus(ctx)
	if err != nil || len(status.Active) != 1 || status.Active[0].GID != m.mds.GID {
		t.Fatalf("mds.%s GID %d was replaced: %+v, %v", m.mds.Name, m.mds.GID, status, err)
	}
	entries, err := observeCluster(m.cluster).BlocklistEntries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == session.Address }) {
		t.Fatalf("client.%d %s was blocklisted: %+v", session.ID, session.Address, entries)
	}
	sessions, err := m.fs.Sessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(sessions, func(s cephfs.Session) bool { return s.ID == opened.Client }) {
		t.Fatalf("client.%d still has a session after it unmounted: %+v", opened.Client, sessions)
	}
	t.Logf("CEPHFS_OSD_RECOVERY_RECEIPT %s: writer client.%d (%s) inode=%d object %s in pg %s acting %v; osd.%d stopped with noout, norecover and nobackfill (%s): %s, %s; the append and fsync returned 0 in %d ms; osd.%d up again: %s; the next append and fsync returned 0 in %d ms, then %s; with recovery released the pools were active+clean after %s through states %v; the writer read %d bytes through its fd and reader client.%d read every byte; the session stayed open, no blocklist entry, same MDS GID",
		m.version, opened.Client, session.Address, opened.Inode, object, placed.PGID, placed.Acting, victim.ID, time.Since(stoppedAt).Round(time.Second),
		down.Value, counted.Value, whileDown.ElapsedMS, victim.ID, returned.Value, whileHeld.ElapsedMS, afterAppend, recoveredIn, states, len(whole), independent.Client)
}
