//go:build all || integration

package cephfs_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// cephFSMDSOSDPathWriter keeps one mount with an admin socket and one O_RDWR
// fd. It writes and fsyncs the base bytes and publishes receipt 1. At the
// "data" gate it appends the tail through the same fd and syncs the data
// only (fsync with syncdataonly), which needs the OSDs and no MDS reply, and
// publishes receipt 2 with the errno and how long it took. Then it fsyncs the
// fd fully, once, without a retry: that also flushes the new size to the MDS
// and waits for its ack. Receipt 3 has the errno and how long that took. At
// "finish" it reads the whole file through the same fd, stats a directory
// that another client made, closes the fd and unmounts, and publishes
// receipt 4. Arguments after the common ones: the admin socket, the file, the
// directory, the hex of the base and of the tail.
const cephFSMDSOSDPathWriter harness.CephFSActorScript = harness.CephFSActorHelpers + `
asok, path, directory = sys.argv[4], sys.argv[5].encode(), sys.argv[6].encode()
base, tail = bytes.fromhex(sys.argv[7]), bytes.fromhex(sys.argv[8])
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.conf_set("admin_socket", asok)
fs.mount(filesystem_name=name.encode())
fd = fs.open(path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
fs.write(fd, base, 0)
fs.fsync(fd, 0)
stat = fs.fstat(fd)
publish(1, client=fs.get_instance_id(), inode=stat.st_ino, size=stat.st_size)
gate("data")
def synced(dataonly):
    started = time.monotonic()
    result = {}
    try:
        fs.fsync(fd, dataonly)
    except cephfs.Error as error:
        result.update(errno=getattr(error, "errno", None) or -1, error=str(error)[:200])
    result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
    return result
fs.write(fd, tail, len(base))
publish(2, size=fs.fstat(fd).st_size, **synced(1))
publish(3, **synced(0))
gate("finish")
closing = {}
try:
    stat = fs.fstat(fd)
    closing.update(inode=stat.st_ino, size=stat.st_size, full=fs.read(fd, 0, stat.st_size + 1).hex())
    closing.update(dir_inode=fs.stat(directory).st_ino)
    fs.close(fd)
    fs.unmount()
except cephfs.Error as error:
    closing.update(close_errno=getattr(error, "errno", None) or -1)
fs.shutdown()
publish(4, closed=True, **closing)
`

// mdsOSDPathState is one look at the clients and the MDS while the MDS's
// packets to the OSD are dropped.
type mdsOSDPathState struct {
	journal     []harness.ObjecterOp
	clientData  []harness.ObjecterOp
	mkdir       harness.MDSRequestAge
	hasMkdir    bool
	pending     []harness.ClientMDSRequest
	caps        harness.ClientInodeCaps
	hasCaps     bool
	mdsSize     uint64
	raw         string
	journalAged bool
}

// testCephFSMDSOSDPath cuts only the paths from the filesystem's one MDS to
// its osdCount OSDs: one peer-selected netem lease (Loss 100) on the MDS's
// endpoint drops every packet that the MDS sends to any OSD's public
// address, with one filter per OSD, while the clients, in other containers,
// reach the same OSDs as before. One OSD is selected with Peer, several with
// Peers. The tuples come from native state: each OSD's public address from
// the OSDMap and the MDS's from the FSMap, each matched to its container's
// endpoint. During the
// cut a live client appends to its open file and syncs the data, which
// returns, and RADOS holds the new bytes. The MDS cannot write its journal:
// its objecter_requests holds writes of 200.* objects to the OSD, a second
// client's mkdir (the first inode of its session, so no early reply) waits
// in dump_ops_in_flight without journal_committed and in that client's
// mds_requests, and the first client's full fsync waits with a flushing cap
// while the MDS still has the base size. Both are seen held for at least
// 5 s, both sessions stay open, and nobody is blocklisted. After Restore the
// same fsync and mkdir return 0, the MDS's history shows the mkdir committed
// then replied, the MDS has the full size, the writer reads every byte and
// the directory through its fd, a third client reads every byte, and the MDS
// keeps its GID.
func testCephFSMDSOSDPath(t *testing.T, osdCount int) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	netem := harness.NetemImage(t)
	m := harness.StartCephFSSingleMDS(t, ctx, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(osdCount)))
	const path, directory = "/mds-osd-path", "/journaled"
	const writerDir, waiterDir = "/tmp/mds-osd-writer", "/tmp/mds-osd-waiter"
	writerAsok, waiterAsok := writerDir+"/client.asok", waiterDir+"/client.asok"
	base := bytes.Repeat([]byte("fsynced before the MDS lost its OSD\n"), 64)
	tail := append([]byte("\x00written while only the MDS was cut\xff"), bytes.Repeat([]byte{0x5c, 0xc5}, 512)...)
	whole := append(append([]byte{}, base...), tail...)

	writer := harness.StartCephFSActor(t, ctx, m.Cluster, m.Client, cephFSMDSOSDPathWriter, m.FS.FilesystemName, writerDir, writerAsok, path, directory, hex.EncodeToString(base), hex.EncodeToString(tail))
	opened := writer.Receipt(t, ctx, 1)
	if opened.Client == 0 || opened.Inode == 0 || opened.Size != int64(len(base)) {
		t.Fatalf("writer receipt 1 = %+v", opened)
	}
	waiter := harness.StartCephFSActor(t, ctx, m.Cluster, harness.ToolkitContainer(t, m.Cluster), harness.CephFSJournalWaiter, m.FS.FilesystemName, waiterDir, waiterAsok, directory)
	waiting := waiter.Receipt(t, ctx, 1)
	if waiting.Client == 0 || waiting.Client == opened.Client {
		t.Fatalf("waiter receipt 1 = %+v, writer %+v", waiting, opened)
	}
	writerSession := harness.CephFSRetainedSession(t, ctx, m.FS, opened.Client, 0)
	waiterSession := harness.CephFSRetainedSession(t, ctx, m.FS, waiting.Client, 0)

	// The tuples: each OSD's public address from the OSDMap, the MDS's v2
	// address from the FSMap, and the containers' endpoints.
	osds := clusteraccess.OSDs(t, m.Cluster)
	if len(osds) != osdCount {
		t.Fatalf("want %d owned OSDs, have %d", osdCount, len(osds))
	}
	daemons, err := harness.ObserveCluster(m.Cluster).DaemonAddresses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(daemons.OSDs) != osdCount {
		t.Fatalf("the OSDMap has %d OSDs, want %d: %+v", len(daemons.OSDs), osdCount, daemons.OSDs)
	}
	network := harness.StaticFacts(m.Cluster).NetworkName
	osdIPs := map[int]netip.Addr{}
	var cutIDs []int
	var peers []fault.NetworkPeer
	for _, osd := range osds {
		i := slices.IndexFunc(daemons.OSDs, func(daemon ceph.OSDAddress) bool { return daemon.ID == osd.ID })
		if i < 0 || !daemons.OSDs[i].Up || len(daemons.OSDs[i].Public) == 0 {
			t.Fatalf("the OSDMap does not have owned osd.%d up with a public address: %+v", osd.ID, daemons.OSDs)
		}
		ip := harness.AddressIP(t, daemons.OSDs[i].Public[0].Addr)
		for _, addr := range daemons.OSDs[i].Public {
			if harness.AddressIP(t, addr.Addr) != ip {
				t.Fatalf("osd.%d has public addresses on more than one IP: %+v", osd.ID, daemons.OSDs[i].Public)
			}
		}
		if have := containerIP(t, ctx, osd.Container, network); have != ip {
			t.Fatalf("osd.%d advertises %s but its container has %s", osd.ID, ip, have)
		}
		osdIPs[osd.ID] = ip
		cutIDs = append(cutIDs, osd.ID)
		peers = append(peers, fault.NetworkPeer{Addr: ip})
	}
	slices.Sort(cutIDs)
	isOSDIP := func(ip netip.Addr) bool {
		for _, osdIP := range osdIPs {
			if osdIP == ip {
				return true
			}
		}
		return false
	}
	rank, err := readAdmissionRank(ctx, m.Cluster, m.FS.FilesystemName)
	if err != nil {
		t.Fatal(err)
	}
	v2, ok := rank.v2()
	if !ok || rank.GID != m.MDS.GID || rank.Name != m.MDS.Name {
		t.Fatalf("rank 0 is not mds.%s (gid %d) with a v2 address: %+v", m.MDS.Name, m.MDS.GID, rank)
	}
	mdsIP := harness.AddressIP(t, v2.Addr)
	var mdsContainer *cephfs.MDS
	for _, member := range m.FS.MDSs() {
		if member.ID == m.MDS.Name {
			mdsContainer = member
		}
	}
	if mdsContainer == nil {
		t.Fatalf("mds.%s is not an owned daemon", m.MDS.Name)
	}
	if ip := containerIP(t, ctx, mdsContainer, network); ip != mdsIP {
		t.Fatalf("mds.%s advertises %s but its container has %s", m.MDS.Name, mdsIP, ip)
	}
	for _, client := range []struct {
		session cephfs.Session
		ctr     string
	}{{writerSession, m.Client.GetContainerID()}, {waiterSession, harness.ToolkitContainer(t, m.Cluster).GetContainerID()}} {
		address, _, _ := strings.Cut(client.session.Address, "/")
		if ip := harness.AddressIP(t, address); ip == mdsIP || isOSDIP(ip) {
			t.Fatalf("client.%d in %s shares an address with the MDS or the OSD: %s", client.session.ID, client.ctr[:12], ip)
		}
	}

	object := fmt.Sprintf("%x.00000000", opened.Inode)
	placed := harness.CephOSDMapObject(t, ctx, m.Cluster, m.FS.DataPool, object)
	journal := harness.CephOSDMapObject(t, ctx, m.Cluster, m.FS.MetadataPool, "200.00000000")
	// Every OSD is cut, so every copy is behind the cut; one OSD holds them
	// alone, more hold a copy each.
	if replicas := min(osdCount, 2); len(placed.Acting) != replicas || len(journal.Acting) != replicas ||
		!slices.ContainsFunc(cutIDs, func(id int) bool { return id == placed.Acting[0] }) ||
		!slices.ContainsFunc(cutIDs, func(id int) bool { return id == journal.Acting[0] }) {
		t.Fatalf("the file (acting %v) and the journal (acting %v) are not on %d of osds %v", placed.Acting, journal.Acting, replicas, cutIDs)
	}
	if ops, raw, err := harness.ObjecterRequests(ctx, m.Cluster, m.Client, writerAsok, ""); err != nil || len(harness.ObjecterOpsOn(ops, placed.PoolID, "")) != 0 {
		t.Fatalf("writer has data ops in flight before the cut: %v: %s", err, raw)
	}

	cutRequest := fault.NetworkProfile{HelperImage: netem, Plane: ceph.PublicNetworkPlane, Loss: 100}
	if osdCount == 1 {
		cutRequest.Peer = peers[0].Addr
	} else {
		cutRequest.Peers = peers
	}
	lease, err := fault.ApplyNetworkProfile(ctx, m.Cluster, mdsContainer, cutRequest)
	if lease != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := lease.Restore(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	cutAt := time.Now()
	applied, err := lease.Status(ctx)
	if err != nil || !applied.Applied || applied.Loss != 100 || !slices.Equal(lease.Peers, peers) {
		t.Fatalf("status of the cut to %v: %+v, %v", lease.Peers, applied, err)
	}

	// Data I/O on the open file reaches the OSD: the tail is appended and
	// synced, and RADOS holds it.
	writer.Open(t, ctx, "data")
	dataSynced := writer.Receipt(t, ctx, 2)
	if dataSynced.Errno != 0 || dataSynced.Error != "" || dataSynced.Size != int64(len(whole)) {
		t.Fatalf("data sync during the cut: %+v", dataSynced)
	}
	stored := readCephFSObject(t, ctx, m.Cluster, m.FS.DataPool, opened.Inode)
	if stored.Size != uint64(len(whole)) || stored.Data != hex.EncodeToString(whole) {
		t.Fatalf("RADOS object %s holds %d bytes during the cut, want %d", stored.Object, stored.Size, len(whole))
	}
	waiter.Open(t, ctx, "mkdir")

	// The MDS's journal writes stay in flight to the OSD, and the mkdir and
	// the cap flush wait on them, until each has been held for 5 s.
	const held = 5.0
	seen, err := harness.Observe(ctx, 2*time.Minute, 40*time.Second, time.Second, func(probeCtx context.Context) (mdsOSDPathState, bool, error) {
		var state mdsOSDPathState
		mdsObjecter, mdsRaw, err := harness.ObjecterRequests(probeCtx, m.Cluster, nil, "", m.MDS.Name)
		if err != nil {
			return state, false, err
		}
		state.journal = harness.ObjecterOpsOn(mdsObjecter, journal.PoolID, "200.")
		state.journalAged = slices.ContainsFunc(state.journal, func(op harness.ObjecterOp) bool { return op.Age >= held })
		clientOps, _, err := harness.ObjecterRequests(probeCtx, m.Cluster, m.Client, writerAsok, "")
		if err != nil {
			return state, false, err
		}
		state.clientData = harness.ObjecterOpsOn(clientOps, placed.PoolID, "")
		inFlight, err := m.Cluster.Ceph(probeCtx, "tell", "mds."+m.MDS.Name, "dump_ops_in_flight")
		if err != nil {
			return state, false, err
		}
		var dump struct {
			Ops []harness.MDSRequestAge `json:"ops"`
		}
		if err := json.Unmarshal(inFlight, &dump); err != nil {
			return state, false, fmt.Errorf("dump_ops_in_flight: %v: %.512s", err, inFlight)
		}
		for _, op := range dump.Ops {
			if _, found := harness.FindMDSRequest([]harness.MDSRequestOp{op.MDSRequestOp}, waiting.Client, "mkdir", 0); found {
				state.mkdir, state.hasMkdir = op, true
			}
		}
		if state.pending, err = clientRequests(probeCtx, m.Cluster, harness.ToolkitContainer(t, m.Cluster), waiterAsok); err != nil {
			return state, false, err
		}
		cache, err := m.Cluster.RunNativeCommand(probeCtx, m.Client, ceph.NativeCommand{
			Argv: []string{"ceph", "--admin-daemon", writerAsok, "dump_cache"}, Timeout: 30 * time.Second,
			MaxStdout: 1 << 20, MaxStderr: 16 << 10, Label: "client dump_cache",
		})
		if err != nil || cache.ExitCode != 0 || !cache.Complete() {
			return state, false, fmt.Errorf("writer dump_cache: exit=%d err=%v stderr=%.512s", cache.ExitCode, err, cache.Stderr)
		}
		if state.caps, state.hasCaps, err = harness.ParseClientCache(cache.Stdout, opened.Inode); err != nil {
			return state, false, err
		}
		if state.mdsSize, err = harness.MDSInodeSizeOf(probeCtx, m.Cluster, m.MDS.Name, opened.Inode); err != nil {
			return state, false, err
		}
		state.raw = fmt.Sprintf("mds objecter %.2048s; mds mkdir %q at %q age %.1f events %v; waiter requests %+v; writer data ops %d, caps size=%d flushing=%q tids=%v; mds size %d",
			mdsRaw, state.mkdir.Description, state.mkdir.TypeData.FlagPoint, state.mkdir.Age, state.mkdir.Events(), state.pending, len(state.clientData),
			state.caps.Size, state.caps.FlushingCaps, state.caps.FlushingTIDs, state.mdsSize)
		return state, state.journalAged && state.hasMkdir && state.mkdir.Age >= held && state.hasCaps && state.caps.Flushing(), nil
	})
	if err != nil {
		t.Fatalf("the cut does not hold the MDS's journal, the mkdir and the cap flush: %s: %v", seen.Value.raw, err)
	}
	state := seen.Value
	for _, op := range state.journal {
		if op.OSD != journal.Acting[0] || !slices.ContainsFunc(op.OSDOps, func(sub string) bool { return strings.HasPrefix(sub, "write ") }) {
			t.Fatalf("an MDS journal op is not a write to the journal's primary osd.%d: %+v", journal.Acting[0], op)
		}
	}
	if events := state.mkdir.Events(); slices.ContainsFunc(events, func(event string) bool {
		return event == "replying" || strings.HasPrefix(event, "journal_committed")
	}) || harness.RequestSafety(state.mkdir.MDSRequestOp) != "no-early-reply" {
		t.Fatalf("the MDS answered the held mkdir: %s", state.raw)
	}
	if !slices.ContainsFunc(state.pending, func(request harness.ClientMDSRequest) bool {
		return request.TID == state.mkdir.TypeData.ReqID.TID && request.Op == "mkdir" && !bool(request.GotUnsafe)
	}) {
		t.Fatalf("the waiter does not hold its mkdir: %s", state.raw)
	}
	if len(state.clientData) != 0 || state.caps.Size != uint64(len(whole)) || state.mdsSize != uint64(len(base)) {
		t.Fatalf("want the writer's data acked, its flush of %d bytes held and the MDS at %d: %s", len(whole), len(base), state.raw)
	}
	if writer.Published(ctx, 3) || waiter.Published(ctx, 2) {
		t.Fatalf("a held call returned during the cut: %s", state.raw)
	}
	for _, session := range []cephfs.Session{writerSession, waiterSession} {
		if during := harness.CephFSRetainedSession(t, ctx, m.FS, session.ID, 0); during.Address != session.Address {
			t.Fatalf("client.%d session during the cut %+v, before %+v", session.ID, during, session)
		}
	}
	cut, err := lease.Status(ctx)
	if err != nil || !cut.Applied || cut.Drops == 0 {
		t.Fatalf("netem status during the cut: %+v, %v", cut, err)
	}
	t.Logf("CEPHFS_MDS_OSD_PATH held: %s", state.raw)

	if err := lease.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	restored := time.Now()
	window := restored.Sub(cutAt)
	fullSync := writer.Receipt(t, ctx, 3)
	if fullSync.Errno != 0 || fullSync.Error != "" || time.Duration(fullSync.ElapsedMS)*time.Millisecond < held*time.Second {
		t.Fatalf("writer's full fsync after the restore: %+v", fullSync)
	}
	made := waiter.Receipt(t, ctx, 2)
	if made.Errno != 0 || made.Error != "" || made.DirInode == 0 || time.Duration(made.ElapsedMS)*time.Millisecond < held*time.Second {
		t.Fatalf("waiter's mkdir after the restore: %+v", made)
	}
	returned := time.Since(restored).Round(time.Millisecond)
	tid := state.mkdir.TypeData.ReqID.TID
	historic, err := harness.Observe(ctx, time.Minute, 30*time.Second, 500*time.Millisecond, func(probeCtx context.Context) (harness.MDSRequestOp, bool, error) {
		out, err := m.Cluster.Ceph(probeCtx, "tell", "mds."+m.MDS.Name, "dump_historic_ops")
		if err != nil {
			return harness.MDSRequestOp{}, false, err
		}
		ops, err := harness.ParseMDSRequestOps(out)
		if err != nil {
			return harness.MDSRequestOp{}, false, fmt.Errorf("dump_historic_ops: %v: %.1024s", err, out)
		}
		op, found := harness.FindMDSRequest(ops, waiting.Client, "mkdir", tid)
		return op, found, nil
	})
	if err != nil || harness.RequestSafety(historic.Value) != "no-early-reply" || !harness.JournaledThenReplied(historic.Value.Events()) {
		t.Fatalf("the MDS's history lacks the committed mkdir tid %d: %+v: %v", tid, historic.Value, err)
	}
	if size := m.MDSInodeSize(t, ctx, m.MDS.Name, opened.Inode); size != uint64(len(whole)) {
		t.Fatalf("mds.%s has size %d after the flush was acked, want %d", m.MDS.Name, size, len(whole))
	}

	writer.Open(t, ctx, "finish")
	closed := writer.Receipt(t, ctx, 4)
	if !closed.Closed || closed.CloseErrno != 0 || closed.Inode != opened.Inode || closed.Full != hex.EncodeToString(whole) || closed.DirInode != made.DirInode {
		t.Fatalf("writer's closing receipt = %+v, want inode %d with %d bytes and directory %d", closed, opened.Inode, len(whole), made.DirInode)
	}
	writer.Wait(t, ctx)
	waiter.Open(t, ctx, "finish")
	if done := waiter.Receipt(t, ctx, 3); !done.Closed || done.CloseErrno != 0 {
		t.Fatalf("waiter's closing receipt = %+v", done)
	}
	waiter.Wait(t, ctx)

	independent := readCephFSIndependently(t, ctx, m, path)
	if independent.Inode != opened.Inode || independent.Data != hex.EncodeToString(whole) || independent.Client == opened.Client || independent.Client == waiting.Client {
		t.Fatalf("independent reader: %+v, want inode %d with %d bytes", independent, opened.Inode, len(whole))
	}
	status, err := m.FS.MDSStatus(ctx)
	if err != nil || len(status.Active) != 1 || status.Active[0].GID != m.MDS.GID {
		t.Fatalf("mds.%s GID %d was replaced: %+v, %v", m.MDS.Name, m.MDS.GID, status, err)
	}
	entries, err := harness.ObserveCluster(m.Cluster).BlocklistEntries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, session := range []cephfs.Session{writerSession, waiterSession} {
		if slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == session.Address }) {
			t.Fatalf("client.%d %s was blocklisted: %+v", session.ID, session.Address, entries)
		}
	}
	t.Logf("CEPHFS_MDS_OSD_PATH_RECEIPT %s mds.%s gid=%d (%s) to osds %v (%v): %s on %s dropped %d packets for %s with filters %s; writer client.%d inode=%d: the data sync returned 0 in %d ms and RADOS object %s held %d bytes; the MDS held %d journal writes to osd.%d, waiter client.%d's mkdir tid %d at %q for %.1f s and the writer's cap flush %v with the MDS at %d bytes; both sessions stayed open; after Restore the full fsync returned 0 after %d ms and the mkdir 0 after %d ms (inode %d), both within %s, history %v; the writer read %d bytes through its fd and found the directory; reader client.%d read every byte; no blocklist entry, same MDS GID",
		m.Version, m.MDS.Name, m.MDS.GID, mdsIP, cutIDs, peers, lease.ContainerID[:12], lease.Interface, cut.Drops, window.Round(time.Millisecond), cut.Filters,
		opened.Client, opened.Inode, dataSynced.ElapsedMS, stored.Object, stored.Size, len(state.journal), journal.Acting[0], waiting.Client, tid,
		state.mkdir.TypeData.FlagPoint, state.mkdir.Age, state.caps.FlushingTIDs, state.mdsSize, fullSync.ElapsedMS, made.ElapsedMS, made.DirInode,
		returned, historic.Value.Events(), len(whole), independent.Client)
}

// cephFSIndependentRead is what CephFSIndependentReader printed.
type cephFSIndependentRead struct {
	Client uint64 `json:"client"`
	Inode  uint64 `json:"inode"`
	Size   int64  `json:"size"`
	Data   string `json:"data"`
}

// readCephFSIndependently reads path through a new mount in the control
// container.
func readCephFSIndependently(t *testing.T, ctx context.Context, m *harness.CephFSSingleMDS, path string) cephFSIndependentRead {
	t.Helper()
	readCtx, stop := context.WithTimeout(ctx, 2*time.Minute)
	defer stop()
	read, err := m.Cluster.RunNativeCommand(readCtx, harness.ToolkitContainer(t, m.Cluster), ceph.NativeCommand{
		Argv: []string{"python3", "-c", harness.CephFSIndependentReader, m.FS.FilesystemName, path}, Timeout: 2 * time.Minute,
		MaxStdout: 1 << 20, MaxStderr: 64 << 10, Label: "independent CephFS reader",
	})
	var independent cephFSIndependentRead
	if err != nil || read.ExitCode != 0 || !read.Complete() || json.Unmarshal([]byte(harness.LastJSONLine(string(read.Stdout))), &independent) != nil {
		t.Fatalf("independent reader: exit=%d err=%v out=%.512s stderr=%.2048s", read.ExitCode, err, read.Stdout, read.Stderr)
	}
	return independent
}
