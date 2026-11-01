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
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfssession "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/session"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

// mdsPathDirection says which side of a client's MDS path is cut.
type mdsPathDirection int

const (
	// mdsToClient drops the MDS's packets to the client, TCP
	// acknowledgements included: the MDS's replies never arrive, and what the
	// client sends reaches the MDS only until its first retransmission
	// timeout, after which the client's TCP holds every later byte unsent.
	mdsToClient mdsPathDirection = iota
	// clientToMDS drops the client's packets to the MDS: nothing the client
	// sends reaches the MDS, while the MDS's messages reach the client.
	clientToMDS
)

func (d mdsPathDirection) String() string {
	if d == mdsToClient {
		return "mds-to-client"
	}
	return "client-to-mds"
}

// trigger is how the actor sends its mkdir: the moment its frozen container
// thaws when the MDS's packets are cut, at the gate when the client's are.
func (d mdsPathDirection) trigger() string {
	if d == mdsToClient {
		return "thaw"
	}
	return "gate"
}

// cutFreeze is how long the client stays frozen around the MDS's cut; the
// actor takes a gap of 2 s as the thaw.
const cutFreeze = 4 * time.Second

// requireMDSMkdirExposed waits, during the MDS's cut, until the MDS lists the
// client's mkdir tid in flight or in its history, and returns where it was
// found. If it never does, the client's TCP socket to the MDS is in the
// failure: a send queue that holds bytes under a running retransmission
// timer means the mkdir never left the client.
func requireMDSMkdirExposed(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, mds, mdsAddr string, clientID, tid uint64) string {
	t.Helper()
	seen, err := harness.Observe(ctx, 30*time.Second, 15*time.Second, 500*time.Millisecond, func(probeCtx context.Context) (string, bool, error) {
		for _, dump := range []string{"dump_ops_in_flight", "dump_historic_ops"} {
			data, err := cluster.Ceph(probeCtx, "tell", "mds."+mds, dump)
			if err != nil {
				return "", false, fmt.Errorf("%s of mds.%s: %w", dump, mds, err)
			}
			ops, err := harness.ParseMDSRequestOps(data)
			if err != nil {
				return "", false, fmt.Errorf("%s of mds.%s: %w", dump, mds, err)
			}
			if op, ok := harness.FindMDSRequest(ops, clientID, "mkdir", tid); ok {
				return fmt.Sprintf("%s %v", dump, op.Events()), true, nil
			}
		}
		return "", false, nil
	})
	if err != nil {
		t.Fatalf("mds.%s never listed client.%d's pending mkdir tid %d during the cut, so the cut held it before the MDS (client socket to %s: %s): %v",
			mds, clientID, tid, mdsAddr, clientSocket(ctx, cluster, client, mdsAddr), err)
	}
	return seen.Value
}

// clientSocket returns the client container's /proc/net/tcp entry for its
// connection to the IPv4 address and port addr, whose tx_queue and
// retransmission columns say whether bytes wait unacknowledged or unsent.
func clientSocket(ctx context.Context, cluster *ceph.Container, client testcontainers.Container, addr string) string {
	peer, err := netip.ParseAddrPort(addr)
	if err != nil || !peer.Addr().Is4() {
		return fmt.Sprintf("address %q is not IPv4", addr)
	}
	b := peer.Addr().As4()
	remote := fmt.Sprintf("%02X%02X%02X%02X:%04X", b[3], b[2], b[1], b[0], peer.Port())
	read, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv: []string{"cat", "/proc/net/tcp"}, Timeout: 15 * time.Second,
		MaxStdout: 256 << 10, MaxStderr: 4 << 10, Label: "client TCP sockets",
	})
	if err != nil || read.ExitCode != 0 {
		return fmt.Sprintf("unread: exit=%d err=%v", read.ExitCode, err)
	}
	var lines []string
	for line := range strings.SplitSeq(string(read.Stdout), "\n") {
		if fields := strings.Fields(line); len(fields) > 6 && fields[2] == remote {
			lines = append(lines, strings.Join(fields[1:7], " "))
		}
	}
	if len(lines) == 0 {
		return "no socket to " + remote
	}
	return "local remote st tx_queue:rx_queue tr:when retrnsmt = " + strings.Join(lines, "; ")
}

// cephFSPathCutActor keeps one libcephfs mount, with its own admin socket,
// and one O_RDWR fd open for its whole run. It writes and fsyncs the base
// bytes and publishes receipt 1. At the "metadata" gate it starts a thread
// that creates a new directory and publishes receipt 4, with its inode, when
// the mkdir returns; the main thread publishes receipt 2 once the thread runs.
// With the trigger "thaw" the thread first waits until its own 10 ms sleep
// takes 2 s or more, which a freeze of the container does, so that the mkdir
// is sent the moment the container thaws; it reports that gap as frozen_ms.
// With "gate" it sends the mkdir at once.
// At the "data" gate the main thread appends the tail through the same fd and
// syncs the data only (fsync with syncdataonly), which needs the OSDs and no
// MDS reply, and publishes receipt 3. After the "finish" gate it joins the
// thread, reads the file back through the fd, closes and unmounts, and
// publishes receipt 5. Arguments: filesystem, work directory, token,
// directory, file, new directory, admin socket, base and tail in hex, and
// the trigger.
const cephFSPathCutActor harness.CephFSActorScript = harness.CephFSActorHelpers + `
import threading
directory, path, created, asok = sys.argv[4].encode(), sys.argv[5].encode(), sys.argv[6].encode(), sys.argv[7]
base, tail, trigger = bytes.fromhex(sys.argv[8]), bytes.fromhex(sys.argv[9]), sys.argv[10]
assert trigger in ("gate", "thaw")
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.conf_set("admin_socket", asok)
fs.mount(filesystem_name=name.encode())
fs.mkdir(directory, 0o755)
# The new directory is made relative to the working directory, so the
# request that goes out into the cut is the mkdir itself: an absolute path
# first looks up every component whose dentry lease ran out, and such a
# lookup is what stays pending instead.
assert os.path.dirname(created) == directory
fs.chdir(directory)
fd = fs.open(path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
fs.write(fd, base, 0)
fs.fsync(fd, 0)
stat = fs.fstat(fd)
publish(1, client=fs.get_instance_id(), inode=stat.st_ino, size=stat.st_size)
gate("metadata")
def thawed():
    deadline = time.monotonic() + 240
    while time.monotonic() < deadline:
        before = time.monotonic()
        time.sleep(0.01)
        gap = time.monotonic() - before
        if gap >= 2:
            return int(gap * 1000)
    return 0
def metadata():
    result = {}
    if trigger == "thaw":
        result["frozen_ms"] = thawed()
        if not result["frozen_ms"]:
            publish(4, errno=-1, error="the container was not frozen")
            return
    started = time.monotonic()
    try:
        fs.mkdir(os.path.basename(created), 0o755)
        result["inode"] = fs.stat(created).st_ino
    except cephfs.Error as error:
        result.update(errno=getattr(error, "errno", None) or -1, error=str(error)[:200])
    result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
    publish(4, **result)
worker = threading.Thread(target=metadata, daemon=True)
worker.start()
publish(2)
gate("data")
started = time.monotonic()
result = {}
try:
    fs.write(fd, tail, len(base))
    fs.fsync(fd, 1)
except cephfs.Error as error:
    result.update(errno=getattr(error, "errno", None) or -1, error=str(error)[:200])
result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
publish(3, **result)
gate("finish")
worker.join()
closing = {}
try:
    stat = fs.fstat(fd)
    closing.update(size=stat.st_size, full=fs.read(fd, 0, stat.st_size).hex())
    fs.close(fd)
    fs.unmount()
except cephfs.Error as error:
    closing.update(close_errno=getattr(error, "errno", None) or -1)
fs.shutdown()
publish(5, closed=True, **closing)
`

// cephFSObjectReader reads a file's first data object straight from RADOS,
// without the MDS, and prints its size and bytes. Arguments: data pool and
// the file's inode.
const cephFSObjectReader = `import json,rados,sys
c=rados.Rados(conffile="/etc/ceph/ceph.conf")
c.connect(timeout=30)
try:
 io=c.open_ioctx(sys.argv[1])
 name="%x.00000000"%int(sys.argv[2])
 size,_=io.stat(name)
 print(json.dumps({"object":name,"size":size,"data":io.read(name,size+1,0).hex()}))
 io.close()
finally:
 c.shutdown()`

// cephFSPathCutReader checks the result through a mount of its own in
// another container: the file's bytes and the new directory's inode.
const cephFSPathCutReader = `import cephfs,json,os,sys
fs=cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout","30")
fs.mount(filesystem_name=sys.argv[1].encode())
try:
 stat=fs.stat(sys.argv[2].encode())
 fd=fs.open(sys.argv[2].encode(),os.O_RDONLY,0)
 data=fs.read(fd,0,stat.st_size+1)
 fs.close(fd)
 created=fs.stat(sys.argv[3].encode())
 print(json.dumps({"client":fs.get_instance_id(),"size":stat.st_size,"data":data.hex(),"created":created.st_ino}))
finally:
 fs.unmount()
 fs.shutdown()`

// mdsPathSample is one look at both sides of the session during the cut.
type mdsPathSample struct {
	At           time.Duration
	MDSState     string
	ClientState  string
	RenewSeq     uint64
	CapTTL       string
	Pending      []harness.ClientMDSRequest
	MDSInodeSize uint64
}

func (s mdsPathSample) String() string {
	ops := make([]string, 0, len(s.Pending))
	for _, request := range s.Pending {
		ops = append(ops, fmt.Sprintf("%s/%d", request.Op, request.TID))
	}
	return fmt.Sprintf("+%s mds=%s client=%s renew_seq=%d cap_ttl=%s pending=%v mds_size=%d", s.At, s.MDSState, s.ClientState, s.RenewSeq, s.CapTTL, ops, s.MDSInodeSize)
}

// containerIP returns a container's address on network.
func containerIP(t *testing.T, ctx context.Context, ctr testcontainers.Container, network string) netip.Addr {
	t.Helper()
	inspect, err := ctr.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := inspect.NetworkSettings.Networks[network]
	if endpoint == nil || !endpoint.IPAddress.IsValid() {
		t.Fatalf("container %s has no endpoint on %s", ctr.GetContainerID(), network)
	}
	return endpoint.IPAddress
}

// mdsOps reads one of an MDS's request dumps.
func mdsOps(t *testing.T, ctx context.Context, cluster *ceph.Container, mds, dump string) []harness.MDSRequestOp {
	t.Helper()
	data, err := cluster.Ceph(ctx, "tell", "mds."+mds, dump)
	if err != nil {
		t.Fatalf("%s of mds.%s: %v", dump, mds, err)
	}
	ops, err := harness.ParseMDSRequestOps(data)
	if err != nil {
		t.Fatalf("%s of mds.%s: %v: %.1024s", dump, mds, err, data)
	}
	return ops
}

// findClientRequest returns the client's request with tid, of any operation.
func findClientRequest(ops []harness.MDSRequestOp, client, tid uint64) (harness.MDSRequestOp, bool) {
	for _, op := range ops {
		reqid := op.TypeData.ReqID
		if reqid.Entity.Type == "client" && reqid.Entity.Num == client && reqid.TID == tid {
			return op, true
		}
	}
	return harness.MDSRequestOp{}, false
}

// testCephFSClientMDSPathCut cuts one direction of the path between a
// libcephfs client and the filesystem's one active MDS with a peer-selected
// netem lease (Loss 100), with the session timeout at 30 s. The tuple comes
// from native state: the MDS's v2 address from the FSMap, the client's from
// the MDS's session, both matched to their container endpoints. With the
// cut applied, the actor's mkdir goes out and stays in the client's
// mds_requests, while an append to its open file and a data-only fsync
// return and RADOS holds the new bytes. For 50 s, past the session timeout,
// both sides keep the session open: the client keeps renewing, cap_renew_seq
// moves on and cap_ttl stays, and the MDS, which defers marking a session
// stale while nobody waits on its caps, keeps it open. With the MDS's packets
// cut, the MDS has completed and journaled the mkdir; with the client's cut,
// it never saw it.
//
// The MDS's cut drops its TCP acknowledgements too. The client sends
// client_metrics every second, so anything it sends after the cut starts a
// retransmission timer, and once that fires the client's TCP sends no new
// byte: a mkdir issued later stays in its socket and never reaches the MDS,
// which then has no record of it. So for that direction the client container
// is frozen with fault.Pause before the cut and thawed after it, and the
// actor sends the mkdir the moment it thaws, together with whatever the
// client's own tick sends then, before any retransmission timeout. The MDS
// must then list the mkdir during the cut, which establishes that it was
// exposed before the window starts. After the restore the mkdir returns, the client holds no
// request, the session is open with the same GID and not blocklisted, and a
// client in another container reads the bytes and the new directory.
func testCephFSClientMDSPathCut(t *testing.T, direction mdsPathDirection) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	netem := harness.NetemImage(t)
	m := harness.StartCephFSSingleMDS(t, ctx)
	cluster, client, fs := m.Cluster, m.Client, m.FS

	// With a 30 s session timeout the client renews every 10 s, and the MDS
	// marks a session that it has not heard from stale after 30 s. Autoclose
	// stays far beyond the window, and no other client wants the caps, so
	// the MDS has no reason to evict.
	limits := cephfssession.Limits{Timeout: 30 * time.Second, Autoclose: 5 * time.Minute}
	change, err := cephfssession.OverrideTimeouts(ctx, fs, limits)
	if change != nil {
		t.Cleanup(func() {
			restoreCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := change.Restore(restoreCtx); err != nil {
				t.Errorf("restore session timeouts: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal("shorten session timeouts:", err)
	}

	base := bytes.Repeat([]byte("mds-path-base\n"), 512)
	tail := append([]byte("\x00mds-path-cut\xff"), bytes.Repeat([]byte{0x3c, 0xc3}, 1024)...)
	whole := append(append([]byte{}, base...), tail...)
	const workdir = "/tmp/mds-path-actor"
	asok := workdir + "/client.asok"
	actor := harness.StartCephFSActor(t, ctx, cluster, client, cephFSPathCutActor, fs.FilesystemName, workdir,
		"/pathcut", "/pathcut/file", "/pathcut/created", asok, hex.EncodeToString(base), hex.EncodeToString(tail), direction.trigger())
	baseline := actor.Receipt(t, ctx, 1)
	if baseline.Client == 0 || baseline.Inode == 0 || baseline.Size != int64(len(base)) {
		t.Fatalf("baseline receipt = %+v", baseline)
	}
	// The client renews with the MDS's 30 s timeout: cap_ttl, which only
	// the MDS's renew reply sets, is the last renew request plus 30 s.
	renewed, err := harness.Observe(ctx, time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) (clientMDSSessions, bool, error) {
		sessions, err := readClientMDSSessions(probeCtx, cluster, client, asok)
		if err != nil {
			return sessions, false, err
		}
		ttl, err := renewalTTL(sessions.Sessions[0].LastRenew, sessions.Sessions[0].CapTTL)
		return sessions, err == nil && ttl == limits.Timeout && sessions.Sessions[0].State == "open", nil
	})
	if err != nil {
		t.Fatalf("the client does not renew with a 30 s timeout: %+v, %v", renewed.Value, err)
	}

	// The tuple: the MDS's v2 address from the FSMap, the client's address
	// from the MDS's session, and both containers' endpoints.
	rank, err := readAdmissionRank(ctx, cluster, fs.FilesystemName)
	if err != nil {
		t.Fatal(err)
	}
	v2, ok := rank.v2()
	if !ok || rank.GID != m.MDS.GID || rank.Name != m.MDS.Name {
		t.Fatalf("rank 0 is not mds.%s (gid %d) with a v2 address: %+v", m.MDS.Name, m.MDS.GID, rank)
	}
	mdsIP := harness.AddressIP(t, v2.Addr)
	sessions, err := admissionSessions(ctx, cluster, m.MDS.Name)
	if err != nil {
		t.Fatal(err)
	}
	session, ok := sessions[baseline.Client]
	if !ok || session.State != "open" {
		t.Fatalf("mds.%s has no open session of client.%d: %+v", m.MDS.Name, baseline.Client, sessions)
	}
	clientIP := harness.AddressIP(t, session.Entity.Addr.Addr)
	var mdsContainer *cephfs.MDS
	for _, member := range fs.MDSs() {
		if member.ID == m.MDS.Name {
			mdsContainer = member
		}
	}
	if mdsContainer == nil {
		t.Fatalf("mds.%s is not an owned daemon", m.MDS.Name)
	}
	network := harness.StaticFacts(cluster).NetworkName
	if ip := containerIP(t, ctx, mdsContainer, network); ip != mdsIP {
		t.Fatalf("mds.%s advertises %s but its container has %s", m.MDS.Name, mdsIP, ip)
	}
	if ip := containerIP(t, ctx, client, network); ip != clientIP {
		t.Fatalf("client.%d's session is from %s but its container has %s", baseline.Client, clientIP, ip)
	}
	daemons, err := harness.ObserveCluster(cluster).DaemonAddresses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, osd := range daemons.OSDs {
		for _, addr := range osd.Public {
			if harness.AddressIP(t, addr.Addr) == mdsIP {
				t.Fatalf("osd.%d shares the MDS's address %s", osd.ID, mdsIP)
			}
		}
	}

	target, peer := testcontainers.Container(mdsContainer), clientIP
	if direction == clientToMDS {
		target, peer = client, mdsIP
	}
	var frozen *fault.PauseLease
	var pausedAt time.Time
	if direction == mdsToClient {
		// The actor's thread waits for the thaw; nothing is pending yet, so
		// the client's connection to the MDS holds no request when it freezes.
		actor.Open(t, ctx, "metadata")
		actor.Receipt(t, ctx, 2)
		if requests, err := clientRequests(ctx, cluster, client, asok); err != nil || len(requests) != 0 {
			t.Fatalf("the client lists requests before the freeze: %+v, %v", requests, err)
		}
		frozen, err = fault.Pause(ctx, cluster, client)
		if frozen != nil {
			t.Cleanup(func() {
				resumeCtx, stop := context.WithTimeout(context.Background(), time.Minute)
				defer stop()
				if err := frozen.Resume(resumeCtx); err != nil {
					t.Errorf("resume the client: %v", err)
				}
			})
		}
		if err != nil {
			t.Fatal("freeze the client:", err)
		}
		pausedAt = time.Now()
	}
	lease, err := fault.ApplyNetworkProfile(ctx, cluster, target, fault.NetworkProfile{HelperImage: netem, Plane: ceph.PublicNetworkPlane, Peer: peer, Loss: 100})
	if lease != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := lease.Restore(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	cutAt := time.Now()
	if status, err := lease.Status(ctx); err != nil || !status.Applied || status.Loss != 100 {
		t.Fatalf("status of the cut: %+v, %v", status, err)
	}

	// The metadata request goes out into the cut; the client's own
	// admin socket names it.
	if direction == mdsToClient {
		// The actor sends the mkdir when it sees a gap of 2 s; hold the
		// freeze past that.
		if hold := time.Until(pausedAt.Add(cutFreeze)); hold > 0 {
			time.Sleep(hold)
		}
		if err := frozen.Resume(ctx); err != nil {
			t.Fatal("thaw the client:", err)
		}
	} else {
		actor.Open(t, ctx, "metadata")
		actor.Receipt(t, ctx, 2)
	}
	submitted, err := harness.Observe(ctx, 30*time.Second, 20*time.Second, 500*time.Millisecond, func(probeCtx context.Context) ([]harness.ClientMDSRequest, bool, error) {
		requests, err := clientRequests(probeCtx, cluster, client, asok)
		return requests, err == nil && len(requests) > 0, err
	})
	if err != nil {
		t.Fatalf("the client lists no pending request: %+v, %v", submitted.Value, err)
	}
	pending, err := admitPendingMkdir(submitted.Value)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("pending at +%s: %+v", time.Since(cutAt).Round(time.Millisecond), pending)
	var exposure string
	if direction == mdsToClient {
		exposure = requireMDSMkdirExposed(t, ctx, cluster, client, m.MDS.Name, v2.Addr, baseline.Client, pending.TID)
		t.Logf("exposed at +%s: %s", time.Since(cutAt).Round(time.Millisecond), exposure)
	}

	// Data I/O on the open file: the tail is appended and synced to the OSDs.
	actor.Open(t, ctx, "data")
	synced := actor.Receipt(t, ctx, 3)
	syncedAt := time.Since(cutAt).Round(time.Millisecond)
	if synced.Errno != 0 || synced.Error != "" {
		t.Fatalf("data sync during the cut: %+v", synced)
	}
	object := readCephFSObject(t, ctx, cluster, fs.DataPool, baseline.Inode)
	if object.Size != uint64(len(whole)) || object.Data != hex.EncodeToString(whole) {
		t.Fatalf("RADOS object %s holds %d bytes during the cut, want %d", object.Object, object.Size, len(whole))
	}

	// The window: both sides' view of the session, the client's pending
	// request and the MDS's inode size, every 5 s for 50 s, which is longer
	// than the 30 s session timeout.
	var samples []mdsPathSample
	for deadline := cutAt.Add(50 * time.Second); ; {
		sample := mdsPathSample{At: time.Since(cutAt).Round(time.Second)}
		raw := harness.CephFSRawSession(t, ctx, cluster, m.MDS.Name, baseline.Client)
		_ = json.Unmarshal(raw["state"], &sample.MDSState)
		own, err := readClientMDSSessions(ctx, cluster, client, asok)
		if err != nil {
			t.Fatal(err)
		}
		sample.ClientState, sample.RenewSeq, sample.CapTTL = own.Sessions[0].State, own.Sessions[0].RenewSeq, own.Sessions[0].CapTTL
		sample.Pending = harness.ClientMDSRequests(t, ctx, cluster, client, asok)
		sample.MDSInodeSize = m.MDSInodeSize(t, ctx, m.MDS.Name, baseline.Inode)
		if actor.Published(ctx, 4) {
			t.Fatalf("the metadata request returned during the cut: %+v", actor.Receipt(t, ctx, 4))
		}
		if sample.MDSState != "open" || sample.ClientState != "open" || len(sample.Pending) != 1 || sample.Pending[0].TID != pending.TID {
			t.Fatalf("during the cut: %s; want both sides open and the client holding tid %d", sample, pending.TID)
		}
		samples = append(samples, sample)
		t.Logf("during the cut: %s", sample)
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Second)
	}
	// The client keeps sending renewals, but no renew reply reaches it:
	// cap_renew_seq moves on while cap_ttl, which only a reply sets, stays.
	first, last := samples[0], samples[len(samples)-1]
	for _, sample := range samples {
		if sample.CapTTL != first.CapTTL {
			t.Fatalf("cap_ttl moved during the cut: %s, first %s", sample, first)
		}
	}
	if last.RenewSeq < first.RenewSeq+3 {
		t.Fatalf("the client did not keep renewing: %s, first %s", last, first)
	}
	// The MDS defers marking a session stale while nobody waits on its
	// caps, so it stays open past the 30 s session timeout either way.
	deferStale := m.DaemonConfig(t, ctx, "mds_defer_session_stale")
	if deferStale != "true" {
		t.Fatalf("mds.%s runs with mds_defer_session_stale=%s", m.MDS.Name, deferStale)
	}
	inFlight, inFlightFound := findClientRequest(mdsOps(t, ctx, cluster, m.MDS.Name, "dump_ops_in_flight"), baseline.Client, pending.TID)
	historic, historicFound := findClientRequest(mdsOps(t, ctx, cluster, m.MDS.Name, "dump_historic_ops"), baseline.Client, pending.TID)
	t.Logf("mds.%s for tid %d: in flight %t %q %v; historic %t %q %v", m.MDS.Name, pending.TID, inFlightFound, inFlight.Description, inFlight.Events(), historicFound, historic.Description, historic.Events())
	switch {
	case inFlightFound:
		t.Fatalf("mds.%s holds tid %d in flight: %v", m.MDS.Name, pending.TID, inFlight.Events())
	case direction == mdsToClient:
		// The request reached the MDS, which created the directory,
		// journaled it and replied; only the replies are lost.
		if err := requireMDSMkdirCompleted(historic, historicFound, baseline.Client, pending.TID, "created"); err != nil {
			t.Fatalf("mds.%s did not complete the client's mkdir, which it held during the cut (%s): %v", m.MDS.Name, exposure, err)
		}
	case direction == clientToMDS && historicFound:
		t.Fatalf("mds.%s saw the client's mkdir through the cut: %q", m.MDS.Name, historic.Description)
	}
	cut, err := lease.Status(ctx)
	if err != nil || !cut.Applied || cut.Drops == 0 {
		t.Fatalf("netem status during the cut: %+v, %v", cut, err)
	}
	if err := lease.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	restored := time.Now()

	created := actor.Receipt(t, ctx, 4)
	took := time.Since(restored).Round(time.Second)
	if created.Errno != 0 || created.Error != "" || created.Inode == 0 || (direction == mdsToClient) != (created.FrozenMS > 0) {
		t.Fatalf("metadata receipt after the restore: %+v", created)
	}
	// The mkdir can return on the early reply; the client drops the request
	// when the safe reply arrives.
	settled, err := harness.Observe(ctx, 2*time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) ([]harness.ClientMDSRequest, bool, error) {
		requests, err := clientRequests(probeCtx, cluster, client, asok)
		return requests, err == nil && len(requests) == 0, err
	})
	if err != nil {
		t.Fatalf("the client still lists requests after the restore: %+v, %v", settled.Value, err)
	}
	safeAfter := time.Since(restored).Round(time.Second)
	reopened, err := harness.Observe(ctx, time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) (string, bool, error) {
		sessions, err := admissionSessions(probeCtx, cluster, m.MDS.Name)
		if err != nil {
			return "", false, err
		}
		current, ok := sessions[baseline.Client]
		if !ok {
			return "gone", false, fmt.Errorf("mds.%s has no session of client.%d", m.MDS.Name, baseline.Client)
		}
		return current.State, current.State == "open", nil
	})
	if err != nil {
		t.Fatalf("client.%d's session is not open after the restore: %s, %v", baseline.Client, reopened.Value, err)
	}
	entries, err := harness.ObserveCluster(cluster).BlocklistEntries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	address := session.Entity.Addr.Addr + "/" + strconv.FormatUint(session.Entity.Addr.Nonce, 10)
	if slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == address }) {
		t.Fatalf("client.%d (%s) is blocklisted", baseline.Client, address)
	}
	after, err := m.FS.MDSStatus(ctx)
	if err != nil || len(after.Active) != 1 || after.Active[0].GID != m.MDS.GID {
		t.Fatalf("rank 0 changed: %+v, %v", after, err)
	}

	readCtx, stop := context.WithTimeout(ctx, 2*time.Minute)
	read, err := cluster.RunNativeCommand(readCtx, harness.ToolkitContainer(t, cluster), ceph.NativeCommand{
		Argv: []string{"python3", "-c", cephFSPathCutReader, fs.FilesystemName, "/pathcut/file", "/pathcut/created"}, Timeout: 2 * time.Minute,
		MaxStdout: 1 << 20, MaxStderr: 64 << 10, Label: "independent CephFS reader",
	})
	stop()
	var independent struct {
		Client  uint64 `json:"client"`
		Size    int64  `json:"size"`
		Data    string `json:"data"`
		Created uint64 `json:"created"`
	}
	if err != nil || read.ExitCode != 0 || json.Unmarshal([]byte(harness.LastJSONLine(string(read.Stdout))), &independent) != nil {
		t.Fatalf("independent reader: exit=%d err=%v out=%.512s stderr=%.1024s", read.ExitCode, err, read.Stdout, read.Stderr)
	}
	if independent.Client == baseline.Client || independent.Size != int64(len(whole)) || independent.Data != hex.EncodeToString(whole) || independent.Created != created.Inode {
		t.Fatalf("independent reader saw client=%d size=%d created=%d, want %d bytes and inode %d", independent.Client, independent.Size, independent.Created, len(whole), created.Inode)
	}
	actor.Open(t, ctx, "finish")
	if closed := actor.Receipt(t, ctx, 5); !closed.Closed || closed.CloseErrno != 0 || closed.Full != hex.EncodeToString(whole) {
		t.Fatalf("closing receipt: closed=%t errno=%d size=%d", closed.Closed, closed.CloseErrno, closed.Size)
	}
	actor.Wait(t, ctx)

	timeline := make([]string, 0, len(samples))
	for _, sample := range samples {
		timeline = append(timeline, sample.String())
	}
	t.Logf("CEPHFS_MDS_PATH_RECEIPT %s %s: %s on %s drops packets to %s (%d dropped); client.%d (%s) and mds.%s gid %d (%s); pending %s tid %d at the MDS: in flight %t, historic %t %v, during the cut %q (sent after a freeze of %d ms); data sync returned in %d ms at +%s and RADOS object %s held %d bytes; window (session_timeout 30 s, mds_defer_session_stale=%s): %s; after the restore the mkdir returned in %s (%d ms in all) and the client held no request after %s, session %s, not blocklisted, same GID; reader client.%d read %d bytes and the new directory",
		m.Version, direction, lease.ContainerID[:12], lease.Interface, peer, cut.Drops, baseline.Client, clientIP, m.MDS.Name, m.MDS.GID, mdsIP,
		pending.Op, pending.TID, inFlightFound, historicFound, historic.Events(), exposure, created.FrozenMS, synced.ElapsedMS, syncedAt, object.Object, object.Size,
		deferStale, strings.Join(timeline, "; "), took, created.ElapsedMS, safeAfter, reopened.Value, independent.Client, independent.Size)
}

// clientRequests is ClientMDSRequests for an observe probe: it returns its
// error instead of failing the test.
func clientRequests(ctx context.Context, cluster *ceph.Container, client testcontainers.Container, asok string) ([]harness.ClientMDSRequest, error) {
	result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv: []string{"ceph", "--admin-daemon", asok, "mds_requests"}, Timeout: 15 * time.Second,
		MaxStdout: 256 << 10, MaxStderr: 16 << 10, Label: "client mds_requests",
	})
	if err != nil || result.ExitCode != 0 {
		return nil, fmt.Errorf("client mds_requests: exit=%d err=%v stderr=%.512s", result.ExitCode, err, result.Stderr)
	}
	return harness.ParseClientMDSRequests(result.Stdout)
}

type cephFSObject struct {
	Object string `json:"object"`
	Size   uint64 `json:"size"`
	Data   string `json:"data"`
}

// readCephFSObject reads a file's first data object from RADOS in the
// control container.
func readCephFSObject(t *testing.T, ctx context.Context, cluster *ceph.Container, pool string, inode uint64) cephFSObject {
	t.Helper()
	readCtx, stop := context.WithTimeout(ctx, time.Minute)
	defer stop()
	read, err := cluster.RunNativeCommand(readCtx, harness.ToolkitContainer(t, cluster), ceph.NativeCommand{
		Argv: []string{"python3", "-c", cephFSObjectReader, pool, strconv.FormatUint(inode, 10)}, Timeout: time.Minute,
		MaxStdout: 1 << 20, MaxStderr: 64 << 10, Label: "RADOS object reader",
	})
	var object cephFSObject
	if err != nil || read.ExitCode != 0 || json.Unmarshal([]byte(harness.LastJSONLine(string(read.Stdout))), &object) != nil {
		t.Fatalf("RADOS object reader: exit=%d err=%v out=%.512s stderr=%.1024s", read.ExitCode, err, read.Stdout, read.Stderr)
	}
	return object
}
