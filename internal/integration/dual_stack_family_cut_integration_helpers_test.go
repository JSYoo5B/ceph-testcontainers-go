//go:build all || integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"io"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// dualStackActor holds one librados session whose MON session
// mon_host_override pins to argv[2], and runs operations on request. For
// each request n the test writes /tmp/<tag>.go.<n>: {"id", "op", "wait"}
// starts the operation op (mon: an fsid MON command, mgr: a pg stat MGR
// command, write: write_full of the hex "data" to "name", read: read "name")
// in its own thread, and {"id", "op": "join", "wait"} waits again for an
// operation started earlier. The actor waits up to "wait" seconds and writes
// to /tmp/<tag>.<n>.json whether the operation is done, its errno, the
// SHA-256 of what it read, its duration, the client's instance ID and the
// process's own established TCP connections. Operation timeouts are 120
// seconds, so an operation on a cut path stays pending instead of failing.
const dualStackActor = dualStackSockets + `import hashlib,sys,threading,rados
tag,mon_host,pool=sys.argv[1:4]
# Both actors share the client container's network namespace, so keep only
# the connections whose socket inode is one of this process's descriptors.
def own_sockets():
    inodes=set()
    for fd in os.listdir('/proc/self/fd'):
        try:
            link=os.readlink('/proc/self/fd/'+fd)
        except OSError:
            continue
        if link.startswith('socket:['):
            inodes.add(link[8:-1])
    found=[]
    def endpoint(text,v6):
        host,port=text.split(':')
        raw=bytes.fromhex(host)
        raw=b''.join(raw[i:i+4][::-1] for i in range(0,len(raw),4))
        ip=socket.inet_ntop(socket.AF_INET6 if v6 else socket.AF_INET,raw)
        if ip.startswith('::ffff:'):
            ip=ip[7:]
        return [ip,int(port,16)]
    for path,v6 in (('/proc/net/tcp',False),('/proc/net/tcp6',True)):
        for line in open(path).read().splitlines()[1:]:
            f=line.split()
            if f[3]=='01' and f[9] in inodes:
                found.append(endpoint(f[1],v6)+endpoint(f[2],v6))
    return found
c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'mon_host':mon_host,'mon_host_override':mon_host,'rados_mon_op_timeout':'120','rados_osd_op_timeout':'120','client_mount_timeout':'30'})
c.connect()
results={}
def run(key,req):
    start=time.monotonic()
    r={'errno':0}
    try:
        if req['op']=='mon':
            ret,_,_=c.mon_command(json.dumps({'prefix':'fsid','format':'json'}),b'')
            r['errno']=abs(ret)
        elif req['op']=='mgr':
            ret,_,_=c.mgr_command(json.dumps({'prefix':'pg stat','format':'json'}),b'')
            r['errno']=abs(ret)
        elif req['op']=='write':
            io.write_full(req['name'],bytes.fromhex(req['data']))
        elif req['op']=='read':
            r['sha']=hashlib.sha256(io.read(req['name'],1<<20)).hexdigest()
    except rados.Error as e:
        r['errno']=abs(getattr(e,'errno',None) or 1)
    r['ms']=(time.monotonic()-start)*1000
    results[key]=r
try:
    io=c.open_ioctx(pool)
    try:
        threads={}
        n=0
        deadline=time.time()+1000
        while True:
            go='/tmp/%s.go.%d'%(tag,n)
            while not os.path.exists(go):
                if os.path.exists('/tmp/'+tag+'.release'):
                    assert not any(th.is_alive() for th in threads.values()),'operations still pending'
                    raise SystemExit(0)
                assert time.time()<deadline,'not released'
                time.sleep(0.2)
            req=json.load(open(go))
            if req['op']!='join':
                threads[req['id']]=threading.Thread(target=run,args=(req['id'],req),daemon=True)
                threads[req['id']].start()
            th=threads[req['id']]
            th.join(req['wait'])
            out={'id':req['id'],'done':not th.is_alive(),'instance':c.get_instance_id(),'sockets':own_sockets()}
            if out['done']:
                out.update(results[req['id']])
            path='/tmp/%s.%d.json'%(tag,n)
            with open(path+'.tmp','w') as f:
                json.dump(out,f)
            os.rename(path+'.tmp',path)
            n+=1
    finally:
        io.close()
finally:
    c.shutdown()
`

// dualStackProbe connects to each endpoint in argv with a 3-second timeout
// and prints the first bytes it received, or the error.
const dualStackProbe = `import json,socket,sys
out={}
for endpoint in sys.argv[1:]:
    host,port=endpoint.rsplit(':',1)
    try:
        s=socket.create_connection((host.strip('[]'),int(port)),timeout=3)
        try:
            out[endpoint]=s.recv(9).decode(errors='replace')
        finally:
            s.close()
    except OSError as e:
        out[endpoint]='error:'+type(e).__name__
print(json.dumps(out))
`

// dualStackOracle reads the objects argv[2:] of pool argv[1] with a new
// librados session and prints the SHA-256 of each.
const dualStackOracle = `import hashlib,json,sys,rados
c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'rados_osd_op_timeout':'30'})
c.connect()
try:
    io=c.open_ioctx(sys.argv[1])
    print(json.dumps({name:hashlib.sha256(io.read(name,1<<20)).hexdigest() for name in sys.argv[2:]}))
    io.close()
finally:
    c.shutdown()
`

// actorResult is the actor's report of one request.
type actorResult struct {
	ID       string  `json:"id"`
	Done     bool    `json:"done"`
	Errno    int     `json:"errno"`
	SHA      string  `json:"sha"`
	MS       float64 `json:"ms"`
	Instance uint64  `json:"instance"`
	session  *dualStackSession
}

func (r actorResult) String() string {
	if !r.Done {
		return r.ID + ":pending"
	}
	return fmt.Sprintf("%s:errno=%d/%.0fms", r.ID, r.Errno, r.MS)
}

// actorRequests holds, by ID, the requests that started an actor's
// operations.
type actorRequests map[string]map[string]any

// check validates result as the actor's report of request: whether the
// operation is done, its errno and, for a read, the SHA-256 of what it read
// against want. A join is checked against the request that started the
// operation it waits for, so a read that completes only after Restore must
// still return the bytes of the object it started to read. For a done read,
// check returns the SHA-256 it compared.
func (started actorRequests) Check(request map[string]any, result actorResult, done bool, want map[string]string) (string, error) {
	id, _ := request["id"].(string)
	origin := request
	if request["op"] == "join" {
		var ok bool
		if origin, ok = started[id]; !ok {
			return "", fmt.Errorf("join %s: no operation was started with that ID", id)
		}
	} else {
		started[id] = request
	}
	if result.ID != id {
		return "", fmt.Errorf("%v: report for %q", request, result.ID)
	}
	if result.Done != done || (done && result.Errno != 0) {
		return "", fmt.Errorf("%v: %+v, want done=%v", request, result, done)
	}
	if !done || origin["op"] != "read" {
		return "", nil
	}
	name, _ := origin["name"].(string)
	if expected, ok := want[name]; !ok || result.SHA != expected {
		return "", fmt.Errorf("%s %s of %s: SHA-256 %q, want %q", id, request["op"], name, result.SHA, expected)
	}
	return fmt.Sprintf("%s:sha256(%s)=%s", id, name, result.SHA), nil
}

// familyActor is a running dualStackActor process.
type familyActor struct {
	ctr     testcontainers.Container
	tag     string
	n       int
	done    chan heldResult
	started actorRequests
}

func startFamilyActor(ctx context.Context, ctr testcontainers.Container, tag, monHost, pool string) *familyActor {
	actor := &familyActor{ctr: ctr, tag: tag, done: make(chan heldResult, 1), started: actorRequests{}}
	go func() {
		code, reader, err := ctr.Exec(ctx, []string{"timeout", "1100", "python3", "-c", dualStackActor, tag, monHost, pool}, tcexec.Multiplexed())
		var output []byte
		if err == nil {
			output, _ = io.ReadAll(reader)
		}
		actor.done <- heldResult{code, output, err}
	}()
	return actor
}

// call sends one request and returns the actor's report.
func (a *familyActor) call(t *testing.T, ctx context.Context, request map[string]any) actorResult {
	t.Helper()
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	n := a.n
	a.n++
	trigger := fmt.Sprintf("/tmp/%s.go.%d", a.tag, n)
	if err := a.ctr.CopyToContainer(ctx, data, trigger+".tmp", 0o644); err != nil {
		t.Fatal(err)
	}
	if code, _, err := a.ctr.Exec(ctx, []string{"mv", trigger + ".tmp", trigger}); err != nil || code != 0 {
		t.Fatalf("%s request %d: exit %d: %v", a.tag, n, code, err)
	}
	wait, _ := request["wait"].(int)
	deadline := time.Now().Add(time.Duration(wait)*time.Second + time.Minute)
	for {
		select {
		case result := <-a.done:
			t.Fatalf("%s ended at request %d: exit %d: %s %v", a.tag, n, result.code, result.output, result.err)
		default:
		}
		code, reader, err := a.ctr.Exec(ctx, []string{"cat", fmt.Sprintf("/tmp/%s.%d.json", a.tag, n)}, tcexec.Multiplexed())
		if err == nil && code == 0 {
			output, _ := io.ReadAll(reader)
			var result actorResult
			if err := json.Unmarshal(output, &result); err != nil {
				t.Fatalf("%s request %d: %s", a.tag, n, output)
			}
			tag := fmt.Sprintf("%s-%s", a.tag, result.ID)
			result.session = &dualStackSession{client: a.ctr, tag: tag, sockets: parseSocketListing(t, tag, output)}
			return result
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not answer request %d", a.tag, n)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (a *familyActor) release(t *testing.T, ctx context.Context) {
	t.Helper()
	if code, _, err := a.ctr.Exec(ctx, []string{"touch", "/tmp/" + a.tag + ".release"}); err != nil || code != 0 {
		t.Fatalf("release %s: exit %d: %v", a.tag, code, err)
	}
	result := <-a.done
	if result.err != nil || result.code != 0 {
		t.Fatalf("%s: exit %d: %s %v", a.tag, result.code, result.output, result.err)
	}
}

// familyCutState is what must not change because of a cut: MON quorum,
// up OSDs, the active MGR and the blocklist.
type familyCutState struct {
	quorum    []string
	up        []int
	mgrGID    uint64
	blocklist int
}

func readFamilyCutState(t *testing.T, ctx context.Context, cluster *ceph.Container) familyCutState {
	t.Helper()
	quorum, err := harness.ObserveCluster(cluster).QuorumStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	addresses, err := harness.ObserveCluster(cluster).DaemonAddresses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var up []int
	for _, osd := range addresses.OSDs {
		if osd.Up && osd.In {
			up = append(up, osd.ID)
		}
	}
	status, err := harness.ObserveCluster(cluster).ManagerStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	output, err := cluster.Ceph(ctx, "osd", "blocklist", "ls", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	// The command prints one JSON list per kind of entry (Tentacle prints the
	// address and the range blocklist, "[][]" when both are empty).
	var blocklist []json.RawMessage
	decoder := json.NewDecoder(strings.NewReader(string(output)))
	for decoder.More() {
		var entries []json.RawMessage
		if err := decoder.Decode(&entries); err != nil {
			t.Fatalf("osd blocklist ls: %v: %s", err, output)
		}
		blocklist = append(blocklist, entries...)
	}
	names := slices.Clone(quorum.QuorumNames)
	slices.Sort(names)
	return familyCutState{quorum: names, up: up, mgrGID: uint64(status.ActiveGID), blocklist: len(blocklist)}
}

func (s familyCutState) String() string {
	return fmt.Sprintf("quorum=%v up=%v mgr_gid=%d blocklist=%d", s.quorum, s.up, s.mgrGID, s.blocklist)
}

// v2Endpoint returns the v2 entry of a vector in the given family.
func v2Endpoint(vector []ceph.EntityAddress, v6 bool) netip.AddrPort {
	for _, entry := range vector {
		addr := netip.MustParseAddrPort(entry.Addr)
		if entry.Type == "v2" && addr.Addr().Is6() == v6 {
			return addr
		}
	}
	return netip.AddrPort{}
}

// testDualStackFamilyCut starts a dual-stack cluster with one MON, one MGR
// and two OSDs, with osd.1 listing its IPv6 address first. A client container
// runs two librados actors: v6, whose MON session mon_host_override pins to
// mon.a's IPv6 address, and v4, pinned to mon.a's IPv4 address. The test then
// blackholes, with NetworkProfile Peer, PeerPort and Loss 100 on the client
// container, first its packets to mon.a's IPv6 v2 address and then those to
// osd.1's IPv6 v2 address. During each cut the operation of the v6 actor on
// that path stays pending while the same daemon keeps serving over IPv4 and
// the actor's other paths complete; after Restore the pending operation
// completes on the same session without the test issuing it again. Last, osd.1's
// container restarts and the v6 actor reads from the new process.
func testDualStackFamilyCut(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	netem := harness.NetemImage(t)
	image, roles := harness.IntegrationImages(t)
	const pool = "tc-family-cut"
	opts := append(roles, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(2).WithPoolDefaults(1, 1)),
		ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithAddressFamily(ceph.AddressDualStack)),
		ceph.WithConfigFile(dualStackOrderConfig(t, "osd.1")))
	cluster, client := harness.NewServiceClusterRun(t, ceph.Run, image, opts...)
	addresses, _ := waitDualStackAddresses(t, ctx, cluster, 1, 2)
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	objects := objectsPerPrimary(t, ctx, cluster, pool, 2)
	mon := addresses.Monitors[0]
	mon4, mon6 := v2Endpoint(mon.Addrs, false), v2Endpoint(mon.Addrs, true)
	osd0, osd1 := addresses.OSDs[0], addresses.OSDs[1]
	if familyOf(firstV2(osd0.Public)) != "ipv4" || familyOf(firstV2(osd1.Public)) != "ipv6" {
		t.Fatalf("osd.0 lists %s and osd.1 %s first", firstV2(osd0.Public), firstV2(osd1.Public))
	}
	osd1v4, osd1v6 := v2Endpoint(osd1.Public, false), v2Endpoint(osd1.Public, true)
	var osd1v1v6 netip.AddrPort
	for _, entry := range osd1.Public {
		if addr := netip.MustParseAddrPort(entry.Addr); entry.Type == "v1" && addr.Addr().Is6() {
			osd1v1v6 = addr
		}
	}
	mgr := addresses.ActiveManager
	monCtr, mgrCtr := monitorContainer(t, cluster, mon.Name), managerContainer(t, cluster, mgr.Name)
	osd0Ctr, osd1Ctr := osdContainer(t, cluster, 0), osdContainer(t, cluster, 1)

	v6 := startFamilyActor(ctx, client, "v6", "[v2:"+mon6.String()+"]", pool)
	v4 := startFamilyActor(ctx, client, "v4", "[v2:"+mon4.String()+"]", pool)
	payload := map[string][]byte{}
	want := map[string]string{}
	for _, name := range []string{objects[0], objects[1]} {
		payload[name] = []byte(strings.Repeat("family-cut/"+name+"/", 64))
		sum := sha256.Sum256(payload[name])
		want[name] = hex.EncodeToString(sum[:])
	}
	// lastHash is the SHA-256 that the last expect compared for a read.
	var lastHash string
	expect := func(actor *familyActor, request map[string]any, done bool) actorResult {
		t.Helper()
		result := actor.call(t, ctx, request)
		hash, err := actor.started.Check(request, result, done, want)
		if err != nil {
			t.Fatalf("%s: %v", actor.tag, err)
		}
		lastHash = hash
		return result
	}
	var instance6, instance4 uint64
	sameInstance := func(result actorResult, want *uint64) {
		t.Helper()
		if *want == 0 {
			*want = result.Instance
		}
		if result.Instance != *want {
			t.Fatalf("%s: client instance %d, was %d", result.session.tag, result.Instance, *want)
		}
	}
	for i, name := range []string{objects[0], objects[1]} {
		sameInstance(expect(v6, map[string]any{"id": fmt.Sprintf("seed%d", i), "op": "write", "name": name, "data": hex.EncodeToString(payload[name]), "wait": 30}, true), &instance6)
	}
	before := expect(v6, map[string]any{"id": "before", "op": "mon", "wait": 30}, true)
	sameInstance(before, &instance6)
	baseline := []string{
		checkSessionTo(t, ctx, before.session, "mon.a", mon6.String(), monCtr),
		checkCompletedSession(t, ctx, before.session, "osd.0", osd0.Public, osd0Ctr),
		checkCompletedSession(t, ctx, before.session, "osd.1", osd1.Public, osd1Ctr),
	}
	sameInstance(expect(v4, map[string]any{"id": "before", "op": "mon", "wait": 30}, true), &instance4)
	state := readFamilyCutState(t, ctx, cluster)

	cut := func(peer netip.AddrPort) *fault.NetworkProfileLease {
		t.Helper()
		lease, err := fault.ApplyNetworkProfile(ctx, cluster, client, fault.NetworkProfile{HelperImage: netem, Plane: ceph.PublicNetworkPlane,
			Peer: peer.Addr(), PeerPort: peer.Port(), Loss: 100})
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
		if status, err := lease.Status(ctx); err != nil || !status.Applied || status.Loss != 100 {
			t.Fatalf("status of the cut to %s: %+v, %v", peer, status, err)
		}
		return lease
	}
	restore := func(lease *fault.NetworkProfileLease, peer netip.AddrPort) string {
		t.Helper()
		status, err := lease.Status(ctx)
		// With Loss 100 netem sends none of the selected packets.
		if err != nil || !status.Applied || status.Drops == 0 || status.Packets != 0 {
			t.Fatalf("the cut to %s sent %d and dropped %d selected packets: %v", peer, status.Packets, status.Drops, err)
		}
		if err := lease.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		if root := harness.RootQdisc(t, ctx, netem, client, lease.Interface); root != "noqueue 0:" {
			t.Fatalf("root qdisc after Restore: %s", root)
		}
		filters, _ := json.Marshal(status.Filters)
		return fmt.Sprintf("dropped=%d sent=%d filters=%s", status.Drops, status.Packets, filters)
	}
	var receipts []string

	// MON: the v6 actor's packets to mon.a's IPv6 v2 address.
	lease := cut(mon6)
	monPending := expect(v6, map[string]any{"id": "mon-cut", "op": "mon", "wait": 8}, false)
	sameInstance(monPending, &instance6)
	var during []string
	for _, request := range []map[string]any{
		{"id": "mon-cut-osd0", "op": "read", "name": objects[0], "wait": 30},
		{"id": "mon-cut-osd1", "op": "read", "name": objects[1], "wait": 30},
		{"id": "mon-cut-mgr", "op": "mgr", "wait": 30},
	} {
		result := expect(v6, request, true)
		sameInstance(result, &instance6)
		during = append(during, "v6:"+result.String())
	}
	// The MGR session of the v6 actor goes to the MGR's first, IPv4, entry.
	mgrDuring := expect(v6, map[string]any{"id": "mon-cut-mgr2", "op": "mgr", "wait": 30}, true)
	sameInstance(mgrDuring, &instance6)
	during = append(during, "v6:"+checkCompletedSession(t, ctx, mgrDuring.session, "mgr."+mgr.Name, mgr.Addrs, mgrCtr))
	monIPv4 := expect(v4, map[string]any{"id": "mon-cut", "op": "mon", "wait": 30}, true)
	sameInstance(monIPv4, &instance4)
	during = append(during, "v4:"+monIPv4.String()+":"+checkSessionTo(t, ctx, monIPv4.session, "mon.a", mon4.String(), monCtr))
	if _, ok := monIPv4.session.to(mon6.String()); ok {
		t.Fatalf("the v4 actor connected to %s", mon6)
	}
	dropped := restore(lease, mon6)
	monAfter := expect(v6, map[string]any{"id": "mon-cut", "op": "join", "wait": 90}, true)
	sameInstance(monAfter, &instance6)
	monAgain := expect(v6, map[string]any{"id": "mon-after", "op": "mon", "wait": 30}, true)
	sameConnection(t, before, monAgain, mon6)
	receipts = append(receipts, fmt.Sprintf("cut=client->mon.a@%s %s pending=%s during=%v after_restore=%s,%s:%s",
		mon6, dropped, monPending, during, monAfter, monAgain, checkSessionTo(t, ctx, monAgain.session, "mon.a", mon6.String(), monCtr)))

	// OSD: the client's packets to osd.1's IPv6 v2 address, which librados
	// dials because osd.1 lists it first.
	lease = cut(osd1v6)
	readPending := expect(v6, map[string]any{"id": "osd-cut", "op": "read", "name": objects[1], "wait": 8}, false)
	sameInstance(readPending, &instance6)
	during = nil
	for _, request := range []map[string]any{
		{"id": "osd-cut-osd0", "op": "read", "name": objects[0], "wait": 30},
		{"id": "osd-cut-mon", "op": "mon", "wait": 30},
		{"id": "osd-cut-mgr", "op": "mgr", "wait": 30},
	} {
		result := expect(v6, request, true)
		sameInstance(result, &instance6)
		during = append(during, "v6:"+result.String())
	}
	// osd.1 keeps serving over IPv4 and on its IPv6 v1 port; only the
	// selected IPv6 v2 path is cut.
	code, reader, err := client.Exec(ctx, []string{"timeout", "60", "python3", "-c", dualStackProbe, osd1v6.String(), osd1v4.String(), osd1v1v6.String()}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, _ := io.ReadAll(reader)
	var probe map[string]string
	if code != 0 || json.Unmarshal([]byte(strings.TrimSpace(string(output))), &probe) != nil {
		t.Fatalf("probe: exit %d: %s", code, output)
	}
	if !strings.HasPrefix(probe[osd1v6.String()], "error:") || !strings.HasPrefix(probe[osd1v4.String()], "ceph v2\n") || !strings.HasPrefix(probe[osd1v1v6.String()], "ceph v") {
		t.Fatalf("probes during the cut to %s: %q", osd1v6, probe)
	}
	dropped = restore(lease, osd1v6)
	// The join checks the bytes of the read started during the cut against
	// objects[1].
	readAfter := expect(v6, map[string]any{"id": "osd-cut", "op": "join", "wait": 90}, true)
	joined := lastHash
	sameInstance(readAfter, &instance6)
	if joined == "" {
		t.Fatalf("the joined read %s compared no SHA-256", readAfter)
	}
	readAgain := expect(v6, map[string]any{"id": "osd-after", "op": "read", "name": objects[1], "wait": 30}, true)
	sameConnection(t, before, readAgain, osd1v6)
	receipts = append(receipts, fmt.Sprintf("cut=client->osd.1@%s %s pending=%s during=%v probe=%q after_restore=%s,%s:%s joined=%s",
		osd1v6, dropped, readPending, during, probe, readAfter, readAgain, checkSessionTo(t, ctx, readAgain.session, "osd.1", osd1v6.String(), osd1Ctr), joined))

	// Restart: osd.1's container stops and starts again. The new process
	// advertises both families again with a new nonce, and the actor reaches
	// it over IPv6 on a new connection.
	var osd1Handle *ceph.OSDContainer
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		if osd.ID == 1 {
			osd1Handle = osd
		}
	}
	timeout := 10 * time.Second
	if err := osd1Handle.Stop(ctx, &timeout); err != nil {
		t.Fatal(err)
	}
	if err := osd1Handle.Start(ctx); err != nil {
		t.Fatal(err)
	}
	var restarted ceph.OSDAddress
	deadline := time.Now().Add(3 * time.Minute)
	for {
		current, err := harness.ObserveCluster(cluster).DaemonAddresses(ctx)
		if err == nil && len(current.OSDs) == 2 && current.OSDs[1].Up && current.OSDs[1].Public[0].Nonce != osd1.Public[0].Nonce {
			restarted = current.OSDs[1]
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("osd.1 did not come back with a new nonce: %+v, %v", current.OSDs, err)
		}
		time.Sleep(time.Second)
	}
	checkDualStackVector(t, "restarted osd.1", restarted.Public)
	if familyOf(firstV2(restarted.Public)) != "ipv6" {
		t.Fatalf("restarted osd.1 lists %s first", firstV2(restarted.Public))
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	restartRead := expect(v6, map[string]any{"id": "osd-restart", "op": "read", "name": objects[1], "wait": 60}, true)
	sameInstance(restartRead, &instance6)
	receipts = append(receipts, fmt.Sprintf("restart=osd.1 %s->%s read=%s:%s", vectorString(osd1.Public), vectorString(restarted.Public),
		restartRead, checkCompletedSession(t, ctx, restartRead.session, "osd.1", restarted.Public, osd1Ctr)))

	sameInstance(expect(v4, map[string]any{"id": "after", "op": "mon", "wait": 30}, true), &instance4)
	v6.release(t, ctx)
	v4.release(t, ctx)
	if after := readFamilyCutState(t, ctx, cluster); !slices.Equal(after.quorum, state.quorum) || !slices.Equal(after.up, state.up) || after.mgrGID != state.mgrGID || after.blocklist != state.blocklist {
		t.Fatalf("cluster state %s after the cuts, %s before", after, state)
	}
	// A new session reads both objects.
	code, reader, err = harness.ToolkitContainer(t, cluster).Exec(ctx, []string{"timeout", "120", "python3", "-c", dualStackOracle, pool, objects[0], objects[1]}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, _ = io.ReadAll(reader)
	var oracle map[string]string
	if code != 0 || json.Unmarshal([]byte(strings.TrimSpace(string(output))), &oracle) != nil || oracle[objects[0]] != want[objects[0]] || oracle[objects[1]] != want[objects[1]] {
		t.Fatalf("oracle read: exit %d: %s, want %v", code, output, want)
	}
	t.Logf("DUAL_STACK_FAMILY_CUT_RECEIPT image=%s ceph=%s netem=%s mon.a=%s osd.0=%s osd.1=%s baseline=%v cuts=%q instances=v6:%d,v4:%d state=%s oracle=%v",
		harness.StaticFacts(cluster).Images.Essential, harness.StaticFacts(cluster).CephVersion, netem, vectorString(mon.Addrs), vectorString(osd0.Public), vectorString(osd1.Public),
		baseline, receipts, instance6, instance4, state, oracle)
}

// sameConnection checks that the actor reaches endpoint over the same TCP
// connection after a cut as before it: the session was not reopened.
func sameConnection(t *testing.T, before, after actorResult, endpoint netip.AddrPort) {
	t.Helper()
	old, ok := before.session.to(endpoint.String())
	current, found := after.session.to(endpoint.String())
	if !ok || !found || old != current {
		t.Fatalf("connection to %s: %v before the cut, %v after Restore", endpoint, old, current)
	}
}
