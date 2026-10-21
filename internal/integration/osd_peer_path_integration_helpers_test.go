//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/testcontainers/testcontainers-go"
)

// osdPeerPathReceipt is what TestOSDReplicationPeerPathCut logs: the PG and
// OSD pair, the one selected direction that was cut, the write the primary
// held, the controls and the packet counters of both directions meanwhile,
// and the copies read after the restore.
type osdPeerPathReceipt struct {
	FSID    string      `json:"fsid"`
	Mapping heldMapping `json:"mapping"`
	Cut     struct {
		From      int    `json:"from_osd"`
		To        int    `json:"to_osd"`
		Interface string `json:"interface"`
		Source    string `json:"source"`
		Peer      string `json:"peer"`
	} `json:"cut"`
	Held struct {
		Primary    string        `json:"primary"`
		Request    string        `json:"request"`
		FlagPoint  string        `json:"flag_point"`
		WaitingFor string        `json:"waiting_for"`
		Events     []string      `json:"events"`
		For        time.Duration `json:"for_ns"`
	} `json:"held"`
	Window struct {
		Length       time.Duration `json:"length_ns"`
		Probes       int           `json:"probes"`
		Grace        string        `json:"osd_heartbeat_grace"`
		MinReporters string        `json:"mon_osd_min_down_reporters"`
		FirstEpoch   uint32        `json:"first_epoch"`
		LastEpoch    uint32        `json:"last_epoch"`
		BothUp       bool          `json:"both_up"`
		// Reports are the MON's "reported failed by" cluster log entries.
		Reports []string `json:"failure_reports"`
	} `json:"window"`
	Control struct {
		Pool    string `json:"pool"`
		PGID    string `json:"pgid"`
		Acting  []int  `json:"acting"`
		Version uint64 `json:"version"`
	} `json:"control"`
	Netem struct {
		Packets uint64 `json:"packets"`
		Drops   uint64 `json:"drops"`
	} `json:"netem"`
	// Counters are the packets that each OSD's cluster interface sent and
	// received during the window. Only the two OSDs are on that bridge.
	Counters struct {
		PrimaryTx uint64 `json:"primary_tx"`
		PrimaryRx uint64 `json:"primary_rx"`
		ReplicaTx uint64 `json:"replica_tx"`
		ReplicaRx uint64 `json:"replica_rx"`
	} `json:"counters"`
	Writer        heldReadback `json:"writer"`
	Reader        heldReadback `json:"reader"`
	ReplicaReader heldReadback `json:"replica_reader"`
}

// peerPathCounters prints the packet counters of the interface that has the
// IPv4 address argv[1].
const peerPathCounters = `import fcntl,json,os,socket,struct,sys
want=sys.argv[1]
s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
for name in sorted(os.listdir("/sys/class/net")):
    try:
        addr=socket.inet_ntoa(fcntl.ioctl(s.fileno(),0x8915,struct.pack("256s",name.encode()[:15]))[20:24])
    except OSError:
        continue
    if addr==want:
        read=lambda key:int(open("/sys/class/net/%s/statistics/%s"%(name,key)).read())
        print(json.dumps({"interface":name,"rx":read("rx_packets"),"tx":read("tx_packets")}))
        sys.exit(0)
sys.exit("no interface has "+want)`

// peerPathControl writes and reads an object in the control pool through a
// client of its own, with bounded operations, and prints its version.
const peerPathControl = `import json,rados
payload=b"control"*512
with rados.Rados(conffile="/etc/ceph/ceph.conf",conf={"client_mount_timeout":"15","rados_mon_op_timeout":"15","rados_osd_op_timeout":"15"}) as cluster:
    with cluster.open_ioctx("peer-control") as io:
        io.write_full("control",payload)
        version=io.get_last_version()
        assert io.read("control",len(payload)+1,0)==payload
        print(json.dumps({"version":version}))`

type interfaceCounters struct {
	Interface string `json:"interface"`
	Rx        uint64 `json:"rx"`
	Tx        uint64 `json:"tx"`
}

func readInterfaceCounters(t *testing.T, ctx context.Context, ctr testcontainers.Container, address netip.Addr) interfaceCounters {
	t.Helper()
	var counters interfaceCounters
	out := delayExec(t, ctx, ctr, "python3", "-c", peerPathCounters, address.String())
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &counters); err != nil {
		t.Fatalf("interface counters %q: %v", out, err)
	}
	return counters
}

// addressIP returns the IP of a Ceph "ip:port" address.
func addressIP(t *testing.T, address string) netip.Addr {
	t.Helper()
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		t.Fatal(err)
	}
	return ip
}

// osdUp returns the OSD map epoch and which OSDs it marks up.
func osdUp(ctx context.Context, cluster *ceph.Container) (string, uint32, map[int]bool, error) {
	out, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		return "", 0, nil, err
	}
	var dump struct {
		FSID  string `json:"fsid"`
		Epoch uint32 `json:"epoch"`
		OSDs  []struct {
			ID int `json:"osd"`
			Up int `json:"up"`
		} `json:"osds"`
	}
	if err := json.Unmarshal(out, &dump); err != nil {
		return "", 0, nil, fmt.Errorf("osd dump: %w", err)
	}
	up := map[int]bool{}
	for _, osd := range dump.OSDs {
		up[osd.ID] = osd.Up == 1
	}
	return dump.FSID, dump.Epoch, up, nil
}

// testOSDReplicationPeerPathCut cuts only the primary's packets to its
// replica's cluster address, with a peer-selected netem loss of 100% on the
// primary's cluster endpoint, while a client writes to their PG.
func testOSDReplicationPeerPathCut(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	image := netemImage(t)
	cluster, client := newServiceCluster(t, ceph.WithSeparateClusterNetwork())
	if !cluster.HasSeparateClusterNetwork() {
		t.Fatal("no separate cluster network")
	}
	// One PG on both OSDs with min_size 2: every write needs both.
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-topology", PGNum: 1, Replicas: 2, MinSize: 2, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	// The control pool has one replica: its writes never cross the cluster
	// network.
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "peer-control", PGNum: 1, Replicas: 1, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	var receipt osdPeerPathReceipt
	out, err := cluster.Ceph(ctx, "osd", "map", "tc-topology", "held", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Mapping, err = parseHeldMapping(out, "tc-topology", "held"); err != nil {
		t.Fatal(err)
	}
	primary, replica := receipt.Mapping.ActingPrimary, receipt.Mapping.replica()
	out, err = cluster.Ceph(ctx, "osd", "map", "peer-control", "control", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var control struct {
		PGID   string `json:"pgid"`
		Acting []int  `json:"acting"`
	}
	if err := json.Unmarshal(out, &control); err != nil || len(control.Acting) != 1 {
		t.Fatalf("control mapping %s: %v", out, err)
	}
	receipt.Control.Pool, receipt.Control.PGID, receipt.Control.Acting = "peer-control", control.PGID, control.Acting
	native, err := cluster.DaemonAddresses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	clusterIP := map[int]netip.Addr{}
	for _, osd := range native.OSDs {
		if len(osd.Cluster) == 0 || len(osd.HeartbeatBack) == 0 {
			t.Fatalf("osd.%d advertises no cluster address: %+v", osd.ID, osd)
		}
		clusterIP[osd.ID] = addressIP(t, osd.Cluster[0].Addr)
		t.Logf("osd.%d cluster %v heartbeat back %v", osd.ID, osd.Cluster, osd.HeartbeatBack)
	}
	containers := map[int]*ceph.OSDContainer{}
	for _, osd := range cluster.OSDs() {
		containers[osd.ID] = osd
	}
	if containers[primary] == nil || containers[replica] == nil || !clusterIP[primary].IsValid() || !clusterIP[replica].IsValid() {
		t.Fatalf("PG %s on osd.%d and osd.%d, owned %v, addresses %v", receipt.Mapping.PGID, primary, replica, containers, clusterIP)
	}
	receipt.Window.Grace = monConfigGet(t, ctx, cluster, "osd", "osd_heartbeat_grace")
	receipt.Window.MinReporters = monConfigGet(t, ctx, cluster, "mon", "mon_osd_min_down_reporters")

	// Cut only osd.P's packets to osd.R's cluster address.
	lease, err := fault.ApplyNetworkProfile(ctx, cluster, containers[primary].Container, fault.NetworkProfile{
		HelperImage: image, Plane: ceph.ClusterNetworkPlane, Peer: clusterIP[replica], Loss: 100,
	})
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
	if status, err := lease.Status(ctx); err != nil || !status.Applied || status.Loss != 100 || lease.Address != clusterIP[primary] {
		t.Fatalf("status of the cut: %+v on %s, %v", status, lease.Address, err)
	}
	receipt.Cut.From, receipt.Cut.To, receipt.Cut.Interface = primary, replica, lease.Interface
	receipt.Cut.Source, receipt.Cut.Peer = lease.Address.String(), clusterIP[replica].String()
	primaryBefore := readInterfaceCounters(t, ctx, containers[primary], clusterIP[primary])
	replicaBefore := readInterfaceCounters(t, ctx, containers[replica], clusterIP[replica])

	write := &heldReplicationWrite{done: make(chan struct{})}
	writeCtx, stopWrite := context.WithCancel(ctx)
	go func() {
		defer close(write.done)
		write.result, write.err = cluster.RunNativeCommand(writeCtx, client, ceph.NativeCommand{
			Argv: []string{"python3", "-c", heldWriter}, Timeout: 5 * time.Minute, MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "peer-cut replicated write",
		})
	}()
	t.Cleanup(func() {
		stopWrite()
		<-write.done
		var unstopped *ceph.NativeCommandUnstoppedError
		if errors.As(write.err, &unstopped) {
			stopCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := unstopped.Stop(stopCtx); err != nil {
				t.Errorf("stop held write: %v", err)
			}
		}
	})
	written := func() bool {
		select {
		case <-write.done:
			return true
		default:
			return false
		}
	}
	primaryName := "osd." + strconv.Itoa(primary)
	// The primary reports the same request waiting for its replica, with no
	// replica commit, for 10 seconds while the writer still waits.
	var since time.Time
	held, err := observe(ctx, 2*time.Minute, 20*time.Second, time.Second, func(ctx context.Context) (heldOp, bool, error) {
		if written() {
			return heldOp{}, false, fmt.Errorf("the write finished across the cut: %v", write.err)
		}
		data, err := cluster.AdminSocket(ctx, primaryName, "dump_ops_in_flight")
		if err != nil {
			return heldOp{}, false, err
		}
		op, found, err := findHeldOp(data, "held")
		switch {
		case err != nil:
			return heldOp{}, false, err
		case !found:
			since = time.Time{}
			return heldOp{}, false, nil
		}
		if since.IsZero() {
			since = time.Now()
		}
		return op, time.Since(since) >= 10*time.Second, nil
	})
	if err != nil {
		t.Fatalf("%s did not hold the write for 10 seconds: %v", primaryName, err)
	}
	if want := strconv.Itoa(replica); held.Value.waitingFor() != want {
		t.Fatalf("%s waits for subops from %q, want osd.%s", primaryName, held.Value.waitingFor(), want)
	}
	receipt.Held.Primary, receipt.Held.Request, receipt.Held.FlagPoint = primaryName, held.Value.request(), held.Value.TypeData.FlagPoint
	receipt.Held.WaitingFor, receipt.Held.Events = held.Value.waitingFor(), held.Value.events()

	// The window outlasts osd_heartbeat_grace: the primary keeps the same
	// request waiting for its replica, the writer still waits, MON commands
	// answer and neither OSD is marked down.
	const window = 45 * time.Second
	seen, err := observe(ctx, window+time.Minute, 20*time.Second, 2*time.Second, func(ctx context.Context) (uint32, bool, error) {
		if written() {
			return 0, false, fmt.Errorf("the write finished across the cut: %v", write.err)
		}
		fsid, epoch, up, err := osdUp(ctx, cluster)
		if err != nil {
			return 0, false, err
		}
		if !up[primary] || !up[replica] {
			return epoch, false, fmt.Errorf("epoch %d marks osd.%d up %t and osd.%d up %t %s after the cut", epoch, primary, up[primary], replica, up[replica], time.Since(cutAt).Round(time.Second))
		}
		if receipt.Window.FirstEpoch == 0 {
			receipt.Window.FirstEpoch = epoch
		}
		receipt.FSID, receipt.Window.LastEpoch = fsid, epoch
		data, err := cluster.AdminSocket(ctx, primaryName, "dump_ops_in_flight")
		if err != nil {
			return epoch, false, err
		}
		op, found, err := findHeldOp(data, "held")
		if err != nil {
			return epoch, false, err
		}
		if !found || op.request() != receipt.Held.Request {
			return epoch, false, fmt.Errorf("%s no longer holds %s waiting for sub ops: %.2048s", primaryName, receipt.Held.Request, data)
		}
		return epoch, time.Since(cutAt) >= window, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	receipt.Window.Probes, receipt.Window.Length = seen.Attempts, time.Since(cutAt).Round(time.Second)
	receipt.Window.BothUp = true
	receipt.Held.For = time.Since(since).Round(time.Second)
	logs, err := cluster.Ceph(ctx, "log", "last", "1000", "debug", "cluster")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(logs), "\n") {
		if _, report, found := strings.Cut(line, "cluster [DBG] "); found && strings.Contains(report, " reported failed by ") && !slices.Contains(receipt.Window.Reports, report) {
			receipt.Window.Reports = append(receipt.Window.Reports, report)
		}
	}
	// Both OSDs reported the other: the primary gets no ping through, and the
	// replica's pings arrive but their replies do not. One reporter each is
	// below mon_osd_min_down_reporters, so neither was marked down.
	for _, report := range []string{fmt.Sprintf("osd.%d reported failed by osd.%d", replica, primary), fmt.Sprintf("osd.%d reported failed by osd.%d", primary, replica)} {
		if !slices.Contains(receipt.Window.Reports, report) {
			t.Fatalf("no %q in the cluster log: %q", report, receipt.Window.Reports)
		}
	}
	result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv: []string{"python3", "-c", peerPathControl}, Timeout: time.Minute, MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "control pool write",
	})
	if err != nil || result.ExitCode != 0 || !result.Complete() {
		t.Fatalf("control write during the cut: exit %d: %v: %.2048s", result.ExitCode, err, result.Stderr)
	}
	var controlWrite struct {
		Version uint64 `json:"version"`
	}
	if err := json.Unmarshal([]byte(lastJSONLine(string(result.Stdout))), &controlWrite); err != nil || controlWrite.Version == 0 {
		t.Fatalf("control writer output %q: %v", result.Stdout, err)
	}
	receipt.Control.Version = controlWrite.Version
	// Every packet that the primary sent to the replica was dropped, while
	// the replica's packets to the primary arrived: the primary's cluster
	// interface received packets, and only the replica shares that bridge.
	status, err := lease.Status(ctx)
	if err != nil || !status.Applied {
		t.Fatalf("status of the cut: %+v, %v", status, err)
	}
	receipt.Netem.Packets, receipt.Netem.Drops = status.Packets, status.Drops
	primaryAfter := readInterfaceCounters(t, ctx, containers[primary], clusterIP[primary])
	replicaAfter := readInterfaceCounters(t, ctx, containers[replica], clusterIP[replica])
	receipt.Counters.PrimaryTx, receipt.Counters.PrimaryRx = primaryAfter.Tx-primaryBefore.Tx, primaryAfter.Rx-primaryBefore.Rx
	receipt.Counters.ReplicaTx, receipt.Counters.ReplicaRx = replicaAfter.Tx-replicaBefore.Tx, replicaAfter.Rx-replicaBefore.Rx
	if status.Drops == 0 || status.Packets != 0 || receipt.Counters.PrimaryRx == 0 || receipt.Counters.ReplicaTx == 0 {
		t.Fatalf("netem sent %d and dropped %d selected packets; cluster interfaces: %+v", status.Packets, status.Drops, receipt.Counters)
	}
	if written() {
		t.Fatalf("the write finished across the cut: %v", write.err)
	}

	if err := lease.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	var restored heldReplicationReceipt
	finishHeldReplicationWrite(t, ctx, cluster, &restored, write)
	receipt.Writer, receipt.Reader = restored.Writer, restored.Reader
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}

	// The replica's copy alone: with min_size 1 and the primary stopped, the
	// PG is served by the replica.
	if _, err := cluster.Ceph(ctx, "osd", "pool", "set", "tc-topology", "min_size", "1"); err != nil {
		t.Fatal(err)
	}
	stopTimeout := 10 * time.Second
	if err := containers[primary].Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	if _, err := observe(ctx, 2*time.Minute, 20*time.Second, 2*time.Second, func(ctx context.Context) (string, bool, error) {
		snapshot, err := cluster.PoolPGs(ctx, "tc-topology")
		if err != nil || len(snapshot.PGs) != 1 {
			return "", false, err
		}
		pg := snapshot.PGs[0]
		return pg.State, strings.HasPrefix(pg.State, "active") && slices.Equal(pg.Acting, []int{replica}), nil
	}); err != nil {
		t.Fatal("PG active on the replica alone:", err)
	}
	read, err := cluster.RunNativeCommand(ctx, toolkitContainer(t, cluster), ceph.NativeCommand{
		Argv: []string{"python3", "-c", heldReader}, Timeout: 2 * time.Minute, MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "replica-only reader",
	})
	if err != nil || read.ExitCode != 0 || !read.Complete() {
		t.Fatalf("replica-only reader: exit %d: %v: %.2048s", read.ExitCode, err, read.Stderr)
	}
	if err := json.Unmarshal([]byte(lastJSONLine(string(read.Stdout))), &receipt.ReplicaReader); err != nil {
		t.Fatal(err)
	}
	got, want := receipt.ReplicaReader, receipt.Writer
	got.Client = ""
	if got != want {
		t.Fatalf("replica-only reader %+v, writer %+v", receipt.ReplicaReader, receipt.Writer)
	}
	if err := containers[primary].Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("OSD_PEER_PATH_RECEIPT %s", encoded)
}

func monConfigGet(t *testing.T, ctx context.Context, cluster *ceph.Container, who, name string) string {
	t.Helper()
	out, err := cluster.Ceph(ctx, "config", "get", who, name)
	if err != nil {
		t.Fatalf("config get %s %s: %v", who, name, err)
	}
	return strings.TrimSpace(string(out))
}
