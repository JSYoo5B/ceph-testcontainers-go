//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	delayPool    = "netem"
	delayPayload = 4096
	delayAmount  = 200 * time.Millisecond
	delayJitter  = 20 * time.Millisecond
)

// delayReadScript reads each named object count times on one new connection
// and prints the median, minimum and maximum read time per object.
const delayReadScript = `import json,rados,sys,time
pool,count,names=sys.argv[1],int(sys.argv[2]),sys.argv[3:]
c=rados.Rados(conffile="/etc/ceph/ceph.conf")
c.connect(timeout=30)
try:
 io=c.open_ioctx(pool)
 out={}
 for name in names:
  times=[]
  for _ in range(count):
   start=time.monotonic(); data=io.read(name,1<<20); times.append((time.monotonic()-start)*1000)
   assert len(data)==` + "4096" + `
  times.sort()
  out[name]={"median_ms":times[len(times)//2],"min_ms":times[0],"max_ms":times[-1]}
 io.close()
finally:
 c.shutdown()
print(json.dumps(out))
`

// delayHeldScript keeps one connection open and logs each object's read
// time with the container clock, until /tmp/delay.stop appears.
const delayHeldScript = `import os,rados,sys,time
pool,names=sys.argv[1],sys.argv[2:]
c=rados.Rados(conffile="/etc/ceph/ceph.conf")
c.connect(timeout=30)
try:
 io=c.open_ioctx(pool)
 while not os.path.exists("/tmp/delay.stop"):
  for name in names:
   start=time.monotonic(); io.read(name,1<<20)
   print("%.3f %s %.3f"%(time.time(),name,(time.monotonic()-start)*1000),flush=True)
  time.sleep(0.2)
 io.close()
finally:
 c.shutdown()
`

type delayTimes struct {
	Median float64 `json:"median_ms"`
	Min    float64 `json:"min_ms"`
	Max    float64 `json:"max_ms"`
}

func netemImage(t *testing.T) string {
	t.Helper()
	image := os.Getenv("CEPH_TEST_NETEM_IMAGE")
	if image == "" {
		image = fault.NetemHelperImage
	}
	t.Logf("netem helper: %s", image)
	return image
}

func delayExec(t *testing.T, ctx context.Context, ctr testcontainers.Container, argv ...string) string {
	t.Helper()
	code, reader, err := ctr.Exec(ctx, argv, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(reader)
	if code != 0 {
		t.Fatalf("%s: exit %d: %s", strings.Join(argv, " "), code, out)
	}
	return string(out)
}

// placeDelayObjects writes objects to a single-replica pool until each OSD is
// the primary of one, and returns the object of each OSD.
func placeDelayObjects(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container) map[int]string {
	t.Helper()
	if _, err := cluster.Ceph(ctx, "osd", "pool", "create", delayPool, "16"); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.Ceph(ctx, "osd", "pool", "application", "enable", delayPool, "rados"); err != nil {
		t.Fatal(err)
	}
	delayExec(t, ctx, client, "sh", "-c", fmt.Sprintf("head -c %d /dev/urandom > /tmp/payload", delayPayload))
	objects := make(map[int]string)
	for i := 0; i < 32 && len(objects) < 2; i++ {
		name := fmt.Sprintf("object-%d", i)
		delayExec(t, ctx, client, "rados", "-p", delayPool, "put", name, "/tmp/payload")
		out, err := cluster.Ceph(ctx, "osd", "map", delayPool, name, "--format", "json")
		var placement struct {
			Primary int `json:"acting_primary"`
		}
		if err != nil || json.Unmarshal(out, &placement) != nil {
			t.Fatalf("osd map %s: %v: %s", name, err, out)
		}
		if _, seen := objects[placement.Primary]; !seen {
			objects[placement.Primary] = name
		}
	}
	if len(objects) != 2 {
		t.Fatalf("objects did not reach both OSDs: %v", objects)
	}
	return objects
}

func readDelayObjects(t *testing.T, ctx context.Context, client testcontainers.Container, names ...string) map[string]delayTimes {
	t.Helper()
	out := delayExec(t, ctx, client, append([]string{"python3", "-c", delayReadScript, delayPool, "9"}, names...)...)
	var times map[string]delayTimes
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &times); err != nil {
		t.Fatalf("parse read times %q: %v", out, err)
	}
	return times
}

// heldDelayReads returns the held session's read times of name between from
// and to, by the container clock.
func heldDelayReads(t *testing.T, ctx context.Context, client testcontainers.Container, name string, from, to float64) []float64 {
	t.Helper()
	var times []float64
	for _, line := range strings.Split(delayExec(t, ctx, client, "cat", "/tmp/delay.log"), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[1] != name {
			continue
		}
		at, _ := strconv.ParseFloat(fields[0], 64)
		ms, _ := strconv.ParseFloat(fields[2], 64)
		if at > from && at < to {
			times = append(times, ms)
		}
	}
	slices.Sort(times)
	return times
}

// rootQdisc reads a container's root qdisc on interface from a disposable
// helper in its namespace, without NET_ADMIN.
func rootQdisc(t *testing.T, ctx context.Context, image string, target testcontainers.Container, iface string) string {
	t.Helper()
	observer := startNetemObserver(t, ctx, image, target, false)
	defer func() { _ = observer.Terminate(context.WithoutCancel(ctx)) }()
	var qdiscs []struct {
		Kind   string `json:"kind"`
		Handle string `json:"handle"`
		Root   bool   `json:"root"`
	}
	if err := json.Unmarshal([]byte(delayExec(t, ctx, observer, "tc", "-j", "qdisc", "show", "dev", iface)), &qdiscs); err != nil {
		t.Fatal(err)
	}
	for _, qdisc := range qdiscs {
		if qdisc.Root {
			return qdisc.Kind + " " + qdisc.Handle
		}
	}
	return ""
}

// startNetemObserver starts the test's own helper in target's namespace, as
// root like the module's helpers.
func startNetemObserver(t *testing.T, ctx context.Context, image string, target testcontainers.Container, netAdmin bool) testcontainers.Container {
	t.Helper()
	observer, err := testcontainers.Run(ctx, image, ceph.WithIdleEntrypoint(),
		testcontainers.WithConfigModifier(func(config *container.Config) { config.User = "0:0" }),
		testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
			host.NetworkMode = container.NetworkMode("container:" + target.GetContainerID())
			host.CapDrop = []string{"ALL"}
			if netAdmin {
				host.CapAdd = []string{"NET_ADMIN"}
			}
		}))
	testcontainers.CleanupContainer(t, observer)
	if err != nil {
		t.Fatal(err)
	}
	return observer
}

// netemHelpers counts the module's helper containers on the engine.
func netemHelpers(t *testing.T, ctx context.Context) int {
	t.Helper()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = docker.Close() }()
	list, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: make(mobycl.Filters).Add("label", "org.testcontainers.ceph.role=netem-helper")})
	if err != nil {
		t.Fatal(err)
	}
	return len(list.Items)
}

func testNetworkDelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	image := netemImage(t)
	cluster, client := newServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithPoolDefaults(1, 1)))
	fsid, err := cluster.Ceph(ctx, "fsid")
	if err != nil {
		t.Fatal(err)
	}
	objects := placeDelayObjects(t, ctx, cluster, client)
	osds := clusteraccess.OSDs(t, cluster)
	delayed, other := osds[0], osds[1]
	slow, fast := objects[delayed.ID], objects[other.ID]
	payload := delayExec(t, ctx, client, "rados", "-p", delayPool, "get", slow, "/dev/stdout")
	request := fault.NetworkProfile{HelperImage: image, Plane: ceph.PublicNetworkPlane, Delay: delayAmount, Jitter: delayJitter}

	t.Run("delay", func(t *testing.T) {
		baseline := readDelayObjects(t, ctx, client, slow, fast)
		if err := client.CopyToContainer(ctx, []byte(delayHeldScript), "/tmp/delay_held.py", 0o644); err != nil {
			t.Fatal(err)
		}
		delayExec(t, ctx, client, "sh", "-c", `rm -f /tmp/delay.stop; python3 /tmp/delay_held.py "$@" > /tmp/delay.log 2> /tmp/delay.err &`, "sh", delayPool, slow, fast)
		defer delayExec(t, context.WithoutCancel(ctx), client, "touch", "/tmp/delay.stop")
		time.Sleep(3 * time.Second)
		lease, err := fault.ApplyNetworkProfile(ctx, cluster, delayed.Container, request)
		if err != nil {
			if lease != nil {
				_ = lease.Restore(context.WithoutCancel(ctx))
			}
			t.Fatal(err)
		}
		applied := containerTime(t, ctx, client)
		status, err := lease.Status(ctx)
		if err != nil || !status.Applied || status.Delay != delayAmount || status.Jitter != delayJitter {
			t.Fatalf("status after the delay: %+v, %v", status, err)
		}
		t.Logf("lease: container %s sandbox %.12s namespace %s interface %s (index %d, %s, %s), helper image %s, root %s %s",
			lease.ContainerID, lease.SandboxID, lease.Namespace, lease.Interface, lease.InterfaceIndex, lease.MACAddress, lease.Address,
			lease.HelperImageID, status.RootKind, status.RootHandle)
		during := readDelayObjects(t, ctx, client, slow, fast)
		time.Sleep(3 * time.Second)
		counted, err := lease.Status(ctx)
		if err != nil || counted.Packets <= status.Packets {
			t.Fatalf("netem counted %d packets after %d: %v", counted.Packets, status.Packets, err)
		}
		restoring := containerTime(t, ctx, client)
		if err := lease.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		restored := containerTime(t, ctx, client)
		after := readDelayObjects(t, ctx, client, slow, fast)
		time.Sleep(2 * time.Second)
		if root := rootQdisc(t, ctx, image, delayed.Container, lease.Interface); root != "noqueue 0:" {
			t.Fatalf("root qdisc after Restore: %s", root)
		}
		if helpers := netemHelpers(t, ctx); helpers != 0 {
			t.Fatalf("%d netem helpers after Restore", helpers)
		}
		// A read waits for one delayed answer: the delay, varied by the jitter.
		limit := float64(delayAmount.Milliseconds())
		lowest := float64((delayAmount - delayJitter).Milliseconds())
		if during[slow].Min < lowest || during[fast].Median >= limit/2 {
			t.Fatalf("new connection while delayed: %s %+v, %s %+v", slow, during[slow], fast, during[fast])
		}
		if after[slow].Median >= limit/2 {
			t.Fatalf("new connection after Restore: %s %+v", slow, after[slow])
		}
		heldSlow := heldDelayReads(t, ctx, client, slow, applied+0.5, restoring)
		heldFast := heldDelayReads(t, ctx, client, fast, applied+0.5, restoring)
		heldAfter := heldDelayReads(t, ctx, client, slow, restored+0.5, restored+60)
		if len(heldSlow) < 3 || len(heldFast) < 3 || len(heldAfter) < 3 {
			t.Fatalf("too few held reads: %d, %d, %d:\n%s", len(heldSlow), len(heldFast), len(heldAfter), containerFile(t, ctx, client, "/tmp/delay.err"))
		}
		if heldSlow[0] < lowest || heldFast[len(heldFast)-1] >= limit/2 || heldAfter[len(heldAfter)/2] >= limit/2 {
			t.Fatalf("held connection: %s while delayed %v, %s while delayed %v, %s after Restore %v", slow, heldSlow, fast, heldFast, slow, heldAfter)
		}
		if again := delayExec(t, ctx, client, "rados", "-p", delayPool, "get", slow, "/dev/stdout"); again != payload {
			t.Fatal("the delayed OSD's object changed")
		}
		if current, err := cluster.Ceph(ctx, "fsid"); err != nil || !bytes.Equal(current, fsid) {
			t.Fatalf("FSID %s after the delay, %s before: %v", current, fsid, err)
		}
		t.Logf("osd.%d delayed by %v±%v: new connection %s %.1f→%.1f→%.1f ms, %s %.1f→%.1f→%.1f ms; held connection %s min %.1f ms, %s max %.1f ms while delayed, %s median %.1f ms after; netem counted %d packets",
			delayed.ID, delayAmount, delayJitter, slow, baseline[slow].Median, during[slow].Median, after[slow].Median,
			fast, baseline[fast].Median, during[fast].Median, after[fast].Median,
			slow, heldSlow[0], fast, heldFast[len(heldFast)-1], slow, heldAfter[len(heldAfter)/2], counted.Packets)
	})

	t.Run("peer", func(t *testing.T) {
		// Only the packets that osd.N sends to the client container are
		// delayed: the client's reads from that OSD wait for a delayed answer,
		// while its reads from the other OSD and another client's reads from
		// the delayed OSD do not.
		address, err := client.ContainerIP(ctx)
		if err != nil {
			t.Fatal(err)
		}
		peer := netip.MustParseAddr(address)
		other := toolkitContainer(t, cluster)
		otherAddress, err := other.ContainerIP(ctx)
		if err != nil || otherAddress == address {
			t.Fatalf("the control container's address %q: %v", otherAddress, err)
		}
		selective := request
		selective.Peer = peer
		lease, err := fault.ApplyNetworkProfile(ctx, cluster, delayed.Container, selective)
		if lease != nil {
			defer func() { _ = lease.Restore(context.WithoutCancel(ctx)) }()
		}
		if err != nil {
			t.Fatal(err)
		}
		status, err := lease.Status(ctx)
		if err != nil || !status.Applied || status.RootKind != "prio" || status.Delay != delayAmount || status.Jitter != delayJitter {
			t.Fatalf("status of the peer delay: %+v, %v", status, err)
		}
		during := readDelayObjects(t, ctx, client, slow, fast)
		elsewhere := readDelayObjects(t, ctx, other, slow)
		counted, err := lease.Status(ctx)
		if err != nil || counted.Packets == 0 {
			t.Fatalf("netem counted %d selected packets: %v", counted.Packets, err)
		}
		if err := lease.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		after := readDelayObjects(t, ctx, client, slow)
		if root := rootQdisc(t, ctx, image, delayed.Container, lease.Interface); root != "noqueue 0:" {
			t.Fatalf("root qdisc after Restore: %s", root)
		}
		if helpers := netemHelpers(t, ctx); helpers != 0 {
			t.Fatalf("%d netem helpers after Restore", helpers)
		}
		limit := float64(delayAmount.Milliseconds())
		lowest := float64((delayAmount - delayJitter).Milliseconds())
		if during[slow].Min < lowest || during[fast].Median >= limit/2 || elsewhere[slow].Median >= limit/2 || after[slow].Median >= limit/2 {
			t.Fatalf("peer delay to %s: %s %+v and %s %+v from it, %s %+v from %s, %s %+v after Restore",
				peer, slow, during[slow], fast, during[fast], slow, elsewhere[slow], otherAddress, slow, after[slow])
		}
		filters, _ := json.Marshal(status.Filters)
		t.Logf("osd.%d delayed by %v±%v to %s only: from it %s median %.1f ms (min %.1f), %s median %.1f ms; from %s %s median %.1f ms; after Restore %.1f ms; netem counted %d selected packets; filters %s",
			delayed.ID, delayAmount, delayJitter, peer, slow, during[slow].Median, during[slow].Min, fast, during[fast].Median,
			otherAddress, slow, elsewhere[slow].Median, after[slow].Median, counted.Packets, filters)
	})

	t.Run("peer foreign filter", func(t *testing.T) {
		// A filter that another owner adds under the lease's prio root is not
		// the lease's: Restore refuses to remove the root with it and keeps
		// the lease, and succeeds after that owner removed its filter.
		address, err := client.ContainerIP(ctx)
		if err != nil {
			t.Fatal(err)
		}
		selective := request
		selective.Peer = netip.MustParseAddr(address)
		lease, err := fault.ApplyNetworkProfile(ctx, cluster, delayed.Container, selective)
		if lease != nil {
			defer func() { _ = lease.Restore(context.WithoutCancel(ctx)) }()
		}
		if err != nil {
			t.Fatal(err)
		}
		status, err := lease.Status(ctx)
		if err != nil || !status.Applied {
			t.Fatalf("status of the peer delay: %+v, %v", status, err)
		}
		root := status.RootHandle
		owner := startNetemObserver(t, ctx, image, delayed.Container, true)
		delayExec(t, ctx, owner, "tc", "filter", "add", "dev", lease.Interface, "parent", root, "protocol", "ip", "pref", "20",
			"handle", "801::1", "u32", "match", "ip", "dst", "192.0.2.1/32", "flowid", root+"1")
		if changed, err := lease.Status(ctx); err != nil || changed.Applied {
			t.Fatalf("status with a foreign filter: %+v, %v", changed, err)
		}
		if err := lease.Restore(ctx); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Fatalf("Restore with a foreign filter: %v", err)
		}
		if kept := rootQdisc(t, ctx, image, delayed.Container, lease.Interface); kept != "prio "+root {
			t.Fatalf("root after the refused Restore: %s", kept)
		}
		if filters := delayExec(t, ctx, owner, "tc", "-j", "filter", "show", "dev", lease.Interface); !strings.Contains(filters, `"pref":20`) {
			t.Fatalf("the foreign filter is gone: %s", filters)
		}
		delayExec(t, ctx, owner, "tc", "filter", "del", "dev", lease.Interface, "parent", root, "protocol", "ip", "pref", "20")
		if err := lease.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		if after := rootQdisc(t, ctx, image, delayed.Container, lease.Interface); after != "noqueue 0:" {
			t.Fatalf("root qdisc after Restore: %s", after)
		}
		t.Logf("a foreign pref 20 filter under %s kept the peer delay: Restore refused, the root and filter stayed, and Restore succeeded after the owner removed it", root)
	})

	t.Run("peer blackhole", func(t *testing.T) {
		// Every packet that osd.N sends to the client container is dropped:
		// the client's read from that OSD times out, while its read from the
		// other OSD and the control container's read from the dropped OSD
		// answer. After Restore the client reads it again.
		address, err := client.ContainerIP(ctx)
		if err != nil {
			t.Fatal(err)
		}
		other := toolkitContainer(t, cluster)
		blackhole := fault.NetworkProfile{HelperImage: image, Plane: ceph.PublicNetworkPlane, Peer: netip.MustParseAddr(address), Loss: 100}
		lease, err := fault.ApplyNetworkProfile(ctx, cluster, delayed.Container, blackhole)
		if lease != nil {
			defer func() { _ = lease.Restore(context.WithoutCancel(ctx)) }()
		}
		if err != nil {
			t.Fatal(err)
		}
		status, err := lease.Status(ctx)
		if err != nil || !status.Applied || status.Loss != 100 || status.Delay != 0 {
			t.Fatalf("status of the blackhole: %+v, %v", status, err)
		}
		dropped := timedRead(t, ctx, client, slow)
		served := timedRead(t, ctx, client, fast)
		elsewhere := timedRead(t, ctx, other, slow)
		counted, err := lease.Status(ctx)
		if err != nil || counted.Drops == 0 {
			t.Fatalf("netem dropped %d selected packets: %v", counted.Drops, err)
		}
		if err := lease.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		after := timedRead(t, ctx, client, slow)
		if dropped.Errno != 110 || served.Errno != 0 || elsewhere.Errno != 0 || after.Errno != 0 {
			t.Fatalf("blackhole to %s: %s %+v and %s %+v from it, %s %+v from the control container, %s %+v after Restore",
				address, slow, dropped, fast, served, slow, elsewhere, slow, after)
		}
		t.Logf("osd.%d drops every packet to %s: its read of %s failed with errno %d after %.1f s, of %s took %.1f ms; the control container read %s in %.1f ms; after Restore %.1f ms; netem dropped %d packets",
			delayed.ID, address, slow, dropped.Errno, dropped.Ms/1000, fast, served.Ms, slow, elsewhere.Ms, after.Ms, counted.Drops)
	})

	t.Run("peers blackhole", func(t *testing.T) {
		// One lease drops every packet that osd.N sends to two client
		// containers, through one filter each: both clients' reads from
		// that OSD time out, while the first client's read from the other
		// OSD and the control container's read from the dropped OSD answer.
		// After Restore both clients read it again.
		clientImage, _ := integrationImages(t)
		second, err := testcontainers.Run(ctx, nativeClientImage(t, clientImage), cluster.WithClient(), ceph.WithIdleEntrypoint(),
			testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status"})))
		testcontainers.CleanupContainer(t, second)
		if err != nil {
			t.Fatal(err)
		}
		var peers []fault.NetworkPeer
		for _, ctr := range []testcontainers.Container{client, second} {
			address, err := ctr.ContainerIP(ctx)
			if err != nil {
				t.Fatal(err)
			}
			peers = append(peers, fault.NetworkPeer{Addr: netip.MustParseAddr(address)})
		}
		other := toolkitContainer(t, cluster)
		if address, err := other.ContainerIP(ctx); err != nil || slices.ContainsFunc(peers, func(peer fault.NetworkPeer) bool { return peer.Addr.String() == address }) {
			t.Fatalf("the control container's address %q: %v", address, err)
		}
		lease, err := fault.ApplyNetworkProfile(ctx, cluster, delayed.Container, fault.NetworkProfile{HelperImage: image, Plane: ceph.PublicNetworkPlane, Peers: peers, Loss: 100})
		if lease != nil {
			defer func() { _ = lease.Restore(context.WithoutCancel(ctx)) }()
		}
		if err != nil {
			t.Fatal(err)
		}
		status, err := lease.Status(ctx)
		if err != nil || !status.Applied || status.Loss != 100 || status.RootKind != "prio" || !slices.Equal(lease.Peers, peers) {
			t.Fatalf("status of the blackhole to %v: %+v, %v", lease.Peers, status, err)
		}
		var filters []struct {
			Options struct {
				Handle string `json:"fh"`
				FlowID string `json:"flowid"`
			} `json:"options"`
		}
		if err := json.Unmarshal(status.Filters, &filters); err != nil {
			t.Fatal(err)
		}
		var leaves []string
		for _, filter := range filters {
			if filter.Options.FlowID != "" {
				leaves = append(leaves, filter.Options.Handle)
			}
		}
		if !slices.Equal(leaves, []string{"800::1", "800::2"}) {
			t.Fatalf("the kernel shows the filters %v: %s", leaves, status.Filters)
		}
		dropped := timedRead(t, ctx, client, slow)
		droppedSecond := timedRead(t, ctx, second, slow)
		served := timedRead(t, ctx, client, fast)
		elsewhere := timedRead(t, ctx, other, slow)
		counted, err := lease.Status(ctx)
		if err != nil || !counted.Applied || counted.Drops == 0 {
			t.Fatalf("netem dropped %d selected packets: %+v, %v", counted.Drops, counted, err)
		}
		if err := lease.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		if root := rootQdisc(t, ctx, image, delayed.Container, lease.Interface); root != "noqueue 0:" {
			t.Fatalf("root qdisc after Restore: %s", root)
		}
		if helpers := netemHelpers(t, ctx); helpers != 0 {
			t.Fatalf("%d netem helpers after Restore", helpers)
		}
		after := timedRead(t, ctx, client, slow)
		afterSecond := timedRead(t, ctx, second, slow)
		if dropped.Errno != 110 || droppedSecond.Errno != 110 || served.Errno != 0 || elsewhere.Errno != 0 || after.Errno != 0 || afterSecond.Errno != 0 {
			t.Fatalf("blackhole to %v: %s %+v and %+v from them, %s %+v from the first, %s %+v from the control container, %s %+v and %+v after Restore",
				peers, slow, dropped, droppedSecond, fast, served, slow, elsewhere, slow, after, afterSecond)
		}
		t.Logf("osd.%d drops every packet to %v through filters %v: its reads of %s failed with errno %d after %.1f s and %d after %.1f s, of %s took %.1f ms; the control container read %s in %.1f ms; after Restore %.1f ms and %.1f ms; netem dropped %d packets",
			delayed.ID, peers, leaves, slow, dropped.Errno, dropped.Ms/1000, droppedSecond.Errno, droppedSecond.Ms/1000, fast, served.Ms, slow, elsewhere.Ms,
			after.Ms, afterSecond.Ms, counted.Drops)
	})

	t.Run("foreign qdisc", func(t *testing.T) {
		// A qdisc that another owner adds under the lease's root, while the
		// delay itself stays intact, is not the lease's either: Restore
		// refuses, keeps every qdisc, and succeeds after the owner removed it.
		address, err := client.ContainerIP(ctx)
		if err != nil {
			t.Fatal(err)
		}
		selective := request
		selective.Peer = netip.MustParseAddr(address)
		for name, delay := range map[string]fault.NetworkProfile{"peer": selective, "interface": request} {
			lease, err := fault.ApplyNetworkProfile(ctx, cluster, delayed.Container, delay)
			if lease != nil {
				defer func() { _ = lease.Restore(context.WithoutCancel(ctx)) }()
			}
			if err != nil {
				t.Fatal(err)
			}
			status, err := lease.Status(ctx)
			if err != nil || !status.Applied {
				t.Fatalf("status of the %s delay: %+v, %v", name, status, err)
			}
			root := status.RootHandle
			owner := startNetemObserver(t, ctx, image, delayed.Container, true)
			delayExec(t, ctx, owner, "tc", "qdisc", "add", "dev", lease.Interface, "parent", root+"1", "handle", "9999:",
				"tbf", "rate", "1gbit", "burst", "64kb", "latency", "50ms")
			if changed, err := lease.Status(ctx); err != nil || changed.Applied {
				t.Fatalf("status of the %s delay with a foreign qdisc: %+v, %v", name, changed, err)
			}
			if err := lease.Restore(ctx); !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("Restore of the %s delay with a foreign qdisc: %v", name, err)
			}
			qdiscs := delayExec(t, ctx, owner, "tc", "-j", "qdisc", "show", "dev", lease.Interface)
			for _, handle := range []string{root, "9999:"} {
				if !strings.Contains(qdiscs, `"handle":"`+handle+`"`) {
					t.Fatalf("the refused Restore of the %s delay removed %s: %s", name, handle, qdiscs)
				}
			}
			if name == "peer" && strings.Count(qdiscs, `"kind":"netem"`) != 1 {
				t.Fatalf("the refused Restore removed the delay's netem child: %s", qdiscs)
			}
			delayExec(t, ctx, owner, "tc", "qdisc", "del", "dev", lease.Interface, "parent", root+"1", "handle", "9999:")
			if err := lease.Restore(ctx); err != nil {
				t.Fatal(err)
			}
			if after := rootQdisc(t, ctx, image, delayed.Container, lease.Interface); after != "noqueue 0:" {
				t.Fatalf("root qdisc after Restore of the %s delay: %s", name, after)
			}
			t.Logf("a foreign tbf 9999: under %s's band 1 kept the %s delay: Restore refused, every qdisc stayed, and Restore succeeded after the owner removed it", root, name)
		}
	})

	t.Run("refusals", func(t *testing.T) {
		// The official Ceph image has neither tc nor ip.
		if lease, err := fault.ApplyNetworkProfile(ctx, cluster, delayed.Container, fault.NetworkProfile{HelperImage: staticFacts(cluster).Images.Essential,
			Plane: ceph.PublicNetworkPlane, Delay: delayAmount}); !errors.Is(err, fault.ErrNetemImageUnsupported) || !errors.Is(err, ceph.ErrImageCapability) || lease != nil {
			t.Fatalf("a helper without tc: %v", err)
		}
		// A root qdisc that the test installed is not replaced.
		owner := startNetemObserver(t, ctx, image, other.Container, true)
		delayExec(t, ctx, owner, "tc", "qdisc", "add", "dev", "eth0", "root", "handle", "8001:", "tbf", "rate", "1gbit", "burst", "64kb", "latency", "50ms")
		if lease, err := fault.ApplyNetworkProfile(ctx, cluster, other.Container, request); err == nil || lease != nil || !strings.Contains(err.Error(), "tbf") {
			t.Fatalf("a custom root qdisc: %v", err)
		}
		if root := rootQdisc(t, ctx, image, other.Container, "eth0"); root != "tbf 8001:" {
			t.Fatalf("root qdisc after the refusal: %s", root)
		}
		delayExec(t, ctx, owner, "tc", "qdisc", "del", "dev", "eth0", "root", "handle", "8001:")
		// A container on another network is not admitted.
		stranger, err := testcontainers.Run(ctx, staticFacts(cluster).Images.Essential, ceph.WithIdleEntrypoint())
		testcontainers.CleanupContainer(t, stranger)
		if err != nil {
			t.Fatal(err)
		}
		if lease, err := fault.ApplyNetworkProfile(ctx, cluster, stranger, request); err == nil || lease != nil {
			t.Fatal("a container outside the cluster's network was delayed")
		}
		if helpers := netemHelpers(t, ctx); helpers != 0 {
			t.Fatalf("%d netem helpers after the refusals", helpers)
		}
	})

	t.Run("external changes", func(t *testing.T) {
		// A root qdisc that replaced the lease's is refused by Restore.
		lease, err := fault.ApplyNetworkProfile(ctx, cluster, delayed.Container, request)
		if err != nil {
			t.Fatal(err)
		}
		editor := startNetemObserver(t, ctx, image, delayed.Container, true)
		delayExec(t, ctx, editor, "tc", "qdisc", "replace", "dev", lease.Interface, "root", "handle", "8002:", "tbf", "rate", "1gbit", "burst", "64kb", "latency", "50ms")
		if err := lease.Restore(ctx); err == nil || !strings.Contains(err.Error(), "tbf") {
			t.Fatalf("Restore over a replaced root: %v", err)
		}
		if root := rootQdisc(t, ctx, image, delayed.Container, lease.Interface); root != "tbf 8002:" {
			t.Fatalf("Restore changed a foreign root: %s", root)
		}
		delayExec(t, ctx, editor, "tc", "qdisc", "del", "dev", lease.Interface, "root", "handle", "8002:")
		if err := lease.Restore(ctx); err != nil {
			t.Fatalf("Restore after the foreign root left: %v", err)
		}
		// A removed helper is replaced, and the qdisc it left is removed.
		lease, err = fault.ApplyNetworkProfile(ctx, cluster, delayed.Container, request)
		if err != nil {
			t.Fatal(err)
		}
		docker, err := testcontainers.NewDockerClientWithOpts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = docker.Close() }()
		list, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: make(mobycl.Filters).Add("label", "org.testcontainers.ceph.role=netem-helper")})
		if err != nil || len(list.Items) != 1 {
			t.Fatalf("helpers: %v, %v", list.Items, err)
		}
		if _, err := docker.ContainerRemove(ctx, list.Items[0].ID, mobycl.ContainerRemoveOptions{Force: true}); err != nil {
			t.Fatal(err)
		}
		if root := rootQdisc(t, ctx, image, delayed.Container, lease.Interface); !strings.HasPrefix(root, "netem ") {
			t.Fatalf("root qdisc after the helper was removed: %s", root)
		}
		if err := lease.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		if root := rootQdisc(t, ctx, image, delayed.Container, lease.Interface); root != "noqueue 0:" {
			t.Fatalf("root qdisc after Restore: %s", root)
		}
	})

	t.Run("caller container restart and Terminate", func(t *testing.T) {
		// A restarted caller container gets a new namespace without the delay.
		lease, err := fault.ApplyNetworkProfile(ctx, cluster, client, request)
		if err != nil {
			t.Fatal(err)
		}
		timeout := 1
		docker, err := testcontainers.NewDockerClientWithOpts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = docker.Close() }()
		if _, err := docker.ContainerRestart(ctx, client.GetContainerID(), mobycl.ContainerRestartOptions{Timeout: &timeout}); err != nil {
			t.Fatal(err)
		}
		if status, err := lease.Status(ctx); err != nil || !status.Gone {
			t.Fatalf("status after the restart: %+v, %v", status, err)
		}
		if err := lease.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		if root := rootQdisc(t, ctx, image, client, lease.Interface); root != "noqueue 0:" {
			t.Fatalf("root qdisc of the restarted container: %s", root)
		}
		// Terminate removes a delay that is still held from a caller-owned
		// container before it tears the cluster down.
		if _, err := fault.ApplyNetworkProfile(ctx, cluster, client, request); err != nil {
			t.Fatal(err)
		}
		// The caller's container is still attached, so the bridge stays until
		// the test's cleanup removes the container and terminates again.
		if err := cluster.Terminate(ctx); err != nil && !strings.Contains(err.Error(), "active endpoints") {
			t.Fatal(err)
		}
		if root := rootQdisc(t, ctx, image, client, lease.Interface); root != "noqueue 0:" {
			t.Fatalf("root qdisc of the caller container after Terminate: %s", root)
		}
		if helpers := netemHelpers(t, ctx); helpers != 0 {
			t.Fatalf("%d netem helpers after Terminate", helpers)
		}
	})
}

// testNetworkDelayPeerIPv6 delays only the packets that an OSD of an IPv6
// cluster sends to the client container's IPv6 address, through the IPv6 u32
// filter: the client's reads from that OSD wait, its reads from the other OSD
// and the control container's reads do not.
func testNetworkDelayPeerIPv6(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	image := netemImage(t)
	cluster, client := newServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithPoolDefaults(1, 1)), ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithAddressFamily(ceph.AddressIPv6)))
	objects := placeDelayObjects(t, ctx, cluster, client)
	osds := clusteraccess.OSDs(t, cluster)
	delayed, other := osds[0], osds[1]
	slow, fast := objects[delayed.ID], objects[other.ID]
	ipv6 := func(ctr testcontainers.Container) netip.Addr {
		t.Helper()
		inspection, err := ctr.Inspect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		endpoint := inspection.NetworkSettings.Networks[staticFacts(cluster).NetworkName]
		if endpoint == nil || !endpoint.GlobalIPv6Address.IsValid() {
			t.Fatalf("no IPv6 address on %s", staticFacts(cluster).NetworkName)
		}
		return endpoint.GlobalIPv6Address
	}
	peer, control := ipv6(client), ipv6(toolkitContainer(t, cluster))
	lease, err := fault.ApplyNetworkProfile(ctx, cluster, delayed.Container, fault.NetworkProfile{HelperImage: image,
		Plane: ceph.PublicNetworkPlane, Delay: delayAmount, Jitter: delayJitter, Peer: peer})
	if lease != nil {
		defer func() { _ = lease.Restore(context.WithoutCancel(ctx)) }()
	}
	if err != nil {
		t.Fatal(err)
	}
	status, err := lease.Status(ctx)
	if err != nil || !status.Applied || status.RootKind != "prio" {
		t.Fatalf("status of the IPv6 peer delay: %+v, %v", status, err)
	}
	during := readDelayObjects(t, ctx, client, slow, fast)
	elsewhere := readDelayObjects(t, ctx, toolkitContainer(t, cluster), slow)
	counted, err := lease.Status(ctx)
	if err != nil || counted.Packets == 0 {
		t.Fatalf("netem counted %d selected packets: %v", counted.Packets, err)
	}
	if err := lease.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	after := readDelayObjects(t, ctx, client, slow)
	if root := rootQdisc(t, ctx, image, delayed.Container, lease.Interface); root != "noqueue 0:" {
		t.Fatalf("root qdisc after Restore: %s", root)
	}
	limit := float64(delayAmount.Milliseconds())
	lowest := float64((delayAmount - delayJitter).Milliseconds())
	if during[slow].Min < lowest || during[fast].Median >= limit/2 || elsewhere[slow].Median >= limit/2 || after[slow].Median >= limit/2 {
		t.Fatalf("IPv6 peer delay to %s: %s %+v and %s %+v from it, %s %+v from %s, %s %+v after Restore",
			peer, slow, during[slow], fast, during[fast], slow, elsewhere[slow], control, slow, after[slow])
	}
	filters, _ := json.Marshal(status.Filters)
	t.Logf("osd.%d delayed by %v±%v to %s only: from it %s median %.1f ms (min %.1f), %s median %.1f ms; from %s %s median %.1f ms; after Restore %.1f ms; netem counted %d selected packets; filters %s",
		delayed.ID, delayAmount, delayJitter, peer, slow, during[slow].Median, during[slow].Min, fast, during[fast].Median,
		control, slow, elsewhere[slow].Median, after[slow].Median, counted.Packets, filters)

	testNetworkDelayMixedPeers(t, ctx, image, cluster, client, delayed, slow, fast)
}

// connectProbeScript connects over TCP to port 9 of each IPv4 address once,
// where nothing listens, and prints how each attempt ended and how long it
// took. The SYN leaves the probe's namespace and the reset comes back.
const connectProbeScript = `import json,socket,sys,time
out=[]
for address in sys.argv[1:]:
 s=socket.socket(socket.AF_INET,socket.SOCK_STREAM); s.settimeout(5); start=time.monotonic()
 try:
  s.connect((address,9)); result="connected"
 except ConnectionRefusedError: result="refused"
 except OSError as e: result=type(e).__name__
 out.append({"address":address,"result":result,"ms":(time.monotonic()-start)*1000}); s.close()
print(json.dumps(out))
`

type connectProbe struct {
	Address string  `json:"address"`
	Result  string  `json:"result"`
	Ms      float64 `json:"ms"`
}

// testNetworkDelayMixedPeers delays, with one lease, osd.N's packets to the
// client's IPv6 address and to the control container's IPv4 address: an
// IPv4 filter in preference 10 and table 800:, and an IPv6 filter in
// preference 11 and table 801:. The client's Ceph reads, over IPv6, wait;
// the control container's, also over IPv6, do not. A TCP connect from the
// OSD's namespace to the control container's IPv4 address waits, one to the
// client's IPv4 address does not.
func testNetworkDelayMixedPeers(t *testing.T, ctx context.Context, image string, cluster *ceph.Container, client testcontainers.Container, delayed *ceph.OSDContainer, slow, fast string) {
	t.Helper()
	endpoint := func(ctr testcontainers.Container) (netip.Addr, netip.Addr) {
		t.Helper()
		inspection, err := ctr.Inspect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		settings := inspection.NetworkSettings.Networks[staticFacts(cluster).NetworkName]
		if settings == nil || !settings.IPAddress.Is4() || !settings.GlobalIPv6Address.IsValid() {
			t.Fatalf("no IPv4 and IPv6 address on %s", staticFacts(cluster).NetworkName)
		}
		return settings.IPAddress, settings.GlobalIPv6Address
	}
	client4, client6 := endpoint(client)
	control4, control6 := endpoint(toolkitContainer(t, cluster))
	peers := []fault.NetworkPeer{{Addr: client6}, {Addr: control4}}
	lease, err := fault.ApplyNetworkProfile(ctx, cluster, delayed.Container, fault.NetworkProfile{HelperImage: image,
		Plane: ceph.PublicNetworkPlane, Delay: delayAmount, Jitter: delayJitter, Peers: peers})
	if lease != nil {
		defer func() { _ = lease.Restore(context.WithoutCancel(ctx)) }()
	}
	if err != nil {
		t.Fatal(err)
	}
	status, err := lease.Status(ctx)
	if err != nil || !status.Applied || status.RootKind != "prio" {
		t.Fatalf("status of the mixed peer delay: %+v, %v", status, err)
	}
	var filters []struct {
		Protocol string `json:"protocol"`
		Pref     int    `json:"pref"`
		Options  struct {
			Handle string `json:"fh"`
			FlowID string `json:"flowid"`
		} `json:"options"`
	}
	if err := json.Unmarshal(status.Filters, &filters); err != nil {
		t.Fatal(err)
	}
	var leaves []string
	for _, filter := range filters {
		if filter.Options.FlowID != "" {
			leaves = append(leaves, fmt.Sprintf("%s/%d/%s", filter.Protocol, filter.Pref, filter.Options.Handle))
		}
	}
	slices.Sort(leaves)
	if !slices.Equal(leaves, []string{"ip/10/800::1", "ipv6/11/801::1"}) {
		t.Fatalf("the kernel shows the filters %v: %s", leaves, status.Filters)
	}
	during := readDelayObjects(t, ctx, client, slow, fast)
	elsewhere := readDelayObjects(t, ctx, toolkitContainer(t, cluster), slow)
	prober := startNetemObserver(t, ctx, image, delayed.Container, false)
	var probes []connectProbe
	if err := json.Unmarshal([]byte(delayExec(t, ctx, prober, "python3", "-c", connectProbeScript, control4.String(), client4.String())), &probes); err != nil || len(probes) != 2 {
		t.Fatalf("connect probes %+v: %v", probes, err)
	}
	counted, err := lease.Status(ctx)
	if err != nil || !counted.Applied || counted.Packets == 0 {
		t.Fatalf("netem counted %d selected packets: %+v, %v", counted.Packets, counted, err)
	}
	if err := lease.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	var restored []connectProbe
	if err := json.Unmarshal([]byte(delayExec(t, ctx, prober, "python3", "-c", connectProbeScript, control4.String())), &restored); err != nil || len(restored) != 1 {
		t.Fatalf("connect probe after Restore %+v: %v", restored, err)
	}
	after := readDelayObjects(t, ctx, client, slow)
	if root := rootQdisc(t, ctx, image, delayed.Container, lease.Interface); root != "noqueue 0:" {
		t.Fatalf("root qdisc after Restore: %s", root)
	}
	limit := float64(delayAmount.Milliseconds())
	lowest := float64((delayAmount - delayJitter).Milliseconds())
	if during[slow].Min < lowest || during[fast].Median >= limit/2 || elsewhere[slow].Median >= limit/2 || after[slow].Median >= limit/2 {
		t.Fatalf("mixed peer delay to %v: %s %+v and %s %+v from it, %s %+v from %s, %s %+v after Restore",
			peers, slow, during[slow], fast, during[fast], slow, elsewhere[slow], control6, slow, after[slow])
	}
	if probes[0].Result != "refused" || probes[0].Ms < lowest || probes[1].Result != "refused" || probes[1].Ms >= limit/2 ||
		restored[0].Result != "refused" || restored[0].Ms >= limit/2 {
		t.Fatalf("connect probes while delayed %+v, after Restore %+v", probes, restored)
	}
	t.Logf("osd.%d delayed by %v±%v to %v through filters %v: the client's reads of %s over IPv6 median %.1f ms (min %.1f), of %s %.1f ms; the control container's read of %s over IPv6 %.1f ms; a connect to %s:9 was refused after %.1f ms, to %s:9 after %.1f ms, and to %s:9 after %.1f ms after Restore; after Restore the client's read took %.1f ms; netem counted %d selected packets",
		delayed.ID, delayAmount, delayJitter, peers, leaves, slow, during[slow].Median, during[slow].Min, fast, during[fast].Median, slow, elsewhere[slow].Median,
		control4, probes[0].Ms, client4, probes[1].Ms, control4, restored[0].Ms, after[slow].Median, counted.Packets)
}

// timedReadScript reads one object once with a 5-second OSD operation timeout
// and prints the errno (0 on success) and the time it took.
const timedReadScript = `import json,rados,sys,time
c=rados.Rados(conffile="/etc/ceph/ceph.conf",conf={"rados_osd_op_timeout":"5"})
c.connect(timeout=30)
try:
 io=c.open_ioctx(sys.argv[1])
 start=time.monotonic(); errno=0
 try: io.read(sys.argv[2],1<<20)
 except rados.Error as e: errno=getattr(e,"errno",None) or -1
 print(json.dumps({"errno":abs(errno),"ms":(time.monotonic()-start)*1000}))
 io.close()
finally:
 c.shutdown()
`

type timedReadResult struct {
	Errno int     `json:"errno"`
	Ms    float64 `json:"ms"`
}

func timedRead(t *testing.T, ctx context.Context, ctr testcontainers.Container, name string) timedReadResult {
	t.Helper()
	out := delayExec(t, ctx, ctr, "python3", "-c", timedReadScript, delayPool, name)
	var result timedReadResult
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &result); err != nil {
		t.Fatalf("parse timed read %q: %v", out, err)
	}
	return result
}
