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

	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
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
		image = ceph.NetemHelperImage
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
	cluster, client := newServiceCluster(t, ceph.WithPoolDefaults(1, 1))
	fsid, err := cluster.Ceph(ctx, "fsid")
	if err != nil {
		t.Fatal(err)
	}
	objects := placeDelayObjects(t, ctx, cluster, client)
	osds := cluster.OSDs()
	delayed, other := osds[0], osds[1]
	slow, fast := objects[delayed.ID], objects[other.ID]
	payload := delayExec(t, ctx, client, "rados", "-p", delayPool, "get", slow, "/dev/stdout")
	request := ceph.NetworkDelay{HelperImage: image, Plane: ceph.PublicNetworkPlane, Delay: delayAmount, Jitter: delayJitter}

	t.Run("delay", func(t *testing.T) {
		baseline := readDelayObjects(t, ctx, client, slow, fast)
		if err := client.CopyToContainer(ctx, []byte(delayHeldScript), "/tmp/delay_held.py", 0o644); err != nil {
			t.Fatal(err)
		}
		delayExec(t, ctx, client, "sh", "-c", `rm -f /tmp/delay.stop; python3 /tmp/delay_held.py "$@" > /tmp/delay.log 2> /tmp/delay.err &`, "sh", delayPool, slow, fast)
		defer delayExec(t, context.WithoutCancel(ctx), client, "touch", "/tmp/delay.stop")
		time.Sleep(3 * time.Second)
		lease, err := cluster.TemporaryNetworkDelay(ctx, delayed.Container, request)
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
		other := cluster.ControlContainer()
		otherAddress, err := other.ContainerIP(ctx)
		if err != nil || otherAddress == address {
			t.Fatalf("the control container's address %q: %v", otherAddress, err)
		}
		selective := request
		selective.Peer = peer
		lease, err := cluster.TemporaryNetworkDelay(ctx, delayed.Container, selective)
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
		lease, err := cluster.TemporaryNetworkDelay(ctx, delayed.Container, selective)
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
		for name, delay := range map[string]ceph.NetworkDelay{"peer": selective, "interface": request} {
			lease, err := cluster.TemporaryNetworkDelay(ctx, delayed.Container, delay)
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
		if lease, err := cluster.TemporaryNetworkDelay(ctx, delayed.Container, ceph.NetworkDelay{HelperImage: cluster.ControlImage(),
			Plane: ceph.PublicNetworkPlane, Delay: delayAmount}); !errors.Is(err, ceph.ErrNetemImageUnsupported) || !errors.Is(err, ceph.ErrImageCapability) || lease != nil {
			t.Fatalf("a helper without tc: %v", err)
		}
		// A root qdisc that the test installed is not replaced.
		owner := startNetemObserver(t, ctx, image, other.Container, true)
		delayExec(t, ctx, owner, "tc", "qdisc", "add", "dev", "eth0", "root", "handle", "8001:", "tbf", "rate", "1gbit", "burst", "64kb", "latency", "50ms")
		if lease, err := cluster.TemporaryNetworkDelay(ctx, other.Container, request); err == nil || lease != nil || !strings.Contains(err.Error(), "tbf") {
			t.Fatalf("a custom root qdisc: %v", err)
		}
		if root := rootQdisc(t, ctx, image, other.Container, "eth0"); root != "tbf 8001:" {
			t.Fatalf("root qdisc after the refusal: %s", root)
		}
		delayExec(t, ctx, owner, "tc", "qdisc", "del", "dev", "eth0", "root", "handle", "8001:")
		// A container on another network is not admitted.
		stranger, err := testcontainers.Run(ctx, cluster.ControlImage(), ceph.WithIdleEntrypoint())
		testcontainers.CleanupContainer(t, stranger)
		if err != nil {
			t.Fatal(err)
		}
		if lease, err := cluster.TemporaryNetworkDelay(ctx, stranger, request); err == nil || lease != nil {
			t.Fatal("a container outside the cluster's network was delayed")
		}
		if helpers := netemHelpers(t, ctx); helpers != 0 {
			t.Fatalf("%d netem helpers after the refusals", helpers)
		}
	})

	t.Run("external changes", func(t *testing.T) {
		// A root qdisc that replaced the lease's is refused by Restore.
		lease, err := cluster.TemporaryNetworkDelay(ctx, delayed.Container, request)
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
		lease, err = cluster.TemporaryNetworkDelay(ctx, delayed.Container, request)
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
		lease, err := cluster.TemporaryNetworkDelay(ctx, client, request)
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
		if _, err := cluster.TemporaryNetworkDelay(ctx, client, request); err != nil {
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
	cluster, client := newServiceCluster(t, ceph.WithPoolDefaults(1, 1), ceph.WithAddressFamily(ceph.AddressIPv6))
	objects := placeDelayObjects(t, ctx, cluster, client)
	osds := cluster.OSDs()
	delayed, other := osds[0], osds[1]
	slow, fast := objects[delayed.ID], objects[other.ID]
	ipv6 := func(ctr testcontainers.Container) netip.Addr {
		t.Helper()
		inspection, err := ctr.Inspect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		endpoint := inspection.NetworkSettings.Networks[cluster.NetworkName()]
		if endpoint == nil || !endpoint.GlobalIPv6Address.IsValid() {
			t.Fatalf("no IPv6 address on %s", cluster.NetworkName())
		}
		return endpoint.GlobalIPv6Address
	}
	peer, control := ipv6(client), ipv6(cluster.ControlContainer())
	lease, err := cluster.TemporaryNetworkDelay(ctx, delayed.Container, ceph.NetworkDelay{HelperImage: image,
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
	elsewhere := readDelayObjects(t, ctx, cluster.ControlContainer(), slow)
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
}
