//go:build all || integration

package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/testcontainers/testcontainers-go"
)

// directionReadTimeout bounds every read of the direction recipes, so a
// read that a cut path holds ends as ETIMEDOUT.
const directionReadTimeout = 8

// directionFreshRead reads each object once through a client of its own and
// prints, per object, whether it answered, its errno and the time it took.
// Arguments: the pool and the objects.
const directionFreshRead = `import errno,json,rados,sys,time
pool,names=sys.argv[1],sys.argv[2:]
timeout=str(` + "%d" + `)
out={}
with rados.Rados(conffile="/etc/ceph/ceph.conf",conf={"client_mount_timeout":"20","rados_mon_op_timeout":"20","rados_osd_op_timeout":timeout}) as c:
    with c.open_ioctx(pool) as io:
        for name in names:
            start=time.monotonic()
            try:
                io.read(name,16,0)
                out[name]={"answered":True,"errno":0}
            except rados.Error as e:
                out[name]={"answered":False,"errno":abs(getattr(e,"errno",0) or 0)}
            out[name]["ms"]=round((time.monotonic()-start)*1000,1)
print(json.dumps(out))`

// directionHeldReader keeps one client: it reads both objects, which opens
// its sessions to both OSDs, and writes /tmp/direction.ready. Each time
// /tmp/direction.go-N appears it reads both objects again on the same
// sessions and writes the result to /tmp/direction.result-N, until
// /tmp/direction.stop appears. Arguments: the pool and the two objects.
const directionHeldReader = `import json,os,rados,sys,time
pool,names=sys.argv[1],sys.argv[2:]
with rados.Rados(conffile="/etc/ceph/ceph.conf",conf={"client_mount_timeout":"20","rados_mon_op_timeout":"20","rados_osd_op_timeout":"` + "%d" + `"}) as c:
    with c.open_ioctx(pool) as io:
        for name in names:
            io.read(name,16,0)
        open("/tmp/direction.ready","w").write(str(c.get_instance_id()))
        n=0
        while not os.path.exists("/tmp/direction.stop"):
            if os.path.exists("/tmp/direction.go-%%d"%%n):
                out={}
                for name in names:
                    start=time.monotonic()
                    try:
                        io.read(name,16,0)
                        out[name]={"answered":True,"errno":0}
                    except rados.Error as e:
                        out[name]={"answered":False,"errno":abs(getattr(e,"errno",0) or 0)}
                    out[name]["ms"]=round((time.monotonic()-start)*1000,1)
                open("/tmp/direction.result-%%d"%%n,"w").write(json.dumps(out))
                n+=1
            time.sleep(0.1)`

type directionRead struct {
	Answered bool    `json:"answered"`
	Errno    int     `json:"errno"`
	MS       float64 `json:"ms"`
}

func freshDirectionReads(t *testing.T, ctx context.Context, client testcontainers.Container, names ...string) map[string]directionRead {
	t.Helper()
	out := DelayExec(t, ctx, client, append([]string{"python3", "-c", fmt.Sprintf(directionFreshRead, directionReadTimeout), delayPool}, names...)...)
	reads := map[string]directionRead{}
	if err := json.Unmarshal([]byte(LastJSONLine(out)), &reads); err != nil || len(reads) != len(names) {
		t.Fatalf("fresh reads %q: %v", out, err)
	}
	return reads
}

// heldDirectionReads asks the held reader for round n and waits for it.
func heldDirectionReads(t *testing.T, ctx context.Context, client testcontainers.Container, n int) map[string]directionRead {
	t.Helper()
	DelayExec(t, ctx, client, "touch", "/tmp/direction.go-"+strconv.Itoa(n))
	path := "/tmp/direction.result-" + strconv.Itoa(n)
	deadline := time.Now().Add(time.Duration(2*directionReadTimeout+20) * time.Second)
	for {
		if code, out, err := SessionExec(ctx, client, "cat", path); err == nil && code == 0 && strings.TrimSpace(out) != "" {
			reads := map[string]directionRead{}
			if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &reads); err != nil {
				t.Fatalf("held reads %q: %v", out, err)
			}
			return reads
		}
		if time.Now().After(deadline) {
			t.Fatalf("held reader gave no round %d:\n%s", n, containerFile(t, ctx, client, "/tmp/direction.err"))
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// osdV2Address returns the msgr2 public address of osd from the OSDMap.
func osdV2Address(t *testing.T, ctx context.Context, cluster *ceph.Container, osd int) (netip.Addr, uint16) {
	t.Helper()
	addresses, err := ObserveCluster(cluster).DaemonAddresses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range addresses.OSDs {
		if entry.ID != osd {
			continue
		}
		for _, address := range entry.Public {
			if address.Type == "v2" {
				port, err := netip.ParseAddrPort(address.Addr)
				if err != nil {
					t.Fatal(err)
				}
				return port.Addr(), port.Port()
			}
		}
	}
	t.Fatalf("osd.%d has no v2 public address in %+v", osd, addresses.OSDs)
	return netip.Addr{}, 0
}

func directionApply(t *testing.T, ctx context.Context, cluster *ceph.Container, target testcontainers.Container, profile fault.NetworkProfile) *fault.NetworkProfileLease {
	t.Helper()
	lease, err := fault.ApplyNetworkProfile(ctx, cluster, target, profile)
	if err != nil {
		if lease != nil {
			_ = lease.Restore(context.WithoutCancel(ctx))
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Restore(context.Background()) })
	return lease
}

// RunNetworkDirectionAndPhase shows the Direction and Phase selectors of
// fault.NetworkProfile against two OSDs, A and B, each the primary of one
// object of a single-replica pool:
//
//   - setup: a ConnectionSetup profile with a Loss of 100 on the client's
//     endpoint toward A's msgr2 address. A client that opened its sessions
//     before the profile still reads both objects at once; a new client
//     reads B's object and times out on A's, because its connection to A
//     never completes. After Restore a new client reads A's object again.
//   - inbound: an Inbound profile with a Loss of 100 toward A's msgr2
//     address. The module puts it on A's endpoint, selecting what A sends
//     from that port to the client, and nothing on the client's. A new
//     client times out on A's object and reads B's; after Restore it reads
//     both.
//
// RUN_RECEIPT logs every read, the leases and the netem counters.
func RunNetworkDirectionAndPhase(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	image := NetemImage(t)
	cluster, client := NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithPoolDefaults(1, 1)))
	objects := placeDelayObjects(t, ctx, cluster, client)
	osds := clusteraccess.OSDs(t, cluster)
	cutOSD, otherOSD := osds[0], osds[1]
	cut, other := objects[cutOSD.ID], objects[otherOSD.ID]
	address, port := osdV2Address(t, ctx, cluster, cutOSD.ID)
	receipt := map[string]any{"cut_osd": cutOSD.ID, "other_osd": otherOSD.ID, "cut_object": cut, "other_object": other,
		"osd_address": address.String(), "osd_port": port}
	defer func() {
		data, _ := json.Marshal(receipt)
		t.Logf("NETWORK_DIRECTION_PHASE_RECEIPT %s", data)
	}()
	if baseline := freshDirectionReads(t, ctx, client, cut, other); !baseline[cut].Answered || !baseline[other].Answered {
		t.Fatalf("baseline reads %+v", baseline)
	}

	t.Run("setup", func(t *testing.T) {
		script := fmt.Sprintf(directionHeldReader, directionReadTimeout)
		if err := client.CopyToContainer(ctx, []byte(script), "/tmp/direction_held.py", 0o644); err != nil {
			t.Fatal(err)
		}
		DelayExec(t, ctx, client, "sh", "-c", `rm -f /tmp/direction.*; python3 /tmp/direction_held.py "$@" > /tmp/direction.log 2> /tmp/direction.err &`, "sh", delayPool, cut, other)
		defer DelayExec(t, context.WithoutCancel(ctx), client, "touch", "/tmp/direction.stop")
		deadline := time.Now().Add(60 * time.Second)
		for {
			if code, _, err := SessionExec(ctx, client, "test", "-s", "/tmp/direction.ready"); err == nil && code == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the held reader did not open its sessions:\n%s", containerFile(t, ctx, client, "/tmp/direction.err"))
			}
			time.Sleep(200 * time.Millisecond)
		}
		lease := directionApply(t, ctx, cluster, client, fault.NetworkProfile{HelperImage: image, Plane: ceph.PublicNetworkPlane,
			Peer: address, PeerPort: port, Phase: fault.ConnectionSetup, Loss: 100})
		status, err := lease.Status(ctx)
		if err != nil || !status.Applied || status.Loss != 100 {
			t.Fatalf("status %+v, %v", status, err)
		}
		held := heldDirectionReads(t, ctx, client, 0)
		fresh := freshDirectionReads(t, ctx, client, cut, other)
		counted, err := lease.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := lease.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		restored := freshDirectionReads(t, ctx, client, cut, other)
		receipt["setup"] = map[string]any{"held": held, "fresh": fresh, "restored": restored, "interface": lease.Interface,
			"netem_packets": counted.Packets, "netem_drops": counted.Drops}
		switch {
		case !held[cut].Answered || !held[other].Answered || held[cut].MS > 1000:
			t.Fatalf("the sessions opened before the profile stopped answering: %+v", held)
		case fresh[cut].Answered || fresh[cut].Errno != 110 || !fresh[other].Answered:
			t.Fatalf("a new client under ConnectionSetup loss: %+v (want ETIMEDOUT on %s only)", fresh, cut)
		case counted.Drops == 0:
			t.Fatalf("netem dropped no SYN: %+v", counted)
		case !restored[cut].Answered || !restored[other].Answered:
			t.Fatalf("reads after Restore: %+v", restored)
		}
	})

	t.Run("inbound", func(t *testing.T) {
		lease := directionApply(t, ctx, cluster, client, fault.NetworkProfile{HelperImage: image, Plane: ceph.PublicNetworkPlane,
			Peer: address, PeerPort: port, Direction: fault.Inbound, Loss: 100})
		if len(lease.PeerLeases) != 1 || lease.PeerLeases[0].ContainerID != cutOSD.GetContainerID() || !lease.PeerLeases[0].SourcePorts {
			t.Fatalf("the Inbound lease is not on osd.%d's endpoint: %+v", cutOSD.ID, lease.PeerLeases)
		}
		peer := lease.PeerLeases[0]
		status, err := peer.Status(ctx)
		if err != nil || !status.Applied {
			t.Fatalf("status of osd.%d's lease %+v, %v", cutOSD.ID, status, err)
		}
		clientRoot := RootQdisc(t, ctx, image, client, "eth0")
		fresh := freshDirectionReads(t, ctx, client, cut, other)
		counted, err := peer.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := lease.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		restored := freshDirectionReads(t, ctx, client, cut, other)
		helpers := NetemHelpers(t, ctx)
		receipt["inbound"] = map[string]any{"fresh": fresh, "restored": restored, "peer_container": peer.ContainerID,
			"peer_selection": peer.Peers, "client_root": clientRoot, "netem_packets": counted.Packets, "netem_drops": counted.Drops, "helpers_after": helpers}
		switch {
		case !strings.HasPrefix(clientRoot, "noqueue "):
			t.Fatalf("the client's endpoint was shaped: %s", clientRoot)
		case fresh[cut].Answered || fresh[cut].Errno != 110 || !fresh[other].Answered:
			t.Fatalf("reads with osd.%d's answers cut: %+v (want ETIMEDOUT on %s only)", cutOSD.ID, fresh, cut)
		case counted.Drops == 0:
			t.Fatalf("osd.%d's netem dropped nothing: %+v", cutOSD.ID, counted)
		case !restored[cut].Answered || !restored[other].Answered:
			t.Fatalf("reads after Restore: %+v", restored)
		case helpers != 0:
			t.Fatalf("%d netem helpers after Restore", helpers)
		}
	})
}
