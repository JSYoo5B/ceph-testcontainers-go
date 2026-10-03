//go:build integration && topology

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

func TestSeparateClusterNetworksAndInterruptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t, ceph.WithSeparateClusterNetwork())
	if !cluster.HasSeparateClusterNetwork() || cluster.NetworkName() == cluster.ClusterNetworkName() {
		t.Fatal("fixture did not create independent public/cluster bridges")
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := docker.Close(); err != nil {
			t.Error(err)
		}
	})
	public, err := docker.NetworkInspect(ctx, cluster.NetworkName(), mobycl.NetworkInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	private, err := docker.NetworkInspect(ctx, cluster.ClusterNetworkName(), mobycl.NetworkInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	publicSubnet, clusterSubnet := public.Network.IPAM.Config[0].Subnet, private.Network.IPAM.Config[0].Subnet
	if publicSubnet.Overlaps(clusterSubnet) {
		t.Fatal("bridge subnets overlap")
	}
	for _, ctr := range []testcontainers.Container{cluster.Container, cluster.ManagerContainer(), client} {
		inspection, err := docker.ContainerInspect(ctx, ctr.GetContainerID(), mobycl.ContainerInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if inspection.Container.NetworkSettings.Networks[cluster.ClusterNetworkName()] != nil || inspection.Container.NetworkSettings.Networks[cluster.NetworkName()] == nil {
			t.Fatal("non-OSD daemon/client joined replication network or lacks public network")
		}
	}
	checkAddresses := func() map[int]string {
		t.Helper()
		data, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		var dump struct {
			OSDs []struct {
				ID     int `json:"osd"`
				Public struct {
					Addrvec []struct {
						Addr string `json:"addr"`
					} `json:"addrvec"`
				} `json:"public_addrs"`
				Cluster struct {
					Addrvec []struct {
						Addr string `json:"addr"`
					} `json:"addrvec"`
				} `json:"cluster_addrs"`
			} `json:"osds"`
		}
		if err := json.Unmarshal(data, &dump); err != nil {
			t.Fatal(err)
		}
		front := map[int]string{}
		for _, osd := range cluster.OSDs() {
			inspection, err := docker.ContainerInspect(ctx, osd.GetContainerID(), mobycl.ContainerInspectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			pub := inspection.Container.NetworkSettings.Networks[cluster.NetworkName()]
			back := inspection.Container.NetworkSettings.Networks[cluster.ClusterNetworkName()]
			if pub == nil || back == nil || !publicSubnet.Contains(pub.IPAddress) || !clusterSubnet.Contains(back.IPAddress) {
				t.Fatal("OSD is not attached to both traffic planes")
			}
			found := false
			for _, native := range dump.OSDs {
				if native.ID != osd.ID {
					continue
				}
				if len(native.Public.Addrvec) == 0 || len(native.Cluster.Addrvec) == 0 {
					t.Fatalf("native OSD lacks advertised vectors: %s", data)
				}
				front[osd.ID] = native.Public.Addrvec[0].Addr
				if !strings.HasPrefix(front[osd.ID], pub.IPAddress.String()+":") || !strings.HasPrefix(native.Cluster.Addrvec[0].Addr, back.IPAddress.String()+":") {
					t.Fatalf("native OSD did not bind distinct public and cluster IPs: %s", data)
				}
				found = true
			}
			if !found {
				t.Fatal("owned OSD missing from native map")
			}
		}
		return front
	}
	front := checkAddresses()
	cephCommand(t, ctx, cluster, "osd", "pool", "create", "tc-topology", "8")
	cephCommand(t, ctx, cluster, "osd", "pool", "application", "enable", "tc-topology", "rados")
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	topologyRados(t, ctx, client, "seed")
	// Disconnect only the caller-owned client's public endpoint. Docker Exec
	// remains available, while a fresh native MON authentication is bounded.
	cut, err := cluster.InterruptNetwork(ctx, client, ceph.PublicNetworkPlane)
	if cut != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := cut.Restore(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	topologyFreshConnection(t, ctx, client, true)
	if err := cut.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	topologyRados(t, ctx, client, "public-endpoint-restored")
	// Interrupt an OSD's backend without stopping its process/public listener.
	osd := cluster.OSDs()[0]
	backend, err := cluster.InterruptNetwork(ctx, osd, ceph.ClusterNetworkPlane)
	if backend != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := backend.Restore(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	inspection, err := docker.ContainerInspect(ctx, osd.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !inspection.Container.State.Running || inspection.Container.NetworkSettings.Networks[cluster.ClusterNetworkName()] != nil || inspection.Container.NetworkSettings.Networks[cluster.NetworkName()] == nil {
		t.Fatal("backend interruption stopped the daemon or removed the public endpoint")
	}
	address := strings.Split(front[osd.ID], "/")[0]
	host, port, err := net.SplitHostPort(address)
	if err != nil || !netip.MustParseAddr(host).Is4() {
		t.Fatalf("invalid native front address %s: %v", address, err)
	}
	execCommand(t, ctx, client, "python3", "-c", `import socket,sys; s=socket.create_connection((sys.argv[1],int(sys.argv[2])),5); s.close()`, host, port)
	if err := backend.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	checkAddresses()
	topologyRados(t, ctx, client, "replication-endpoint-restored")
	added, err := cluster.AddOSD(ctx)
	if err != nil {
		t.Fatal(err)
	}
	checkAddresses()
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.RemoveOSD(ctx, added.ID); err != nil {
		t.Fatal(err)
	}
	topologyRados(t, ctx, client, "dual-network-OSD-add-remove")
	t.Logf("public=%s cluster=%s: native advertised addresses, public-only clients, endpoint cuts/restoration without daemon restart, OSD 2→3→2 verified", publicSubnet, clusterSubnet)
}

func TestFiveMonitorQuorumAndNetworkRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t, ceph.WithMonitorCount(5))
	if len(cluster.Monitors()) != 5 {
		t.Fatal("fixture did not construct five monitors")
	}
	cephCommand(t, ctx, cluster, "osd", "pool", "create", "tc-topology", "8")
	cephCommand(t, ctx, cluster, "osd", "pool", "application", "enable", "tc-topology", "rados")
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	topologyRados(t, ctx, client, "seed")
	var cuts []*ceph.NetworkInterruption
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		for _, cut := range cuts {
			if err := cut.Restore(cleanup); err != nil {
				t.Error(err)
			}
		}
	})
	for i, mon := range cluster.Monitors()[:2] {
		cut, err := cluster.InterruptNetwork(ctx, mon, ceph.PublicNetworkPlane)
		if cut != nil {
			cuts = append(cuts, cut)
		}
		if err != nil {
			t.Fatal(err)
		}
		topologyWait(t, ctx, func() bool { q, err := cluster.QuorumStatus(ctx); return err == nil && len(q.QuorumNames) == 4-i })
		if !mon.IsRunning() {
			t.Fatal("network cut stopped the monitor process")
		}
	}
	topologyRados(t, ctx, client, "three-of-five-quorum")
	for _, cut := range cuts {
		if err := cut.Restore(ctx); err != nil {
			t.Fatal(err)
		}
	}
	topologyWait(t, ctx, func() bool { q, err := cluster.QuorumStatus(ctx); return err == nil && len(q.QuorumNames) == 5 })
	topologyRados(t, ctx, client, "five-monitor-network-recovery")
	t.Log(fmt.Sprintf("five MONs: network-isolated two running members, 3/5 quorum retained, restored original endpoint IPs and 5/5 membership"))
}
