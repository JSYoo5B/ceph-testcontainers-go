//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_topology_extensions_network_recovery))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"reflect"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	dockernetwork "github.com/moby/moby/api/types/network"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

func TestSeparateClusterNetworksAndInterruptions(t *testing.T) {
	separateNetworkScenario(t, ceph.AddressIPv4)
}

// The same planes with IPv6: both bridges are dual-stack, and every daemon
// binds and advertises only addresses from their IPv6 subnets.
func TestSeparateIPv6ClusterNetworksAndInterruptions(t *testing.T) {
	separateNetworkScenario(t, ceph.AddressIPv6)
}

func separateNetworkScenario(t *testing.T, family ceph.AddressFamily) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t, ceph.WithSeparateClusterNetwork(), ceph.WithAddressFamily(family))
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
	publicSubnet, clusterSubnet := planeSubnet(t, public.Network.IPAM.Config, family), planeSubnet(t, private.Network.IPAM.Config, family)
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
	// checkAddresses compares every advertised address with the plane and
	// the container endpoint it belongs to, and returns the OSDs' entries.
	checkAddresses := func() map[int]ceph.OSDAddress {
		t.Helper()
		native, err := cluster.DaemonAddresses(ctx)
		if err != nil {
			t.Fatal(err)
		}
		inPlane := func(what string, vector []ceph.EntityAddress, subnet netip.Prefix, want netip.Addr) {
			t.Helper()
			if len(vector) == 0 {
				t.Fatalf("%s advertises no address: %+v", what, native)
			}
			for _, entry := range vector {
				ip := entityIP(t, entry.Addr)
				if !subnet.Contains(ip) || (want.IsValid() && ip != want) || ip.Is4() != (family == ceph.AddressIPv4) {
					t.Fatalf("%s advertises %s, not %v in %s: %+v", what, entry.Addr, want, subnet, native)
				}
			}
		}
		for _, mon := range native.Monitors {
			inPlane("mon."+mon.Name, mon.Addrs, publicSubnet, netip.Addr{})
		}
		if native.ActiveManager == nil {
			t.Fatalf("no active MGR: %+v", native)
		}
		inPlane("mgr."+native.ActiveManager.Name, native.ActiveManager.Addrs, publicSubnet, netip.Addr{})
		osds := map[int]ceph.OSDAddress{}
		for _, osd := range cluster.OSDs() {
			inspection, err := docker.ContainerInspect(ctx, osd.GetContainerID(), mobycl.ContainerInspectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			pub := inspection.Container.NetworkSettings.Networks[cluster.NetworkName()]
			back := inspection.Container.NetworkSettings.Networks[cluster.ClusterNetworkName()]
			if pub == nil || back == nil {
				t.Fatal("OSD is not attached to both traffic planes")
			}
			front, rear := endpointAddress(pub, family), endpointAddress(back, family)
			if !publicSubnet.Contains(front) || !clusterSubnet.Contains(rear) {
				t.Fatalf("OSD endpoints %v and %v are outside %s and %s", front, rear, publicSubnet, clusterSubnet)
			}
			for _, entry := range native.OSDs {
				if entry.ID != osd.ID {
					continue
				}
				what := fmt.Sprintf("osd.%d", osd.ID)
				inPlane(what+" public", entry.Public, publicSubnet, front)
				inPlane(what+" heartbeat front", entry.HeartbeatFront, publicSubnet, front)
				inPlane(what+" cluster", entry.Cluster, clusterSubnet, rear)
				inPlane(what+" heartbeat back", entry.HeartbeatBack, clusterSubnet, rear)
				osds[osd.ID] = entry
			}
			if _, found := osds[osd.ID]; !found {
				t.Fatal("owned OSD missing from native map")
			}
		}
		return osds
	}
	before := checkAddresses()
	for id, osd := range before {
		t.Logf("osd.%d public %v cluster %v heartbeat front %v back %v", id, osd.Public, osd.Cluster, osd.HeartbeatFront, osd.HeartbeatBack)
	}
	// One PG on both OSDs with min_size 2: every write needs both, and the
	// held write below has one placement to observe.
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-topology", PGNum: 1, Replicas: 2, MinSize: 2, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	topologyRados(t, ctx, client, "seed")
	// Disconnect only the caller-owned client's public endpoint. Docker Exec
	// remains available, while a fresh native MON authentication is bounded.
	cut, err := fault.InterruptNetwork(ctx, cluster, client, ceph.PublicNetworkPlane)
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
	backend, err := fault.InterruptNetwork(ctx, cluster, osd, ceph.ClusterNetworkPlane)
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
	address := before[osd.ID].Public[0].Addr
	host, _, err := net.SplitHostPort(address)
	if err != nil || netip.MustParseAddr(host).Is4() != (family == ceph.AddressIPv4) {
		t.Fatalf("invalid native front address %s: %v", address, err)
	}
	receipt := heldReplicationReceipt{Family: family.String()}
	receipt.Images.Monitor, receipt.Images.OSD, receipt.Images.Client = imageID(t, ctx, cluster.Container), imageID(t, ctx, osd), imageID(t, ctx, client)
	receipt.Interrupted.OSD, receipt.Interrupted.CID = osd.ID, osd.GetContainerID()
	receipt.Interrupted.Public, receipt.Interrupted.Cluster = before[osd.ID].Public, before[osd.ID].Cluster
	// Both OSDs hold the PG and neither can be marked down by a single
	// reporter, so a write waits for its replica while the backend is cut.
	held := startHeldReplicationWrite(t, ctx, cluster, client, &receipt)
	checkHeldControls(t, ctx, cluster, client, &receipt, held)
	if err := backend.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	finishHeldReplicationWrite(t, ctx, cluster, &receipt, held)
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	out, err := cluster.Ceph(ctx, "osd", "map", "tc-topology", "held", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	if receipt.MappingAfter, err = parseHeldMapping(out, "tc-topology", "held"); err != nil || !receipt.MappingAfter.samePlacement(receipt.Mapping) {
		t.Fatalf("placement after the restore %+v, held in %+v: %v", receipt.MappingAfter, receipt.Mapping, err)
	}
	if encoded, err := json.Marshal(receipt); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("HELD_REPLICATION_RECEIPT %s", encoded)
	}
	// The same OSD instances, with the same addresses and nonces, recovered.
	if after := checkAddresses(); !reflect.DeepEqual(after, before) {
		t.Fatalf("OSD addresses changed across the backend interruption: %+v, before %+v", after, before)
	}
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
	t.Logf("%s public=%s cluster=%s: native advertised addresses, public-only clients, endpoint cuts/restoration without daemon restart, held replication, OSD 2→3→2 verified", family, publicSubnet, clusterSubnet)
}

// planeSubnet returns a bridge's subnet of the family's addresses.
func planeSubnet(t *testing.T, configs []dockernetwork.IPAMConfig, family ceph.AddressFamily) netip.Prefix {
	t.Helper()
	for _, config := range configs {
		if config.Subnet.IsValid() && config.Subnet.Addr().Is4() == (family == ceph.AddressIPv4) {
			return config.Subnet
		}
	}
	t.Fatalf("bridge has no %s subnet: %+v", family, configs)
	return netip.Prefix{}
}

// endpointAddress returns a container endpoint's address of the family.
func endpointAddress(endpoint *dockernetwork.EndpointSettings, family ceph.AddressFamily) netip.Addr {
	if family == ceph.AddressIPv6 {
		return endpoint.GlobalIPv6Address
	}
	return endpoint.IPAddress
}

// entityIP returns the IP of an address that Ceph prints as "ip:port" or
// "[ip]:port".
func entityIP(t *testing.T, addr string) netip.Addr {
	t.Helper()
	parsed, err := netip.ParseAddrPort(addr)
	if err != nil {
		t.Fatalf("native address %q: %v", addr, err)
	}
	return parsed.Addr()
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
	var cuts []*fault.NetworkInterruption
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
		cut, err := fault.InterruptNetwork(ctx, cluster, mon, ceph.PublicNetworkPlane)
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
	t.Log("five MONs: network-isolated two running members, 3/5 quorum retained, restored original endpoint IPs and 5/5 membership")
}

// One OSD's cluster network is cut in a cluster of three OSDs; its peers'
// heartbeat reports get it marked down, it exits, and it starts again.
func TestOSDHeartbeatFailure(t *testing.T) {
	testOSDHeartbeatFailure(t)
}
