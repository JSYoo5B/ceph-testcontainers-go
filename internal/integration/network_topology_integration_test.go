//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_topology_extensions_network_recovery))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
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
	address := before[osd.ID].Public[0].Addr
	host, port, err := net.SplitHostPort(address)
	if err != nil || netip.MustParseAddr(host).Is4() != (family == ceph.AddressIPv4) {
		t.Fatalf("invalid native front address %s: %v", address, err)
	}
	execCommand(t, ctx, client, "python3", "-c", `import socket,sys; s=socket.create_connection((sys.argv[1],int(sys.argv[2])),5); s.close()`, host, port)
	// Both OSDs hold every PG and neither can be marked down by a single
	// reporter, so a write waits for its replica while the backend is cut.
	held := separateNetworkHeldWrite(t, ctx, cluster, client)
	if err := backend.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := held(); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
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

// heldOp is an operation from an OSD's dump_ops_in_flight.
type heldOp struct {
	Description string `json:"description"`
	TypeData    struct {
		FlagPoint string `json:"flag_point"`
		Events    []struct {
			Event string `json:"event"`
		} `json:"events"`
	} `json:"type_data"`
}

// request returns the client request that the operation serves, such as
// "client.4211.0:1".
func (op heldOp) request() string {
	request, _, _ := strings.Cut(strings.TrimPrefix(op.Description, "osd_op("), " ")
	return request
}

// waitingForReplica reports the write to the object held, waiting for its
// replica, which has not reported a commit.
func (op heldOp) waitingForReplica() bool {
	if !strings.Contains(op.Description, ":::held:head") || op.TypeData.FlagPoint != "waiting for sub ops" {
		return false
	}
	for _, event := range op.TypeData.Events {
		if strings.HasPrefix(event.Event, "sub_op_commit_rec") {
			return false
		}
	}
	return true
}

// separateNetworkHeldWrite starts a write from client and waits until its
// primary OSD has reported the same request waiting for the replica, without
// a replica commit, for 10 seconds, while the writer still waits. A normal
// replicated write passes that state in milliseconds. The returned function
// waits for the write to finish and reads it back.
func separateNetworkHeldWrite(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container) func() error {
	t.Helper()
	out, err := cluster.Ceph(ctx, "osd", "map", "tc-topology", "held", "--format", "json")
	var mapping struct {
		Acting  []int `json:"acting"`
		Primary int   `json:"acting_primary"`
	}
	if err != nil || json.Unmarshal(out, &mapping) != nil || len(mapping.Acting) != 2 {
		t.Fatalf("map of the held object: %s, %v", out, err)
	}
	done := make(chan error, 1)
	go func() {
		result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
			Argv: []string{"python3", "-c", `import rados
payload=bytes(range(256))*64
with rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'rados_osd_op_timeout':'0'}) as c:
    with c.open_ioctx('tc-topology') as io:
        io.write_full('held',payload)
        assert io.read('held',len(payload),0)==payload
print('held write replicated and read back')`},
			Timeout: 5 * time.Minute, MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "held replicated write",
		})
		if err == nil && (!result.Complete() || result.ExitCode != 0) {
			err = fmt.Errorf("held write: exit %d, complete %t: %s", result.ExitCode, result.Complete(), result.Stderr)
		}
		done <- err
	}()
	primary := fmt.Sprintf("osd.%d", mapping.Primary)
	var request string
	var since time.Time
	for deadline := time.Now().Add(2 * time.Minute); ; {
		select {
		case err := <-done:
			t.Fatalf("the write finished while the backend was cut: %v", err)
		default:
		}
		data, err := cluster.AdminSocket(ctx, primary, "dump_ops_in_flight")
		if err != nil {
			t.Fatal(err)
		}
		var dump struct {
			Ops []heldOp `json:"ops"`
		}
		if err := json.Unmarshal(data, &dump); err != nil {
			t.Fatal(err)
		}
		index := slices.IndexFunc(dump.Ops, heldOp.waitingForReplica)
		switch {
		case index < 0:
			request = ""
		case dump.Ops[index].request() != request:
			request, since = dump.Ops[index].request(), time.Now()
		case time.Since(since) >= 10*time.Second:
			select {
			case err := <-done:
				t.Fatalf("the write finished while the backend was cut: %v", err)
			default:
			}
			t.Logf("%s held %s to held for %v, waiting for its replica across the cut backend", primary, request, time.Since(since).Round(time.Second))
			return func() error {
				select {
				case err := <-done:
					return err
				case <-time.After(2 * time.Minute):
					return fmt.Errorf("the held write did not finish after the backend was restored")
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not hold the write waiting for its replica for 10 seconds: %s", primary, data)
		}
		time.Sleep(time.Second)
	}
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
	t.Log("five MONs: network-isolated two running members, 3/5 quorum retained, restored original endpoint IPs and 5/5 membership")
}
