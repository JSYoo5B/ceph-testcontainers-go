//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"io"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// dualStackClient opens a librados session to the MON addresses in argv[1]
// only: mon_host_override keeps its MON session on them instead of the
// monmap's first, IPv6, entries. It sends a MON command and a MGR command,
// writes and reads an object, and prints the remote endpoints of its
// established TCP connections, read from the container's /proc/net/tcp{,6}
// while the session is open, before shutdown(). The MGR and OSD sessions
// follow the first entry of their advertised vectors, which is IPv4 with
// ms_bind_prefer_ipv4; client bind options do not select a peer's family.
const dualStackClient = `import json,socket,sys,rados
mon_host,profile,pool=sys.argv[1],sys.argv[2],sys.argv[3]
conf={'mon_host':mon_host,'mon_host_override':mon_host,'rados_mon_op_timeout':'10','rados_osd_op_timeout':'10','client_mount_timeout':'10'}
def peers():
    found=[]
    for path,v6 in (('/proc/net/tcp',False),('/proc/net/tcp6',True)):
        for line in open(path).read().splitlines()[1:]:
            f=line.split()
            if f[3]!='01':
                continue
            host,port=f[2].split(':')
            raw=bytes.fromhex(host)
            raw=b''.join(raw[i:i+4][::-1] for i in range(0,len(raw),4))
            ip=socket.inet_ntop(socket.AF_INET6 if v6 else socket.AF_INET,raw)
            if ip.startswith('::ffff:'):
                ip=ip[7:]
            found.append([ip,int(port,16)])
    return found
c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf=conf)
c.connect()
try:
    r,_,err=c.mon_command(json.dumps({'prefix':'fsid','format':'json'}),b'')
    assert r==0,('mon',r,err)
    r,out,err=c.mgr_command(json.dumps({'prefix':'pg stat','format':'json'}),b'')
    assert r==0,('mgr',r,err)
    io=c.open_ioctx(pool)
    data=('dual-stack-'+profile).encode()
    io.write_full('dual-stack-'+profile,data)
    assert io.read('dual-stack-'+profile)==data,'readback'
    io.close()
    print(json.dumps({'peers':peers()}))
finally:
    c.shutdown()
`

// dualStackBanner connects to every v2 endpoint in argv and prints the ones
// that answered with the Messenger v2 banner.
const dualStackBanner = `import json,socket,sys
answered=[]
for endpoint in sys.argv[1:]:
    host,port=endpoint.rsplit(':',1)
    host=host.strip('[]')
    s=socket.create_connection((host,int(port)),timeout=5)
    try:
        if s.recv(8)==b'ceph v2\n':
            answered.append(endpoint)
    finally:
        s.close()
print(json.dumps(answered))
`

// dualStackEndpoints is the concrete v2 address of each family of one
// daemon's advertised vector.
type dualStackEndpoints struct{ v4, v6 string }

// checkDualStackVector checks that a vector has, for each protocol, exactly
// one concrete IPv4 and one concrete global IPv6 address, and returns the v2
// pair.
func checkDualStackVector(t *testing.T, daemon string, addrs []ceph.EntityAddress) dualStackEndpoints {
	t.Helper()
	count := map[string][2]int{}
	var v2 dualStackEndpoints
	for _, addr := range addrs {
		port, err := netip.ParseAddrPort(addr.Addr)
		if err != nil {
			t.Fatalf("%s advertises the invalid address %s", daemon, addr.Addr)
		}
		ip := port.Addr()
		if ip.IsUnspecified() || ip.Is4In6() || ip.IsLinkLocalUnicast() || ip.IsLoopback() {
			t.Fatalf("%s advertises %s:%s in %v", daemon, addr.Type, addr.Addr, addrs)
		}
		pair := count[addr.Type]
		if ip.Is4() {
			pair[0]++
			if addr.Type == "v2" {
				v2.v4 = addr.Addr
			}
		} else {
			pair[1]++
			if addr.Type == "v2" {
				v2.v6 = addr.Addr
			}
		}
		count[addr.Type] = pair
	}
	for protocol, pair := range count {
		if pair != [2]int{1, 1} {
			t.Fatalf("%s advertises %d IPv4 and %d IPv6 %s addresses: %v", daemon, pair[0], pair[1], protocol, addrs)
		}
	}
	if v2.v4 == "" || v2.v6 == "" {
		t.Fatalf("%s advertises no v2 address pair: %v", daemon, addrs)
	}
	return v2
}

// waitDualStackAddresses waits until the MGR is available and every OSD is up,
// then checks every MON, the active MGR and the four vectors of every OSD.
func waitDualStackAddresses(t *testing.T, ctx context.Context, cluster *ceph.Container, monitors, osds int) (ceph.DaemonAddresses, map[string]dualStackEndpoints) {
	t.Helper()
	var addresses ceph.DaemonAddresses
	deadline := time.Now().Add(2 * time.Minute)
	for {
		var err error
		addresses, err = harness.ObserveCluster(cluster).DaemonAddresses(ctx)
		up := 0
		for _, osd := range addresses.OSDs {
			if osd.Up {
				up++
			}
		}
		if err == nil && addresses.ManagerAvailable && addresses.ActiveManager != nil && len(addresses.Monitors) == monitors && up == osds {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("daemon addresses did not settle: %+v, %v", addresses, err)
		}
		time.Sleep(time.Second)
	}
	endpoints := map[string]dualStackEndpoints{}
	for _, mon := range addresses.Monitors {
		pair := checkDualStackVector(t, "mon."+mon.Name, mon.Addrs)
		// The MGR picks the family of its MON session from the first entry.
		if first, _ := netip.ParseAddrPort(mon.Addrs[0].Addr); !first.Addr().Is6() {
			t.Fatalf("mon.%s lists %s first, not its IPv6 address", mon.Name, mon.Addrs[0].Addr)
		}
		endpoints["mon."+mon.Name] = pair
	}
	mgr := addresses.ActiveManager
	endpoints["mgr."+mgr.Name] = checkDualStackVector(t, "mgr."+mgr.Name, mgr.Addrs)
	for _, osd := range addresses.OSDs {
		name := fmt.Sprintf("osd.%d", osd.ID)
		endpoints[name] = checkDualStackVector(t, name+" public", osd.Public)
		checkDualStackVector(t, name+" cluster", osd.Cluster)
		checkDualStackVector(t, name+" heartbeat front", osd.HeartbeatFront)
		checkDualStackVector(t, name+" heartbeat back", osd.HeartbeatBack)
	}
	return addresses, endpoints
}

// runDualStackClient runs dualStackClient in client and returns the families
// of the MON, MGR and OSD connections it had open.
func runDualStackClient(t *testing.T, ctx context.Context, client testcontainers.Container, monHost, profile, pool string, endpoints map[string]dualStackEndpoints) string {
	t.Helper()
	code, reader, err := client.Exec(ctx, []string{"timeout", "90", "python3", "-c", dualStackClient, monHost, profile, pool}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, _ := io.ReadAll(reader)
	if code != 0 {
		t.Fatalf("%s client with mon_host %s: exit %d: %s", profile, monHost, code, output)
	}
	var result struct {
		Peers [][2]any `json:"peers"`
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &result); err != nil {
		t.Fatalf("%s client output: %s", profile, output)
	}
	// Name each established connection by the advertised endpoint it reached.
	owners := map[string]string{}
	for name, pair := range endpoints {
		owners[pair.v4] = name + "/ipv4"
		owners[pair.v6] = name + "/ipv6"
	}
	var paths []string
	monFamily := ""
	for _, peer := range result.Peers {
		ip, _ := peer[0].(string)
		port, _ := peer[1].(float64)
		addr, err := netip.ParseAddr(ip)
		if err != nil {
			continue
		}
		owner, ok := owners[netip.AddrPortFrom(addr, uint16(port)).String()]
		if !ok {
			continue
		}
		paths = append(paths, owner)
		if strings.HasPrefix(owner, "mon.") {
			monFamily = owner[strings.LastIndex(owner, "/")+1:]
		}
	}
	if monFamily != profile {
		t.Fatalf("%s client with mon_host %s kept a MON session over %q: %v", profile, monHost, monFamily, paths)
	}
	return strings.Join(paths, ",")
}

// testDualStackAddressFamily starts a dual-stack cluster with two MONs, two
// MGRs and two OSDs, checks every advertised vector, and runs a native client
// bootstrapped from the IPv4 and then the IPv6 MON addresses. It then fails
// the active MGR over through RemoveManager, which admits the new active MGR,
// and repeats the IPv6 client against it.
func testDualStackAddressFamily(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(2).WithMonitorCount(2).WithManagerCount(2)),
		ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithAddressFamily(ceph.AddressDualStack)))
	if harness.StaticFacts(cluster).AddressFamily != ceph.AddressDualStack {
		t.Fatalf("cluster reports %s", harness.StaticFacts(cluster).AddressFamily)
	}
	addresses, endpoints := waitDualStackAddresses(t, ctx, cluster, 2, 2)

	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-dual-stack", Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	client, err := testcontainers.Run(ctx, harness.NativeClientImage(t, harness.StaticFacts(cluster).Images.Client), cluster.WithClient(), ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, client)
	if err != nil {
		t.Fatal(err)
	}
	monHost := func(v6 bool) string {
		var hosts []string
		for _, mon := range addresses.Monitors {
			pair := endpoints["mon."+mon.Name]
			if v6 {
				hosts = append(hosts, "[v2:"+pair.v6+"]")
			} else {
				hosts = append(hosts, "[v2:"+pair.v4+"]")
			}
		}
		return strings.Join(hosts, ",")
	}
	ipv4Paths := runDualStackClient(t, ctx, client, monHost(false), "ipv4", "tc-dual-stack", endpoints)
	ipv6Paths := runDualStackClient(t, ctx, client, monHost(true), "ipv6", "tc-dual-stack", endpoints)

	// Every advertised v2 endpoint of both families accepts a connection
	// and sends the Messenger v2 banner.
	var all []string
	for _, pair := range endpoints {
		all = append(all, pair.v4, pair.v6)
	}
	code, reader, err := client.Exec(ctx, append([]string{"timeout", "60", "python3", "-c", dualStackBanner}, all...), tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, _ := io.ReadAll(reader)
	var answered []string
	if code != 0 || json.Unmarshal([]byte(strings.TrimSpace(string(output))), &answered) != nil || len(answered) != len(all) {
		t.Fatalf("banners: exit %d: %s, want %d endpoints %v", code, output, len(all), all)
	}

	// Fail the active MGR over. RemoveManager admits the replacement's
	// advertised addresses before it returns.
	first := addresses.ActiveManager.Name
	if err := harness.ManagerHandle(t, ctx, cluster, first).Remove(ctx); err != nil {
		t.Fatalf("fail over from mgr.%s: %v", first, err)
	}
	addresses, endpoints = waitDualStackAddresses(t, ctx, cluster, 2, 2)
	if addresses.ActiveManager.Name == first {
		t.Fatalf("mgr.%s is still active", first)
	}
	failoverPaths := runDualStackClient(t, ctx, client, monHost(true), "ipv6", "tc-dual-stack", endpoints)

	mgr := addresses.ActiveManager
	t.Logf("DUAL_STACK_RECEIPT layout=single image=%s ceph=%s mons=%v first_mgr=%s active_mgr=%s:%v osds=%d ipv4_client=%s ipv6_client=%s after_failover=%s banners=%d/%d",
		harness.StaticFacts(cluster).Images.Essential, harness.StaticFacts(cluster).CephVersion, addresses.Monitors, first, mgr.Name, mgr.Addrs, len(addresses.OSDs),
		ipv4Paths, ipv6Paths, failoverPaths, len(answered), len(all))
}

// testDualStackSeparateClusterNetwork starts a dual-stack cluster with a
// dual-stack replication bridge and checks that every OSD advertises both
// families from the public bridge's subnets on its public and heartbeat
// front vectors and from the replication bridge's subnets on its cluster and
// heartbeat back vectors, and that MONs and the MGR use the public bridge.
// A replicated object written by a client crosses the replication plane.
func testDualStackSeparateClusterNetwork(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(2).WithPoolDefaults(2, 2)), ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithSeparateClusterNetwork().WithAddressFamily(ceph.AddressDualStack)))
	if !harness.StaticFacts(cluster).HasSeparateClusterNetwork {
		t.Fatal("no replication bridge")
	}
	addresses, endpoints := waitDualStackAddresses(t, ctx, cluster, 1, 2)
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = docker.Close() }()
	subnets := map[string][]netip.Prefix{}
	for plane, name := range map[string]string{"public": harness.StaticFacts(cluster).NetworkName, "cluster": harness.StaticFacts(cluster).ClusterNetworkName} {
		inspection, err := docker.NetworkInspect(ctx, name, mobycl.NetworkInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for _, config := range inspection.Network.IPAM.Config {
			subnets[plane] = append(subnets[plane], config.Subnet)
		}
		if len(subnets[plane]) != 2 {
			t.Fatalf("%s bridge %s is not dual-stack: %v", plane, name, subnets[plane])
		}
	}
	inPlane := func(daemon, plane string, vector []ceph.EntityAddress) {
		t.Helper()
		for _, addr := range vector {
			ip := netip.MustParseAddrPort(addr.Addr).Addr()
			found := false
			for _, subnet := range subnets[plane] {
				found = found || subnet.Contains(ip)
			}
			if !found {
				t.Fatalf("%s advertises %s outside the %s subnets %v", daemon, addr.Addr, plane, subnets[plane])
			}
		}
	}
	for _, mon := range addresses.Monitors {
		inPlane("mon."+mon.Name, "public", mon.Addrs)
	}
	inPlane("mgr."+addresses.ActiveManager.Name, "public", addresses.ActiveManager.Addrs)
	for _, osd := range addresses.OSDs {
		name := fmt.Sprintf("osd.%d", osd.ID)
		inPlane(name+" public", "public", osd.Public)
		inPlane(name+" heartbeat front", "public", osd.HeartbeatFront)
		inPlane(name+" cluster", "cluster", osd.Cluster)
		inPlane(name+" heartbeat back", "cluster", osd.HeartbeatBack)
	}
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-dual-stack", Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	client, err := testcontainers.Run(ctx, harness.NativeClientImage(t, harness.StaticFacts(cluster).Images.Client), cluster.WithClient(), ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, client)
	if err != nil {
		t.Fatal(err)
	}
	mon := endpoints["mon."+addresses.Monitors[0].Name]
	paths := runDualStackClient(t, ctx, client, "[v2:"+mon.v6+"]", "ipv6", "tc-dual-stack", endpoints)
	t.Logf("DUAL_STACK_RECEIPT layout=separate image=%s ceph=%s public=%v cluster=%v osd.0=%v/%v ipv6_client=%s",
		harness.StaticFacts(cluster).Images.Essential, harness.StaticFacts(cluster).CephVersion, subnets["public"], subnets["cluster"], addresses.OSDs[0].Public, addresses.OSDs[0].Cluster, paths)
}
