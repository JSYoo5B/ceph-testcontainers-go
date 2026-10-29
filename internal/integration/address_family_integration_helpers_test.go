//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// addressOfFamily reports whether a Ceph address string has the family.
func addressOfFamily(family ceph.AddressFamily, addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	switch family {
	case ceph.AddressIPv6:
		return ip.Is6() && !ip.Is4In6() && !ip.IsLinkLocalUnicast()
	case ceph.AddressIPv4MappedIPv6:
		return ip.Is4In6()
	}
	return ip.Is4()
}

// testAddressFamily starts a cluster of the family with three MONs, so that
// joining MONs are covered, and a CephFS MDS. It checks the advertised address
// of every MON, MGR, OSD and MDS, the sockets that MONs and OSDs listen on, and
// a client's object I/O.
func testAddressFamily(t *testing.T, family ceph.AddressFamily, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, _ := newServiceCluster(t, append([]testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1).WithMonitorCount(3)), ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithAddressFamily(family)), cephfs.WithFilesystems(cephfs.Config{Name: "tc-family-fs"})}, opts...)...)
	if staticFacts(cluster).AddressFamily != family {
		t.Fatalf("cluster reports %s", staticFacts(cluster).AddressFamily)
	}

	var addresses ceph.DaemonAddresses
	deadline := time.Now().Add(2 * time.Minute)
	for {
		var err error
		addresses, err = observeCluster(cluster).DaemonAddresses(ctx)
		if err == nil && addresses.ManagerAvailable && len(addresses.OSDs) == 1 && addresses.OSDs[0].Up {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("daemon addresses did not settle: %+v, %v", addresses, err)
		}
		time.Sleep(time.Second)
	}
	check := func(daemon string, addrs []ceph.EntityAddress) {
		t.Helper()
		if len(addrs) == 0 {
			t.Fatalf("%s advertises no address", daemon)
		}
		for _, addr := range addrs {
			if !addressOfFamily(family, addr.Addr) {
				t.Fatalf("%s advertises %s %s, not %s", daemon, addr.Type, addr.Addr, family)
			}
		}
	}
	for _, mon := range addresses.Monitors {
		check("mon."+mon.Name, mon.Addrs)
	}
	check("mgr."+addresses.ActiveManager.Name, addresses.ActiveManager.Addrs)
	for _, osd := range addresses.OSDs {
		name := fmt.Sprintf("osd.%d", osd.ID)
		check(name+" public", osd.Public)
		check(name+" cluster", osd.Cluster)
		check(name+" heartbeat front", osd.HeartbeatFront)
		check(name+" heartbeat back", osd.HeartbeatBack)
	}

	check("mds", waitActiveMDSAddrs(t, ctx, cluster))
	// Docker forwards an IPv4 connection to the MON's published port to the
	// container's IPv4 address. Only an IPv4-mapped IPv6 socket accepts it.
	if reached := monitorBannerReachable(t, ctx, cluster); reached != (family == ceph.AddressIPv4MappedIPv6) {
		t.Fatalf("%s: the published MON port reached the MON: %t", family, reached)
	}
	for _, ctr := range []testcontainers.Container{clusteraccess.Monitors(t, cluster)[0].Container, clusteraccess.Monitors(t, cluster)[2].Container, clusteraccess.OSDs(t, cluster)[0].Container} {
		checkMessengerSockets(t, ctx, ctr, family)
	}

	// An interrupted OSD endpoint comes back at its original IPv4 and, on the
	// dual-stack bridge, IPv6 address, so its advertised address still
	// reaches it.
	osd := clusteraccess.OSDs(t, cluster)[0]
	endpointBefore := osdEndpoint(t, ctx, cluster, osd.Container)
	link, err := fault.InterruptNetwork(ctx, cluster, osd.Container, ceph.PublicNetworkPlane)
	if err != nil {
		if link != nil {
			_ = link.Restore(ctx)
		}
		t.Fatal(err)
	}
	if err := link.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if endpointAfter := osdEndpoint(t, ctx, cluster, osd.Container); endpointAfter != endpointBefore {
		t.Fatalf("%s: OSD endpoint %s became %s after restoration", family, endpointBefore, endpointAfter)
	}
	if family == ceph.AddressIPv6 && !strings.Contains(endpointBefore, ":") {
		t.Fatalf("the dual-stack OSD endpoint %s has no IPv6 address", endpointBefore)
	}

	// A client container reads the generated configuration and does object
	// I/O over the family's addresses.
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-family", Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	client, err := testcontainers.Run(ctx, nativeClientImage(t, staticFacts(cluster).Images.Client), cluster.WithClient(), ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, client)
	if err != nil {
		t.Fatal(err)
	}
	script := "echo family-object > /tmp/in && rados -p tc-family put object /tmp/in && rados -p tc-family get object /tmp/out && cat /tmp/out && grep -i '^mon host' /etc/ceph/ceph.conf"
	code, reader, err := client.Exec(ctx, []string{"timeout", "60", "sh", "-c", script}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, _ := io.ReadAll(reader)
	if code != 0 || !strings.Contains(string(output), "family-object") {
		t.Fatalf("client object I/O over %s: exit %d: %s", family, code, output)
	}
	t.Logf("%s: client config %s", family, strings.TrimSpace(string(output[strings.LastIndex(string(output), "mon host"):])))
	t.Logf("%s: MONs %v, MGR %v, OSD public %v", family, addresses.Monitors, addresses.ActiveManager.Addrs, addresses.OSDs[0].Public)
}

// waitActiveMDSAddrs returns the address vector of the filesystem's active
// MDS from the FSMap.
func waitActiveMDSAddrs(t *testing.T, ctx context.Context, cluster *ceph.Container) []ceph.EntityAddress {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for {
		out, err := cluster.Ceph(ctx, "fs", "dump", "--format", "json")
		var dump struct {
			Filesystems []struct {
				MDSMap struct {
					Info map[string]struct {
						State string `json:"state"`
						Addrs struct {
							Addrvec []struct {
								Type  string `json:"type"`
								Addr  string `json:"addr"`
								Nonce uint32 `json:"nonce"`
							} `json:"addrvec"`
						} `json:"addrs"`
					} `json:"info"`
				} `json:"mdsmap"`
			} `json:"filesystems"`
		}
		if err == nil && json.Unmarshal(out, &dump) == nil && len(dump.Filesystems) == 1 {
			for _, info := range dump.Filesystems[0].MDSMap.Info {
				if info.State == "up:active" {
					var addrs []ceph.EntityAddress
					for _, addr := range info.Addrs.Addrvec {
						addrs = append(addrs, ceph.EntityAddress{Type: addr.Type, Addr: addr.Addr, Nonce: addr.Nonce})
					}
					return addrs
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no active MDS: %s %v", out, err)
		}
		time.Sleep(2 * time.Second)
	}
}

// checkMessengerSockets checks that every TCP socket that ctr listens on, other
// than Docker's DNS on loopback, is an IPv6 socket: bound to the IPv6
// wildcard, as MGRs, OSDs and MDSs are, or to an address of the family, as
// MONs are.
func checkMessengerSockets(t *testing.T, ctx context.Context, ctr testcontainers.Container, family ceph.AddressFamily) {
	t.Helper()
	listening := func(file string) []string {
		code, reader, err := ctr.Exec(ctx, []string{"cat", file}, tcexec.Multiplexed())
		if err != nil || code != 0 {
			t.Fatalf("read %s: exit %d, %v", file, code, err)
		}
		output, _ := io.ReadAll(reader)
		var local []string
		for _, line := range strings.Split(string(output), "\n")[1:] {
			fields := strings.Fields(line)
			// The fourth field is the state; 0A is LISTEN.
			if len(fields) > 3 && fields[3] == "0A" {
				local = append(local, fields[1])
			}
		}
		return local
	}
	for _, local := range listening("/proc/net/tcp") {
		// 7F is 127 in the low byte of a little-endian IPv4 address.
		if !strings.HasPrefix(local[6:8], "7F") {
			t.Fatalf("%s listens on IPv4 socket %s", family, local)
		}
	}
	v6 := listening("/proc/net/tcp6")
	if len(v6) == 0 {
		t.Fatalf("%s: no IPv6 listener", family)
	}
	for _, local := range v6 {
		wildcard := strings.HasPrefix(local, strings.Repeat("0", 32)+":")
		mapped := strings.HasPrefix(local, "0000000000000000FFFF0000")
		if !wildcard && mapped != (family == ceph.AddressIPv4MappedIPv6) {
			t.Fatalf("%s listens on IPv6 socket %s", family, local)
		}
	}
}

// monitorBannerReachable reports whether an IPv4 connection to MON a's
// published v2 port from the test process receives the Messenger v2 banner.
func monitorBannerReachable(t *testing.T, ctx context.Context, cluster *ceph.Container) bool {
	t.Helper()
	endpoint, err := cluster.PortEndpoint(ctx, "3300/tcp", "")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp4", endpoint, 5*time.Second)
	if err != nil {
		return false
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	banner := make([]byte, len("ceph v2\n"))
	_, err = io.ReadFull(conn, banner)
	return err == nil && string(banner) == "ceph v2\n"
}

// osdEndpoint returns the IPv4 and IPv6 address of ctr on the cluster's
// public network.
func osdEndpoint(t *testing.T, ctx context.Context, cluster *ceph.Container, ctr testcontainers.Container) string {
	t.Helper()
	inspection, err := ctr.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := inspection.NetworkSettings.Networks[staticFacts(cluster).NetworkName]
	if endpoint == nil {
		t.Fatal("the OSD is not attached to the public network")
	}
	address := endpoint.IPAddress.String()
	if endpoint.GlobalIPv6Address.IsValid() {
		address += " " + endpoint.GlobalIPv6Address.String()
	}
	return address
}
