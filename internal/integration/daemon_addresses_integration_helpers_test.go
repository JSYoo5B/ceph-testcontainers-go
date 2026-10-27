//go:build all || integration

package integration_test

import (
	"context"
	"net/netip"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/testcontainers/testcontainers-go"
)

// testDaemonAddresses checks the advertised names and addresses against the
// daemon containers and proves them with a client that uses each address.
func testDaemonAddresses(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	opts = append([]testcontainers.ContainerCustomizer{ceph.WithMonitorCount(2), ceph.WithManagerCount(2), ceph.WithOSDCount(1), ceph.WithPoolDefaults(1, 1)}, opts...)
	cluster, client := newServiceCluster(t, opts...)
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	addresses, err := cluster.DaemonAddresses(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Each advertised address names the daemon's own container, or the
	// advertised host address in host mode.
	expectedIP := func(ctr testcontainers.Container) string {
		if staticFacts(cluster).UsesHostNetwork {
			return staticFacts(cluster).PublicAddress
		}
		ip, err := ctr.ContainerIP(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return ip
	}
	checkVector := func(label string, vector []ceph.EntityAddress, want string) {
		t.Helper()
		if len(vector) == 0 {
			t.Fatalf("%s advertises no address", label)
		}
		for _, entry := range vector {
			addr, err := netip.ParseAddrPort(entry.Addr)
			if err != nil || addr.Addr().String() != want || (entry.Type != "v2" && entry.Type != "v1") {
				t.Fatalf("%s advertises %+v, want IP %s", label, entry, want)
			}
		}
	}
	monitors := clusteraccess.Monitors(t, cluster)
	if len(addresses.Monitors) != len(monitors) {
		t.Fatalf("monmap members %+v, owned %d", addresses.Monitors, len(monitors))
	}
	for i, mon := range monitors {
		if addresses.Monitors[i].Name != mon.DaemonName {
			t.Fatalf("monmap rank %d is %s, want %s", i, addresses.Monitors[i].Name, mon.DaemonName)
		}
		checkVector("mon."+mon.DaemonName, addresses.Monitors[i].Addrs, expectedIP(mon))
	}
	managers := map[string]*ceph.ManagerContainer{}
	for _, mgr := range clusteraccess.Managers(t, cluster) {
		managers[mgr.DaemonName] = mgr
	}
	active := addresses.ActiveManager
	if active == nil || !addresses.ManagerAvailable || managers[active.Name] == nil || len(addresses.StandbyManagers) != 1 || addresses.StandbyManagers[0] == active.Name {
		t.Fatalf("mgrmap active %+v standbys %v", active, addresses.StandbyManagers)
	}
	checkVector("mgr."+active.Name, active.Addrs, expectedIP(managers[active.Name]))
	osd := clusteraccess.OSDs(t, cluster)[0]
	if len(addresses.OSDs) != 1 || addresses.OSDs[0].ID != osd.ID || !addresses.OSDs[0].Up || !addresses.OSDs[0].In {
		t.Fatalf("osdmap entries %+v", addresses.OSDs)
	}
	for label, vector := range map[string][]ceph.EntityAddress{"public": addresses.OSDs[0].Public, "cluster": addresses.OSDs[0].Cluster} {
		checkVector("osd.0 "+label, vector, expectedIP(osd))
	}

	// A client reaches the second MON at exactly its advertised v2 address,
	// and writes through the OSD at the address it learns from the OSDMap.
	second := addresses.Monitors[1]
	v2 := second.Addrs[slices.IndexFunc(second.Addrs, func(entry ceph.EntityAddress) bool { return entry.Type == "v2" })]
	execCommand(t, ctx, client, "ceph", "-m", "v2:"+v2.Addr, "--connect-timeout", "10", "mon", "stat")
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-addresses", Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	execCommand(t, ctx, client, "sh", "-c", "head -c 4096 /dev/urandom > /tmp/object && rados -p tc-addresses put probe /tmp/object && rados -p tc-addresses stat probe")

	// Failing the active MGR advertises the standby at its own address.
	previous := active.Name
	if _, err := cluster.Ceph(ctx, "mgr", "fail", previous); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		addresses, err = cluster.DaemonAddresses(ctx)
		if err == nil && addresses.ActiveManager != nil && addresses.ActiveManager.Name != previous && addresses.ManagerAvailable {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("standby MGR did not become active: %+v %v", addresses.ActiveManager, err)
		}
		time.Sleep(time.Second)
	}
	checkVector("mgr."+addresses.ActiveManager.Name, addresses.ActiveManager.Addrs, expectedIP(managers[addresses.ActiveManager.Name]))
	t.Logf("advertised addresses match daemon containers: monmap e%d, mgrmap e%d (active %s after failing %s), osdmap e%d", addresses.MonMapEpoch, addresses.MgrMapEpoch, addresses.ActiveManager.Name, previous, addresses.OSDMapEpoch)
}
