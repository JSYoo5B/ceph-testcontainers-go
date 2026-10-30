//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

// monitorMembershipOracle connects a new librados client and prints how long
// the connection took and the monmap that "mon dump" returns.
const monitorMembershipOracle = `import json, rados, time
started = time.monotonic()
cluster = rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"client_mount_timeout": "8", "rados_mon_op_timeout": "8"})
try:
    cluster.connect(timeout=8)
    connected = time.monotonic()
    ret, out, err = cluster.mon_command(json.dumps({"prefix": "mon dump", "format": "json"}), b"", timeout=8)
    if ret != 0:
        raise SystemExit("mon dump: %d %s" % (ret, err))
    dump = json.loads(out)
    print(json.dumps({"connect": connected - started, "epoch": dump["epoch"], "members": [mon["name"] for mon in dump["mons"]]}))
finally:
    cluster.shutdown()`

// monitorMembership is one quorum_status answer.
type monitorMembership struct {
	Election int      `json:"election_epoch"`
	Quorum   []string `json:"quorum_names"`
	MonMap   struct {
		Epoch int `json:"epoch"`
		Mons  []struct {
			Name string `json:"name"`
		} `json:"mons"`
	} `json:"monmap"`
}

func (m monitorMembership) members() []string {
	var names []string
	for _, mon := range m.MonMap.Mons {
		names = append(names, mon.Name)
	}
	slices.Sort(names)
	return names
}

// waitMonitorMembership waits until quorum_status lists members in the monmap
// and quorum in the quorum, with the same election epoch in two answers at
// least two seconds apart. A monmap change starts an election, and the MONs
// answer commands again only when it ends; a member that never answers makes
// them wait for its election timeout. It returns the settled answer and how
// long the wait took.
func waitMonitorMembership(t *testing.T, ctx context.Context, cluster *ceph.Container, members, quorum []string) (monitorMembership, time.Duration) {
	t.Helper()
	slices.Sort(members)
	slices.Sort(quorum)
	started := time.Now()
	var settled *monitorMembership
	var since time.Time
	for deadline := started.Add(3 * time.Minute); ; {
		queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		out, err := cluster.Ceph(queryCtx, "quorum_status", "--format", "json")
		cancel()
		var current monitorMembership
		if err == nil && json.Unmarshal(out, &current) == nil {
			slices.Sort(current.Quorum)
			switch {
			case !slices.Equal(current.members(), members) || !slices.Equal(current.Quorum, quorum):
				settled = nil
			case settled == nil || settled.Election != current.Election || settled.MonMap.Epoch != current.MonMap.Epoch:
				settled, since = &current, time.Now()
			case time.Since(since) >= 2*time.Second:
				return current, time.Since(started)
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("monmap %v with quorum %v did not settle: %s, %v", members, quorum, out, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// readMonitorMembers runs monitorMembershipOracle in client within the 10
// seconds that a native oracle of a client suite may get.
func readMonitorMembers(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container) (float64, int, []string) {
	t.Helper()
	result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv: []string{"python3", "-c", monitorMembershipOracle}, Timeout: 10 * time.Second,
		MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "fresh mon dump",
	})
	if err != nil || result.ExitCode != 0 || !result.Complete() {
		t.Fatalf("fresh mon dump: exit %d, timed out %t: %v: %.2048s", result.ExitCode, result.TimedOut, err, result.Stderr)
	}
	var dump struct {
		Connect float64  `json:"connect"`
		Epoch   int      `json:"epoch"`
		Members []string `json:"members"`
	}
	if err := json.Unmarshal([]byte(harness.LastJSONLine(string(result.Stdout))), &dump); err != nil {
		t.Fatalf("fresh mon dump: %v: %.512s", err, result.Stdout)
	}
	slices.Sort(dump.Members)
	return dump.Connect, dump.Epoch, dump.Members
}

// testMessengerClientUnreachableMonitor adds a MON that never answers to the
// monmap with "mon add", on the topology of a Messenger client suite, and
// removes it again. After each change it waits for the election to settle;
// then a new librados client connects within 10 seconds and reads the new
// monmap, and the quorum keeps the three running MONs.
func testMessengerClientUnreachableMonitor(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	image, roles := harness.IntegrationImages(t)
	base := append(slices.Clone(roles), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3).WithManagerCount(2).WithNoInitialOSDs()),
		ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithMessengerMode(ceph.MessengerV2Only).WithMessengerPolicy(ceph.MessengerPolicy{Monitors: modeSecure, Managers: modeSecure})))
	auth := profileAuth(t, ctx, image, base, ceph.AuthConfig{KeyType: ceph.CipherAES256K, ServiceCipher: ceph.CipherAES256K,
		MonTicketTTL: 12 * time.Second, ServiceTicketTTL: 12 * time.Second})
	cluster, client := harness.NewServiceClusterWithOptions(t, image, append(base, ceph.WithAuth(auth))...)
	waitProfileTopology(t, ctx, cluster, 3, 1)
	running := []string{"a", "b", "c"}
	before, _ := waitMonitorMembership(t, ctx, cluster, running, running)

	// Nothing listens on this port: with Messenger v2 only, MON a binds 3300.
	addresses, err := harness.ObserveCluster(cluster).DaemonAddresses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	host, _, err := net.SplitHostPort(addresses.Monitors[0].Addrs[0].Addr)
	if err != nil {
		t.Fatal(err)
	}
	spare := net.JoinHostPort(host, "33320")
	harness.MustCeph(t, ctx, cluster, "mon", "add", "spare", spare)
	added, addWait := waitMonitorMembership(t, ctx, cluster, append(slices.Clone(running), "spare"), running)
	if added.MonMap.Epoch <= before.MonMap.Epoch || added.Election <= before.Election {
		t.Fatalf("mon add: monmap %d and election %d, before %d and %d", added.MonMap.Epoch, added.Election, before.MonMap.Epoch, before.Election)
	}
	addConnect, addEpoch, addMembers := readMonitorMembers(t, ctx, cluster, client)
	if addEpoch != added.MonMap.Epoch || !slices.Equal(addMembers, added.members()) {
		t.Fatalf("a new client read monmap %d %v, want %d %v", addEpoch, addMembers, added.MonMap.Epoch, added.members())
	}

	harness.MustCeph(t, ctx, cluster, "mon", "remove", "spare")
	removed, removeWait := waitMonitorMembership(t, ctx, cluster, running, running)
	if removed.MonMap.Epoch <= added.MonMap.Epoch || removed.Election <= added.Election {
		t.Fatalf("mon remove: monmap %d and election %d, before %d and %d", removed.MonMap.Epoch, removed.Election, added.MonMap.Epoch, added.Election)
	}
	removeConnect, removeEpoch, removeMembers := readMonitorMembers(t, ctx, cluster, client)
	if removeEpoch != removed.MonMap.Epoch || !slices.Equal(removeMembers, running) {
		t.Fatalf("a new client read monmap %d %v, want %d %v", removeEpoch, removeMembers, removed.MonMap.Epoch, running)
	}
	t.Logf("MON_MEMBERSHIP add %s: monmap %d->%d, election %d->%d, settled in %s, new client connected in %.2fs; remove: monmap %d, election %d, settled in %s, new client connected in %.2fs",
		spare, before.MonMap.Epoch, added.MonMap.Epoch, before.Election, added.Election, addWait.Round(100*time.Millisecond), addConnect,
		removed.MonMap.Epoch, removed.Election, removeWait.Round(100*time.Millisecond), removeConnect)
}
