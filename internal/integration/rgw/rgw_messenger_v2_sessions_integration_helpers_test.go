//go:build all || integration

package rgw_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

// gatewayHTTPPort is the beast port that a gateway listens on inside its
// container.
const gatewayHTTPPort = 7480

// dockerResolver is Docker's embedded DNS server, which listens inside every
// container on a user-defined network.
const dockerResolver = "0B00007F"

// listeningPorts reads the listening TCP sockets of /proc/net/tcp and
// /proc/net/tcp6 inside ctr, except Docker's resolver, as hex address and
// decimal port.
func listeningPorts(t *testing.T, ctx context.Context, ctr testcontainers.Container) map[string]int {
	t.Helper()
	result := map[string]int{}
	for _, table := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		for _, line := range strings.Split(harness.ExecOutput(t, ctx, ctr, "cat", table), "\n")[1:] {
			fields := strings.Fields(line)
			if len(fields) < 4 || fields[3] != "0A" {
				continue
			}
			address, port, ok := strings.Cut(fields[1], ":")
			number, err := strconv.ParseInt(port, 16, 32)
			if !ok || err != nil {
				t.Fatalf("%s: %q", table, line)
			}
			if address != dockerResolver {
				result[address+":"+strconv.FormatInt(number, 10)] = int(number)
			}
		}
	}
	return result
}

// gatewayAsok returns the one admin socket in the gateway's container: the
// radosgw process's, which librados names after its PID.
func gatewayAsok(t *testing.T, ctx context.Context, gateway testcontainers.Container) string {
	t.Helper()
	sockets := strings.Fields(harness.ExecOutput(t, ctx, gateway, "sh", "-c", "ls /var/run/ceph/*.asok"))
	if len(sockets) != 1 {
		t.Fatalf("gateway admin sockets: %v", sockets)
	}
	return sockets[0]
}

// rgwV2Daemon is one daemon's part of the RGW v2 sessions receipt: the READY
// sessions it logged with a peer in the gateway's container.
type rgwV2Daemon struct {
	Generation string   `json:"generation"`
	Ready      int      `json:"ready"`
	Problems   []string `json:"problems,omitempty"`
}

// testRGWMessengerV2Sessions starts an RGW cluster without Messenger v1 whose
// MONs, MGRs and OSDs log their READY sessions. The gateway's container
// listens on the beast port only, so radosgw binds no messenger; every READY
// session that a MON, MGR or OSD logged with a peer at the gateway's address
// is msgr2.1, and a MON and an OSD logged at least one. With messenger dump,
// radosgw's own messengers listen on no socket, advertise no v1 address and
// have READY msgr2.1 connections to a MON and an OSD.
func testRGWMessengerV2Sessions(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	image, roles := harness.IntegrationImages(t)
	config := filepath.Join(t.TempDir(), "ceph.conf")
	if err := os.WriteFile(config, []byte("[mon]\ndebug_ms = 1\n[mgr]\ndebug_ms = 1\n[osd]\ndebug_ms = 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cluster, _ := harness.NewServiceClusterRun(t, harness.RGWRun, image, append(roles, ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithMessengerMode(ceph.MessengerV2Only)),
		ceph.WithConfigFile(config))...)
	services := cluster.ServiceContainers()
	if len(services) != 1 {
		t.Fatalf("%d gateway containers, want 1", len(services))
	}
	gateway := services[0]
	inspect, err := gateway.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := inspect.NetworkSettings.Networks[harness.StaticFacts(cluster).NetworkName]
	if endpoint == nil || !endpoint.IPAddress.IsValid() {
		t.Fatal("the gateway has no address on the Ceph network")
	}
	address := endpoint.IPAddress.String()
	dump := harness.CapabilitySupported(t, ctx, cluster, harness.CapabilityMessengerDump)
	receipt := map[string]any{"gateway_address": address, "messenger_dump": dump}
	daemons := map[string]*rgwV2Daemon{}
	receipt["daemons"] = daemons
	var problems []string
	defer func() {
		receipt["problems"] = problems
		data, err := json.Marshal(receipt)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("MESSENGER_V2_RGW_RECEIPT %s", data)
	}()
	version, err := cluster.Ceph(ctx, "version")
	if err != nil {
		t.Fatal(err)
	}
	receipt["native_version"] = strings.TrimSpace(string(version))

	listening := listeningPorts(t, ctx, gateway)
	receipt["listening"] = listening
	for socket, port := range listening {
		if port != gatewayHTTPPort {
			problems = append(problems, fmt.Sprintf("the gateway's container listens on %s", socket))
		}
	}
	if len(listening) == 0 {
		problems = append(problems, "the gateway's container listens on no TCP socket")
	}

	type daemon struct {
		name string
		ctr  testcontainers.Container
	}
	var owned []daemon
	for _, mon := range clusteraccess.Monitors(t, cluster) {
		owned = append(owned, daemon{"mon." + mon.DaemonName, mon.Container})
	}
	for _, mgr := range clusteraccess.Managers(t, cluster) {
		owned = append(owned, daemon{"mgr." + mgr.DaemonName, mgr.Container})
	}
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		owned = append(owned, daemon{fmt.Sprintf("osd.%d", osd.ID), osd.Container})
	}
	peer := " >> " + address + ":"
	for _, d := range owned {
		inspect, err := d.ctr.Inspect(ctx)
		if err != nil || inspect.State == nil {
			t.Fatalf("inspect %s: %v", d.name, err)
		}
		entry := &rgwV2Daemon{Generation: d.ctr.GetContainerID() + "@" + inspect.State.StartedAt}
		daemons[d.name] = entry
		lines := harness.ContainerLogLines(t, ctx, d.ctr)
		var selected []string
		for _, line := range lines {
			if strings.Contains(line, peer) {
				selected = append(selected, line)
			}
		}
		binding := harness.ReadyRevisionBinding{Daemon: d.name, Generation: entry.Generation, Capture: fmt.Sprintf("container log lines 0-%d, peer %s", len(lines), address)}
		observations := harness.ParseReadyRevisionLines(binding, selected)
		entry.Ready = len(observations)
		if entry.Ready > 0 {
			entry.Problems = harness.ReadyRevision1Problems(observations)
		}
		for _, problem := range entry.Problems {
			problems = append(problems, d.name+": "+problem)
		}
	}
	for _, kind := range []string{"mon.", "osd."} {
		if !slices.ContainsFunc(owned, func(d daemon) bool { return strings.HasPrefix(d.name, kind) && daemons[d.name].Ready > 0 }) {
			problems = append(problems, fmt.Sprintf("no %s daemon logged a READY session with the gateway", strings.TrimSuffix(kind, ".")))
		}
	}

	if dump {
		asok := gatewayAsok(t, ctx, gateway)
		names, err := harness.ParseMessengerCatalog([]byte(harness.ExecOutput(t, ctx, gateway, "ceph", "--admin-daemon", asok, "messenger", "dump")))
		if err != nil {
			t.Fatal(err)
		}
		peers := map[string]int{}
		var selves []harness.MessengerSelf
		for _, name := range names {
			data := []byte(harness.ExecOutput(t, ctx, gateway, "ceph", "--admin-daemon", asok, "messenger", "dump", name))
			self, err := harness.ParseMessengerSelf(data)
			if err != nil {
				t.Fatal(err)
			}
			selves = append(selves, self)
			if self.ListenSockets != 0 || slices.ContainsFunc(self.Addrs, func(a harness.MessengerAddress) bool { return a.Type == "v1" }) {
				problems = append(problems, fmt.Sprintf("radosgw messenger %s listens on %d sockets with %+v", name, self.ListenSockets, self.Addrs))
			}
			connections, err := harness.ParseMessengerConnections(data)
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range connections {
				if !c.Ready() {
					continue
				}
				peers[c.PeerType]++
				if !c.Revision1 {
					problems = append(problems, fmt.Sprintf("radosgw messenger %s connection to %s.%d is not msgr2.1", name, c.PeerType, c.PeerID))
				}
			}
		}
		receipt["radosgw_messengers"], receipt["radosgw_ready_peers"] = selves, peers
		if peers["mon"] == 0 || peers["osd"] == 0 {
			problems = append(problems, fmt.Sprintf("radosgw's READY connections reach %v, not a MON and an OSD", peers))
		}
	}
	if len(problems) != 0 {
		t.Fatalf("RGW v2 sessions: %s", strings.Join(problems, "; "))
	}
}
