//go:build all || integration

package rgw_test

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net"
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

// radosgwSession is one client session that a MON holds with a messenger in
// the gateway's container: the messenger's address nonce and the global ID
// the MON gave it.
type radosgwSession struct {
	Nonce    uint64 `json:"nonce"`
	GlobalID int64  `json:"global_id"`
}

// parseGatewaySessions returns the client sessions in a MON's sessions output
// whose socket address is in the gateway's container at address. Sampled
// after rgw.Run has returned, when the radosgw-admin processes that created
// the gateway's S3 user have exited, these are radosgw's own messengers.
func parseGatewaySessions(data []byte, address string) ([]radosgwSession, error) {
	var sessions []struct {
		ConType    string                   `json:"con_type"`
		SocketAddr harness.MessengerAddress `json:"socket_addr"`
		GlobalID   int64                    `json:"global_id"`
	}
	if err := json.Unmarshal(data, &sessions); err != nil {
		return nil, fmt.Errorf("decode MON sessions: %w", err)
	}
	var result []radosgwSession
	for _, session := range sessions {
		host, _, err := net.SplitHostPort(session.SocketAddr.Addr)
		if session.ConType != "client" || err != nil || host != address {
			continue
		}
		if session.SocketAddr.Nonce == 0 || session.GlobalID <= 0 {
			return nil, fmt.Errorf("MON session from %s has nonce %d and global ID %d", address, session.SocketAddr.Nonce, session.GlobalID)
		}
		result = append(result, radosgwSession{Nonce: session.SocketAddr.Nonce, GlobalID: session.GlobalID})
	}
	return result, nil
}

// selectRadosgwReady returns the READY lines whose peer is one of sessions'
// messengers at address. A line with a peer at address from any other
// messenger, such as a radosgw-admin process's, is counted in others and not
// selected. A selected line whose authenticated entity is not its session's
// client.<global ID> is a problem; a MON logs client.? before it
// authenticates a client.
func selectRadosgwReady(lines []string, address string, sessions []radosgwSession) (selected []string, others int, problems []string) {
	identities := map[uint64]string{}
	for _, session := range sessions {
		identities[session.Nonce] = fmt.Sprintf("client.%d", session.GlobalID)
	}
	for _, line := range lines {
		_, rest, ok := strings.Cut(line, " >> ")
		if !ok || !strings.Contains(line, " s=READY ") {
			continue
		}
		peer, _, _ := strings.Cut(rest, " ")
		hostPort, nonceText, ok := strings.Cut(peer, "/")
		host, _, err := net.SplitHostPort(hostPort)
		if !ok || err != nil || host != address {
			continue
		}
		nonce, err := strconv.ParseUint(nonceText, 10, 64)
		identity, owned := identities[nonce]
		if err != nil || !owned {
			others++
			continue
		}
		_, tail, _ := strings.Cut(line, " entity=")
		entity, _, _ := strings.Cut(tail, " ")
		if entity != identity && entity != "client.?" {
			problems = append(problems, fmt.Sprintf("READY session with radosgw messenger %d is %q, not %s: %s", nonce, entity, identity, line))
			continue
		}
		selected = append(selected, line)
	}
	return selected, others, problems
}

// rgwV2Daemon is one daemon's part of the RGW v2 sessions receipt: the READY
// sessions it logged with radosgw's messengers, and how many READY sessions
// it logged with other processes in the gateway's container.
type rgwV2Daemon struct {
	Generation string   `json:"generation"`
	Ready      int      `json:"ready"`
	Others     int      `json:"other_processes"`
	Problems   []string `json:"problems,omitempty"`
}

// testRGWMessengerV2Sessions starts an RGW cluster without Messenger v1 whose
// MONs, MGRs and OSDs log their READY sessions. The gateway's container
// listens on the beast port only, so radosgw binds no messenger. radosgw's
// messengers are the ones the MONs hold client sessions with from the
// gateway's container; every READY session that a MON, MGR or OSD logged
// with one of them is msgr2.1, and a MON and an OSD logged at least one.
// Sessions of radosgw-admin, which also runs in that container, are left
// out. With messenger dump, radosgw's own messengers are exactly the MONs'
// sessions, listen on no socket, advertise no v1 address and have READY
// msgr2.1 connections to a MON and an OSD.
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
	var sessions []radosgwSession
	for _, mon := range clusteraccess.Monitors(t, cluster) {
		held, err := parseGatewaySessions([]byte(harness.ExecOutput(t, ctx, mon.Container, "ceph", "daemon", "mon."+mon.DaemonName, "sessions")), address)
		if err != nil {
			t.Fatalf("mon.%s: %v", mon.DaemonName, err)
		}
		sessions = append(sessions, held...)
	}
	receipt["radosgw_sessions"] = sessions
	if len(sessions) == 0 {
		problems = append(problems, "no MON holds a client session from the gateway's container")
	}
	for _, d := range owned {
		inspect, err := d.ctr.Inspect(ctx)
		if err != nil || inspect.State == nil {
			t.Fatalf("inspect %s: %v", d.name, err)
		}
		entry := &rgwV2Daemon{Generation: d.ctr.GetContainerID() + "@" + inspect.State.StartedAt}
		daemons[d.name] = entry
		lines := harness.ContainerLogLines(t, ctx, d.ctr)
		selected, others, attribution := selectRadosgwReady(lines, address, sessions)
		entry.Others = others
		binding := harness.ReadyRevisionBinding{Daemon: d.name, Generation: entry.Generation, Capture: fmt.Sprintf("container log lines 0-%d, radosgw messengers at %s", len(lines), address)}
		observations := harness.ParseReadyRevisionLines(binding, selected)
		entry.Ready = len(observations)
		entry.Problems = attribution
		if entry.Ready > 0 {
			entry.Problems = append(entry.Problems, harness.ReadyRevision1Problems(observations)...)
		}
		for _, problem := range entry.Problems {
			problems = append(problems, d.name+": "+problem)
		}
	}
	for _, kind := range []string{"mon.", "osd."} {
		if !slices.ContainsFunc(owned, func(d daemon) bool { return strings.HasPrefix(d.name, kind) && daemons[d.name].Ready > 0 }) {
			problems = append(problems, fmt.Sprintf("no %s daemon logged a READY session with radosgw", strings.TrimSuffix(kind, ".")))
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
		dumped := map[uint64]bool{}
		for _, name := range names {
			data := []byte(harness.ExecOutput(t, ctx, gateway, "ceph", "--admin-daemon", asok, "messenger", "dump", name))
			self, err := harness.ParseMessengerSelf(data)
			if err != nil {
				t.Fatal(err)
			}
			selves = append(selves, self)
			for _, a := range self.Addrs {
				dumped[a.Nonce] = true
			}
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
		held := map[uint64]bool{}
		for _, session := range sessions {
			held[session.Nonce] = true
		}
		if !maps.Equal(dumped, held) {
			problems = append(problems, fmt.Sprintf("radosgw's messengers have nonces %v, but the MONs hold sessions from the gateway's container with %v", slices.Sorted(maps.Keys(dumped)), slices.Sorted(maps.Keys(held))))
		}
		if peers["mon"] == 0 || peers["osd"] == 0 {
			problems = append(problems, fmt.Sprintf("radosgw's READY connections reach %v, not a MON and an OSD", peers))
		}
	}
	if len(problems) != 0 {
		t.Fatalf("RGW v2 sessions: %s", strings.Join(problems, "; "))
	}
}
