//go:build all || integration

package rgw_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const (
	ingressBucket  = "tc-regional"
	ingressKey     = "regional-object"
	ingressOneWay  = 20 * time.Millisecond
	ingressTimeout = 3 * time.Second
)

// ingressGateway is one RGW endpoint of the regional S3 service: its zone,
// its address on the public bridge and the RADOS instance ID that the
// transaction IDs it issues end with.
type ingressGateway struct {
	Name, Zone string
	Container  string
	Endpoint   string
	Addr       netip.AddrPort
	Instance   string
}

// ingressAttempt is one GET of the consumer's routing policy: where it was
// sent, what came back and which gateway issued the response.
type ingressAttempt struct {
	Endpoint  string
	Status    int
	CurlExit  int
	Elapsed   time.Duration
	RequestID string
	Served    string
	SHA256    string
}

// ingressPolicy is the consumer's regional entry point, owned by the test as
// the consumer, not by the fixture: try the client's own zone's gateway
// first, each attempt bounded by a timeout, and fall back to the next one in
// order on a timeout or a non-200 answer.
type ingressPolicy struct {
	Client    testcontainers.Container
	Order     []ingressGateway
	Timeout   time.Duration
	Signer    harness.S3HTTPClient
	Instances map[string]string
}

// testRGWZoneIngress answers whether an application in zone a, whose
// routing policy prefers its zone's gateway, reaches the same object through
// zone b's gateway when the local path fails, within one regional cluster and
// one RGW zone. Gateways in zones a and b serve the same storage. The serving
// gateway of every attempt is identified by the transaction ID it put in its
// response, whose instance part each gateway proved in a setup request.
func testRGWZoneIngress(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	helper := harness.NetemImage(t)
	cluster := harness.NewRegionalCluster(t, harness.RGWRun,
		ceph.WithZoneConfig(harness.RegionalZoneConfig(t)),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithInitialOSDs(
			ceph.OSDConfig{Zone: "a", Host: "zone-a-host-0"},
			ceph.OSDConfig{Zone: "b", Host: "zone-b-host-0"},
		).WithPoolDefaults(2, 1)),
		rgw.WithGateways(rgw.Config{Name: "zone-a"}))
	facts, err := ceph.FactsOf(cluster)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("release %s, images %+v", facts.CephVersion, facts.Images)
	gateways, err := rgw.Gateways(ctx, cluster)
	if err != nil || len(gateways) != 1 {
		t.Fatalf("gateways %v %v", gateways, err)
	}
	gwA := gateways[0]
	gwB, err := rgw.Start(ctx, cluster, rgw.Config{Name: "zone-b", SkipUserCreation: true}, cluster.WithZone("b"))
	if err != nil {
		t.Fatal(err)
	}
	clientA := harness.RegionalClient(t, ctx, cluster, "a")
	clientB := harness.RegionalClient(t, ctx, cluster, "b")
	layout, err := cluster.Zones(ctx)
	if err != nil {
		t.Fatal(err)
	}
	clientAMember, _ := harness.ZoneMemberByContainer(t, layout, clientA.GetContainerID())
	var all []ingressGateway
	for _, gw := range []*rgw.Gateway{gwA, gwB} {
		member, zone := harness.ZoneMemberByContainer(t, layout, gw.GetContainerID())
		endpoint, err := gw.DaemonEndpoint(ctx)
		if err != nil {
			t.Fatal(err)
		}
		u, err := url.Parse(endpoint)
		if err != nil {
			t.Fatal(err)
		}
		addr, err := netip.ParseAddrPort(u.Host)
		if err != nil || addr.Addr() != member.PublicAddress {
			t.Fatalf("gateway %s endpoint %s is not its member address %v: %v", gw.GatewayName, endpoint, member.PublicAddress, err)
		}
		all = append(all, ingressGateway{Name: gw.GatewayName, Zone: zone, Container: gw.GetContainerID(), Endpoint: endpoint, Addr: addr})
	}
	if all[0].Zone != "a" || all[1].Zone != "b" {
		t.Fatalf("gateways are in zones %s and %s, want a and b", all[0].Zone, all[1].Zone)
	}
	fsid, err := cluster.Ceph(ctx, "fsid")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("cluster %s; gateway %s in zone a at %s, gateway %s in zone b at %s; client %s in zone a",
		strings.TrimSpace(string(fsid)), all[0].Name, all[0].Endpoint, all[1].Name, all[1].Endpoint, clientAMember.Name)

	// Setup control: one object written once, read through both gateways.
	host, err := gwA.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	signer := harness.S3HTTPClient{Endpoint: host, AccessKey: gwA.AccessKey, SecretKey: gwA.SecretKey, Region: gwA.Region, HTTP: &http.Client{Timeout: 30 * time.Second}}
	payload := []byte(strings.Repeat("regional S3 object through either zone\n", 4096))
	want := harness.S3Hash(payload)
	signer.Request(t, ctx, http.MethodPut, "/"+ingressBucket, nil, http.StatusOK)
	signer.Request(t, ctx, http.MethodPut, "/"+ingressBucket+"/"+ingressKey, payload, http.StatusOK)
	instances := map[string]string{}
	for i := range all {
		attempt := ingressGet(t, ctx, clientA, signer, all[i], 30*time.Second, nil)
		if attempt.Status != http.StatusOK || attempt.SHA256 != want {
			t.Fatalf("setup GET through %s: %+v, want 200 and sha256 %s", all[i].Name, attempt, want)
		}
		all[i].Instance = ingressInstance(attempt.RequestID)
		if previous, found := instances[all[i].Instance]; found {
			t.Fatalf("gateways %s and %s issue the same instance %s", previous, all[i].Name, all[i].Instance)
		}
		instances[all[i].Instance] = all[i].Name
		t.Logf("setup GET through %s: 200, sha256 %s, request ID %s (instance %s)", all[i].Name, attempt.SHA256, attempt.RequestID, all[i].Instance)
	}
	ingressServiceMap(t, ctx, cluster, all)
	fromA := ingressPolicy{Client: clientA, Order: []ingressGateway{all[0], all[1]}, Timeout: ingressTimeout, Signer: signer, Instances: instances}
	fromB := ingressPolicy{Client: clientB, Order: []ingressGateway{all[1], all[0]}, Timeout: ingressTimeout, Signer: signer, Instances: instances}
	probe := harness.NewZoneProbe(t, ctx, clientAMember.Name, clientA.GetContainerID())
	rtt := func(phase string) (harness.ZoneRTT, harness.ZoneRTT) {
		t.Helper()
		local, err := probe.ConnectRTT(ctx, all[0].Addr, 20, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		remote, err := probe.ConnectRTT(ctx, all[1].Addr, 20, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("RTT %-10s client-a to local %s: %s; to remote %s: %s", phase, all[0].Name, local, all[1].Name, remote)
		return local, remote
	}
	expect := func(what string, attempts []ingressAttempt, served []string) {
		t.Helper()
		var got []string
		for i, attempt := range attempts {
			t.Logf("%s attempt %d: %s status %d curl exit %d in %v, request ID %q served by %q, sha256 %s",
				what, i, attempt.Endpoint, attempt.Status, attempt.CurlExit, attempt.Elapsed.Round(time.Millisecond), attempt.RequestID, attempt.Served, attempt.SHA256)
			got = append(got, attempt.Served)
		}
		if strings.Join(got, ",") != strings.Join(served, ",") {
			t.Fatalf("%s: attempts served by %q, want %q", what, got, served)
		}
		if last := attempts[len(attempts)-1]; last.Status != http.StatusOK || last.SHA256 != want {
			t.Fatalf("%s: the final attempt returned %d with sha256 %s, want 200 and %s", what, last.Status, last.SHA256, want)
		}
	}

	// Baseline: each zone's client is served by its own zone's gateway.
	baseLocal, baseRemote := rtt("baseline")
	expect("baseline from zone a", fromA.get(t, ctx), []string{all[0].Name})
	expect("baseline from zone b", fromB.get(t, ctx), []string{all[1].Name})

	// The cross-zone path between client-a and the zone b gateway is
	// delayed; the local path stays healthy and keeps serving.
	var delays []*fault.NetworkProfileLease
	for zone, peer := range map[string]netip.Addr{"a": all[1].Addr.Addr(), "b": clientAMember.PublicAddress} {
		lease, err := fault.ApplyZoneProfile(ctx, cluster, zone, fault.NetworkProfile{HelperImage: helper, Plane: ceph.PublicNetworkPlane, Delay: ingressOneWay, Peer: peer})
		if lease != nil {
			delays = append(delays, lease)
			t.Cleanup(func() {
				if err := lease.Restore(context.Background()); err != nil {
					t.Errorf("restore the zone %s delay: %v", zone, err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	shapedLocal, shapedRemote := rtt("delayed")
	if err := harness.CheckZoneControl(baseLocal, shapedLocal); err != nil {
		t.Fatalf("the local ingress path changed: %v", err)
	}
	if err := harness.CheckZoneShaped(baseRemote, shapedRemote, 2*ingressOneWay); err != nil {
		t.Fatalf("the remote ingress path did not gain the delay once: %v", err)
	}
	expect("delayed remote path, from zone a", fromA.get(t, ctx), []string{all[0].Name})
	direct := ingressGet(t, ctx, clientA, signer, all[1], 30*time.Second, instances)
	t.Logf("direct GET to the remote %s under the delay: status %d in %v, served by %s", all[1].Name, direct.Status, direct.Elapsed.Round(time.Millisecond), direct.Served)
	if direct.Served != all[1].Name || direct.SHA256 != want {
		t.Fatalf("the direct remote GET: %+v", direct)
	}

	// The local gateway path is cut; the remote gateway and the storage
	// stay healthy, and the policy falls back within its bound.
	cut, err := fault.ApplyNetworkProfile(ctx, cluster, clientA, fault.NetworkProfile{HelperImage: helper, Plane: ceph.PublicNetworkPlane, Loss: 100, Peer: all[0].Addr.Addr(), PeerPort: all[0].Addr.Port()})
	if cut != nil {
		t.Cleanup(func() {
			if err := cut.Restore(context.Background()); err != nil {
				t.Errorf("restore the local path: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	fallback := fromA.get(t, ctx)
	elapsed := time.Since(start)
	expect("local path cut, from zone a", fallback, []string{"", all[1].Name})
	if fallback[0].CurlExit != 28 {
		t.Fatalf("the local attempt ended with curl exit %d, want the timeout 28", fallback[0].CurlExit)
	}
	if bound := ingressTimeout + 15*time.Second; elapsed > bound {
		t.Fatalf("the fallback took %v, more than %v", elapsed, bound)
	}
	t.Logf("fallback within %v: the local attempt timed out after %v, %s served the object", elapsed.Round(time.Millisecond), fallback[0].Elapsed.Round(time.Millisecond), fallback[1].Served)
	viaB := ingressGet(t, ctx, clientB, signer, all[0], 30*time.Second, instances)
	if viaB.Served != all[0].Name || viaB.SHA256 != want {
		t.Fatalf("the zone a gateway did not serve zone b during the local cut: %+v", viaB)
	}
	t.Logf("the zone a gateway itself stayed healthy: served client-b in %v", viaB.Elapsed.Round(time.Millisecond))

	// Restoration: failback to the local gateway.
	if err := cut.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	expect("local path restored, from zone a", fromA.get(t, ctx), []string{all[0].Name})
	for i := len(delays) - 1; i >= 0; i-- {
		if err := delays[i].Restore(ctx); err != nil {
			t.Fatal(err)
		}
	}
	restoredLocal, restoredRemote := rtt("restored")
	for name, pair := range map[string][2]harness.ZoneRTT{"local": {baseLocal, restoredLocal}, "remote": {baseRemote, restoredRemote}} {
		if err := harness.CheckZoneControl(pair[0], pair[1]); err != nil {
			t.Fatalf("the %s ingress path after restoration: %v", name, err)
		}
	}
}

// get runs the policy once: each gateway in order until one answers 200.
func (p ingressPolicy) get(t *testing.T, ctx context.Context) []ingressAttempt {
	t.Helper()
	var attempts []ingressAttempt
	for _, gw := range p.Order {
		attempt := ingressGet(t, ctx, p.Client, p.Signer, gw, p.Timeout, p.Instances)
		attempts = append(attempts, attempt)
		if attempt.Status == http.StatusOK {
			break
		}
	}
	return attempts
}

// ingressGet sends one signed GET of the object from client to gw with curl,
// bounded by timeout, and reads the status, the transaction ID and the bytes'
// digest. instances maps a transaction ID's instance to its gateway.
func ingressGet(t *testing.T, ctx context.Context, client testcontainers.Container, signer harness.S3HTTPClient, gw ingressGateway, timeout time.Duration, instances map[string]string) ingressAttempt {
	t.Helper()
	target := gw.Endpoint + "/" + ingressBucket + "/" + ingressKey
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	signer.Sign(req, nil, time.Now().UTC())
	script := `rm -f /tmp/ingress.body /tmp/ingress.head
code=$(curl -s -o /tmp/ingress.body -D /tmp/ingress.head -w '%{http_code}' --max-time "$1" -H "X-Amz-Date: $2" -H "X-Amz-Content-Sha256: $3" -H "Authorization: $4" "$5")
exit=$?
id=$(tr -d '\r' < /tmp/ingress.head 2>/dev/null | awk -F': ' 'tolower($1) == "x-amz-request-id" {print $2}')
sum=$(sha256sum /tmp/ingress.body 2>/dev/null | cut -d' ' -f1)
printf '{"code":"%s","exit":%d,"id":"%s","sha":"%s"}\n' "$code" "$exit" "$id" "$sum"`
	args := []string{"sh", "-c", script, "ingress", strconv.FormatFloat(timeout.Seconds(), 'f', 3, 64),
		req.Header.Get("X-Amz-Date"), req.Header.Get("X-Amz-Content-Sha256"), req.Header.Get("Authorization"), target}
	execCtx, cancel := context.WithTimeout(ctx, timeout+time.Minute)
	defer cancel()
	start := time.Now()
	code, r, err := client.Exec(execCtx, args, tcexec.Multiplexed())
	elapsed := time.Since(start)
	if err != nil || code != 0 {
		t.Fatalf("GET through %s: exit %d %v", gw.Name, code, err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Code string `json:"code"`
		Exit int    `json:"exit"`
		ID   string `json:"id"`
		SHA  string `json:"sha"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(out))), &result); err != nil {
		t.Fatalf("GET through %s printed %q: %v", gw.Name, out, err)
	}
	status, _ := strconv.Atoi(result.Code)
	attempt := ingressAttempt{Endpoint: gw.Endpoint, Status: status, CurlExit: result.Exit, Elapsed: elapsed, RequestID: result.ID}
	if status == http.StatusOK {
		attempt.SHA256 = result.SHA
	}
	if result.ID != "" && instances != nil {
		served, found := instances[ingressInstance(result.ID)]
		if !found {
			t.Fatalf("request ID %s through %s names no known gateway instance %v", result.ID, gw.Name, instances)
		}
		attempt.Served = served
	}
	return attempt
}

// ingressInstance returns the instance part of an RGW transaction ID
// tx<counter>-<time>-<instance>-<zone>: the RADOS instance of the gateway
// process that issued it.
func ingressInstance(id string) string {
	parts := strings.Split(id, "-")
	if len(parts) < 4 {
		return ""
	}
	return parts[2]
}

// ingressServiceMap checks that each gateway's transaction instance is the
// global ID under which a daemon on the gateway's own container registered
// in the service map, an identity that does not depend on the request.
func ingressServiceMap(t *testing.T, ctx context.Context, cluster *ceph.Container, gateways []ingressGateway) {
	t.Helper()
	out, err := cluster.Ceph(ctx, "service", "dump", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var dump struct {
		Services map[string]struct {
			Daemons map[string]json.RawMessage `json:"daemons"`
		} `json:"services"`
	}
	if err := json.Unmarshal(out, &dump); err != nil {
		t.Fatal(err)
	}
	hosts := map[string]string{}
	for _, raw := range dump.Services["rgw"].Daemons {
		var daemon struct {
			GID      uint64            `json:"gid"`
			Metadata map[string]string `json:"metadata"`
		}
		if json.Unmarshal(raw, &daemon) != nil || daemon.GID == 0 {
			continue
		}
		hosts[strconv.FormatUint(daemon.GID, 10)] = daemon.Metadata["hostname"]
	}
	for _, gw := range gateways {
		host, found := hosts[gw.Instance]
		t.Logf("gateway %s (container %.12s): transaction instance %s, service map daemon gid %s on host %q", gw.Name, gw.Container, gw.Instance, gw.Instance, host)
		if !found || host == "" || !strings.HasPrefix(gw.Container, host) {
			t.Fatalf("gateway %s's instance %s is not the service map daemon of its container: %v", gw.Name, gw.Instance, hosts)
		}
	}
}
