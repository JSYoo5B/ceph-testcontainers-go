//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// The fixture profiles of a Messenger client test suite: three MONs, two MGRs
// or none, v2-only listeners, per-role connection modes, Cephx key types and
// ticket lifetimes, and the daemon controls that such a suite uses.
// docs/guides/messenger-client-fixtures.md maps each profile to its options.

// profileAuth returns want when the image can make the keys it names.
// Otherwise it checks that Run refuses want before any daemon starts, and
// returns the all-AES configuration with the same lifetimes.
func profileAuth(t *testing.T, ctx context.Context, image string, base []testcontainers.ContainerCustomizer, want ceph.AuthConfig) ceph.AuthConfig {
	t.Helper()
	aes256k := slices.Contains([]ceph.CephxCipher{want.KeyType, want.ServiceCipher, want.PreferredCipher}, ceph.CipherAES256K) ||
		slices.Contains(want.AllowedCiphers, ceph.CipherAES256K)
	if !aes256k || imageHasKeyTypes(t, ctx, image) {
		return want
	}
	started := time.Now()
	refused, err := ceph.Run(ctx, image, append(slices.Clone(base), ceph.WithAuth(want))...)
	testcontainers.CleanupContainer(t, refused)
	if !errors.Is(err, ceph.ErrUnsupported) || !strings.Contains(err.Error(), "ceph-authtool --key-type") || time.Since(started) > 2*time.Minute {
		t.Fatalf("an image without --key-type: Run with %+v returned %v after %s", want, err, time.Since(started))
	}
	t.Logf("the image cannot make AES256K keys: Run refused %+v before any daemon started (%v); the profile runs with AES", want, err)
	return ceph.AuthConfig{KeyType: ceph.CipherAES, MonTicketTTL: want.MonTicketTTL, ServiceTicketTTL: want.ServiceTicketTTL}
}

// profileConfigFile writes a WithConfigFile fragment into the test's
// temporary directory.
func profileConfigFile(t *testing.T, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "profile.conf")
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// waitProfileTopology waits until every MON of the map is in the quorum and
// the MGRs report an active one with the given number of standbys, the
// readiness gate of a suite that starts its daemons itself.
func waitProfileTopology(t *testing.T, ctx context.Context, cluster *ceph.Container, monitors, standbys int) {
	t.Helper()
	var quorum ceph.QuorumStatus
	var managers ceph.ManagerStatus
	var err error
	for deadline := time.Now().Add(3 * time.Minute); ; {
		quorum, err = cluster.QuorumStatus(ctx)
		if err == nil && len(quorum.QuorumNames) == monitors && len(quorum.MonMap.Mons) == monitors {
			managers, err = cluster.ManagerStatus(ctx)
			if err == nil && (standbys < 0 || managers.Available && len(managers.Standbys) == standbys) {
				return
			}
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatalf("want %d MONs in quorum and %d standby MGRs: quorum %v of %d, managers %+v, %v",
				monitors, standbys, quorum.QuorumNames, len(quorum.MonMap.Mons), managers, err)
		}
		time.Sleep(time.Second)
	}
}

// checkProfileCiphers checks the MonMap ciphers and the key types of the keys
// that the fixture and the MONs generated: KeyType for mon. and client.admin,
// PreferredCipher for the MGRs.
func checkProfileCiphers(t *testing.T, ctx context.Context, cluster *ceph.Container, auth ceph.AuthConfig) {
	t.Helper()
	service, preferred := auth.ServiceCipher, auth.PreferredCipher
	if service == "" {
		service = auth.KeyType
	}
	if preferred == "" {
		preferred = auth.KeyType
	}
	if keyTypeOption(t, ctx, cluster) {
		allowed := auth.AllowedCiphers
		if allowed == nil {
			for _, cipher := range []ceph.CephxCipher{auth.KeyType, service, preferred} {
				if !slices.Contains(allowed, cipher) {
					allowed = append(allowed, cipher)
				}
			}
		}
		gotService, gotAllowed, gotPreferred := readMonMapCiphers(t, ctx, cluster)
		var want []string
		for _, cipher := range allowed {
			want = append(want, string(cipher))
		}
		slices.Sort(want)
		slices.Sort(gotAllowed)
		if gotService != string(service) || gotPreferred != string(preferred) || !slices.Equal(gotAllowed, want) {
			t.Fatalf("MonMap ciphers service=%s allowed=%v preferred=%s, want %s %v %s", gotService, gotAllowed, gotPreferred, service, want, preferred)
		}
	}
	_, keyring, err := cluster.ConnectionConfig()
	if err != nil || cephxKeyType(t, keyring, "client.admin") != auth.KeyType {
		t.Fatalf("client.admin key type: %v", err)
	}
	if got := authEntityKeyType(t, ctx, cluster, "mon."); got != auth.KeyType {
		t.Fatalf("mon. has a %s key, want %s", got, auth.KeyType)
	}
	for _, mgr := range cluster.Managers() {
		if got := authEntityKeyType(t, ctx, cluster, "mgr."+mgr.DaemonName); got != preferred {
			t.Fatalf("mgr.%s has a %s key, want %s", mgr.DaemonName, got, preferred)
		}
	}
}

// checkProfileTickets checks the ticket lifetimes that a MON and a MGR run
// with, and that global_id reclaim is not allowed insecurely.
func checkProfileTickets(t *testing.T, ctx context.Context, cluster *ceph.Container, monTTL, serviceTTL float64) {
	t.Helper()
	for _, daemon := range []string{"mon.a", "mgr." + cluster.Managers()[0].DaemonName} {
		if got := runningOption(t, ctx, cluster, daemon, "auth_mon_ticket_ttl"); got != monTTL {
			t.Fatalf("%s auth_mon_ticket_ttl = %v, want %v", daemon, got, monTTL)
		}
		if got := runningOption(t, ctx, cluster, daemon, "auth_service_ticket_ttl"); got != serviceTTL {
			t.Fatalf("%s auth_service_ticket_ttl = %v, want %v", daemon, got, serviceTTL)
		}
	}
	if out, err := cluster.Ceph(ctx, "config", "show", "mon.a", "auth_allow_insecure_global_id_reclaim"); err != nil || strings.TrimSpace(string(out)) != "false" {
		t.Fatalf("mon.a auth_allow_insecure_global_id_reclaim = %q: %v", out, err)
	}
}

// profileClient runs the Ceph CLI as one client identity in its own container.
type profileClient struct {
	identity *ceph.ClientConfig
	ctr      testcontainers.Container
}

func startProfileClient(t *testing.T, ctx context.Context, cluster *ceph.Container, identity *ceph.ClientConfig, opts ...ceph.ClientContainerOption) profileClient {
	t.Helper()
	ctr, err := testcontainers.Run(ctx, nativeClientImage(t, cluster.ControlImage()), cluster.WithClientIdentity(identity, opts...), ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatal(err)
	}
	return profileClient{identity: identity, ctr: ctr}
}

func (client profileClient) ceph(t *testing.T, ctx context.Context, args ...string) (int, string) {
	t.Helper()
	command := append([]string{"timeout", policyRejectTimeout, "ceph", "--id", client.identity.User(), "--connect-timeout", "10"}, args...)
	code, reader, err := client.ctr.Exec(ctx, command, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, _ := io.ReadAll(reader)
	return code, string(output)
}

// createProfileIdentities creates the client identities of a suite with keys
// of keyType: full access, read-only, one to revoke and restore, and one that
// may run only the fsid command. It checks each key's type and what the
// limited identities may do.
func createProfileIdentities(t *testing.T, ctx context.Context, cluster *ceph.Container, keyType ceph.CephxCipher) map[string]*ceph.ClientConfig {
	t.Helper()
	identities := map[string]*ceph.ClientConfig{}
	for name, caps := range map[string]ceph.ClientCaps{
		"test":        {Mon: "allow *", MGR: "allow *"},
		"readonly":    {Mon: "allow r", MGR: "allow r"},
		"revocable":   {Mon: "allow *", MGR: "allow *"},
		"configwatch": {Mon: `allow command "fsid"`, MGR: "allow r"},
	} {
		identity, err := cluster.CreateClient(ctx, name, caps)
		if err != nil {
			t.Fatal(err)
		}
		if got := cephxKeyType(t, clientKeyring(t, identity), identity.Name()); got != keyType {
			t.Fatalf("%s has a %s key, want %s", identity.Name(), got, keyType)
		}
		identities[name] = identity
	}
	status, err := cluster.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	watch := startProfileClient(t, ctx, cluster, identities["configwatch"])
	if code, out := watch.ceph(t, ctx, "fsid"); code != 0 || strings.TrimSpace(out) != status.FSID {
		t.Fatalf("configwatch fsid: exit %d: %s", code, out)
	}
	if code, out := watch.ceph(t, ctx, "mon", "stat"); code == 0 || !strings.Contains(out, "denied") {
		t.Fatalf("configwatch ran mon stat: exit %d: %s", code, out)
	}
	reader := startProfileClient(t, ctx, cluster, identities["readonly"])
	if code, out := reader.ceph(t, ctx, "mon", "stat"); code != 0 {
		t.Fatalf("readonly mon stat: exit %d: %s", code, out)
	}
	if code, out := reader.ceph(t, ctx, "config-key", "set", "tc-profile", "value"); code == 0 || !strings.Contains(out, "denied") {
		t.Fatalf("readonly wrote a config key: exit %d: %s", code, out)
	}
	t.Logf("client identities test, readonly, revocable and configwatch have %s keys; configwatch may run only fsid and readonly may not write", keyType)
	return identities
}

// checkProfileClientModes checks the modes of the client connections that
// daemon reports with messenger dump. A release without messenger dump, such
// as Squid, leaves the evidence to accepted and refused clients.
func checkProfileClientModes(t *testing.T, ctx context.Context, cluster *ceph.Container, daemon, want string) {
	t.Helper()
	var modes []string
	if err := json.Unmarshal([]byte(execOutput(t, ctx, toolkitContainer(t, cluster), "python3", "-c", messengerClientModes, daemon)), &modes); err != nil {
		t.Fatal(err)
	}
	switch {
	case modes == nil:
		t.Logf("%s has no messenger dump", daemon)
	case len(modes) == 0 || slices.ContainsFunc(modes, func(mode string) bool { return mode != want }):
		t.Fatalf("%s client connections use %v, not only %s", daemon, modes, want)
	default:
		t.Logf("%s client connection modes from messenger dump: %v", daemon, modes)
	}
}

// checkProfileAddressFamily checks that every MON and the active MGR
// advertise addresses of the family: IPv6, or IPv4-mapped IPv6.
func checkProfileAddressFamily(t *testing.T, ctx context.Context, cluster *ceph.Container, family ceph.AddressFamily) {
	t.Helper()
	native, err := cluster.DaemonAddresses(ctx)
	if err != nil || native.ActiveManager == nil {
		t.Fatalf("daemon addresses: %+v, %v", native, err)
	}
	vectors := map[string][]ceph.EntityAddress{"mgr." + native.ActiveManager.Name: native.ActiveManager.Addrs}
	for _, mon := range native.Monitors {
		vectors["mon."+mon.Name] = mon.Addrs
	}
	for name, vector := range vectors {
		for _, entry := range vector {
			address, err := netip.ParseAddrPort(entry.Addr)
			if err != nil {
				t.Fatalf("%s advertises %q: %v", name, entry.Addr, err)
			}
			mapped := address.Addr().Is4In6()
			if !address.Addr().Is6() || (family == ceph.AddressIPv4MappedIPv6) != mapped {
				t.Fatalf("%s advertises %s, not an address of %v", name, entry.Addr, family)
			}
		}
	}
	t.Logf("%d daemons advertise only %v addresses", len(vectors), family)
}

// probeOpenPort reports whether a TCP connection to address opens, and
// whether the daemon then sends its Messenger v2 banner within two seconds.
// A paused daemon's socket still accepts connections in the kernel, but the
// daemon never answers.
const probeOpenPort = `import json,socket,sys
host,port=sys.argv[1].rsplit(':',1)
s=socket.create_connection((host.strip('[]'),int(port)),timeout=5)
s.settimeout(2)
try:
    banner=s.recv(8)
except socket.timeout:
    banner=b''
s.close()
print(json.dumps({'open':True,'banner':banner.decode('latin-1')}))
`

// checkPausedButOpen checks that address accepts a TCP connection and sends
// no banner, as from a paused daemon.
func checkPausedButOpen(t *testing.T, ctx context.Context, cluster *ceph.Container, name, address string) {
	t.Helper()
	var probe struct {
		Open   bool   `json:"open"`
		Banner string `json:"banner"`
	}
	out := execOutput(t, ctx, toolkitContainer(t, cluster), "python3", "-c", probeOpenPort, address)
	if err := json.Unmarshal([]byte(out), &probe); err != nil || !probe.Open || probe.Banner != "" {
		t.Fatalf("paused %s at %s: %s", name, address, out)
	}
}

// waitQuorumNames waits until the quorum is exactly names.
func waitQuorumNames(t *testing.T, ctx context.Context, cluster *ceph.Container, names ...string) {
	t.Helper()
	slices.Sort(names)
	var got []string
	for deadline := time.Now().Add(3 * time.Minute); ; {
		quorum, err := cluster.QuorumStatus(ctx)
		if err == nil {
			got = slices.Clone(quorum.QuorumNames)
			slices.Sort(got)
			if slices.Equal(got, names) {
				return
			}
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatalf("quorum %v, want %v: %v", got, names, err)
		}
		time.Sleep(time.Second)
	}
}

// profileMonitorAddress returns the advertised v2 address of a MON.
func profileMonitorAddress(t *testing.T, ctx context.Context, cluster *ceph.Container, name string) string {
	t.Helper()
	native, err := cluster.DaemonAddresses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, mon := range native.Monitors {
		if mon.Name == name && len(mon.Addrs) > 0 {
			return mon.Addrs[0].Addr
		}
	}
	t.Fatalf("no address for mon.%s in %+v", name, native.Monitors)
	return ""
}

// exerciseProfileControls runs the daemon controls of a suite on a cluster
// with MONs a, b and c and two MGRs, whose MON tickets live monTTL:
//   - the active MGR paused, then failed over to the standby;
//   - one MGR stopped and started again;
//   - one MON restarted;
//   - all MONs stopped for longer than a MON ticket lives, then started;
//   - a paused MON keeps its TCP port open, leaves the quorum and rejoins on
//     resume;
//   - MON a stopped for good, with the quorum and clients carrying on.
func exerciseProfileControls(t *testing.T, ctx context.Context, cluster *ceph.Container, identities map[string]*ceph.ClientConfig, monTTL time.Duration) {
	t.Helper()
	monitors := map[string]*ceph.MonitorContainer{}
	for _, mon := range cluster.Monitors() {
		monitors[mon.DaemonName] = mon
	}
	if len(monitors) != 3 || monitors["a"] == nil || monitors["b"] == nil || monitors["c"] == nil {
		t.Fatalf("want MONs a, b and c: %v", monitors)
	}
	client := startProfileClient(t, ctx, cluster, identities["test"])
	expectMON := func(step string) {
		t.Helper()
		if code, out := client.ceph(t, ctx, "mon", "stat"); code != 0 {
			t.Fatalf("%s: client mon stat: exit %d: %s", step, code, out)
		}
	}
	expectClient := func(step string) {
		t.Helper()
		expectMON(step)
		if code, out := client.ceph(t, ctx, "pg", "stat"); code != 0 {
			t.Fatalf("%s: client pg stat: exit %d: %s", step, code, out)
		}
	}
	expectClient("before the controls")

	// Pause the active MGR, fail it over, then resume it as the standby.
	before, err := cluster.ManagerStatus(ctx)
	if err != nil || !before.Available || len(before.Standbys) != 1 {
		t.Fatalf("managers before the pause: %+v, %v", before, err)
	}
	native, err := cluster.DaemonAddresses(ctx)
	if err != nil || native.ActiveManager == nil || len(native.ActiveManager.Addrs) == 0 {
		t.Fatalf("active MGR address: %+v, %v", native, err)
	}
	var active *ceph.ManagerContainer
	for _, mgr := range cluster.Managers() {
		if mgr.DaemonName == before.ActiveName {
			active = mgr
		}
	}
	if active == nil {
		t.Fatalf("active MGR %s is not owned", before.ActiveName)
	}
	pause, err := fault.Pause(ctx, cluster, active.Container)
	if err != nil {
		t.Fatal(err)
	}
	checkPausedButOpen(t, ctx, cluster, "mgr."+active.DaemonName, native.ActiveManager.Addrs[0].Addr)
	if _, err := cluster.Ceph(ctx, "mgr", "fail", before.ActiveName); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(2 * time.Minute); ; {
		after, err := cluster.ManagerStatus(ctx)
		if err == nil && after.Available && after.ActiveName == before.Standbys[0].Name {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the standby did not take over: %+v, %v", after, err)
		}
		time.Sleep(time.Second)
	}
	if err := pause.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	waitProfileTopology(t, ctx, cluster, 3, 1)
	expectClient("MGR failover")

	// Stop the standby MGR and start it again.
	status, err := cluster.ManagerStatus(ctx)
	if err != nil || len(status.Standbys) != 1 {
		t.Fatalf("managers: %+v, %v", status, err)
	}
	var standby *ceph.ManagerContainer
	for _, mgr := range cluster.Managers() {
		if mgr.DaemonName == status.Standbys[0].Name {
			standby = mgr
		}
	}
	if standby == nil {
		t.Fatalf("standby MGR %s is not owned", status.Standbys[0].Name)
	}
	stop := 10 * time.Second
	if err := standby.Stop(ctx, &stop); err != nil {
		t.Fatal(err)
	}
	waitProfileTopology(t, ctx, cluster, 3, 0)
	if err := standby.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitProfileTopology(t, ctx, cluster, 3, 1)

	// Restart MON c.
	if err := monitors["c"].Stop(ctx, &stop); err != nil {
		t.Fatal(err)
	}
	waitQuorumNames(t, ctx, cluster, "a", "b")
	if err := monitors["c"].Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitQuorumNames(t, ctx, cluster, "a", "b", "c")
	expectClient("mon.c restarted")

	// Stop every MON for longer than a MON ticket lives. MON a's container
	// waits for the quorum when it starts, so b and c start first, and each
	// MON must come back at the address in the monmap.
	addresses := map[string]string{}
	for name := range monitors {
		addresses[name] = profileMonitorAddress(t, ctx, cluster, name)
	}
	for _, name := range []string{"a", "b", "c"} {
		if err := monitors[name].Stop(ctx, &stop); err != nil {
			t.Fatal(err)
		}
	}
	stopped := time.Now()
	time.Sleep(monTTL + 3*time.Second)
	for _, name := range []string{"b", "c", "a"} {
		if err := monitors[name].Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	waitQuorumNames(t, ctx, cluster, "a", "b", "c")
	for name, address := range addresses {
		advertised, err := netip.ParseAddrPort(address)
		if err != nil {
			t.Fatal(err)
		}
		ip, err := monitors[name].ContainerIP(ctx)
		if err != nil || ip != advertised.Addr().String() {
			t.Fatalf("mon.%s came back at %s, not at its monmap address %s: %v", name, ip, address, err)
		}
	}
	waitProfileTopology(t, ctx, cluster, 3, 1)
	expectClient("every MON restarted")
	t.Logf("every MON was down for %s, longer than the %s MON ticket lifetime; the quorum, both MGRs and the client came back", time.Since(stopped).Round(time.Second), monTTL)

	// Pause MON b: its socket stays open, and the other two form the quorum.
	// This runs after the MGR controls and is followed only by MON checks: a
	// daemon whose MON session was on the paused MON, once its MON tickets
	// expired, can be refused with EACCES when it reconnects, and its MON
	// client then stops authenticating. That happened to the active MGR in
	// two of four runs with 12-second tickets and a 25-second pause.
	address := profileMonitorAddress(t, ctx, cluster, "b")
	pause, err = fault.Pause(ctx, cluster, monitors["b"].Container)
	if err != nil {
		t.Fatal(err)
	}
	checkPausedButOpen(t, ctx, cluster, "mon.b", address)
	waitQuorumNames(t, ctx, cluster, "a", "c")
	expectMON("mon.b paused")
	if err := pause.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	waitQuorumNames(t, ctx, cluster, "a", "b", "c")
	expectMON("mon.b resumed")

	// Stop MON a for good. The other two keep the quorum, and the fixture CLI
	// and clients keep working.
	if err := monitors["a"].Stop(ctx, &stop); err != nil {
		t.Fatal(err)
	}
	waitQuorumNames(t, ctx, cluster, "b", "c")
	if _, err := cluster.Ceph(ctx, "mon", "stat"); err != nil {
		t.Fatalf("fixture CLI without mon.a: %v", err)
	}
	expectMON("mon.a stopped for good")
	t.Log("controls: paused MON and MGR kept their sockets open; MGR failover, standby stop and start, MON restart, all MONs restarted, and MON a stopped for good")
}

// profileRenewalClient keeps one librados session for seconds while it sends
// a MON command and a MGR command every half second, so that the session
// renews its tickets when they expire. It prints the number of calls and the
// seconds the session lasted.
const profileRenewalClient = `import json,sys,time,rados
seconds=float(sys.argv[1])
c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'rados_mon_op_timeout':'10','rados_osd_op_timeout':'10'})
c.connect()
calls=0
try:
    started=time.monotonic()
    while time.monotonic()-started<seconds:
        r,_,err=c.mon_command(json.dumps({'prefix':'fsid','format':'json'}),b'')
        assert r==0,('mon',r,err)
        r,_,err=c.mgr_command(json.dumps({'prefix':'pg stat','format':'json'}),b'')
        assert r==0,('mgr',r,err)
        calls+=1
        time.sleep(0.5)
    print(json.dumps({'calls':calls,'seconds':time.monotonic()-started}))
finally:
    c.shutdown()
`

// checkProfileRenewal runs profileRenewalClient in client for seconds, which
// should exceed the ticket lifetimes, and checks that every call of the one
// session succeeded until the end.
func checkProfileRenewal(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, seconds int) {
	t.Helper()
	result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv: []string{"python3", "-c", profileRenewalClient, fmt.Sprint(seconds)}, Timeout: time.Duration(seconds+60) * time.Second,
		MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "ticket renewal session",
	})
	var session struct {
		Calls   int     `json:"calls"`
		Seconds float64 `json:"seconds"`
	}
	if err != nil || result.ExitCode != 0 || json.Unmarshal(result.Stdout, &session) != nil || session.Calls < 2 || session.Seconds < float64(seconds) {
		t.Fatalf("renewal session: %v, exit %d: %s %s", err, result.ExitCode, result.Stdout, result.Stderr)
	}
	t.Logf("one librados session made %d rounds of MON and MGR calls over %.1f seconds", session.Calls, session.Seconds)
}

// testMessengerClientSecureProfile starts the default secure profile: three
// MONs, two MGRs, no OSD, every connection secure on Messenger v2 only,
// AES256K keys and 12-second tickets. It checks the native result, creates
// the suite's identities and runs the daemon controls.
func testMessengerClientSecureProfile(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	image, roles := integrationImages(t)
	base := append(slices.Clone(roles), ceph.WithMonitorCount(3), ceph.WithManagerCount(2), ceph.WithNoInitialOSDs(),
		ceph.WithMessengerMode(ceph.MessengerV2Secure))
	auth := profileAuth(t, ctx, image, base, ceph.AuthConfig{KeyType: ceph.CipherAES256K, MonTicketTTL: 12 * time.Second, ServiceTicketTTL: 12 * time.Second})
	cluster, admin := newServiceClusterWithOptions(t, image, append(base, ceph.WithAuth(auth))...)
	waitProfileTopology(t, ctx, cluster, 3, 1)
	checkMessengerV2Only(t, ctx, cluster)
	checkProfileCiphers(t, ctx, cluster, auth)
	checkProfileTickets(t, ctx, cluster, 12, 12)
	if osds, err := cluster.DaemonAddresses(ctx); err != nil || len(osds.OSDs) != 0 || len(cluster.OSDs()) != 0 {
		t.Fatalf("the profile has OSDs: %+v, %v", osds.OSDs, err)
	}
	daemons := profileDaemons(cluster)
	for _, option := range []string{"ms_cluster_mode", "ms_service_mode", "ms_client_mode", "ms_mon_cluster_mode", "ms_mon_service_mode", "ms_mon_client_mode"} {
		waitEffectiveMode(t, ctx, cluster, option, "secure", daemons...)
	}
	identities := createProfileIdentities(t, ctx, cluster, auth.KeyType)
	checkProfileClientModes(t, ctx, cluster, "mon.a", "secure")
	// One session outlives two lifetimes of each ticket.
	checkProfileRenewal(t, ctx, cluster, admin, 30)
	exerciseProfileControls(t, ctx, cluster, identities, 12*time.Second)
}

// testMessengerClientModeRejection starts three MONs and two MGRs on
// Messenger v2 only, with one daemon type that accepts only CRC and the other
// only secure, and checks native clients that offer secure, CRC, or both.
// After the MGRs-CRC profile it makes the MGRs secure, restarts both and
// checks that the change survived.
func testMessengerClientModeRejection(t *testing.T, monitors, managers []ceph.MessengerConnectionMode, keyType ceph.CephxCipher) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	image, roles := integrationImages(t)
	base := append(slices.Clone(roles), ceph.WithMonitorCount(3), ceph.WithManagerCount(2), ceph.WithNoInitialOSDs(),
		ceph.WithMessengerMode(ceph.MessengerV2Only), ceph.WithMessengerPolicy(ceph.MessengerPolicy{Monitors: monitors, Managers: managers}))
	auth := profileAuth(t, ctx, image, base, ceph.AuthConfig{KeyType: keyType, MonTicketTTL: 12 * time.Second, ServiceTicketTTL: 12 * time.Second})
	cluster, _ := newServiceClusterWithOptions(t, image, append(base, ceph.WithAuth(auth))...)
	waitProfileTopology(t, ctx, cluster, 3, 1)
	checkMessengerV2Only(t, ctx, cluster)
	checkProfileCiphers(t, ctx, cluster, auth)
	mgrs := []string{}
	for _, mgr := range cluster.Managers() {
		mgrs = append(mgrs, "mgr."+mgr.DaemonName)
	}
	waitEffectiveMode(t, ctx, cluster, "ms_mon_service_mode", strings.Join(modeNames(monitors), " "), "mon.a", "mon.b", "mon.c")
	waitEffectiveMode(t, ctx, cluster, "ms_mon_service_mode", strings.Join(modeNames(managers), " "), mgrs...)

	clientWith := func(name string, modes []ceph.MessengerConnectionMode) policyClient {
		return startPolicyClient(t, ctx, cluster, name, cluster.WithClient(
			ceph.WithClientMessengerPolicy(ceph.ClientMessengerPolicy{MonitorsAndManagers: modes, OtherDaemons: modes})))
	}
	secure, crc, ordered := clientWith("secure", modeSecure), clientWith("crc", modeCRC), clientWith("secure, then crc", modeSecureCRC)
	expectMode := func(step string, client policyClient, accepts []ceph.MessengerConnectionMode, offered []ceph.MessengerConnectionMode, daemon string, args ...string) {
		t.Helper()
		want := "accepted"
		if !slices.ContainsFunc(offered, func(mode ceph.MessengerConnectionMode) bool { return slices.Contains(accepts, mode) }) {
			want = "rejected"
			if daemon == "mgr" {
				want = "unanswered"
			}
		}
		client.expect(t, ctx, step, want, args...)
	}
	check := func(step string, monitors, managers []ceph.MessengerConnectionMode) {
		t.Helper()
		for _, client := range []struct {
			client  policyClient
			offered []ceph.MessengerConnectionMode
		}{{secure, modeSecure}, {crc, modeCRC}, {ordered, modeSecureCRC}} {
			expectMode(step, client.client, monitors, client.offered, "mon", "mon", "stat")
			// A client reaches a MGR only through a MON that accepted it.
			if slices.ContainsFunc(client.offered, func(mode ceph.MessengerConnectionMode) bool { return slices.Contains(monitors, mode) }) {
				expectMode(step, client.client, managers, client.offered, "mgr", "pg", "stat")
			}
		}
	}
	check("bootstrap policy", monitors, managers)

	if !slices.Equal(managers, modeCRC) {
		return
	}
	// Make the MGRs secure, restart both and check that they kept the mode.
	override, err := cluster.OverrideMessengerPolicy(ctx, ceph.MessengerPolicy{Managers: modeSecure})
	if override != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := override.Restore(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	waitEffectiveMode(t, ctx, cluster, "ms_mon_service_mode", "secure", mgrs...)
	stop := 10 * time.Second
	for _, mgr := range cluster.Managers() {
		if err := mgr.Stop(ctx, &stop); err != nil {
			t.Fatal(err)
		}
		if err := mgr.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	waitProfileTopology(t, ctx, cluster, 3, 1)
	waitEffectiveMode(t, ctx, cluster, "ms_mon_service_mode", "secure", mgrs...)
	check("MGRs secure after their restart", monitors, modeSecure)
	if err := override.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	waitEffectiveMode(t, ctx, cluster, "ms_mon_service_mode", "crc", mgrs...)
	check("restored", monitors, managers)
}

func modeNames(modes []ceph.MessengerConnectionMode) []string {
	names := make([]string, 0, len(modes))
	for _, mode := range modes {
		names = append(names, string(mode))
	}
	return names
}

// testMessengerClientTicketProfile starts three MONs and two MGRs with the
// ticket lifetimes and MON timing of a profile, checks what the daemons and a
// client container run with, and sends a MON and a MGR command from the
// client.
func testMessengerClientTicketProfile(t *testing.T, monTTL, serviceTTL time.Duration, file string, options map[string]float64) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	image, roles := integrationImages(t)
	base := append(slices.Clone(roles), ceph.WithMonitorCount(3), ceph.WithManagerCount(2), ceph.WithNoInitialOSDs(),
		ceph.WithMessengerMode(ceph.MessengerV2Secure), ceph.WithConfigFile(profileConfigFile(t, file)))
	auth := profileAuth(t, ctx, image, base, ceph.AuthConfig{KeyType: ceph.CipherAES256K, MonTicketTTL: monTTL, ServiceTicketTTL: serviceTTL})
	cluster, client := newServiceClusterWithOptions(t, image, append(base, ceph.WithAuth(auth))...)
	waitProfileTopology(t, ctx, cluster, 3, 1)
	checkMessengerV2Only(t, ctx, cluster)
	checkProfileTickets(t, ctx, cluster, monTTL.Seconds(), serviceTTL.Seconds())
	for name, want := range options {
		for _, daemon := range []string{"mon.a", "mgr." + cluster.Managers()[0].DaemonName} {
			if got := runningOption(t, ctx, cluster, daemon, name); got != want {
				t.Fatalf("%s %s = %v, want %v", daemon, name, got, want)
			}
		}
		out := strings.TrimSpace(execOutput(t, ctx, client, "ceph-conf", "-c", "/etc/ceph/ceph.conf", "--name", "client.admin", "--show-config-value", name))
		if got, err := strconv.ParseFloat(out, 64); err != nil || got != want {
			t.Fatalf("client %s = %q, want %v: %v", name, out, want, err)
		}
	}
	execCommand(t, ctx, client, "ceph", "--connect-timeout", "10", "fsid")
	execCommand(t, ctx, client, "ceph", "--connect-timeout", "10", "pg", "stat")
}

// testMessengerClientDelayedManagers starts three MONs with no MGR, then two
// MGRs one after the other, as a suite that tests manager availability does.
func testMessengerClientDelayedManagers(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	image, roles := integrationImages(t)
	base := append(slices.Clone(roles), ceph.WithMonitorCount(3), ceph.WithNoInitialManagers(), ceph.WithNoInitialOSDs(),
		ceph.WithMessengerMode(ceph.MessengerV2Secure))
	auth := profileAuth(t, ctx, image, base, ceph.AuthConfig{KeyType: ceph.CipherAES256K, MonTicketTTL: 12 * time.Second, ServiceTicketTTL: 12 * time.Second})
	cluster, _ := newServiceClusterWithOptions(t, image, append(base, ceph.WithAuth(auth))...)
	waitProfileTopology(t, ctx, cluster, 3, -1)
	status, err := cluster.ManagerStatus(ctx)
	if err != nil || status.Available || status.ActiveGID != 0 || len(status.Standbys) != 0 || len(cluster.Managers()) != 0 {
		t.Fatalf("a MGR exists before any was started: %+v, %v", status, err)
	}
	if _, err := cluster.AddManager(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	waitProfileTopology(t, ctx, cluster, 3, 0)
	if _, err := cluster.AddManager(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	waitProfileTopology(t, ctx, cluster, 3, 1)
	checkMessengerV2Only(t, ctx, cluster)
	checkProfileCiphers(t, ctx, cluster, auth)
	status, err = cluster.ManagerStatus(ctx)
	if err != nil || status.ActiveName != "a" || status.Standbys[0].Name != "b" {
		t.Fatalf("managers after the delayed starts: %+v, %v", status, err)
	}
	t.Log("three MONs ran without a MGR; mgr.a became active and mgr.b its standby when started later")
}

// testMessengerClientCRCProfile starts three MONs, two MGRs and a MemStore OSD
// on Messenger v2 only, with every daemon accepting only CRC from clients. The
// OSD gets the legacy CRUSH map and a one-PG, size-one pool without
// hashpspool, and a CRC-only client writes, stats and reads an object.
func testMessengerClientCRCProfile(t *testing.T, auth ceph.AuthConfig, extra ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	image, roles := integrationImages(t)
	base := append(slices.Clone(roles), ceph.WithMonitorCount(3), ceph.WithManagerCount(2), ceph.WithOSDCount(1),
		ceph.WithPoolDefaults(1, 1), ceph.WithOSDObjectStore(ceph.OSDMemStore), ceph.WithOSDBlockSize(256<<20), legacyPoolFlags(t),
		ceph.WithMessengerMode(ceph.MessengerV2Only), ceph.WithMessengerPolicy(ceph.MessengerPolicy{Monitors: modeCRC, Managers: modeCRC, OSDs: modeCRC}))
	base = append(base, extra...)
	auth = profileAuth(t, ctx, image, base, auth)
	cluster, _ := newServiceClusterWithOptions(t, image, append(base, ceph.WithAuth(auth))...)
	waitProfileTopology(t, ctx, cluster, 3, 1)
	checkMessengerV2Only(t, ctx, cluster)
	checkProfileCiphers(t, ctx, cluster, auth)
	monsAndMgrs := slices.DeleteFunc(profileDaemons(cluster), func(daemon string) bool { return strings.HasPrefix(daemon, "osd.") })
	waitEffectiveMode(t, ctx, cluster, "ms_mon_service_mode", "crc", monsAndMgrs...)
	waitEffectiveMode(t, ctx, cluster, "ms_service_mode", "crc", "osd.0")
	if got := authEntityKeyType(t, ctx, cluster, "osd.0"); got != auth.KeyType {
		t.Fatalf("osd.0 has a %s key, want %s", got, auth.KeyType)
	}

	installCRUSHMap(t, ctx, cluster, legacyCRUSHMap)
	assertLegacyCRUSHMap(t, ctx, cluster)
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-msgr-profile", PGNum: 1, Replicas: 1, MinSize: 1, NoHashPSPool: true, Application: "rados"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	checkMemStoreLegacyPool(t, ctx, cluster, pool.Name)

	crc := startPolicyClient(t, ctx, cluster, "crc", cluster.WithClient(
		ceph.WithClientMessengerPolicy(ceph.ClientMessengerPolicy{MonitorsAndManagers: modeCRC, OtherDaemons: modeCRC})))
	secure := startPolicyClient(t, ctx, cluster, "secure", cluster.WithClient(
		ceph.WithClientMessengerPolicy(ceph.ClientMessengerPolicy{MonitorsAndManagers: modeSecure, OtherDaemons: modeSecure})))
	crc.expect(t, ctx, "crc only", "accepted", "mon", "stat")
	crc.expect(t, ctx, "crc only", "accepted", "pg", "stat")
	crc.expect(t, ctx, "crc only", "accepted", "tell", "osd.0", "version")
	secure.expect(t, ctx, "crc only", "rejected", "mon", "stat")
	execCommand(t, ctx, crc.ctr, "sh", "-c", "head -c 10513 /dev/urandom > /tmp/object && rados -p "+pool.Name+" put profile /tmp/object && rados -p "+pool.Name+" stat profile && rados -p "+pool.Name+" get profile /tmp/read")
	assertContainerFilesEqual(t, ctx, crc.ctr, "/tmp/object", "/tmp/read")
	checkProfileClientModes(t, ctx, cluster, "mon.a", "crc")
	if cluster.UsesHostNetwork() {
		checkHostProcessReachesDaemons(t, ctx, cluster)
	}
	t.Logf("CRC profile: v2 only, %s keys, legacy CRUSH and pool %s without hashpspool on a MemStore OSD; a CRC-only client wrote and read an object, a secure-only client was refused", auth.KeyType, pool.Name)
}

// testMessengerClientAddressProfile starts three MONs and two MGRs on
// Messenger v2 only with IPv6 or IPv4-mapped IPv6 addresses, and checks the
// addresses and a client.
func testMessengerClientAddressProfile(t *testing.T, family ceph.AddressFamily) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	image, roles := integrationImages(t)
	base := append(slices.Clone(roles), ceph.WithMonitorCount(3), ceph.WithManagerCount(2), ceph.WithNoInitialOSDs(),
		ceph.WithMessengerMode(ceph.MessengerV2Secure), ceph.WithAddressFamily(family))
	auth := profileAuth(t, ctx, image, base, ceph.AuthConfig{KeyType: ceph.CipherAES256K, MonTicketTTL: 12 * time.Second, ServiceTicketTTL: 12 * time.Second})
	cluster, client := newServiceClusterWithOptions(t, image, append(base, ceph.WithAuth(auth))...)
	waitProfileTopology(t, ctx, cluster, 3, 1)
	checkMessengerV2Only(t, ctx, cluster)
	checkProfileAddressFamily(t, ctx, cluster, family)
	checkProfileCiphers(t, ctx, cluster, auth)
	execCommand(t, ctx, client, "ceph", "--connect-timeout", "10", "pg", "stat")
}

// testMessengerClientWithoutKeyTypes runs the secure profile on an image
// without key types: AES256K is refused before any daemon starts, and the
// AES profile works.
func testMessengerClientWithoutKeyTypes(t *testing.T, image string) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	base := []testcontainers.ContainerCustomizer{ceph.WithMonitorCount(3), ceph.WithManagerCount(2), ceph.WithNoInitialOSDs(),
		ceph.WithMessengerMode(ceph.MessengerV2Secure)}
	if imageHasKeyTypes(t, ctx, image) {
		t.Fatalf("%s has ceph-authtool --key-type", image)
	}
	auth := profileAuth(t, ctx, image, base, ceph.AuthConfig{KeyType: ceph.CipherAES256K, MonTicketTTL: 12 * time.Second, ServiceTicketTTL: 12 * time.Second})
	cluster, _ := newServiceClusterWithOptions(t, image, append(base, ceph.WithAuth(auth))...)
	waitProfileTopology(t, ctx, cluster, 3, 1)
	checkMessengerV2Only(t, ctx, cluster)
	checkProfileCiphers(t, ctx, cluster, auth)
	checkProfileTickets(t, ctx, cluster, 12, 12)
	createProfileIdentities(t, ctx, cluster, ceph.CipherAES)
	t.Logf("Ceph %s: v2-only secure profile with three MONs, two MGRs and AES keys", cluster.CephVersion())
}

// checkHostProcessReachesDaemons dials every address that the MON, MGR and
// OSD maps advertise from the test process itself, not from a container, and
// reads the Messenger v2 banner that the daemon sends first.
func checkHostProcessReachesDaemons(t *testing.T, ctx context.Context, cluster *ceph.Container) {
	t.Helper()
	native, err := cluster.DaemonAddresses(ctx)
	if err != nil || native.ActiveManager == nil || len(native.ActiveManager.Addrs) == 0 {
		t.Fatalf("daemon addresses: %+v, %v", native, err)
	}
	endpoints := map[string]string{}
	add := func(name string, vector []ceph.EntityAddress) {
		for _, entry := range vector {
			endpoints[entry.Addr] = name
		}
	}
	add("mgr."+native.ActiveManager.Name, native.ActiveManager.Addrs)
	for _, mon := range native.Monitors {
		add("mon."+mon.Name, mon.Addrs)
	}
	for _, osd := range native.OSDs {
		add(fmt.Sprintf("osd.%d", osd.ID), slices.Concat(osd.Public, osd.Cluster, osd.HeartbeatFront, osd.HeartbeatBack))
	}
	var dialer net.Dialer
	for address, name := range endpoints {
		dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		conn, err := dialer.DialContext(dialCtx, "tcp", address)
		cancel()
		if err != nil {
			t.Fatalf("the test process cannot reach %s at %s: %v", name, address, err)
		}
		banner := make([]byte, 8)
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err = io.ReadFull(conn, banner)
		_ = conn.Close()
		if err != nil || string(banner) != "ceph v2\n" {
			t.Fatalf("%s at %s sent %q: %v", name, address, banner, err)
		}
	}
	t.Logf("the test process read the Messenger v2 banner from %d advertised daemon addresses", len(endpoints))
}

// profileDaemons names every owned MON, MGR and OSD.
func profileDaemons(cluster *ceph.Container) []string {
	var daemons []string
	for _, mon := range cluster.Monitors() {
		daemons = append(daemons, "mon."+mon.DaemonName)
	}
	for _, mgr := range cluster.Managers() {
		daemons = append(daemons, "mgr."+mgr.DaemonName)
	}
	for _, osd := range cluster.OSDs() {
		daemons = append(daemons, fmt.Sprintf("osd.%d", osd.ID))
	}
	return daemons
}
