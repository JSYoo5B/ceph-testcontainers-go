//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

var (
	modeCRC       = []ceph.MessengerConnectionMode{ceph.MessengerCRC}
	modeSecure    = []ceph.MessengerConnectionMode{ceph.MessengerSecure}
	modeSecureCRC = []ceph.MessengerConnectionMode{ceph.MessengerSecure, ceph.MessengerCRC}
)

// policyRejectTimeout bounds a CLI call that MGRs and OSDs reject: the native
// client retries the connection instead of failing.
const policyRejectTimeout = "15"

// policyClient is a native client container with its own Messenger policy.
type policyClient struct {
	name string
	ctr  testcontainers.Container
	args []string
}

func startPolicyClient(t *testing.T, ctx context.Context, cluster *ceph.Container, name string, attach testcontainers.CustomizeRequestOption, args ...string) policyClient {
	t.Helper()
	ctr, err := testcontainers.Run(ctx, nativeClientImage(t, cluster.ControlImage()), attach, ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatal(err)
	}
	return policyClient{name: name, ctr: ctr, args: args}
}

// run runs the Ceph CLI under timeout and returns its exit code and output;
// 124 means that the call did not finish in time.
func (client policyClient) run(t *testing.T, ctx context.Context, args ...string) (int, string) {
	t.Helper()
	command := append([]string{"timeout", policyRejectTimeout, "ceph", "--connect-timeout", "10"}, client.args...)
	code, reader, err := client.ctr.Exec(ctx, append(command, args...), tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, _ := io.ReadAll(reader)
	return code, string(output)
}

// expect checks how the daemon behind args treats the client's modes:
// accepted, rejected by a MON with an authentication error, or left
// unanswered by a MGR or an OSD.
func (client policyClient) expect(t *testing.T, ctx context.Context, step, want string, args ...string) {
	t.Helper()
	code, output := client.run(t, ctx, args...)
	if policyOutcome(code, output) != want {
		t.Fatalf("%s: %s %v: want %s, got exit %d: %s", step, client.name, args, want, code, output)
	}
}

// policyOutcome classifies a policy client's CLI call by what the CLI itself
// reports: "accepted" when it succeeded, "unanswered" when timeout stopped
// it, and "rejected" when the MON refused its authentication, so that the
// connect failed with EACCES.
//
// The MON client's handle_auth_bad_method line is no evidence either way.
// Ceph writes it from an asynchronous log thread, and after a failed connect
// the CLI leaves with os._exit without shutting librados down, so the line
// is lost whenever the process exits before that thread writes it.
func policyOutcome(code int, output string) string {
	switch {
	case code == 0:
		return "accepted"
	case code == 124:
		return "unanswered"
	case code == 13 && strings.Contains(output, "RADOS permission denied"):
		return "rejected"
	}
	return "unexpected"
}

// testMessengerPolicy starts a cluster whose MONs accept only secure, MGRs
// only CRC and OSDs only secure connections from clients, and checks each
// daemon type with native clients that offer different modes, before, during
// and after a temporary policy.
func testMessengerPolicy(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, _ := newServiceCluster(t, append([]testcontainers.ContainerCustomizer{ceph.WithOSDCount(1), ceph.WithPoolDefaults(1, 1),
		ceph.WithMessengerPolicy(ceph.MessengerPolicy{Monitors: modeSecure, Managers: modeCRC, OSDs: modeSecure})}, opts...)...)
	if cluster.MessengerMode() == ceph.MessengerV2Only {
		checkMessengerV2Only(t, ctx, cluster)
	}

	for _, entry := range []struct{ who, name, want string }{
		{"mon", "ms_mon_service_mode", "secure"},
		{"mgr", "ms_mon_service_mode", "crc"},
		{"osd", "ms_service_mode", "secure"},
		{"mon.a", "ms_mon_service_mode", "secure"},
		{"mgr.a", "ms_mon_service_mode", "crc"},
		{"osd.0", "ms_service_mode", "secure"},
	} {
		// config get reads the central entry; config show the running daemon.
		verb := "get"
		if strings.Contains(entry.who, ".") {
			verb = "show"
		}
		out, err := cluster.Ceph(ctx, "config", verb, entry.who, entry.name)
		if err != nil || strings.TrimSpace(string(out)) != entry.want {
			t.Fatalf("%s %s = %q, want %s: %v", entry.who, entry.name, out, entry.want, err)
		}
	}
	// The fixture's own CLI offers both modes and keeps working.
	for _, args := range [][]string{{"health"}, {"crash", "ls"}, {"tell", "osd.0", "version"}} {
		if _, err := cluster.Ceph(ctx, args...); err != nil {
			t.Fatalf("fixture CLI %v under the policy: %v", args, err)
		}
	}

	reader, err := cluster.CreateClient(ctx, "policy-reader", ceph.ClientCaps{Mon: "allow r", MGR: "allow r", OSD: "allow r"})
	if err != nil {
		t.Fatal(err)
	}
	monCRC := startPolicyClient(t, ctx, cluster, "MON and MGR crc", cluster.WithClientIdentity(reader,
		ceph.WithClientMessengerPolicy(ceph.ClientMessengerPolicy{MonitorsAndManagers: modeCRC})), "--id", reader.User())
	monSecure := startPolicyClient(t, ctx, cluster, "MON and MGR secure", cluster.WithClient(
		ceph.WithClientMessengerPolicy(ceph.ClientMessengerPolicy{MonitorsAndManagers: modeSecure})))
	osdCRC := startPolicyClient(t, ctx, cluster, "OSD crc", cluster.WithClient(
		ceph.WithClientMessengerPolicy(ceph.ClientMessengerPolicy{MonitorsAndManagers: modeSecureCRC, OtherDaemons: modeCRC})))
	osdSecure := startPolicyClient(t, ctx, cluster, "OSD secure", cluster.WithClient(
		ceph.WithClientMessengerPolicy(ceph.ClientMessengerPolicy{MonitorsAndManagers: modeSecureCRC, OtherDaemons: modeSecure})))

	monCRC.expect(t, ctx, "bootstrap", "rejected", "mon", "stat")
	monSecure.expect(t, ctx, "bootstrap", "accepted", "mon", "stat")
	monSecure.expect(t, ctx, "bootstrap", "unanswered", "crash", "ls")
	osdCRC.expect(t, ctx, "bootstrap", "accepted", "crash", "ls")
	osdCRC.expect(t, ctx, "bootstrap", "unanswered", "tell", "osd.0", "version")
	osdSecure.expect(t, ctx, "bootstrap", "accepted", "tell", "osd.0", "version")

	// A temporary policy applies to new connections without a restart, and
	// Restore brings back the bootstrap policy.
	override, err := cluster.OverrideMessengerPolicy(ctx, ceph.MessengerPolicy{Monitors: modeCRC, OSDs: modeCRC})
	if err != nil {
		if override != nil {
			_ = override.Restore(ctx)
		}
		t.Fatal(err)
	}
	waitEffectiveMode(t, ctx, cluster, "ms_mon_service_mode", "crc", "mon.a")
	waitEffectiveMode(t, ctx, cluster, "ms_service_mode", "crc", "osd.0")
	monCRC.expect(t, ctx, "temporary", "accepted", "mon", "stat")
	monSecure.expect(t, ctx, "temporary", "rejected", "mon", "stat")
	osdCRC.expect(t, ctx, "temporary", "accepted", "tell", "osd.0", "version")
	osdSecure.expect(t, ctx, "temporary", "unanswered", "tell", "osd.0", "version")
	if err := override.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := override.Restore(ctx); err != nil {
		t.Fatalf("a repeated restore failed: %v", err)
	}
	waitEffectiveMode(t, ctx, cluster, "ms_mon_service_mode", "secure", "mon.a")
	waitEffectiveMode(t, ctx, cluster, "ms_service_mode", "secure", "osd.0")
	monCRC.expect(t, ctx, "restored", "rejected", "mon", "stat")
	monSecure.expect(t, ctx, "restored", "accepted", "mon", "stat")
	osdSecure.expect(t, ctx, "restored", "accepted", "tell", "osd.0", "version")

	// One daemon's own entry takes precedence over its type's policy.
	single, err := cluster.OverrideConfig(ctx, ceph.ConfigSetting{Section: "osd.0", Name: "ms_service_mode", Value: "crc"})
	if err != nil {
		if single != nil {
			_ = single.Restore(ctx)
		}
		t.Fatal(err)
	}
	waitEffectiveMode(t, ctx, cluster, "ms_service_mode", "crc", "osd.0")
	osdCRC.expect(t, ctx, "osd.0 only", "accepted", "tell", "osd.0", "version")
	if err := single.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	waitEffectiveMode(t, ctx, cluster, "ms_service_mode", "secure", "osd.0")
	osdSecure.expect(t, ctx, "osd.0 restored", "accepted", "tell", "osd.0", "version")
	if out, err := cluster.Ceph(ctx, "health"); err != nil || strings.Contains(string(out), "HEALTH_ERR") {
		t.Fatalf("cluster health after the policy changes: %s %v", out, err)
	}
	t.Log("MONs, MGRs and OSDs accepted only their policy's modes from native clients, before, during and after a temporary policy, while the fixture CLI kept working")
}

// testClientPolicyOverridesSecureOnly checks that a native client's policy
// overrides the global modes of MessengerV2Secure: a CRC-only client is
// rejected instead of connecting with the global secure mode.
func testClientPolicyOverridesSecureOnly(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, _ := newServiceCluster(t, append([]testcontainers.ContainerCustomizer{ceph.WithOSDCount(1), ceph.WithPoolDefaults(1, 1),
		ceph.WithMessengerMode(ceph.MessengerV2Secure)}, opts...)...)
	crc := startPolicyClient(t, ctx, cluster, "crc", cluster.WithClient(
		ceph.WithClientMessengerPolicy(ceph.ClientMessengerPolicy{MonitorsAndManagers: modeCRC})))
	inherited := startPolicyClient(t, ctx, cluster, "inherited", cluster.WithClient())
	crc.expect(t, ctx, "secure-only", "rejected", "mon", "stat")
	inherited.expect(t, ctx, "secure-only", "accepted", "mon", "stat")
	if _, err := cluster.OverrideMessengerPolicy(ctx, ceph.MessengerPolicy{Monitors: modeCRC}); err == nil {
		t.Fatal("a temporary policy was accepted with MessengerV2Secure")
	}
	t.Log("MessengerV2Secure: a CRC-only client policy is rejected by the MON; a client without a policy connects")
}

// checkMessengerV2Only checks that nothing in the cluster speaks or
// advertises Messenger v1: the bootstrap addresses, the connection
// configuration, every map address, each daemon's bind options, the MON's
// published ports and its listening sockets.
func checkMessengerV2Only(t *testing.T, ctx context.Context, cluster *ceph.Container) {
	t.Helper()
	bootstrap, err := cluster.MonitorBootstrapAddresses(ctx)
	if err != nil || !strings.Contains(bootstrap, "v2:") || strings.Contains(bootstrap, "v1:") {
		t.Fatalf("MON bootstrap addresses are not v2-only: %q, %v", bootstrap, err)
	}
	cc, err := cluster.ConnectionConfig(ctx)
	config := cc.Conf
	if err != nil || strings.Contains(string(config), "v1:") {
		t.Fatalf("the connection configuration names a v1 address: %v", err)
	}
	native, err := cluster.DaemonAddresses(ctx)
	if err != nil || native.ActiveManager == nil {
		t.Fatalf("daemon addresses: %+v, %v", native, err)
	}
	vectors := map[string][]ceph.EntityAddress{"mgr." + native.ActiveManager.Name: native.ActiveManager.Addrs}
	monPorts := map[string]uint16{}
	for _, mon := range native.Monitors {
		vectors["mon."+mon.Name] = mon.Addrs
		if len(mon.Addrs) > 0 {
			if address, err := netip.ParseAddrPort(mon.Addrs[0].Addr); err == nil {
				monPorts[mon.Name] = address.Port()
			}
		}
	}
	for _, osd := range native.OSDs {
		vectors[fmt.Sprintf("osd.%d", osd.ID)] = slices.Concat(osd.Public, osd.Cluster, osd.HeartbeatFront, osd.HeartbeatBack)
	}
	for name, vector := range vectors {
		if len(vector) == 0 || slices.ContainsFunc(vector, func(entry ceph.EntityAddress) bool { return entry.Type != "v2" }) {
			t.Fatalf("%s advertises %+v, not only v2", name, vector)
		}
	}
	// Every owned daemon, standby MGRs included, through its admin socket.
	daemons := []string{}
	for _, mon := range clusteraccess.Monitors(t, cluster) {
		daemons = append(daemons, "mon."+mon.DaemonName)
	}
	for _, mgr := range clusteraccess.Managers(t, cluster) {
		daemons = append(daemons, "mgr."+mgr.DaemonName)
	}
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		daemons = append(daemons, fmt.Sprintf("osd.%d", osd.ID))
	}
	for _, daemon := range daemons {
		for option, want := range map[string]string{"ms_bind_msgr1": "false", "ms_bind_msgr2": "true"} {
			out, err := adminSocket(t, ctx, cluster, daemon, "config", "get", option)
			var value map[string]string
			if err != nil || json.Unmarshal(out, &value) != nil || value[option] != want {
				t.Fatalf("%s %s = %s, want %s: %v", daemon, option, out, want, err)
			}
		}
	}
	if !cluster.UsesHostNetwork() {
		if port, err := cluster.MappedPort(ctx, "6789/tcp"); err == nil {
			t.Fatalf("the MON publishes the v1 port as %s", port)
		}
	}
	for _, mon := range clusteraccess.Monitors(t, cluster) {
		out := execOutput(t, ctx, mon.Container, "python3", "-c", `import json
listening=set()
for table in ('/proc/net/tcp','/proc/net/tcp6'):
    with open(table) as lines:
        next(lines)
        for line in lines:
            fields=line.split()
            if fields[3]=='0A':
                listening.add(int(fields[1].rsplit(':',1)[1],16))
print(json.dumps(sorted(listening)))`)
		var ports []int
		if err := json.Unmarshal([]byte(out), &ports); err != nil {
			t.Fatalf("listening ports of mon.%s: %s", mon.DaemonName, out)
		}
		advertised, ok := monPorts[mon.DaemonName]
		if !ok || !slices.Contains(ports, int(advertised)) {
			t.Fatalf("mon.%s does not listen on its advertised v2 port %d: %v", mon.DaemonName, advertised, ports)
		}
		// In host mode the MON shares the host's sockets, where 6789 can
		// belong to anything else.
		if !cluster.UsesHostNetwork() && slices.Contains(ports, 6789) {
			t.Fatalf("mon.%s listens on the v1 port: %v", mon.DaemonName, ports)
		}
	}
	t.Logf("v2 only: bootstrap %s; %d daemons bind only v2, every map address is v2, and each MON listens on its v2 port and not on 6789", bootstrap, len(daemons))
}

// imageHasKeyTypes reports whether image's ceph-authtool generates keys of
// other types than AES, the capability that AES256K keys need.
func imageHasKeyTypes(t *testing.T, ctx context.Context, image string) bool {
	t.Helper()
	ctr, err := testcontainers.Run(ctx, image, ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatal(err)
	}
	_, reader, err := ctr.Exec(ctx, []string{"ceph-authtool", "--help"}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	usage, _ := io.ReadAll(reader)
	if err := ctr.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	return strings.Contains(string(usage), "--key-type")
}

// messengerEstablishedClient keeps one MON session open while the test
// changes the policy, then uses it again: it prints the result of a MON
// command made before and after.
const messengerEstablishedClient = `import json,os,sys,time,rados
ready,proceed=sys.argv[1],sys.argv[2]
c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'rados_mon_op_timeout':'10'})
c.connect()
try:
    before,_,_=c.mon_command(json.dumps({'prefix':'fsid','format':'json'}),b'')
    open(ready,'w').close()
    deadline=time.monotonic()+120
    while not os.path.exists(proceed):
        assert time.monotonic()<deadline,'never told to proceed'
        time.sleep(0.2)
    after,_,_=c.mon_command(json.dumps({'prefix':'fsid','format':'json'}),b'')
    print(json.dumps({'before':before,'after':after}))
finally:
    c.shutdown()
`

// messengerClientModes lists the modes of the READY Messenger v2
// connections from clients that daemon reports with messenger dump, or nil
// when the daemon has no messenger dump, as Squid.
const messengerClientModes = `import json,subprocess,sys
daemon=sys.argv[1]
def tell(*args):
    result=subprocess.run(['ceph','--connect-timeout','10','tell',daemon,'messenger','dump',*args,'--format','json'],capture_output=True,text=True,timeout=30)
    return result.returncode,result.stdout,result.stderr
# Ceph 19 has no messenger dump; the daemon's own command list decides.
listing=subprocess.run(['ceph','--connect-timeout','10','tell',daemon,'help'],capture_output=True,text=True,timeout=30)
assert listing.returncode==0,listing.stderr[-300:]
if 'messenger dump' not in listing.stdout+listing.stderr:
    print(json.dumps(None)); sys.exit(0)
code,out,err=tell()
assert code==0,err[-300:]
catalog=json.loads(out); catalog=catalog.get('status',catalog)
modes=[]
for name in catalog['messengers']:
    code,out,err=tell(name)
    assert code==0,(name,err[-300:])
    record=json.loads(out); record=record.get('status',record)
    messenger=record['messenger']
    for connection in [item['async_connection'] for item in messenger['connections']]+messenger.get('anon_conns',[]):
        protocol=connection['protocol']
        if not connection['status']['connected'] or 'v2' not in protocol or protocol['v2']['state']!='READY': continue
        if connection['peer']['type']=='client':
            modes.append(protocol['v2']['con_mode'])
print(json.dumps(modes))
`

// waitEffectiveMode waits until each daemon runs with option set to want.
// OverrideConfig confirms the central database; a daemon applies the change
// when the MON delivers it.
func waitEffectiveMode(t *testing.T, ctx context.Context, cluster *ceph.Container, option, want string, daemons ...string) {
	t.Helper()
	for _, daemon := range daemons {
		for deadline := time.Now().Add(time.Minute); ; {
			out, err := adminSocket(t, ctx, cluster, daemon, "config", "get", option)
			var value map[string]string
			if err == nil && json.Unmarshal(out, &value) == nil && value[option] == want {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s %s = %s, want %s: %v", daemon, option, out, want, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
}

// testMessengerV2OnlyCRC starts a v2-only cluster whose MONs, MGRs and OSDs
// accept only CRC from clients, with AES256K keys when the image can make
// them, and checks the native protocols and modes through policy changes
// and daemon additions.
func testMessengerV2OnlyCRC(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	image, roles := integrationImages(t)
	keyType := ceph.CipherAES
	if imageHasKeyTypes(t, ctx, image) {
		keyType = ceph.CipherAES256K
	}
	cluster, _ := newServiceClusterWithOptions(t, image, append(roles, ceph.WithOSDCount(1), ceph.WithPoolDefaults(1, 1),
		ceph.WithMessengerMode(ceph.MessengerV2Only), ceph.WithAuth(ceph.AuthConfig{KeyType: keyType}),
		ceph.WithMessengerPolicy(ceph.MessengerPolicy{Monitors: modeCRC, Managers: modeCRC, OSDs: modeCRC}))...)
	if cluster.MessengerMode() != ceph.MessengerV2Only {
		t.Fatal("the cluster reports another Messenger mode")
	}
	checkMessengerV2Only(t, ctx, cluster)
	for _, daemon := range []string{"mon.a", "mgr.a", "osd.0"} {
		name := "ms_mon_service_mode"
		if strings.HasPrefix(daemon, "osd") {
			name = "ms_service_mode"
		}
		if out, err := cluster.Ceph(ctx, "config", "show", daemon, name); err != nil || strings.TrimSpace(string(out)) != "crc" {
			t.Fatalf("%s %s = %q, want crc: %v", daemon, name, out, err)
		}
	}

	crc := startPolicyClient(t, ctx, cluster, "crc", cluster.WithClient(
		ceph.WithClientMessengerPolicy(ceph.ClientMessengerPolicy{MonitorsAndManagers: modeCRC, OtherDaemons: modeCRC})))
	secure := startPolicyClient(t, ctx, cluster, "secure", cluster.WithClient(
		ceph.WithClientMessengerPolicy(ceph.ClientMessengerPolicy{MonitorsAndManagers: modeSecure, OtherDaemons: modeSecure})))
	ordered := startPolicyClient(t, ctx, cluster, "secure, then crc", cluster.WithClient(
		ceph.WithClientMessengerPolicy(ceph.ClientMessengerPolicy{MonitorsAndManagers: modeSecureCRC, OtherDaemons: modeSecureCRC})))
	for _, client := range []policyClient{crc, ordered} {
		client.expect(t, ctx, "crc only", "accepted", "mon", "stat")
		client.expect(t, ctx, "crc only", "accepted", "crash", "ls")
		client.expect(t, ctx, "crc only", "accepted", "tell", "osd.0", "version")
	}
	secure.expect(t, ctx, "crc only", "rejected", "mon", "stat")
	// The MON's client connections, the fixture CLI's among them, use CRC.
	var modes []string
	if err := json.Unmarshal([]byte(execOutput(t, ctx, toolkitContainer(t, cluster), "python3", "-c", messengerClientModes, "mon.a")), &modes); err != nil {
		t.Fatal(err)
	}
	switch {
	case modes == nil:
		t.Log("mon.a has no messenger dump; the accepted and refused clients and ms_mon_service_mode show the CRC policy")
	case len(modes) == 0 || slices.ContainsFunc(modes, func(mode string) bool { return mode != "crc" }):
		t.Fatalf("mon.a client connections use %v, not only crc", modes)
	default:
		t.Logf("mon.a client connection modes from messenger dump: %v", modes)
	}

	// A policy change applies to new sessions. A CRC session that the MON
	// accepted before keeps working.
	started := make(chan error, 1)
	var established ceph.NativeCommandResult
	go func() {
		var err error
		established, err = cluster.RunNativeCommand(ctx, crc.ctr, ceph.NativeCommand{
			Argv: []string{"python3", "-c", messengerEstablishedClient, "/tmp/established-ready", "/tmp/established-proceed"}, Timeout: 3 * time.Minute,
			MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "established crc session",
		})
		started <- err
	}()
	for deadline := time.Now().Add(time.Minute); ; {
		if code, _, err := crc.ctr.Exec(ctx, []string{"test", "-f", "/tmp/established-ready"}); err == nil && code == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the established CRC session did not connect")
		}
		time.Sleep(200 * time.Millisecond)
	}
	override, err := cluster.OverrideMessengerPolicy(ctx, ceph.MessengerPolicy{Monitors: modeSecure, Managers: modeSecure})
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
	waitEffectiveMode(t, ctx, cluster, "ms_mon_service_mode", "secure", "mon.a", "mgr.a")
	crc.expect(t, ctx, "secure", "rejected", "mon", "stat")
	secure.expect(t, ctx, "secure", "accepted", "mon", "stat")
	secure.expect(t, ctx, "secure", "accepted", "crash", "ls")
	execCommand(t, ctx, crc.ctr, "touch", "/tmp/established-proceed")
	if err := <-started; err != nil || established.ExitCode != 0 || !strings.Contains(string(established.Stdout), `"after": 0`) {
		t.Fatalf("the established CRC session after the change: %v, exit %d: %s %s", err, established.ExitCode, established.Stdout, established.Stderr)
	}
	if err := override.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	waitEffectiveMode(t, ctx, cluster, "ms_mon_service_mode", "crc", "mon.a", "mgr.a")
	crc.expect(t, ctx, "restored", "accepted", "mon", "stat")
	secure.expect(t, ctx, "restored", "rejected", "mon", "stat")

	// MONs that accept CRC with MGRs that accept only secure.
	mixed, err := cluster.OverrideMessengerPolicy(ctx, ceph.MessengerPolicy{Managers: modeSecure})
	if mixed != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := mixed.Restore(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	waitEffectiveMode(t, ctx, cluster, "ms_mon_service_mode", "secure", "mgr.a")
	crc.expect(t, ctx, "MON crc, MGR secure", "accepted", "mon", "stat")
	crc.expect(t, ctx, "MON crc, MGR secure", "unanswered", "crash", "ls")
	ordered.expect(t, ctx, "MON crc, MGR secure", "accepted", "crash", "ls")
	if err := mixed.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	waitEffectiveMode(t, ctx, cluster, "ms_mon_service_mode", "crc", "mgr.a")

	// Daemons added later listen on v2 only and take the central policy.
	if _, err := cluster.AddMonitor(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.AddManager(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	added, err := cluster.AddOSD(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	checkMessengerV2Only(t, ctx, cluster)
	crc.expect(t, ctx, "grown", "accepted", "tell", fmt.Sprintf("osd.%d", added.ID), "version")
	secure.expect(t, ctx, "grown", "rejected", "mon", "stat")
	// The added MGR advertises v2 only and takes the central policy once it
	// becomes active.
	waitEffectiveMode(t, ctx, cluster, "ms_mon_service_mode", "crc", "mgr.b")
	if _, err := cluster.Ceph(ctx, "mgr", "fail", "a"); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(2 * time.Minute); ; {
		// The former active MGR respawns; wait until it is a standby again.
		native, err := cluster.DaemonAddresses(ctx)
		if err == nil && native.ActiveManager != nil && native.ActiveManager.Name == "b" && native.ManagerAvailable && slices.Contains(native.StandbyManagers, "a") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("mgr.b did not become the available active MGR with mgr.a as standby: %+v, %v", native, err)
		}
		time.Sleep(time.Second)
	}
	checkMessengerV2Only(t, ctx, cluster)
	crc.expect(t, ctx, "mgr.b active", "accepted", "crash", "ls")
	t.Logf("MessengerV2Only with %s keys: CRC-only and mixed policies, established and new sessions, and later MON, MGR and OSD kept v2 only", keyType)
}
