//go:build all || integration

package integration_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
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
	ctr, err := testcontainers.Run(ctx, cluster.ControlImage(), attach, ceph.WithIdleEntrypoint())
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
	switch {
	case want == "accepted" && code == 0,
		want == "rejected" && code != 0 && code != 124 && strings.Contains(output, "handle_auth_bad_method"),
		want == "unanswered" && code == 124:
		return
	}
	t.Fatalf("%s: %s %v: want %s, got exit %d: %s", step, client.name, args, want, code, output)
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
	override, err := cluster.TemporaryMessengerPolicy(ctx, ceph.MessengerPolicy{Monitors: modeCRC, OSDs: modeCRC})
	if err != nil {
		if override != nil {
			_ = override.Restore(ctx)
		}
		t.Fatal(err)
	}
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
	monCRC.expect(t, ctx, "restored", "rejected", "mon", "stat")
	monSecure.expect(t, ctx, "restored", "accepted", "mon", "stat")
	osdSecure.expect(t, ctx, "restored", "accepted", "tell", "osd.0", "version")

	// One daemon's own entry takes precedence over its type's policy.
	single, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{Section: "osd.0", Name: "ms_service_mode", Value: "crc"})
	if err != nil {
		if single != nil {
			_ = single.Restore(ctx)
		}
		t.Fatal(err)
	}
	osdCRC.expect(t, ctx, "osd.0 only", "accepted", "tell", "osd.0", "version")
	if err := single.Restore(ctx); err != nil {
		t.Fatal(err)
	}
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
	if _, err := cluster.TemporaryMessengerPolicy(ctx, ceph.MessengerPolicy{Monitors: modeCRC}); err == nil {
		t.Fatal("a temporary policy was accepted with MessengerV2Secure")
	}
	t.Log("MessengerV2Secure: a CRC-only client policy is rejected by the MON; a client without a policy connects")
}
