//go:build all || integration

package integration_test

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// cephxKeyType decodes the type field of a Cephx secret: 1 is AES and 2 is
// AES256KRB5.
func cephxKeyType(t *testing.T, keyring []byte, entity string) ceph.CephxCipher {
	t.Helper()
	active := false
	for _, line := range strings.Split(string(keyring), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "[") {
			active = line == "["+entity+"]"
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !active || !ok || strings.TrimSpace(key) != "key" {
			continue
		}
		secret, err := base64.StdEncoding.DecodeString(strings.TrimSpace(value))
		if err != nil || len(secret) < 2 {
			t.Fatalf("undecodable key for %s", entity)
		}
		switch binary.LittleEndian.Uint16(secret) {
		case 1:
			return ceph.CipherAES
		case 2:
			return ceph.CipherAES256K
		}
		t.Fatalf("unknown key type %d for %s", binary.LittleEndian.Uint16(secret), entity)
	}
	t.Fatalf("no key for %s", entity)
	return ""
}

func clientKeyring(t *testing.T, client *ceph.ClientConfig) []byte {
	t.Helper()
	_, keyring, err := client.ConnectionConfig()
	if err != nil {
		t.Fatal(err)
	}
	return keyring
}

func authEntityKeyType(t *testing.T, ctx context.Context, cluster *ceph.Container, entity string) ceph.CephxCipher {
	t.Helper()
	keyring, err := cluster.Ceph(ctx, "auth", "get", entity)
	if err != nil {
		t.Fatal(err)
	}
	return cephxKeyType(t, keyring, entity)
}

// clientAuthenticates reports whether a container that holds only the
// client's keyring can authenticate and read the monmap.
func clientAuthenticates(t *testing.T, ctx context.Context, cluster *ceph.Container, client *ceph.ClientConfig) bool {
	t.Helper()
	ctr, err := testcontainers.Run(ctx, cluster.ControlImage(), cluster.WithClientIdentity(client), ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatal(err)
	}
	code, reader, err := ctr.Exec(ctx, []string{"ceph", "--id", client.User(), "--connect-timeout", "10", "mon", "stat"}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, _ := io.ReadAll(reader)
	if code != 0 && !strings.Contains(string(output), "Permission denied") && !strings.Contains(string(output), "permission denied") {
		t.Fatalf("%s failed for another reason than authentication: exit %d: %s", client.Name(), code, output)
	}
	return code == 0
}

type monMapCiphers struct {
	Service   struct{ Name string }   `json:"auth_service_cipher"`
	Allowed   []struct{ Name string } `json:"auth_allowed_ciphers"`
	Preferred struct{ Name string }   `json:"auth_preferred_cipher"`
}

func readMonMapCiphers(t *testing.T, ctx context.Context, cluster *ceph.Container) (service string, allowed []string, preferred string) {
	t.Helper()
	out, err := cluster.Ceph(ctx, "mon", "dump", "--format", "json")
	var dump monMapCiphers
	if err != nil || json.Unmarshal(out, &dump) != nil {
		t.Fatalf("mon dump %q: %v", out, err)
	}
	for _, cipher := range dump.Allowed {
		allowed = append(allowed, cipher.Name)
	}
	return dump.Service.Name, allowed, dump.Preferred.Name
}

func runningOption(t *testing.T, ctx context.Context, cluster *ceph.Container, daemon, name string) float64 {
	t.Helper()
	out, err := cluster.Ceph(ctx, "config", "show", daemon, name)
	value, convErr := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil || convErr != nil {
		t.Fatalf("config show %s %s: %q %v", daemon, name, out, err)
	}
	return value
}

// testAuthCiphers starts a cluster whose keys are all AES with fractional
// ticket lifetimes, and on Ceph 20 a second one whose service cipher differs
// from its key type.
func testAuthCiphers(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	base := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1), ceph.WithPoolDefaults(1, 1)}
	cluster, _ := newServiceCluster(t, append(append(base, ceph.WithAuth(ceph.AuthConfig{
		KeyType: ceph.CipherAES, MonTicketTTL: 1500 * time.Millisecond, ServiceTicketTTL: 120 * time.Second,
	})), opts...)...)
	tentacle := !strings.HasPrefix(cluster.CephVersion(), "19.")

	// Every key the fixture created is AES, and the daemons run with the
	// fractional lifetimes.
	_, keyring, err := cluster.ConnectionConfig()
	if err != nil || cephxKeyType(t, keyring, "client.admin") != ceph.CipherAES {
		t.Fatalf("client.admin key type: %v", err)
	}
	for _, entity := range []string{"mon.", "mgr.a", "osd.0"} {
		if got := authEntityKeyType(t, ctx, cluster, entity); got != ceph.CipherAES {
			t.Fatalf("%s has a %s key", entity, got)
		}
	}
	if got := runningOption(t, ctx, cluster, "mon.a", "auth_mon_ticket_ttl"); got != 1.5 {
		t.Fatalf("mon.a auth_mon_ticket_ttl = %v", got)
	}
	if got := runningOption(t, ctx, cluster, "osd.0", "auth_service_ticket_ttl"); got != 120 {
		t.Fatalf("osd.0 auth_service_ticket_ttl = %v", got)
	}
	aesClient, err := cluster.CreateClient(ctx, "aes-reader", ceph.ClientCaps{Mon: "allow r"})
	if err != nil || cephxKeyType(t, clientKeyring(t, aesClient), aesClient.Name()) != ceph.CipherAES || !clientAuthenticates(t, ctx, cluster, aesClient) {
		t.Fatalf("default AES client: %v", err)
	}

	if !tentacle {
		// Ceph 19 has only AES keys: an AES256K client and cluster are refused
		// before anything is created.
		if _, err := cluster.CreateClient(ctx, "aes256k-reader", ceph.ClientCaps{Mon: "allow r"}, ceph.WithClientKeyType(ceph.CipherAES256K)); err == nil || !strings.Contains(err.Error(), "Ceph 20") {
			t.Fatalf("Squid created an AES256K client: %v", err)
		}
		image, imageOpts := integrationImages(t)
		started := time.Now()
		refused, err := ceph.Run(ctx, image, append(append(imageOpts, base...), ceph.WithAuth(ceph.AuthConfig{KeyType: ceph.CipherAES256K}))...)
		testcontainers.CleanupContainer(t, refused)
		if err == nil || !strings.Contains(err.Error(), "needs Ceph 20") || time.Since(started) > 2*time.Minute {
			t.Fatalf("Squid bootstrap with AES256K keys: %v after %s", err, time.Since(started))
		}
		t.Log("Squid: AES keys and fractional ticket lifetimes; AES256K clients and clusters refused before creation")
		return
	}

	// The MONs allow only AES, so an AES256K client cannot authenticate.
	service, allowed, preferred := readMonMapCiphers(t, ctx, cluster)
	if service != "aes" || strings.Join(allowed, ",") != "aes" || preferred != "aes" {
		t.Fatalf("monmap ciphers service=%s allowed=%v preferred=%s", service, allowed, preferred)
	}
	strong, err := cluster.CreateClient(ctx, "aes256k-reader", ceph.ClientCaps{Mon: "allow r"}, ceph.WithClientKeyType(ceph.CipherAES256K))
	if err != nil || cephxKeyType(t, clientKeyring(t, strong), strong.Name()) != ceph.CipherAES256K {
		t.Fatalf("AES256K client: %v", err)
	}
	if clientAuthenticates(t, ctx, cluster, strong) {
		t.Fatal("an AES256K client authenticated although the MONs allow only AES")
	}
	// The MonMap ciphers can change on a running cluster.
	if _, err := cluster.Ceph(ctx, "mon", "set", "auth_allowed_ciphers", "aes,aes256k"); err != nil {
		t.Fatal(err)
	}
	if _, allowed, _ = readMonMapCiphers(t, ctx, cluster); !slices.Contains(allowed, "aes256k") {
		t.Fatalf("allowed ciphers after mon set: %v", allowed)
	}
	if !clientAuthenticates(t, ctx, cluster, strong) {
		t.Fatal("the AES256K client could not authenticate after the MONs allowed AES256K")
	}

	// A service cipher other than the key type allows both types.
	mixed, _ := newServiceCluster(t, append(append(base, ceph.WithAuth(ceph.AuthConfig{
		KeyType: ceph.CipherAES256K, ServiceCipher: ceph.CipherAES,
	})), opts...)...)
	service, allowed, preferred = readMonMapCiphers(t, ctx, mixed)
	// The MonMap lists allowed ciphers in its own order.
	slices.Sort(allowed)
	if service != "aes" || strings.Join(allowed, ",") != "aes,aes256k" || preferred != "aes256k" {
		t.Fatalf("mixed monmap ciphers service=%s allowed=%v preferred=%s", service, allowed, preferred)
	}
	if got := authEntityKeyType(t, ctx, mixed, "mgr.a"); got != ceph.CipherAES256K {
		t.Fatalf("mixed cluster mgr.a has a %s key", got)
	}
	for _, cipher := range []ceph.CephxCipher{ceph.CipherAES, ceph.CipherAES256K} {
		client, err := mixed.CreateClient(ctx, "mixed-"+string(cipher), ceph.ClientCaps{Mon: "allow r"}, ceph.WithClientKeyType(cipher))
		if err != nil || !clientAuthenticates(t, ctx, mixed, client) {
			t.Fatalf("%s client on the mixed cluster: %v", cipher, err)
		}
	}
	t.Log("Tentacle: AES-only cluster refuses AES256K clients until mon set allows them; AES256K keys with an AES service cipher accept both key types")
}
