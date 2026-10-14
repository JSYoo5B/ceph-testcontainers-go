//go:build all || integration

package integration_test

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
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

// tentacleWithoutKeyTypes is the last Tentacle release whose ceph-authtool
// has no --key-type and whose monmaptool has no cipher options.
const tentacleWithoutKeyTypes = "quay.io/ceph/ceph:v20.2.3@sha256:d195020de02512030118e772cef7859e92904e91eb4cb21acb503f8b94118137"

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

// keyTypeOption reports whether the cluster's control image has
// ceph-authtool --key-type, which Squid and Tentacle before 20.2.4 lack.
func keyTypeOption(t *testing.T, ctx context.Context, cluster *ceph.Container) bool {
	t.Helper()
	_, reader, err := cluster.ControlContainer().Exec(ctx, []string{"ceph-authtool", "--help"}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	usage, _ := io.ReadAll(reader)
	return strings.Contains(string(usage), "--key-type")
}

// testAuthCiphers starts a cluster of image whose keys are all AES with
// fractional ticket lifetimes. The MON ticket lifetime stays above the MON
// clients' renewal cadence: with 1.5 seconds a busy MGR lost its MON session
// now and then and Ceph aborted it during activation (#23). With key type support it also checks AES256K
// clients and a second cluster whose service cipher differs from its key
// type; without it, that AES256K requests are refused before any change.
func testAuthCiphers(t *testing.T, image string, imageOpts []testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	base := append(slices.Clone(imageOpts), ceph.WithOSDCount(1), ceph.WithPoolDefaults(1, 1))
	cluster, _ := newServiceClusterWithOptions(t, image, append(slices.Clone(base), ceph.WithAuth(ceph.AuthConfig{
		KeyType: ceph.CipherAES, MonTicketTTL: 10500 * time.Millisecond, ServiceTicketTTL: 120 * time.Second,
	}))...)
	keyTypes := keyTypeOption(t, ctx, cluster)

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
	if got := runningOption(t, ctx, cluster, "mon.a", "auth_mon_ticket_ttl"); got != 10.5 {
		t.Fatalf("mon.a auth_mon_ticket_ttl = %v", got)
	}
	if got := runningOption(t, ctx, cluster, "osd.0", "auth_service_ticket_ttl"); got != 120 {
		t.Fatalf("osd.0 auth_service_ticket_ttl = %v", got)
	}
	aesClient, err := cluster.CreateClient(ctx, "aes-reader", ceph.ClientCaps{Mon: "allow r"})
	if err != nil || cephxKeyType(t, clientKeyring(t, aesClient), aesClient.Name()) != ceph.CipherAES || !clientAuthenticates(t, ctx, cluster, aesClient) {
		t.Fatalf("default AES client: %v", err)
	}
	explicit, err := cluster.CreateClient(ctx, "explicit-aes-reader", ceph.ClientCaps{Mon: "allow r"}, ceph.WithClientKeyType(ceph.CipherAES))
	if err != nil || cephxKeyType(t, clientKeyring(t, explicit), explicit.Name()) != ceph.CipherAES || !clientAuthenticates(t, ctx, cluster, explicit) {
		t.Fatalf("explicit AES client: %v", err)
	}

	if !keyTypes {
		// Without key type support only AES keys exist: an AES256K client and
		// cluster are refused before anything is created.
		_, err := cluster.CreateClient(ctx, "aes256k-reader", ceph.ClientCaps{Mon: "allow r"}, ceph.WithClientKeyType(ceph.CipherAES256K))
		var refusal *ceph.OperationError
		if !errors.As(err, &refusal) || refusal.Kind != ceph.ErrUnsupported || refusal.Capability != "ceph-authtool --key-type" ||
			refusal.Version != cluster.CephVersion() || !strings.HasPrefix(refusal.Image, "sha256:") {
			t.Fatalf("an image without --key-type created an AES256K client: %v", err)
		}
		if _, err := cluster.Ceph(ctx, "auth", "get", "client.aes256k-reader"); err == nil {
			t.Fatal("the refused AES256K client exists")
		}
		started := time.Now()
		refused, err := ceph.Run(ctx, image, append(slices.Clone(base), ceph.WithAuth(ceph.AuthConfig{KeyType: ceph.CipherAES256K}))...)
		testcontainers.CleanupContainer(t, refused)
		if !errors.Is(err, ceph.ErrUnsupported) || !strings.Contains(err.Error(), "ceph-authtool --key-type") || time.Since(started) > 2*time.Minute {
			t.Fatalf("bootstrap with AES256K keys without --key-type: %v after %s", err, time.Since(started))
		}
		t.Logf("Ceph %s without --key-type: AES keys, explicit AES clients and fractional ticket lifetimes; AES256K clients and clusters refused before creation", cluster.CephVersion())
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
	mixed, _ := newServiceClusterWithOptions(t, image, append(slices.Clone(base), ceph.WithAuth(ceph.AuthConfig{
		KeyType: ceph.CipherAES256K, ServiceCipher: ceph.CipherAES,
	}))...)
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
	t.Logf("Ceph %s with --key-type: AES-only cluster refuses AES256K clients until mon set allows them; AES256K keys with an AES service cipher accept both key types", cluster.CephVersion())
}
