//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/dashboard"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

// dashboardServerName is the only name in the test certificates. Consumers
// connect to the advertised address and verify this name.
const dashboardServerName = "dashboard.test"

// dashboardCA is a test CA and a leaf certificate it signed for
// dashboardServerName.
type dashboardCA struct {
	caPEM []byte
	pool  *x509.CertPool
	key   *ecdsa.PrivateKey
	cert  *x509.Certificate
}

func newDashboardCA(t *testing.T) *dashboardCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "tc dashboard test CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(6 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &dashboardCA{caPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pool: pool, key: key, cert: cert}
}

// leaf issues a certificate for dashboardServerName with an RSA key, the only
// key type the module's certificate check accepts.
func (ca *dashboardCA) leaf(t *testing.T, serial int64) dashboard.TLSConfig {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: dashboardServerName}, DNSNames: []string{dashboardServerName},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(6 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return dashboard.TLSConfig{CertificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), PrivateKeyPEM: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})}
}

// dashboardConsumerScript is the test's own Dashboard client in a container on
// the cluster network. It verifies the listener with the test CA and
// dashboardServerName, as a SUT would, and never follows a redirect.
const dashboardConsumerScript = `import http.client, json, socket, ssl, sys, urllib.parse
req = json.loads(sys.argv[1])
u = urllib.parse.urlsplit(req["url"])
ctx = ssl.create_default_context(cafile=req["ca"])
class Conn(http.client.HTTPSConnection):
    def connect(self):
        sock = socket.create_connection((self.host, self.port), self.timeout)
        self.sock = self._context.wrap_socket(sock, server_hostname=req["name"])
c = Conn(u.hostname, u.port, timeout=60, context=ctx)
headers = {"Accept": "application/vnd.ceph.api.v1.0+json", "Content-Type": "application/json"}
if req.get("token"):
    headers["Authorization"] = "Bearer " + req["token"]
body = req.get("body")
c.request(req["method"], req["path"], body=None if body is None else json.dumps(body), headers=headers)
r = c.getresponse()
print(json.dumps({"status": r.status, "location": r.getheader("Location") or "", "body": r.read().decode("utf-8", "replace")}))
`

// dashboardReply is one answer to the test's Dashboard client.
type dashboardReply struct {
	Status   int    `json:"status"`
	Location string `json:"location"`
	Body     string `json:"body"`
}

// dashboardConsumer is the test's Dashboard client in a container on the
// cluster network, with the test CA.
type dashboardConsumer struct {
	client testcontainers.Container
}

const dashboardConsumerCA = "/tmp/tc-dashboard-ca.pem"

func newDashboardConsumer(t *testing.T, ctx context.Context, client testcontainers.Container, ca *dashboardCA) dashboardConsumer {
	t.Helper()
	if err := client.CopyToContainer(ctx, ca.caPEM, dashboardConsumerCA, 0o644); err != nil {
		t.Fatal(err)
	}
	return dashboardConsumer{client: client}
}

func (c dashboardConsumer) request(t *testing.T, ctx context.Context, base, method, path, token string, body any) dashboardReply {
	t.Helper()
	request, err := json.Marshal(map[string]any{"url": base, "name": dashboardServerName, "ca": dashboardConsumerCA, "method": method, "path": path, "token": token, "body": body})
	if err != nil {
		t.Fatal(err)
	}
	output := harness.ExecOutput(t, ctx, c.client, "python3", "-I", "-c", dashboardConsumerScript, string(request))
	var reply dashboardReply
	if err := json.Unmarshal([]byte(output), &reply); err != nil {
		t.Fatalf("decode dashboard reply of %s %s: %v", method, path, err)
	}
	return reply
}

// login logs in as username and returns the session token; want is the
// expected status.
func (c dashboardConsumer) login(t *testing.T, ctx context.Context, base, username, password string, want int) string {
	t.Helper()
	reply := c.request(t, ctx, base, "POST", "/api/auth", "", map[string]string{"username": username, "password": password})
	if reply.Status != want {
		t.Fatalf("dashboard login of %s at %s: HTTP %d, want %d", username, base, reply.Status, want)
	}
	if want != 201 {
		return ""
	}
	var token struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal([]byte(reply.Body), &token); err != nil || token.Token == "" {
		t.Fatalf("dashboard login of %s returned no token", username)
	}
	return token.Token
}

// summaryManager returns the manager that answered GET /api/summary.
func (c dashboardConsumer) summaryManager(t *testing.T, ctx context.Context, base, token string) string {
	t.Helper()
	reply := c.request(t, ctx, base, "GET", "/api/summary", token, nil)
	var summary struct {
		ManagerID string `json:"mgr_id"`
	}
	if reply.Status != 200 || json.Unmarshal([]byte(reply.Body), &summary) != nil || summary.ManagerID == "" {
		t.Fatalf("dashboard summary at %s: HTTP %d", base, reply.Status)
	}
	return summary.ManagerID
}

// hostDashboardRequest is the test's host-side Dashboard client with the test
// CA, at a host-network listener or a bridge-mode manager's published port.
func hostDashboardRequest(t *testing.T, ctx context.Context, ca *dashboardCA, base, method, path, token string, body any) dashboardReply {
	t.Helper()
	client := &http.Client{
		Timeout:       time.Minute,
		Transport:     &http.Transport{TLSClientConfig: &tls.Config{RootCAs: ca.pool, ServerName: dashboardServerName, MinVersion: tls.VersionTLS12}},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	defer client.CloseIdleConnections()
	var payload io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		payload = bytes.NewReader(data)
	}
	target, err := url.JoinPath(base, path)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, payload)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/vnd.ceph.api.v1.0+json")
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(req)
	if err != nil {
		t.Fatalf("host-side dashboard %s %s: %v", method, path, err)
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return dashboardReply{Status: response.StatusCode, Location: response.Header.Get("Location"), Body: string(data)}
}

// hostDashboardLogin logs in from the test process at base and requires the
// summary to be answered by the manager active.
func hostDashboardLogin(t *testing.T, ctx context.Context, ca *dashboardCA, base, username, password, active string) {
	t.Helper()
	if reply := hostDashboardRequest(t, ctx, ca, base, "POST", "/api/auth", "", map[string]string{"username": username, "password": password + "x"}); reply.Status != 400 {
		t.Fatalf("host-side login with a wrong password: HTTP %d", reply.Status)
	}
	reply := hostDashboardRequest(t, ctx, ca, base, "POST", "/api/auth", "", map[string]string{"username": username, "password": password})
	if reply.Status != 201 {
		t.Fatalf("host-side login: HTTP %d", reply.Status)
	}
	var hostToken struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal([]byte(reply.Body), &hostToken); err != nil {
		t.Fatal(err)
	}
	if reply := hostDashboardRequest(t, ctx, ca, base, "GET", "/api/summary", hostToken.Token, nil); reply.Status != 200 || !strings.Contains(reply.Body, `"mgr_id": "`+active+`"`) {
		t.Fatalf("host-side summary: HTTP %d", reply.Status)
	}
}

// dashboardState is the cluster state the Dashboard fixture may change: the
// module's membership, the manager options and config-keys of the dashboard
// module, and the advertised services.
type dashboardState struct {
	Enabled    bool
	Options    []string
	ConfigKeys []string
	Service    bool
}

func readDashboardState(t *testing.T, ctx context.Context, cluster *ceph.Container) dashboardState {
	t.Helper()
	var state dashboardState
	// A module change makes the managers respawn; read once one is active.
	var modules []ceph.MGRModuleState
	var err error
	for deadline := time.Now().Add(2 * time.Minute); ; {
		if modules, err = harness.ObserveCluster(cluster).MGRModules(ctx); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
	}
	for _, module := range modules {
		if module.Name == "dashboard" {
			state.Enabled = module.Enabled
		}
	}
	var entries []struct {
		Section string `json:"section"`
		Name    string `json:"name"`
	}
	if err := json.Unmarshal(harness.MustCeph(t, ctx, cluster, "config", "dump", "--format", "json"), &entries); err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name, "mgr/dashboard/") {
			state.Options = append(state.Options, entry.Section+" "+entry.Name)
		}
	}
	var keys []string
	if err := json.Unmarshal(harness.MustCeph(t, ctx, cluster, "config-key", "ls", "--format", "json"), &keys); err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if strings.HasPrefix(key, "mgr/dashboard/") {
			state.ConfigKeys = append(state.ConfigKeys, key)
		}
	}
	services, err := harness.ObserveCluster(cluster).ManagerServices(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, state.Service = services["dashboard"]
	slices.Sort(state.Options)
	slices.Sort(state.ConfigKeys)
	return state
}

// startOfficialMGRCluster starts a one-OSD cluster whose managers run the
// official image of the essential image's release, with the integration
// images for the other daemons and a client container.
func startOfficialMGRCluster(t *testing.T, ctx context.Context, managers int, opts ...testcontainers.ContainerCustomizer) (*ceph.Container, testcontainers.Container) {
	t.Helper()
	images := harness.IntegrationImageSet(t)
	images.MGR = officialMGRImage(t, ctx, mgrImageDocker(t, ctx), essentialBase(images))
	opts = append([]testcontainers.ContainerCustomizer{ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(images)), harness.FunctionalOSDConfig(t),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithManagerCount(managers).WithPoolDefaults(1, 1)), ceph.WithStartupTimeout(4 * time.Minute)}, opts...)
	return harness.NewServiceClusterWithOptions(t, "", opts...)
}

// restoreDashboardOnCleanup restores d when the test ends early.
func restoreDashboardOnCleanup(t *testing.T, d *dashboard.Dashboard) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		if err := d.Restore(ctx); err != nil {
			t.Errorf("restore dashboard: %v", err)
		}
	})
}

// standbyURL returns the listener of the standby manager name.
func standbyURL(t *testing.T, ctx context.Context, cluster *ceph.Container, active, name string) string {
	t.Helper()
	parsed, err := url.Parse(active)
	if err != nil {
		t.Fatal(err)
	}
	if harness.StaticFacts(cluster).UsesHostNetwork {
		port := strings.TrimSpace(string(harness.MustCeph(t, ctx, cluster, "config", "get", "mgr", "mgr/dashboard/"+name+"/ssl_server_port")))
		parsed.Host = parsed.Hostname() + ":" + port
		return parsed.String()
	}
	for _, manager := range clusteraccess.Managers(t, cluster) {
		if manager.DaemonName == name {
			address, err := manager.ContainerIP(ctx)
			if err != nil {
				t.Fatal(err)
			}
			parsed.Host = address + ":8443"
			return parsed.String()
		}
	}
	t.Fatalf("no manager %s", name)
	return ""
}

// testDashboardOwnedHTTPS prepares the Dashboard on an official-image MGR
// pair, checks the handoff from an independent client, a failover and the
// restoration, including the refusal of an outside edit.
func testDashboardOwnedHTTPS(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	cluster, client := startOfficialMGRCluster(t, ctx, 2, opts...)
	host := harness.StaticFacts(cluster).UsesHostNetwork
	before := readDashboardState(t, ctx, cluster)
	if before.Enabled || before.Service {
		t.Fatalf("dashboard already active before Start: %+v", before)
	}
	ca := newDashboardCA(t)
	material := ca.leaf(t, 2)
	started := time.Now()
	d, err := dashboard.Start(ctx, cluster, dashboard.Config{TLS: material})
	if d != nil {
		restoreDashboardOnCleanup(t, d)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("DASHBOARD started in %s: %s", time.Since(started).Round(time.Second), d)
	if _, err := dashboard.Start(ctx, cluster, dashboard.Config{TLS: material}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("second Start: %v", err)
	}
	username, password := d.Credentials()
	endpoint, err := d.Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	active := dashboardActiveManager(t, ctx, cluster)
	consumer := newDashboardConsumer(t, ctx, client, ca)
	consumer.login(t, ctx, endpoint, username, password+"x", 400)
	token := consumer.login(t, ctx, endpoint, username, password, 201)
	if got := consumer.summaryManager(t, ctx, endpoint, token); got != active {
		t.Fatalf("summary answered by mgr.%s, the MGR map names mgr.%s", got, active)
	}
	if reply := consumer.request(t, ctx, endpoint, "GET", "/api/summary", "", nil); reply.Status != 401 {
		t.Fatalf("summary without a token: HTTP %d", reply.Status)
	}
	if reply := consumer.request(t, ctx, endpoint, "POST", "/api/pool", token, map[string]any{"pool": "tc-dashboard-denied", "pg_num": 1, "pool_type": "replicated"}); reply.Status != 403 {
		t.Fatalf("read-only account creating a pool: HTTP %d %s", reply.Status, reply.Body)
	}
	if _, err := cluster.Ceph(ctx, "osd", "pool", "get", "tc-dashboard-denied", "size"); err == nil {
		t.Fatal("the refused pool exists")
	}
	hostEndpoint, err := d.HostEndpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if host != (hostEndpoint == endpoint) {
		t.Fatalf("host-side endpoint %s, advertised %s (host network %t)", hostEndpoint, endpoint, host)
	}
	hostDashboardLogin(t, ctx, ca, hostEndpoint, username, password, active)
	t.Logf("DASHBOARD host-side endpoint %s of mgr.%s: login and summary from the test process", hostEndpoint, active)
	status, err := harness.ObserveCluster(cluster).ManagerStatus(ctx)
	if err != nil || len(status.Standbys) != 1 {
		t.Fatalf("manager status %+v %v", status, err)
	}
	standby := standbyURL(t, ctx, cluster, endpoint, status.Standbys[0].Name)
	if reply := consumer.request(t, ctx, standby, "GET", "/api/summary", token, nil); reply.Status != 303 || reply.Location != endpoint {
		t.Fatalf("standby %s answered HTTP %d to %q, want 303 to %s", standby, reply.Status, reply.Location, endpoint)
	}
	// Every readiness check sends one wrong password; the account must not
	// reach the module's lockout of ten failed attempts.
	for range 11 {
		if _, err := d.Endpoint(ctx); err != nil {
			t.Fatal(err)
		}
	}
	consumer.login(t, ctx, endpoint, username, password, 201)
	t.Logf("DASHBOARD endpoint %s active=mgr.%s standby %s redirects; consumer login, 400 wrong password, 401 no token, 403 read-only write", endpoint, active, standby)

	harness.MustCeph(t, ctx, cluster, "mgr", "fail", active)
	promoted := waitPromotion(t, ctx, cluster, active)
	failover, err := d.Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if failover == endpoint {
		t.Fatalf("the endpoint %s did not change after the failover to mgr.%s", failover, promoted)
	}
	if got := consumer.summaryManager(t, ctx, failover, token); got != promoted {
		t.Fatalf("the earlier token reached mgr.%s, want mgr.%s", got, promoted)
	}
	t.Logf("DASHBOARD failover mgr.%s -> mgr.%s: endpoint %s -> %s; the session token of mgr.%s is accepted by mgr.%s", active, promoted, endpoint, failover, active, promoted)
	hostFailover, err := d.HostEndpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if hostFailover == hostEndpoint || host != (hostFailover == failover) {
		t.Fatalf("host-side endpoint after the failover %s, before %s, advertised %s", hostFailover, hostEndpoint, failover)
	}
	hostDashboardLogin(t, ctx, ca, hostFailover, username, password, promoted)
	t.Logf("DASHBOARD host-side failover mgr.%s -> mgr.%s: %s -> %s, login and summary from the test process", active, promoted, hostEndpoint, hostFailover)

	// An outside edit of the certificate is refused and left as it is.
	other := ca.leaf(t, 3)
	runDashboardCommand(t, ctx, cluster, other.CertificatePEM, "dashboard", "set-ssl-certificate", "-i", "-")
	if err := d.Restore(ctx); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("Restore after an outside certificate edit: %v", err)
	}
	if current := harness.MustCeph(t, ctx, cluster, "config-key", "get", "mgr/dashboard/crt"); !bytes.Equal(bytes.TrimSpace(current), bytes.TrimSpace(other.CertificatePEM)) {
		t.Fatal("the refused Restore changed the outside certificate")
	}
	if !readDashboardState(t, ctx, cluster).Enabled {
		t.Fatal("the refused Restore disabled the module")
	}
	runDashboardCommand(t, ctx, cluster, material.CertificatePEM, "dashboard", "set-ssl-certificate", "-i", "-")
	if err := d.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Endpoint(ctx); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("Endpoint after Restore: %v", err)
	}
	after := readDashboardState(t, ctx, cluster)
	// The module keeps its access database and token signing key.
	residual := slices.DeleteFunc(slices.Clone(after.ConfigKeys), func(key string) bool {
		return key == "mgr/dashboard/accessdb_v2" || key == "mgr/dashboard/jwt_secret"
	})
	if after.Enabled || after.Service || !slices.Equal(after.Options, before.Options) || !slices.Equal(residual, before.ConfigKeys) {
		t.Fatalf("after Restore %+v, before Start %+v", after, before)
	}
	t.Logf("DASHBOARD restored: module disabled, options %v, config-keys %v (module records kept)", after.Options, after.ConfigKeys)
}

// runDashboardCommand runs a ceph command with input on its standard input in
// the control container.
func runDashboardCommand(t *testing.T, ctx context.Context, cluster *ceph.Container, input []byte, args ...string) {
	t.Helper()
	stdin := ceph.NewNativeInput()
	go func() {
		_ = stdin.Write(ctx, input)
		_ = stdin.Close()
	}()
	result, err := cluster.RunNativeCommand(ctx, cluster.Container, ceph.NativeCommand{Argv: append([]string{"ceph"}, args...), Timeout: time.Minute,
		MaxStdout: 1 << 16, MaxStderr: 1 << 16, Input: stdin, MaxInput: len(input)})
	if err != nil || result.ExitCode != 0 {
		t.Fatalf("ceph %v: exit %d %v %s", args[:2], result.ExitCode, err, result.Stderr)
	}
}

// dashboardActiveManager returns the MGR map's available active manager.
func dashboardActiveManager(t *testing.T, ctx context.Context, cluster *ceph.Container) string {
	t.Helper()
	status, err := harness.ObserveCluster(cluster).ManagerStatus(ctx)
	if err != nil || !status.Available || status.ActiveName == "" {
		t.Fatalf("active manager unavailable: %+v %v", status, err)
	}
	return status.ActiveName
}

// waitPromotion waits until a manager other than previous is active.
func waitPromotion(t *testing.T, ctx context.Context, cluster *ceph.Container, previous string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		status, err := harness.ObserveCluster(cluster).ManagerStatus(ctx)
		if err == nil && status.Available && status.ActiveName != "" && status.ActiveName != previous {
			return status.ActiveName
		}
		if time.Now().After(deadline) {
			t.Fatalf("no manager replaced mgr.%s: %+v %v", previous, status, err)
		}
		time.Sleep(time.Second)
	}
}

// testDashboardEssentialMGRRefused requires that Start refuses a manager image
// without the dashboard module before any change.
func testDashboardEssentialMGRRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	images := harness.IntegrationImageSet(t)
	cluster, _ := harness.NewServiceClusterWithOptions(t, "", ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(images)), harness.FunctionalOSDConfig(t),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)))
	before := readDashboardState(t, ctx, cluster)
	d, err := dashboard.Start(ctx, cluster, dashboard.Config{TLS: newDashboardCA(t).leaf(t, 2)})
	if d != nil || !errors.Is(err, ceph.ErrImageCapability) {
		t.Fatalf("Start on the essential MGR image: %v %v", d, err)
	}
	if after := readDashboardState(t, ctx, cluster); !slices.Equal(after.Options, before.Options) || !slices.Equal(after.ConfigKeys, before.ConfigKeys) || after.Enabled {
		t.Fatalf("the refused Start changed %+v to %+v", before, after)
	}
	t.Logf("DASHBOARD essential MGR image refused: %v", err)
}

// testDashboardRGWGateway wires an owned gateway with TLS into the Dashboard and
// checks that the Dashboard reports the fixture's own user and bucket, that a
// refused identity and an unreachable gateway are attributed while the
// Dashboard itself stays ready, the failover, and the removal.
func testDashboardRGWGateway(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	cluster, client := startOfficialMGRCluster(t, ctx, 2)
	ca := newDashboardCA(t)
	// A gateway with TLS also serves plain HTTP, which the Dashboard uses.
	gatewayTLS := ca.leaf(t, 4)
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{Name: "dash", TLS: &rgw.TLSConfig{CertificatePEM: gatewayTLS.CertificatePEM, PrivateKeyPEM: gatewayTLS.PrivateKeyPEM}})
	if err != nil {
		t.Fatal(err)
	}
	d, err := dashboard.Start(ctx, cluster, dashboard.Config{TLS: ca.leaf(t, 2)})
	if d != nil {
		restoreDashboardOnCleanup(t, d)
	}
	if err != nil {
		t.Fatal(err)
	}
	// This bridge-mode cluster's managers publish no port.
	if got, err := d.HostEndpoint(ctx); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("HostEndpoint without WithManagerPorts: %q, %v", got, err)
	}
	beforeLink := readDashboardState(t, ctx, cluster)
	link, err := d.AddGateway(ctx, gateway)
	if err != nil {
		t.Fatal(err)
	}
	linkUser := link.User().ID()
	info, err := link.User().Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("DASHBOARD RGW link user %s caps %+v", linkUser, info.AdminCaps)

	// The S3 data plane, through the fixture's own user, apart from the
	// Dashboard.
	owner, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "tc-dashboard-owner", DisplayName: "dashboard owner"})
	if err != nil {
		t.Fatal(err)
	}
	access, secret, err := owner.CreationKey().Credentials()
	if err != nil {
		t.Fatal(err)
	}
	s3Endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s3 := harness.S3HTTPClient{Endpoint: s3Endpoint, AccessKey: access, SecretKey: secret, Region: gateway.Region, HTTP: &http.Client{Timeout: time.Minute}}
	const bucket = "tc-dashboard-bucket"
	payload := []byte("dashboard fixture object")
	s3.Request(t, ctx, "PUT", "/"+bucket, nil, 200)
	s3.Request(t, ctx, "PUT", "/"+bucket+"/object", payload, 200)
	if got := s3.Request(t, ctx, "GET", "/"+bucket+"/object", nil, 200); !bytes.Equal(got, payload) {
		t.Fatalf("S3 read back %q", got)
	}

	endpoint, err := d.Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	username, password := d.Credentials()
	consumer := newDashboardConsumer(t, ctx, client, ca)
	token := consumer.login(t, ctx, endpoint, username, password, 201)
	waitDashboardRGW(t, ctx, consumer, endpoint, token, bucket, owner.ID())
	var daemons []struct {
		ServiceMapID string `json:"service_map_id"`
		Port         int    `json:"port"`
		ZoneName     string `json:"zone_name"`
	}
	reply := consumer.request(t, ctx, endpoint, "GET", "/api/rgw/daemon", token, nil)
	if reply.Status != 200 || json.Unmarshal([]byte(reply.Body), &daemons) != nil || len(daemons) != 1 {
		t.Fatalf("dashboard RGW daemons: HTTP %d %s", reply.Status, reply.Body)
	}
	if gid := serviceMapRGW(t, ctx, cluster); daemons[0].ServiceMapID != gid {
		t.Fatalf("dashboard reports RGW daemon %s, the service map has %s", daemons[0].ServiceMapID, gid)
	}
	t.Logf("DASHBOARD RGW daemon %+v; bucket %s of %s listed", daemons[0], bucket, owner.ID())

	// A refused identity is attributed to the gateway; the Dashboard stays
	// ready.
	if err := link.User().Suspend(ctx, true); err != nil {
		t.Fatal(err)
	}
	refused := waitLinkError(t, ctx, link, dashboard.ErrGatewayRefused)
	if _, err := d.Endpoint(ctx); err != nil {
		t.Fatalf("dashboard readiness with a refused RGW identity: %v", err)
	}
	if err := link.User().Suspend(ctx, false); err != nil {
		t.Fatal(err)
	}
	waitLinkReady(t, ctx, link)

	// An unreachable gateway is attributed too; login keeps working.
	if err := gateway.Stop(ctx, nil); err != nil {
		t.Fatal(err)
	}
	unreachable := waitLinkError(t, ctx, link, dashboard.ErrGatewayUnreachable)
	if _, err := d.Endpoint(ctx); err != nil {
		t.Fatalf("dashboard readiness with a stopped gateway: %v", err)
	}
	consumer.login(t, ctx, endpoint, username, password, 201)
	if err := gateway.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitLinkReady(t, ctx, link)
	t.Logf("DASHBOARD RGW refused identity: %v; stopped gateway: %v", refused, unreachable)

	// The link's keys are cluster-wide manager options.
	active := dashboardActiveManager(t, ctx, cluster)
	harness.MustCeph(t, ctx, cluster, "mgr", "fail", active)
	promoted := waitPromotion(t, ctx, cluster, active)
	waitLinkReady(t, ctx, link)
	failover, err := d.Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	waitDashboardRGW(t, ctx, consumer, failover, consumer.login(t, ctx, failover, username, password, 201), bucket, owner.ID())
	t.Logf("DASHBOARD RGW link persists across the failover mgr.%s -> mgr.%s at %s", active, promoted, failover)

	// An outside edit of the link's key is refused and left as it is; the
	// link's user is still removed, and a later Remove finishes once the
	// key is back.
	linkAccess, _, err := link.User().CreationKey().Credentials()
	if err != nil {
		t.Fatal(err)
	}
	runDashboardCommand(t, ctx, cluster, []byte("TCOUTSIDEEDIT"), "dashboard", "set-rgw-api-access-key", "-i", "-")
	if err := link.Remove(ctx); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("Remove after an outside key edit: %v", err)
	}
	if got := strings.TrimSpace(string(harness.MustCeph(t, ctx, cluster, "dashboard", "get-rgw-api-access-key"))); got != "TCOUTSIDEEDIT" {
		t.Fatal("the refused Remove changed the outside access key")
	}
	runDashboardCommand(t, ctx, cluster, []byte(linkAccess), "dashboard", "set-rgw-api-access-key", "-i", "-")
	if err := link.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	if err := link.Check(ctx); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("Check after Remove: %v", err)
	}
	if _, err := gateway.Admin(ctx, "user", "info", "--uid", linkUser); err == nil {
		t.Fatalf("the link's RGW user %s remains", linkUser)
	}
	if after := readDashboardState(t, ctx, cluster); !slices.Equal(after.Options, beforeLink.Options) {
		t.Fatalf("manager options after Remove %v, before AddGateway %v", after.Options, beforeLink.Options)
	}
	// The restarted gateway can have another mapped port.
	if s3.Endpoint, err = gateway.S3Endpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if got := s3.Request(t, ctx, "GET", "/"+bucket+"/object", nil, 200); !bytes.Equal(got, payload) {
		t.Fatalf("S3 read back after Remove %q", got)
	}
	if err := d.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("DASHBOARD RGW link removed: an outside key edit refused, then user %s gone, keys restored; owner data intact", linkUser)
}

// waitDashboardRGW waits until the Dashboard lists bucket with its owner.
func waitDashboardRGW(t *testing.T, ctx context.Context, consumer dashboardConsumer, endpoint, token, bucket, owner string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		buckets := consumer.request(t, ctx, endpoint, "GET", "/api/rgw/bucket", token, nil)
		detail := consumer.request(t, ctx, endpoint, "GET", "/api/rgw/bucket/"+bucket, token, nil)
		var names []string
		var info struct {
			Bucket string `json:"bucket"`
			Owner  string `json:"owner"`
		}
		if buckets.Status == 200 && json.Unmarshal([]byte(buckets.Body), &names) == nil && slices.Contains(names, bucket) &&
			detail.Status == 200 && json.Unmarshal([]byte(detail.Body), &info) == nil && info.Bucket == bucket && info.Owner == owner {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("dashboard does not list %s of %s: HTTP %d %s; HTTP %d", bucket, owner, buckets.Status, buckets.Body, detail.Status)
		}
		time.Sleep(2 * time.Second)
	}
}

// waitLinkReady polls Check until it passes.
func waitLinkReady(t *testing.T, ctx context.Context, link *dashboard.GatewayLink) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for {
		err := link.Check(ctx)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("link check: %v", err)
		}
		time.Sleep(2 * time.Second)
	}
}

// waitLinkError polls Check until it returns an error matching want, and
// returns it.
func waitLinkError(t *testing.T, ctx context.Context, link *dashboard.GatewayLink, want error) error {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for {
		err := link.Check(ctx)
		if errors.Is(err, want) {
			return err
		}
		if time.Now().After(deadline) {
			t.Fatalf("link check: %v, want %v", err, want)
		}
		time.Sleep(2 * time.Second)
	}
}

// serviceMapRGW returns the GID of the service map's only RGW daemon.
func serviceMapRGW(t *testing.T, ctx context.Context, cluster *ceph.Container) string {
	t.Helper()
	var dump struct {
		Services map[string]struct {
			Daemons map[string]json.RawMessage `json:"daemons"`
		} `json:"services"`
	}
	if err := json.Unmarshal(harness.MustCeph(t, ctx, cluster, "service", "dump", "--format", "json"), &dump); err != nil {
		t.Fatal(err)
	}
	var gids []string
	for gid := range dump.Services["rgw"].Daemons {
		if gid != "summary" {
			gids = append(gids, gid)
		}
	}
	if len(gids) != 1 {
		t.Fatalf("service map RGW daemons %v", gids)
	}
	return gids[0]
}
