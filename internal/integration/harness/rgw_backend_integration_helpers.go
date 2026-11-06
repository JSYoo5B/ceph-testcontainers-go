//go:build all || (integration && features)

package harness

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

type RGWBackendResponse struct {
	Code      int
	Body      []byte
	Headers   http.Header
	Operation string
}

func RGWBackendSigned(t *testing.T, ctx context.Context, client S3HTTPClient, service, method, path string, payload []byte, headers http.Header) RGWBackendResponse {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, method, client.Endpoint+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	for key, values := range headers {
		request.Header[key] = slices.Clone(values)
	}
	S3FeatureSign(request, client, payload, time.Now().UTC(), service)
	return RGWBackendDo(t, client.HTTP, request)
}

func RGWBackendDo(t *testing.T, client *http.Client, request *http.Request) RGWBackendResponse {
	t.Helper()
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return RGWBackendResponse{Code: response.StatusCode, Body: body, Headers: response.Header.Clone(), Operation: request.Method + " " + request.URL.EscapedPath()}
}

func RGWBackendRequire(t *testing.T, response RGWBackendResponse, allowed ...int) {
	t.Helper()
	if !slices.Contains(allowed, response.Code) {
		t.Fatalf("native protocol %s status=%d want=%v code=%s (body redacted)", response.Operation, response.Code, allowed, RGWBackendErrorCode(response.Body))
	}
}

// RGWBackendBucketCreator returns the creation step of RGWOwnBuckets for
// client: a signed PUT of the bucket, which counts as created only when RGW
// answers 200. bucket is a name or the path /name.
func RGWBackendBucketCreator(t *testing.T, ctx context.Context, client S3HTTPClient) func(bucket string) error {
	return func(bucket string) error {
		r := RGWBackendSigned(t, ctx, client, "s3", http.MethodPut, "/"+strings.TrimPrefix(bucket, "/"), nil, nil)
		if r.Code != http.StatusOK {
			return fmt.Errorf("status %d code %s", r.Code, RGWBackendErrorCode(r.Body))
		}
		return nil
	}
}

func RGWBackendWaitStatus(t *testing.T, parent context.Context, probe func(context.Context) RGWBackendResponse, want int) RGWBackendResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
		response := probe(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
		if response.Code == want {
			return response
		}
		select {
		case <-ctx.Done():
			RGWBackendRequire(t, response, want)
			return response
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// Count completed native Vault transactions, not two audit entries for each
// attempt. Values remain HMAC-redacted and never enter diagnostics.
func RGWBackendVaultAuditProof(data []byte) (map[string]int, error) {
	type auditEvent struct {
		Type  string `json:"type"`
		Error string `json:"error"`
		Auth  struct {
			Policies      []string `json:"policies"`
			PolicyResults *struct {
				Allowed bool `json:"allowed"`
			} `json:"policy_results"`
		} `json:"auth"`
		Request struct {
			ID        string `json:"id"`
			Path      string `json:"path"`
			Operation string `json:"operation"`
		} `json:"request"`
		Response struct {
			Data struct {
				Data map[string]json.RawMessage `json:"data"`
			} `json:"data"`
		} `json:"response"`
	}
	counts := map[string]int{}
	requests := map[string]auditEvent{}
	responses := map[string]bool{}
	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var event auditEvent
		if json.Unmarshal(line, &event) != nil {
			return nil, errors.New("decode real Vault audit event (redacted)")
		}
		if event.Request.Operation != "read" || !slices.Equal(event.Auth.Policies, []string{"rgw-read-owned"}) || (event.Request.Path != "tc/data/allowed" && event.Request.Path != "tc/data/denied") {
			continue
		}
		if event.Request.ID == "" {
			return nil, errors.New("the Vault key read audit is missing native request identity (redacted)")
		}
		if event.Type == "request" {
			if _, duplicate := requests[event.Request.ID]; duplicate {
				return nil, errors.New("the Vault key read audit has duplicate native request identity (redacted)")
			}
			requests[event.Request.ID] = event
			continue
		}
		if event.Type != "response" {
			return nil, errors.New("the Vault key read audit has unknown native entry type (redacted)")
		}
		request, exists := requests[event.Request.ID]
		if !exists || request.Request != event.Request || responses[event.Request.ID] {
			return nil, errors.New("the Vault key read response lacks a unique matching request (redacted)")
		}
		responses[event.Request.ID] = true
		if event.Request.Path == "tc/data/allowed" {
			var key string
			if event.Error == "" && json.Unmarshal(event.Response.Data.Data["key"], &key) == nil && key != "" && (event.Auth.PolicyResults == nil || event.Auth.PolicyResults.Allowed) {
				counts[event.Request.Path]++
			}
		} else if strings.Contains(strings.ToLower(event.Error), "permission denied") && (event.Auth.PolicyResults == nil || !event.Auth.PolicyResults.Allowed) {
			counts[event.Request.Path]++
		}
	}
	return counts, nil
}

func RGWBackendJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func RGWBackendRandom(t *testing.T, size int) string {
	t.Helper()
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(data)
}

// RGWTestAuthority is a private CA that issues gateway certificates.
type RGWTestAuthority struct {
	key         *ecdsa.PrivateKey
	certificate *x509.Certificate
	// PEM is the CA certificate, Roots the pool that trusts only it.
	PEM   []byte
	Roots *x509.CertPool
}

func NewRGWTestAuthority(t *testing.T) *RGWTestAuthority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: RGWTestSerial(t), Subject: pkix.Name{CommonName: "Testcontainers fixture CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	authority := &RGWTestAuthority{key: key, certificate: certificate, PEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), Roots: x509.NewCertPool()}
	authority.Roots.AddCert(certificate)
	return authority
}

// RGWTestCertificate returns a gateway certificate of a new CA and the pool
// that trusts that CA only.
func RGWTestCertificate(t *testing.T) (*rgw.TLSConfig, *x509.CertPool) {
	t.Helper()
	authority := NewRGWTestAuthority(t)
	return authority.Issue(t), authority.Roots
}

// Issue returns a new leaf certificate for localhost, 127.0.0.1 and ::1 with
// its own key, followed by the CA certificate in the chain.
func (a *RGWTestAuthority) Issue(t *testing.T) *rgw.TLSConfig {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	leaf := &x509.Certificate{SerialNumber: RGWTestSerial(t), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, leaf, a.certificate, &key.PublicKey, a.key)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	chain := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), a.PEM...)
	return &rgw.TLSConfig{CertificatePEM: chain, PrivateKeyPEM: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})}
}

// File writes the CA certificate to a file of the test, as the SUT's CA
// configuration.
func (a *RGWTestAuthority) File(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, a.PEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func RGWTestSerial(t *testing.T) *big.Int {
	t.Helper()
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		t.Fatal(err)
	}
	return serial
}

type RGWBackendListedObject struct {
	Key  string `xml:"Key"`
	ETag string `xml:"ETag"`
	Size int64  `xml:"Size"`
}

type RGWBackendBucket struct {
	ID    string `json:"id"`
	Name  string `json:"bucket"`
	Owner string `json:"owner"`
}

func RGWBackendAdmin(t *testing.T, ctx context.Context, gateway *rgw.Gateway, args ...string) []byte {
	t.Helper()
	result, err := gateway.Admin(ctx, args...)
	if err != nil {
		// Only command/subcommand are safe: later arguments can be role
		// policies or secret keys. Native output remains redacted by Admin.
		t.Fatalf("RGW backend native %s: %v", strings.Join(args[:min(2, len(args))], " "), err)
	}
	return result
}
