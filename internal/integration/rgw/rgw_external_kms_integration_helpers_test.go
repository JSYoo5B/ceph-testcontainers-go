//go:build all || (integration && features)

package rgw_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/netip"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

func rgwExternalKMS(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	f := harness.NewRGWExternalKMS(t, ctx)
	t.Run("backend-trust", func(t *testing.T) {
		f.ReadExact(t, f.Trusted, "trusted CA")
		for round := range 2 {
			refused := harness.RGWBackendSigned(t, ctx, f.WrongCA, "s3", http.MethodGet, f.Bucket+"/encrypted", nil, nil)
			// The KMS TLS handshake fails inside RGW; it maps that to 500.
			harness.RGWBackendRequire(t, refused, http.StatusInternalServerError)
			f.ReadPlain(t, f.WrongCA, "unrelated CA")
			f.ReadExact(t, f.Trusted, "sibling after refusal")
			t.Logf("round %d: unrelated KMS CA status=%d code=%s; sibling exact bytes", round, refused.Code, harness.RGWBackendErrorCode(refused.Body))
		}
		denied := f.Selected.Clone()
		denied.Set("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id", "denied")
		failed := harness.RGWBackendSigned(t, ctx, f.Trusted, "s3", http.MethodPut, f.Bucket+"/denied-key", []byte("outside the token policy"), denied)
		harness.RGWBackendRequire(t, failed, http.StatusBadRequest, http.StatusForbidden, http.StatusInternalServerError)
		harness.RGWBackendRequire(t, harness.RGWBackendSigned(t, ctx, f.Trusted, "s3", http.MethodGet, f.Bucket+"/denied-key", nil, nil), http.StatusNotFound)
		audit := f.Vault.Audit(t, ctx)
		counts, err := harness.RGWBackendVaultAuditProof(audit)
		if err != nil {
			t.Fatal(err)
		}
		if counts["tc/data/allowed"] < 3 || counts["tc/data/denied"] == 0 {
			t.Fatalf("Vault audit lacks the RGW key reads: allowed=%d denied=%d", counts["tc/data/allowed"], counts["tc/data/denied"])
		}
		trusted, err := f.Gateways[0].ContainerIP(ctx)
		if err != nil {
			t.Fatal(err)
		}
		wrong, err := f.Gateways[1].ContainerIP(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := rgwKMSAuditSources(audit, trusted, wrong); err != nil {
			t.Fatal(err)
		}
		// Change only the Vault CA in the gateway that returned 500, then
		// restart it: the same container, options, token and object must
		// now decrypt. RGW does not promise to read the CA file again while
		// it runs. Umbrella's radosgw, built against libcurl 8, keeps
		// refusing after the file is replaced until it restarts; Tentacle's
		// reads the new file at once.
		if err := f.Gateways[1].CopyToContainer(ctx, f.Vault.CA, harness.RGWKMSCAFile, 0o600); err != nil {
			t.Fatal("replace the unrelated Vault CA")
		}
		stop := 10 * time.Second
		if err := f.Gateways[1].Stop(ctx, &stop); err != nil {
			t.Fatal(err)
		}
		if err := f.Gateways[1].Start(ctx); err != nil {
			t.Fatal(err)
		}
		endpoint, err := f.Gateways[1].S3SecureEndpoint(ctx)
		if err != nil {
			t.Fatal(err)
		}
		f.WrongCA.Endpoint = endpoint
		f.ReadExact(t, f.WrongCA, "same gateway restarted after replacing its Vault CA")
		t.Logf("SSE-KMS over verified HTTPS on both legs; Vault audit allowed=%d denied=%d", counts["tc/data/allowed"], counts["tc/data/denied"])
	})

	f.Close(t)
}

// rgwKMSAuditSources rejects every selected-key read from the wrong-CA peer,
// including attempts with no authenticated identity or response. The positive
// control counts the trusted gateway's expected token policy separately.
func rgwKMSAuditSources(data []byte, trusted, wrong string) error {
	good, err := netip.ParseAddr(trusted)
	if err != nil {
		return errors.New("trusted gateway address unavailable")
	}
	bad, err := netip.ParseAddr(wrong)
	if err != nil || good.Unmap() == bad.Unmap() {
		return errors.New("gateways need distinct Vault peers")
	}
	reads := 0
	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var event struct {
			Type    string
			Auth    struct{ Policies []string }
			Request struct {
				Path, Operation string
				Remote          string `json:"remote_address"`
			}
		}
		if json.Unmarshal(line, &event) != nil {
			return errors.New("malformed Vault audit record")
		}
		if event.Type != "request" || event.Request.Path != "tc/data/allowed" || event.Request.Operation != "read" {
			continue
		}
		peer, err := netip.ParseAddr(event.Request.Remote)
		if err != nil {
			return errors.New("Vault selected-key request has no usable peer address")
		}
		if peer.Unmap() == bad.Unmap() {
			return errors.New("Vault received a selected-key request from the wrong-CA gateway")
		}
		if slices.Equal(event.Auth.Policies, []string{"rgw-read-owned"}) {
			if peer.Unmap() != good.Unmap() {
				return errors.New("Vault received the expected token policy from an unexpected peer")
			}
			reads++
		}
	}
	if reads == 0 {
		return errors.New("no Vault read attributed to the trusted gateway")
	}
	return nil
}
