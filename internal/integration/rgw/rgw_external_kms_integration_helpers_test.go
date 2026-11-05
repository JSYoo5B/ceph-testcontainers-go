//go:build all || (integration && features)

package rgw_test

import (
	"context"
	"net/http"
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
		counts, err := harness.RGWBackendVaultAuditProof(f.Vault.Audit(t, ctx))
		if err != nil {
			t.Fatal(err)
		}
		if counts["tc/data/allowed"] < 3 || counts["tc/data/denied"] == 0 {
			t.Fatalf("Vault audit lacks the RGW key reads: allowed=%d denied=%d", counts["tc/data/allowed"], counts["tc/data/denied"])
		}
		t.Logf("SSE-KMS over verified HTTPS on both legs; Vault audit allowed=%d denied=%d", counts["tc/data/allowed"], counts["tc/data/denied"])
	})

	f.Close(t)
}
