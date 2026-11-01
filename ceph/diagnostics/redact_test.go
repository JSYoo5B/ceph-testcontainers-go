package diagnostics

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
)

// Redact masks the cluster's keyring keys and the given values in a copy and
// leaves its input and the bytes around the secrets as they are. The client
// keys of the inventory are covered by the ceph tests of the inventory.
func TestRedactUsesTheClusterSecrets(t *testing.T) {
	cluster := testhelper.TestCluster(nil, testhelper.TestConfig{})
	testhelper.TestSetKeyring(cluster, []byte("[client.admin]\n\tkey = AQBsecretadminkey==\n"))
	data := []byte("admin AQBsecretadminkey== app opaque-value \x00\xff")
	redacted, err := Redact(t.Context(), cluster, data, "opaque-value")
	if err != nil {
		t.Fatal(err)
	}
	if want := "admin [REDACTED] app [REDACTED] \x00\xff"; string(redacted) != want {
		t.Fatalf("redacted %q, want %q", redacted, want)
	}
	if string(data) != "admin AQBsecretadminkey== app opaque-value \x00\xff" {
		t.Fatal("Redact changed its input")
	}
	if _, err := Redact(t.Context(), (*ceph.Container)(nil), data); err == nil {
		t.Fatal("a nil cluster was accepted")
	}
}
