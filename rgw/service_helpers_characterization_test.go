package rgw

import "testing"

// TestRGWBootstrapKindValue pins the provisioning kind and extension-store key.
// It runs without Docker.
func TestRGWBootstrapKindValue(t *testing.T) {
	if got := newRGWBootstrap([]Config{{}}).Kind(); got != "rgw" {
		t.Fatalf("bootstrap kind = %q, want %q", got, "rgw")
	}
}
