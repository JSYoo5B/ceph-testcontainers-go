package rgw

import "testing"

// TestRGWBootstrapKindValue pins the service kind that the tenant adapter, the
// bootstrap and the gateway registry's extension key share, so that moving the
// constant into a leaf internal package
// (docs/design/shared-harness-boundary.md, decision 8) is checked against the
// same value. It runs without Docker.
func TestRGWBootstrapKindValue(t *testing.T) {
	if got := newRGWBootstrap([]Config{{}}).Kind(); got != "rgw" {
		t.Fatalf("bootstrap kind = %q, want %q", got, "rgw")
	}
}
