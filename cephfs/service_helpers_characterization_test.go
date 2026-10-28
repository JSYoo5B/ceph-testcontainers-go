package cephfs

import "testing"

// TestCephFSBootstrapKindValue pins the service kind that the tenant adapter,
// the bootstrap and the extension store share, so that moving the constant
// into a leaf internal package (docs/design/shared-harness-boundary.md,
// decision 8) is checked against the same value. It runs without Docker.
func TestCephFSBootstrapKindValue(t *testing.T) {
	if got := newCephFSBootstrap([]Config{{}}).Kind(); got != "cephfs" {
		t.Fatalf("bootstrap kind = %q, want %q", got, "cephfs")
	}
}
