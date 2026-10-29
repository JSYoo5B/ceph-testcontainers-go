package cephfs

import "testing"

// TestCephFSBootstrapKindValue pins the provisioning kind and extension-store key.
// It runs without Docker.
func TestCephFSBootstrapKindValue(t *testing.T) {
	if got := newCephFSBootstrap([]Config{{}}).Kind(); got != "cephfs" {
		t.Fatalf("bootstrap kind = %q, want %q", got, "cephfs")
	}
}
