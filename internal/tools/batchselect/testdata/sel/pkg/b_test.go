//go:build all || (ci && ci_batch_b)

package pkg_test

import "testing"

func TestB(t *testing.T) {
	if drive(slowRunner{}) != 2 {
		t.Fatal("drive")
	}
}
