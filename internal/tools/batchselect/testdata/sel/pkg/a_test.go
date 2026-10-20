//go:build all || (ci && ci_batch_a)

package pkg_test

import "testing"

func TestA(t *testing.T) {
	if helperA() != "A" {
		t.Fatal("helperA")
	}
}
