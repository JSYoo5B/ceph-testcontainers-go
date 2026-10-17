//go:build all || (ci && ci_batch_c)

package pkg_test

import "testing"

func TestC(t *testing.T) {
	if (fastRunner{}).run() != 1 {
		t.Fatal("run")
	}
}
