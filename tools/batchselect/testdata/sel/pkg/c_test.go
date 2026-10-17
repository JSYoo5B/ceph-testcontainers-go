//go:build all || (ci && ci_batch_c)

package pkg_test

import (
	"os"
	"testing"
)

func TestC(t *testing.T) {
	if (fastRunner{}).run() != 1 {
		t.Fatal("run")
	}
	if expected, err := os.ReadFile("testdata/expected.md"); err != nil || len(expected) == 0 {
		t.Fatal("expected output", err)
	}
}
