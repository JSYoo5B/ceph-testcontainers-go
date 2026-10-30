//go:build all || (ci && ci_batch_f)

package pkg_test

import (
	"testing"

	"example.com/sel/kit"
)

func TestF(t *testing.T) {
	if kit.Two() != 2 {
		t.Fatal("two")
	}
}
