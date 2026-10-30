//go:build all || (ci && ci_batch_e)

package pkg_test

import (
	"testing"

	"example.com/sel/kit"
)

func TestE(t *testing.T) {
	if kit.Upper() != "E" {
		t.Fatal("upper")
	}
}
