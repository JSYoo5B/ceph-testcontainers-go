//go:build all || (ci && ci_batch_d)

package pkg_test

import (
	_ "embed"
	"testing"
)

//go:embed data.txt
var data string

//go:embed notes.md
var notes string

func TestD(t *testing.T) {
	if data == "" || notes == "" || helperB() != 2 || shared != 3 {
		t.Fatal("data")
	}
}
