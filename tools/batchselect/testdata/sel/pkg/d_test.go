//go:build all || (ci && ci_batch_d)

package pkg_test

import (
	_ "embed"
	"testing"
)

//go:embed data.txt
var data string

func TestD(t *testing.T) {
	if data == "" || helperB() != 2 || shared != 3 {
		t.Fatal("data")
	}
}
