//go:build all || (ci && ci_batch_c)

package pkg_test

import (
	"reflect"
	"testing"
)

// poolSuite stands for a testify suite: its methods are called only by
// reflection, never by name.
type poolSuite struct{ t *testing.T }

func (s *poolSuite) TestCreate() {
	if helperSuite() != 5 {
		s.t.Fatal("create")
	}
}

func helperSuite() int { return 5 }

func runSuite(t *testing.T, s *poolSuite) {
	s.t = t
	v := reflect.ValueOf(s)
	for i := range v.NumMethod() {
		v.Method(i).Call(nil)
	}
}

func TestSuiteC(t *testing.T) { runSuite(t, &poolSuite{}) }
