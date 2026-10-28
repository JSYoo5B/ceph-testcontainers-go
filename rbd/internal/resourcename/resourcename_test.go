package resourcename

import (
	"strings"
	"testing"
)

func TestCheckAcceptsAndRefusesTheSameNamesAsBeforeTheMove(t *testing.T) {
	for _, name := range []string{"a", "_", "0", "pool.1", "a-b", "A_b.c-d", strings.Repeat("a", 128)} {
		if err := Check("pool", name); err != nil {
			t.Errorf("%q refused: %v", name, err)
		}
	}
	for _, name := range []string{"", "-lead", ".lead", "with space", "a/b", "tab\t", "café", strings.Repeat("a", 129)} {
		err := Check("image", name)
		want := "RBD image name must use letters, digits, underscores, dots or dashes and cannot start with a dot or dash"
		if err == nil || err.Error() != want {
			t.Errorf("%q: error %v, want %q", name, err, want)
		}
	}
}
