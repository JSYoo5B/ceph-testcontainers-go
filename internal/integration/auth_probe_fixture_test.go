//go:build all || (integration && auth && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"fmt"
	"os/exec"
	"strings"
	"testing"
)

// This substitute raises errors at connect in the actual Python producer.
// It does not authenticate, use credentials, or substitute a classified result.
func TestAuthProbeSeparatesConnectionFailures(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	for _, test := range []struct {
		name     string
		errno    int
		rejected bool
	}{
		{"permission denied", 13, true}, {"operation denied", 1, true},
		{"timeout", 110, false}, {"connection refused", 111, false}, {"other error", 5, false}, {"connected", 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			stub := `import sys, types
rados = types.ModuleType("rados")
class Error(Exception):
    def __init__(self, errno): self.errno = errno
class Rados:
    def __init__(self, **kwargs): pass
    def conf_set(self, *args): pass
    def shutdown(self): pass
    def connect(self, **kwargs):
        if error_number: raise Error(error_number)
rados.Error = Error
rados.Rados = Rados
sys.modules["rados"] = rados
error_number = int(sys.argv[1])
source = sys.argv[2]
sys.argv = ["probe", "client.synthetic", "bad-auth", "unused"]
exec(source)
`
			out, err := exec.CommandContext(t.Context(), python, "-c", stub, fmt.Sprint(test.errno), authNativeProbe).CombinedOutput()
			rejected := err == nil && strings.Contains(string(out), "fresh authentication rejected")
			if rejected != test.rejected {
				t.Fatalf("producer rejected=%t, want %t: %v %s", rejected, test.rejected, err, out)
			}
		})
	}
}
