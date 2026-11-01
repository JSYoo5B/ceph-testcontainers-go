package diagnosticshook

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

func TestRegisteredHooksSatisfyTheContract(t *testing.T) {
	if _, ok := hookreg.Diagnostics.(hooks); !ok {
		t.Error("package diagnostics registered hooks that do not satisfy the diagnosticshook contract")
	}
}
