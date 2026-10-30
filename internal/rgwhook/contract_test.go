package rgwhook

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

func TestRegisteredHooksSatisfyTheContract(t *testing.T) {
	if _, ok := hookreg.RGW.(hooks); !ok {
		t.Error("package rgw registered hooks that do not satisfy the rgwhook contract")
	}
}
