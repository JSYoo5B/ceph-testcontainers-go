package evidencehook

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

func TestRegisteredHooksSatisfyTheContract(t *testing.T) {
	if _, ok := hookreg.Evidence.(hooks); !ok {
		t.Error("package ceph registered evidence hooks that do not satisfy the evidencehook contract")
	}
}
