package harness

import (
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/harnesshook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

// The values that package ceph registers must satisfy the typed contract of
// internal/harnesshook: ceph cannot name those types, so only a test on the
// importing side can prove it. These controls run without Docker.

func realBackend(t *testing.T, c *ceph.Container) any {
	t.Helper()
	access, ok := hookreg.Test.(interface{ UnitBackend(c *ceph.Container) any })
	if !ok {
		t.Fatal("package ceph registered no unit-test access with UnitBackend")
	}
	return access.UnitBackend(c)
}

func TestCoreBackendSatisfiesTheContract(t *testing.T) {
	cluster := &ceph.Container{}
	value := realBackend(t, cluster)
	backend, ok := value.(harnesshook.Backend)
	if !ok {
		t.Fatalf("the core's backend %T does not satisfy harnesshook.Backend", value)
	}
	if got := harnesshook.Lend(backend); got != cluster {
		t.Fatalf("Lend returned %p, want the backend's own cluster %p", got, cluster)
	}
	ops := harnesshook.AsOps(backend.Ops())
	if ops == nil {
		t.Fatal("the core's operations do not satisfy harnesshook.Ops")
	}
	// The adapters turn the contract's values into the orchestration's.
	adapted := hookBackend{b: backend}
	if adapted.ops().raw() != cluster {
		t.Fatal("the adapter does not forward Lend")
	}
}

func TestContractPartsOfTheModelBackendAreNil(t *testing.T) {
	if harnesshook.AsOps(nil) != nil {
		t.Fatal("a nil operations value must give nil")
	}
	defer func() {
		if recover() == nil {
			t.Error("a value that is not Ops was accepted")
		}
	}()
	harnesshook.AsOps(struct{}{})
}

func TestCoreErrorContractKeepsKindsAndMessages(t *testing.T) {
	cause := errors.New("boom")
	if err := classify(ceph.ErrNativeFailure, cause); !errors.Is(err, ceph.ErrNativeFailure) || !errors.Is(err, cause) {
		t.Fatalf("classify = %v", err)
	}
	if classify(ceph.ErrNativeFailure, nil) != nil {
		t.Fatal("classify wrapped a nil error")
	}
	err := invalidRequest("tenant %s is released", "t1")
	var op *ceph.OperationError
	if !errors.Is(err, ceph.ErrInvalidRequest) || !errors.As(err, &op) || op.Err.Error() != "tenant t1 is released" {
		t.Fatalf("invalidRequest = %#v", err)
	}
	if got := classifyDocker(t.Context(), nil); got != nil {
		t.Fatalf("classifyDocker of a nil error = %v", got)
	}
}
