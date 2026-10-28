package faulthook

import (
	"reflect"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

type onlyAdmit struct {
	hooks
	admitted *int
}

func (o onlyAdmit) Admit(*ceph.Container, string) error { *o.admitted++; return nil }

func TestAssertedHooksHaveNoTestAccessMethod(t *testing.T) {
	typ := reflect.TypeFor[hooks]()
	for i := range typ.NumMethod() {
		if name := typ.Method(i).Name; strings.HasPrefix(name, "Test") {
			t.Errorf("test-only method %s re-entered the asserted production interface", name)
		}
	}
	if reflect.TypeFor[testHooks]().NumMethod() == 0 {
		t.Fatal("the test-access interface is empty")
	}
}

func TestProductionCallDoesNotNeedTestAccess(t *testing.T) {
	saved := hookreg.Fault
	defer func() { hookreg.Fault = saved }()

	calls := 0
	hookreg.Fault = onlyAdmit{admitted: &calls}
	if _, ok := hookreg.Fault.(testHooks); ok {
		t.Fatal("the stub unexpectedly satisfies the test-access interface")
	}
	if err := Admit(nil, "op"); err != nil || calls != 1 {
		t.Fatalf("Admit: err=%v calls=%d", err, calls)
	}
	defer func() {
		if recover() == nil {
			t.Error("TestFaults ran against hooks without test access")
		}
	}()
	TestFaults(nil, ContainerPause)
}

func TestRegisteredHooksSatisfyBothContracts(t *testing.T) {
	if _, ok := hookreg.Fault.(hooks); !ok {
		t.Error("package ceph registered fault hooks that do not satisfy the production contract")
	}
	if _, ok := hookreg.Fault.(testHooks); !ok {
		t.Error("package ceph registered fault hooks that do not satisfy the test-access contract")
	}
}
