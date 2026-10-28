//go:build test_support

package faulthook

import (
	"reflect"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

func TestTestAccessInterfaceIsNotEmpty(t *testing.T) {
	if reflect.TypeFor[testHooks]().NumMethod() == 0 {
		t.Fatal("the test-access interface is empty")
	}
}

func TestTestAccessNeedsTheTestHooks(t *testing.T) {
	saved := hookreg.Fault
	defer func() { hookreg.Fault = saved }()

	calls := 0
	hookreg.Fault = onlyAdmit{admitted: &calls}
	if _, ok := hookreg.Fault.(testHooks); ok {
		t.Fatal("the stub unexpectedly satisfies the test-access interface")
	}
	defer func() {
		if recover() == nil {
			t.Error("TestFaults ran against hooks without test access")
		}
	}()
	TestFaults(nil, ContainerPause)
}

func TestRegisteredHooksSatisfyTestAccessContract(t *testing.T) {
	if _, ok := hookreg.Fault.(testHooks); !ok {
		t.Error("package ceph registered fault hooks that do not satisfy the test-access contract")
	}
}
