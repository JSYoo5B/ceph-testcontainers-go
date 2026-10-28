//go:build test_support

package servicehook

import (
	"reflect"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

func TestTestAccessInterfaceHoldsOnlyTestMethods(t *testing.T) {
	names := methodNames(reflect.TypeFor[testHooks]())
	if len(names) == 0 {
		t.Fatal("the test-access interface is empty")
	}
	for _, name := range names {
		if !strings.HasPrefix(name, "Test") {
			t.Errorf("production method %s is in the test-access interface", name)
		}
	}
}

func TestTestAccessNeedsTheTestHooks(t *testing.T) {
	registrationMu.Lock()
	defer registrationMu.Unlock()
	saved := hookreg.Value
	defer func() { hookreg.Value = saved }()

	calls := 0
	hookreg.Value = onlyLock{locked: &calls}
	if _, ok := hookreg.Value.(testHooks); ok {
		t.Fatal("the stub unexpectedly satisfies the test-access interface")
	}
	defer func() {
		if recover() == nil {
			t.Error("a test accessor ran against hooks without test access")
		}
	}()
	TestOSDCount(nil)
}

func TestRegisteredHooksSatisfyTestAccessContract(t *testing.T) {
	registrationMu.Lock()
	defer registrationMu.Unlock()
	if _, ok := hookreg.Value.(testHooks); !ok {
		t.Error("package ceph registered hooks that do not satisfy the test-access contract")
	}
}
