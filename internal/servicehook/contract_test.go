package servicehook

import (
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

// onlyLock satisfies hooks (through the embedded nil interface) and nothing
// else. Calling any method it does not define panics with a nil dereference.
type onlyLock struct {
	hooks
	locked *int
}

func (o onlyLock) Lock(*ceph.Container) { *o.locked++ }

func methodNames(t reflect.Type) []string {
	names := make([]string, 0, t.NumMethod())
	for i := range t.NumMethod() {
		names = append(names, t.Method(i).Name)
	}
	return names
}

// This file runs in every build. The assertions about the test-access
// interface need the test_support build tag and are in
// contract_testsupport_test.go.

func TestAssertedHooksHaveNoTestAccessMethod(t *testing.T) {
	for _, name := range methodNames(reflect.TypeFor[hooks]()) {
		if strings.HasPrefix(name, "Test") {
			t.Errorf("test-only method %s re-entered the asserted production interface", name)
		}
	}
}

var registrationMu sync.Mutex

func TestProductionCallDoesNotNeedTestAccess(t *testing.T) {
	registrationMu.Lock()
	defer registrationMu.Unlock()
	saved := hookreg.Value
	defer func() { hookreg.Value = saved }()

	calls := 0
	hookreg.Value = onlyLock{locked: &calls}
	Lock(nil)
	if calls != 1 {
		t.Fatalf("Lock reached the hooks %d times", calls)
	}
}

func TestRegisteredHooksSatisfyProductionContract(t *testing.T) {
	registrationMu.Lock()
	defer registrationMu.Unlock()
	if _, ok := hookreg.Value.(hooks); !ok {
		t.Error("package ceph registered hooks that do not satisfy the production contract")
	}
}
