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

func TestAssertedHooksHaveNoTestAccessMethod(t *testing.T) {
	for _, name := range methodNames(reflect.TypeFor[hooks]()) {
		if strings.HasPrefix(name, "Test") {
			t.Errorf("test-only method %s re-entered the asserted production interface", name)
		}
	}
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

var registrationMu sync.Mutex

func TestProductionCallDoesNotNeedTestAccess(t *testing.T) {
	registrationMu.Lock()
	defer registrationMu.Unlock()
	saved := hookreg.Value
	defer func() { hookreg.Value = saved }()

	calls := 0
	hookreg.Value = onlyLock{locked: &calls}
	if _, ok := hookreg.Value.(testHooks); ok {
		t.Fatal("the stub unexpectedly satisfies the test-access interface")
	}
	Lock(nil)
	if calls != 1 {
		t.Fatalf("Lock reached the hooks %d times", calls)
	}
	defer func() {
		if recover() == nil {
			t.Error("a test accessor ran against hooks without test access")
		}
	}()
	TestOSDCount(nil)
}

func TestRegisteredHooksSatisfyBothContracts(t *testing.T) {
	registrationMu.Lock()
	defer registrationMu.Unlock()
	if _, ok := hookreg.Value.(hooks); !ok {
		t.Error("package ceph registered hooks that do not satisfy the production contract")
	}
	if _, ok := hookreg.Value.(testHooks); !ok {
		t.Error("package ceph registered hooks that do not satisfy the test-access contract")
	}
}
