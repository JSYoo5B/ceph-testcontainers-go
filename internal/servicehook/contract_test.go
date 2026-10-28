package servicehook

import (
	"reflect"
	"slices"
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
}

// testOnlyAccessors are accessors that only test files call. They belong to the
// unit-test seam (internal/testhelper), not to the asserted production contract
// nor to the exported wrappers of this package.
var testOnlyAccessors = []string{"TenantCluster"}

func TestAssertedHooksHaveNoTestOnlyAccessor(t *testing.T) {
	names := methodNames(reflect.TypeFor[hooks]())
	for _, accessor := range testOnlyAccessors {
		if slices.Contains(names, accessor) {
			t.Errorf("test-only accessor %s re-entered the asserted production interface", accessor)
		}
	}
}

func TestRegisteredHooksDoNotImplementTestOnlyAccessors(t *testing.T) {
	registrationMu.Lock()
	defer registrationMu.Unlock()
	names := methodNames(reflect.TypeOf(hookreg.Value))
	for _, accessor := range testOnlyAccessors {
		if slices.Contains(names, accessor) {
			t.Errorf("the registered production hooks still implement test-only accessor %s", accessor)
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
