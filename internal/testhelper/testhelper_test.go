package testhelper

import (
	"reflect"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

func TestSeamHoldsOnlyUnitMethods(t *testing.T) {
	typ := reflect.TypeFor[unitHooks]()
	if typ.NumMethod() == 0 {
		t.Fatal("the unit-test seam is empty")
	}
	for i := range typ.NumMethod() {
		if name := typ.Method(i).Name; !strings.HasPrefix(name, "Unit") {
			t.Errorf("seam method %s does not start with Unit", name)
		}
	}
}

func TestPackageCephRegistersTheSeam(t *testing.T) {
	if _, ok := hookreg.Test.(unitHooks); !ok {
		t.Fatal("package ceph registered no unit-test seam that satisfies the helper's interface")
	}
}

func TestMissingSeamPanics(t *testing.T) {
	saved := hookreg.Test
	defer func() { hookreg.Test = saved }()
	hookreg.Test = nil
	defer func() {
		if recover() == nil {
			t.Error("a helper function ran without a registered seam")
		}
	}()
	TestOSDCount(nil)
}

func TestMissingHarnessSlotPanicsForTheTenantClusterAccessor(t *testing.T) {
	saved := hookreg.HarnessTenant
	defer func() { hookreg.HarnessTenant = saved }()
	hookreg.HarnessTenant = nil
	defer func() {
		msg, _ := recover().(string)
		if !strings.Contains(msg, "ceph/harness did not register") {
			t.Errorf("TestTenantCluster without the harness slot recovered %q, want a panic naming ceph/harness", msg)
		}
	}()
	TestTenantCluster(nil)
}
