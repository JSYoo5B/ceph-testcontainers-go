package faulthook

import (
	"reflect"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

func TestAssertedHooksHaveNoTestAccessMethod(t *testing.T) {
	typ := reflect.TypeFor[hooks]()
	for i := range typ.NumMethod() {
		if name := typ.Method(i).Name; strings.HasPrefix(name, "Test") {
			t.Errorf("test-only method %s re-entered the asserted production interface", name)
		}
	}
}

func TestRegisteredHooksSatisfyProductionContract(t *testing.T) {
	if _, ok := hookreg.Fault.(hooks); !ok {
		t.Error("package ceph registered fault hooks that do not satisfy the production contract")
	}
}
