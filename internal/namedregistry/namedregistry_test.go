package namedregistry

import (
	"slices"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

type value struct{ secret string }

func store() func(string, func() any) any {
	m := map[string]any{}
	return func(kind string, create func() any) any {
		if v, ok := m[kind]; ok {
			return v
		}
		m[kind] = create()
		return m[kind]
	}
}

func TestFromCreatesOneRegistryPerKind(t *testing.T) {
	ext := store()
	a := From[*value](ext, "k")
	if a == nil || a.ByName == nil || From[*value](ext, "k") != a {
		t.Fatal("registry not created once per kind")
	}
	if From[*value](ext, "other") == a {
		t.Fatal("two kinds share a registry")
	}
}

func TestSortedOrdersByName(t *testing.T) {
	r := From[*value](store(), "k")
	x, y, z := &value{"x"}, &value{"y"}, &value{"z"}
	r.ByName["b"], r.ByName["c"], r.ByName["a"] = y, z, x
	if got := r.Sorted(); !slices.Equal(got, []*value{x, y, z}) {
		t.Fatalf("Sorted = %v", got)
	}
	got := r.Sorted()
	got[0] = nil
	if r.ByName["a"] != x {
		t.Fatal("Sorted result aliases the map")
	}
}

func TestUndescribedRegistryReportsNothing(t *testing.T) {
	r := From[*value](store(), "k")
	r.ByName["a"] = &value{"s"}
	if r.Secrets() != nil || len(r.ConfigTargets()) != 0 {
		t.Fatal("an undescribed registry reported state")
	}
}

func TestDescribeFiltersTargets(t *testing.T) {
	r := From[*value](store(), "k").Describe(
		func(v *value) []string { return []string{v.secret} },
		func(name string, v *value) (string, testcontainers.Container) { return "", nil })
	r.ByName["a"] = &value{"s"}
	if got := r.Secrets(); !slices.Equal(got, []string{"s"}) {
		t.Fatalf("Secrets = %v", got)
	}
	if len(r.ConfigTargets()) != 0 {
		t.Fatal("a nil container became a target")
	}
}
