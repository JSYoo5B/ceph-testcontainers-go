package ceph

import (
	"testing"
)

func TestCRUSHRuleDumpIsDecodedStrictly(t *testing.T) {
	for _, tc := range []struct{ name, dump string }{
		{"missing rule_id", `[{"rule_id":0,"rule_name":"replicated_rule"},{"rule_name":"tc-p-replicated"}]`},
		{"missing rule_name", `[{"rule_id":0}]`},
		{"empty rule_name", `[{"rule_id":0,"rule_name":""}]`},
		{"negative rule_id", `[{"rule_id":-1,"rule_name":"r"}]`},
		{"duplicate rule_id", `[{"rule_id":0,"rule_name":"a"},{"rule_id":0,"rule_name":"b"}]`},
		{"duplicate rule_name", `[{"rule_id":0,"rule_name":"a"},{"rule_id":1,"rule_name":"a"}]`},
		{"not a list", `{"rule_id":0,"rule_name":"a"}`},
		{"null", `null`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			control := &poolFixtureContainer{Output: map[string]string{"osd crush rule dump --format json": tc.dump}}
			c := &Container{Container: control}
			if names, err := (poolCatalog{c.conn()}).crushRuleNames(t.Context()); err == nil {
				t.Fatalf("Pool.Remove's rule names accepted %s: %v", tc.dump, names)
			}
			if _, err := (poolCatalog{c.conn()}).rulePlacement(t.Context(), 0); err == nil {
				t.Fatal("the policy reader accepted the dump")
			}
			if _, _, err := (poolCatalog{c.conn()}).placementRule(t.Context(), "p", PoolConfig{CRUSHRoot: "default", FailureDomain: "osd"}); err == nil {
				t.Fatal("the relocation reader accepted the dump")
			}
		})
	}
}

func TestCRUSHRuleDumpRunsOncePerRelocationLookup(t *testing.T) {
	desired := PoolConfig{CRUSHRoot: "default", FailureDomain: "osd"}
	hashed := poolPlacementRuleName("p", desired)
	control := &poolFixtureContainer{Output: map[string]string{"osd crush rule dump --format json": `[` +
		`{"rule_id":0,"rule_name":"replicated_rule"},` +
		`{"rule_id":1,"rule_name":"tc-p-replicated","steps":[{"op":"take","item_name":"default"},{"op":"chooseleaf_firstn","type":"host","num":0},{"op":"emit"}]},` +
		`{"rule_id":2,"rule_name":"` + hashed + `","steps":[{"op":"take","item_name":"default"},{"op":"choose_firstn","type":"osd","num":0},{"op":"emit"}]}]`}}
	c := &Container{Container: control}
	name, exists, err := (poolCatalog{c.conn()}).placementRule(t.Context(), "p", desired)
	if err != nil || !exists || name != hashed {
		t.Fatalf("placementRule = %q %v %v, want the existing %q", name, exists, err, hashed)
	}
	if len(control.Calls) != 1 {
		t.Fatalf("the rule dump ran %d times: %v", len(control.Calls), control.Calls)
	}
}

func TestPoolKind(t *testing.T) {
	for native, want := range map[int]string{1: "replicated", 3: "erasure"} {
		if got, err := poolKind(native); err != nil || got != want {
			t.Errorf("poolKind(%d) = %q %v", native, got, err)
		}
	}
	for _, native := range []int{0, 2, 4} {
		if _, err := poolKind(native); err == nil {
			t.Errorf("poolKind(%d) accepted", native)
		}
	}
}
