package ceph

import (
	"errors"
	"testing"
)

func TestOverrideAdmissionKeepsMethodPolicies(t *testing.T) {
	// The Container methods are gone, so the policy of each is pinned here.
	for op, expected := range map[string]sharedPolicy{
		"override.AddBlocklist":  sharedExclusive, // TemporaryBlocklist
		"override.SetFullRatios": sharedExclusive, // OverrideFullRatios
	} {
		if policy, ok := sharedOverridePolicies[op]; !ok || policy != expected {
			t.Errorf("%s has policy %v, want %v", op, policy, expected)
		}
	}
	if len(sharedOverridePolicies) != 2 {
		t.Errorf("sharedOverridePolicies has %d entries, want the one that is pinned", len(sharedOverridePolicies))
	}
	c := &Container{}
	if err := c.admitOverride("override.AddBlocklist"); err != nil {
		t.Fatalf("a cluster of its own refused an override: %v", err)
	}
	if err := c.admitOverride("override.Unknown"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("an unclassified override was admitted: %v", err)
	}
	c.sharing.Store(int32(sharedWithTenants))
	err := c.admitOverride("override.AddBlocklist")
	var op *OperationError
	if !errors.Is(err, ErrSharedCluster) || !errors.As(err, &op) || op.Op != "override.AddBlocklist" {
		t.Fatalf("a shared cluster admitted an override: %v", err)
	}
	c.sharing.Store(int32(sharedExclusively))
	if err := c.admitOverride("override.AddBlocklist"); err != nil {
		t.Fatalf("an exclusive lease refused an override: %v", err)
	}
}

func TestOverrideKindsAreRegistryKinds(t *testing.T) {
	for name, kind := range overrideKindNames {
		if faultKinds[kind].phase != faultTracked {
			t.Errorf("override kind %s is undone by Terminate", name)
		}
	}
}
