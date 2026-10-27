package ceph

import (
	"errors"
	"testing"
)

func TestOverrideAdmissionKeepsMethodPolicies(t *testing.T) {
	// Every override operation is exclusive; the table is pinned entry by entry.
	for op, expected := range map[string]sharedPolicy{
		"override.AddBlocklist":       sharedExclusive,
		"override.SetDebugLevel":      sharedExclusive,
		"override.SetMGRModule":       sharedExclusive,
		"override.SetFullRatios":      sharedExclusive,
		"override.SetOSDFlag":         sharedExclusive,
		"override.SetConfig":          sharedExclusive,
		"override.SetMessengerPolicy": sharedExclusive,
	} {
		if policy, ok := sharedOverridePolicies[op]; !ok || policy != expected {
			t.Errorf("%s has policy %v, want %v", op, policy, expected)
		}
	}
	if len(sharedOverridePolicies) != 7 {
		t.Errorf("sharedOverridePolicies has %d entries, want the seven pinned entries", len(sharedOverridePolicies))
	}
	c := &Container{}
	if err := c.admitOverride("override.AddBlocklist"); err != nil {
		t.Fatalf("a cluster of its own refused an override: %v", err)
	}
	if err := c.admitOverride("override.Unknown"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("an unclassified override was admitted: %v", err)
	}
	for opName := range sharedOverridePolicies {
		c.sharing.Store(int32(sharedWithTenants))
		err := c.admitOverride(opName)
		var op *OperationError
		if !errors.Is(err, ErrSharedCluster) || !errors.As(err, &op) || op.Op != opName {
			t.Fatalf("a shared cluster admitted %s: %v", opName, err)
		}
		c.sharing.Store(int32(sharedExclusively))
		if err := c.admitOverride(opName); err != nil {
			t.Fatalf("an exclusive lease refused %s: %v", opName, err)
		}
		c.sharing.Store(int32(0))
	}
}

func TestOverrideKindsAreRegistryKinds(t *testing.T) {
	for name, kind := range overrideKindNames {
		if faultKinds[kind].phase != faultTracked {
			t.Errorf("override kind %s is undone by Terminate", name)
		}
	}
}

func TestProtectMGRModuleHookRefusesOwnedVolumesBeforeAnyCommand(t *testing.T) {
	// The cluster has no control container: a command would fail differently.
	c := &Container{extensions: map[string]any{"cephfs": testExtension{modules: []string{"volumes"}}}}
	err := overrideHooks{}.ProtectMGRModule(t.Context(), c, "volumes")
	if err == nil || err.Error() != "MGR volumes module is required by configured or owned CephFS filesystems" {
		t.Fatalf("an owned filesystem's volumes module was not protected: %v", err)
	}
	// A module that nothing protects passes without a command.
	if err := (overrideHooks{}).ProtectMGRModule(t.Context(), c, "prometheus"); err != nil {
		t.Fatalf("an unprotected module was refused: %v", err)
	}
}

func TestOverrideHooksValidateMGRModuleNames(t *testing.T) {
	for name, want := range map[string]bool{"mirroring": true, "rbd_support": true, "": false, "--force": false, "Bad": false, "a b": false} {
		if got := (overrideHooks{}).ValidMGRModuleName(name); got != want {
			t.Errorf("ValidMGRModuleName(%q) = %v, want %v", name, got, want)
		}
	}
}
