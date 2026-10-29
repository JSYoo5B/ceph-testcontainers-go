package ceph

import (
	"testing"
)

func TestOverrideKindsAreRegistryKinds(t *testing.T) {
	for name, kind := range overrideKindNames {
		if faultKinds[kind].phase != faultTracked {
			t.Errorf("override kind %s is undone by Terminate", name)
		}
	}
}

func TestProtectMGRModuleHookRefusesOwnedVolumesBeforeAnyCommand(t *testing.T) {
	// The cluster has no control container: a command would fail differently.
	c := &Container{extensions: extensionStore{states: map[string]any{"cephfs": testExtension{modules: []string{"volumes"}}}}}
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
