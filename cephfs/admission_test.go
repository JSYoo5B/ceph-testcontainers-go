package cephfs

import (
	"context"
	"go/ast"
	"reflect"
	"slices"
	"testing"
)

// TestAdmissionKeepsEachOperationsOwnNameAuthority starts from the offline
// characterization in the owner's comment on issue #313 (synthetic FSMap with
// an owned filesystem41 and a sibling99; no native cluster) and records the
// decided identity (F1): the private name captured at creation is the only
// source of which filesystem an operation targets. Editing the exported
// FilesystemName no longer redirects MDSStatus to the sibling. The explicit
// descriptor-integrity refusal of the session admission is retained, and the
// data-pool admission still admits without a native command. It does not show
// that a whole data-pool operation succeeds.
func TestAdmissionKeepsEachOperationsOwnNameAuthority(t *testing.T) {
	fs, control, _, _ := cephFSContextAdmissionFixture()
	control.Output["fs dump --format json"] = `{"filesystems":[` +
		`{"id":41,"mdsmap":{"fs_name":"fixture","max_mds":1,"metadata_pool":1,"data_pools":[2,3],"info":{"gid_101":{"name":"a","rank":0,"gid":101,"state":"up:active","join_fscid":41}}}},` +
		`{"id":99,"mdsmap":{"fs_name":"sibling","max_mds":1,"metadata_pool":1,"data_pools":[2],"info":{"gid_202":{"name":"b","rank":0,"gid":202,"state":"up:active","join_fscid":99}}}}],"standbys":[]}`

	status, err := fs.MDSStatus(t.Context())
	if err != nil || len(status.Active) != 1 || status.Active[0].Name != "a" || !status.Active[0].Owned {
		t.Fatalf("original descriptor does not report the owned filesystem41 daemon: %+v %v", status, err)
	}

	fs.FilesystemName = "sibling"
	status, err = fs.MDSStatus(t.Context())
	if err != nil || status.FilesystemID != 41 || len(status.Active) != 1 || status.Active[0].Name != "a" || !status.Active[0].Owned {
		t.Fatalf("MDSStatus follows the edited exported FilesystemName instead of the creation identity: %+v %v", status, err)
	}
	if fs.config.Name != "fixture" || fs.base().Name() != "fixture" || fs.base().Native().ID != 41 {
		t.Fatal("the stored config, name or native identity changed with the descriptor")
	}

	if _, _, done, err := fs.beginSessionOperation(t.Context()); err == nil {
		done()
		t.Fatal("session admission accepted an edited descriptor")
	}
	assertCephFSContextAdmissionReleased(t, fs.base().Cluster())

	before := len(control.Calls)
	_, done, err := fs.beginDataPoolOperation(t.Context())
	if err != nil {
		t.Fatalf("data-pool admission refused the edited descriptor: %v", err)
	}
	done()
	if len(control.Calls) != before {
		t.Fatalf("data-pool admission issued native commands: %v", control.Calls[before:])
	}
	assertCephFSContextAdmissionReleased(t, fs.base().Cluster())

	fs.FilesystemName = "fixture"
	_, id, done, err := fs.beginSessionOperation(t.Context())
	if err != nil || id != 41 {
		t.Fatalf("restored descriptor was refused or lost the FSMap id: %d %v", id, err)
	}
	done()
	assertCephFSContextAdmissionReleased(t, fs.base().Cluster())
}

// The shared lifetime keeps the two differences that are not predicates: the
// cancellation recheck after the setup gate, and the refusal text.
func TestAdmissionKeepsRefusalTextsAndNoOpDone(t *testing.T) {
	fs, _, _, _ := cephFSContextAdmissionFixture()
	fs.base().ReplaceNative(nil)
	_, _, _, err := fs.beginSubvolumeOperation(t.Context())
	if err == nil || err.Error() != "CephFS filesystem must be an active filesystem created by this cluster" {
		t.Fatalf("subvolume refusal changed: %v", err)
	}
	_, _, _, err = fs.beginSessionOperation(t.Context())
	if err == nil || err.Error() != "CephFS filesystem must be an active filesystem created by this cluster" {
		t.Fatalf("session refusal changed: %v", err)
	}
	ctx, done, err := fs.beginDataPoolOperation(t.Context())
	if err == nil || err.Error() != "filesystem must have a confirmed creation identity in this active cluster" || ctx == nil || done == nil {
		t.Fatalf("data-pool refusal changed: %v", err)
	}
	done() // a no-op on refusal
	assertCephFSContextAdmissionReleased(t, fs.base().Cluster())
	var nilFS *Filesystem
	if _, done, err := nilFS.beginDataPoolOperation(t.Context()); err == nil || err.Error() != "CephFS filesystem is unavailable" {
		t.Fatalf("nil handle refusal changed: %v", err)
	} else {
		done()
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for name, begin := range map[string]func() error{
		"subvolume": func() error { _, _, _, err := fs.beginSubvolumeOperation(canceled); return err },
		"data pool": func() error { _, _, err := fs.beginDataPoolOperation(canceled); return err },
		"session":   func() error { _, _, _, err := fs.beginSessionOperation(canceled); return err },
	} {
		if err := begin(); err != context.Canceled {
			t.Fatalf("%s admission did not return the cancellation: %v", name, err)
		}
	}
	assertCephFSContextAdmissionReleased(t, fs.base().Cluster())
}

// TestAdmissionLifetimeHasOneOwnerAndKeepsPredicatesLocal pins the caller table
// and the division of work. The core's Admit (fsbase) owns the admission
// lifetime: it is called only by the three flows, each flow passes its own
// Check function, and none takes or releases setupMu or the topology mutex
// itself. fsbase's TestAdmitHoldsNoPredicateAndTakesTheLocksInOrder pins that
// Admit takes setupMu first and holds no predicate.
func TestAdmissionLifetimeHasOneOwnerAndKeepsPredicatesLocal(t *testing.T) {
	begins := []string{"beginSubvolumeOperation", "beginDataPoolOperation", "beginSessionOperation"}
	callers := map[string][]string{}
	lockCalls := map[string][]string{}
	files := parseCephFSProductionFiles(t)
	for _, parsed := range files {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if fn.Name.Name == "admitOperation" || fn.Name.Name == "Admit" {
				t.Errorf("package cephfs declares %s: the admission lifetime belongs to fsbase", fn.Name.Name)
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if name := admissionCallee(call); name == "Admit" {
					callers[name] = append(callers[name], fn.Name.Name)
				}
				if slices.Contains(begins, fn.Name.Name) {
					switch {
					case isSetupMuAcquire(call):
						lockCalls[fn.Name.Name] = append(lockCalls[fn.Name.Name], "setupMu")
					case isClusterLockAcquire(call):
						lockCalls[fn.Name.Name] = append(lockCalls[fn.Name.Name], "topology")
					}
				}
				return true
			})
		}
	}
	slices.Sort(callers["Admit"])
	if !reflect.DeepEqual(callers, map[string][]string{"Admit": {"beginDataPoolOperation", "beginSessionOperation", "beginSubvolumeOperation"}}) {
		t.Errorf("Admit caller table changed: %v", callers)
	}
	if len(lockCalls) != 0 {
		t.Errorf("an admission flow takes a lock itself; only Admit may: %v", lockCalls)
	}
	for _, parsed := range files {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !slices.Contains(begins, fn.Name.Name) {
				continue
			}
			checks := 0
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if kv, ok := n.(*ast.KeyValueExpr); ok {
					if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Check" {
						if _, lit := kv.Value.(*ast.FuncLit); lit {
							checks++
						}
					}
				}
				if call, ok := n.(*ast.CallExpr); ok && admissionCallee(call) == "Unlock" {
					t.Errorf("%s unlocks directly; release belongs to Admit", fn.Name.Name)
				}
				return true
			})
			if checks != 1 {
				t.Errorf("%s must pass exactly one own check function, has %d", fn.Name.Name, checks)
			}
		}
	}
}

func admissionCallee(call *ast.CallExpr) string {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		return fun.Sel.Name
	}
	return ""
}
