package cephfs

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// Decision F1 (issue #313): the unexported name captured from the validated
// Config before publication is the only identity every registry key, ownership
// predicate and native command target uses. These controls pin the behavior
// that changed with it. Editing the exported descriptor copy must not redirect
// WaitReady or ScaleMDS, which no admission step guards.

// WaitReady judges readiness from the filesystem it created. Before the
// decision it followed an edited FilesystemName to the sibling, whose daemon is
// not owned, and could not become ready.
func TestWaitReadyUsesCreationIdentityNotEditedDescriptor(t *testing.T) {
	fs, control, _, _ := cephFSContextAdmissionFixture()
	control.Output["fs dump --format json"] = `{"filesystems":[` +
		`{"id":41,"mdsmap":{"fs_name":"fixture","max_mds":1,"metadata_pool":1,"data_pools":[2,3],"info":{"gid_101":{"name":"a","rank":0,"gid":101,"state":"up:active","join_fscid":41}}}},` +
		`{"id":99,"mdsmap":{"fs_name":"sibling","max_mds":1,"metadata_pool":1,"data_pools":[2],"info":{"gid_202":{"name":"b","rank":0,"gid":202,"state":"up:active","join_fscid":99}}}}],"standbys":[]}`
	fs.FilesystemName = "sibling"
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady followed the edited FilesystemName: %v", err)
	}
}

// ScaleMDS keyed its ownership check by the exported copy, so an edit made it
// refuse with the ownership text. It now passes that check with the creation
// identity and goes on to read native state (the fixture has no pool map, so
// it stops there with a different error).
func TestScaleMDSOwnershipUsesCreationIdentityNotEditedDescriptor(t *testing.T) {
	const ownership = "cephfs must be an initialized filesystem owned by a running cluster"
	fs, _, _, _ := cephFSContextAdmissionFixture()
	if err := fs.ScaleMDS(t.Context(), 1, 0); err != nil && err.Error() == ownership {
		t.Fatalf("the control is refused before the edit: %v", err)
	}
	fs.FilesystemName = "sibling"
	if err := fs.ScaleMDS(t.Context(), 1, 0); err != nil && err.Error() == ownership {
		t.Fatalf("ScaleMDS ownership followed the edited FilesystemName: %v", err)
	}
	assertCephFSContextAdmissionReleased(t, fs.id.cluster)
}

// F5: ScaleMDS replaces the whole config struct under the cluster mutex while
// the snapshot-schedule methods run without either lock. They read the
// immutable name, so overlapping them is race free. Run it with -race: before
// the name was separated, the unlocked read of config.Name was a data race.
func TestScheduleNameReadDoesNotRaceWithConfigReplacement(t *testing.T) {
	fs := snapScheduleFilesystem(newFakeSnapSchedule())
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 200 {
			servicehook.Lock(fs.id.cluster)
			desired := fs.id.config
			desired.ActiveMDS = i%2 + 1
			fs.id.config = desired
			servicehook.Unlock(fs.id.cluster)
		}
	}()
	go func() {
		defer wg.Done()
		for range 200 {
			if _, err := fs.id.volumes().snapshotScheduleStatuses(t.Context(), "/"); err != nil {
				t.Errorf("status read failed: %v", err)
				return
			}
		}
	}()
	wg.Wait()
}

// The identity is read from the identity's name everywhere. The only reads of
// the exported copy are the descriptor-integrity refusals listed here, through
// a Filesystem receiver or through the MDS fleet's descriptor, which
// Filesystem.fleet binds to the exported field exactly once; no code reads the
// name through config (an unlocked read of config is the F5 question). The list
// is exact so a new reader is reviewed.
func TestFilesystemIdentityIsReadOnlyFromImmutableName(t *testing.T) {
	allowedExported := []string{"beginSubvolumeOperation", "beginSessionOperation", "checkStoppedMDSOwner", "scaleColdMDS"}
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var exportedReads, bindings []string
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			bound := map[*ast.SelectorExpr]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if unary, ok := n.(*ast.UnaryExpr); ok && unary.Op == token.AND {
					if sel, ok := unary.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "FilesystemName" {
						bound[sel] = true
						bindings = append(bindings, fn.Name.Name)
					}
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if inner, ok := sel.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "config" && sel.Sel.Name == "Name" {
					t.Errorf("%s: %s reads the name through config", fset.Position(sel.Pos()), fn.Name.Name)
				}
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "fs" && sel.Sel.Name == "FilesystemName" && !bound[sel] {
					exportedReads = append(exportedReads, fn.Name.Name)
				}
				if inner, ok := sel.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "descriptor" && sel.Sel.Name == "name" {
					exportedReads = append(exportedReads, fn.Name.Name)
				}
				return true
			})
		}
	}
	for _, name := range exportedReads {
		if !slices.Contains(allowedExported, name) {
			t.Errorf("%s reads the exported FilesystemName copy; use the immutable name", name)
		}
	}
	for _, name := range allowedExported {
		if !slices.Contains(exportedReads, name) {
			t.Errorf("%s no longer reads the exported copy; shrink the allow-list", name)
		}
	}
	if !slices.Equal(bindings, []string{"fleet"}) {
		t.Errorf("the exported FilesystemName is bound by address in %v, want only Filesystem.fleet", bindings)
	}
}
