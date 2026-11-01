package monstore

import (
	"context"
	"errors"
	"go/ast"
	"slices"
	"strings"
	"testing"
)

func TestMonStoreStateBeginPublishesOneRecordPerMON(t *testing.T) {
	var s State
	if _, err := s.Begin(""); err == nil || s.count() != 0 {
		t.Fatalf("a record without a MON name was published: %v", err)
	}
	a, err := s.Begin("a")
	if err != nil || s.Store("a") != a || s.count() != 1 || s.volumeName("a") != a.name || a.Phase != Planned {
		t.Fatalf("begin: %v", err)
	}
	if _, err := s.Begin("a"); err == nil || s.Store("a") != a || s.count() != 1 {
		t.Fatalf("a second store was allocated for the same MON: %v", err)
	}
	b, err := s.Begin("b")
	if err != nil || b.name == a.name || b.owner == a.owner {
		t.Fatalf("two MONs share a volume or an owner token: %v", err)
	}
	if got := s.names(); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("names = %v", got)
	}
	if s.volumeName("c") != "" || s.Store("c") != nil {
		t.Fatal("an unpublished MON has a volume")
	}
	if err := s.DiscardFresh(t.Context(), "c"); err == nil {
		t.Fatal("a MON without a store was discarded")
	}
}

func TestMonStoreStateWiresTheDockerSeamAtBegin(t *testing.T) {
	s, r, d := newMonStoreFixture()
	if client, err := r.client(t.Context()); err != nil || client != monStoreDocker(d) {
		t.Fatalf("begin did not wire the Docker seam: %v", err)
	}
	_ = s
}

func TestMonStoreStateDiscardFreshForgetsOnlyAfterSuccess(t *testing.T) {
	s, r, d := newMonStoreFixture()
	if err := r.Create(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.dispatch(t.Context()); err != nil {
		t.Fatal(err)
	}
	d.existing["ctr-1"] = true
	_ = r.recordContainer("ctr-1")
	if err := s.DiscardFresh(t.Context(), "a"); err == nil || s.Store("a") != r {
		t.Fatalf("a live fresh store was forgotten: %v", err)
	}
	delete(d.existing, "ctr-1")
	busy := errors.New("volume is in use")
	d.removeErr = busy
	if err := s.DiscardFresh(t.Context(), "a"); !errors.Is(err, busy) || s.Store("a") != r {
		t.Fatalf("a failed discard forgot the record: %v", err)
	}
	d.removeErr = nil
	if err := s.DiscardFresh(t.Context(), "a"); err != nil || s.Store("a") != nil {
		t.Fatalf("discard: %v", err)
	}
	// The retry of a failed first start publishes a new record and volume.
	again, err := s.Begin("a")
	if err != nil || again == r || again.name == r.name {
		t.Fatalf("the retry reused the discarded store: %v", err)
	}
}

func TestMonStoreStateReleaseStagesForgetOnlyAfterSuccessAndKeepTheOthers(t *testing.T) {
	d := &monStoreDockerFake{existing: map[string]bool{}}
	s := &State{monDocker: func(context.Context) (monStoreDocker, error) { return d, nil }}
	// Two unrequested records settle without Docker; one record with a live
	// container does not.
	for _, mon := range []string{"c", "a", "b"} {
		if _, err := s.Begin(mon); err != nil {
			t.Fatal(err)
		}
	}
	live := s.Store("b")
	live.Attachment = &Attachment{generation: 1, ID: "ctr-b"}
	live.Phase = Bound
	d.existing["ctr-b"] = true
	live.createRequested = true
	live.createdAt = "synthetic-created-identity"
	d.stored.Name, d.stored.Driver, d.stored.CreatedAt = live.name, "local", "synthetic-created-identity"
	d.stored.Labels = map[string]string{monStoreOwnerLabel: live.owner, monStoreMonLabel: "b"}

	err := s.ReleaseStores(t.Context())
	if err == nil || s.count() != 1 || s.Store("b") != live || d.removals != 0 {
		t.Fatalf("stage 1: %v, left %v, removals %d", err, s.names(), d.removals)
	}
	// Stage 2 repeats only after a failure, and returns a clean previous as it is.
	if got := s.RetryStores(t.Context(), nil); got != nil || s.count() != 1 {
		t.Fatalf("retry ran for a stage that had not failed: %v", got)
	}
	stage1 := err
	if got := s.RetryStores(t.Context(), stage1); got == nil || s.count() != 1 {
		t.Fatalf("retry released a live store: %v", got)
	}
	delete(d.existing, "ctr-b")
	if got := s.RetryStores(t.Context(), stage1); got != nil || s.count() != 0 || d.removals != 1 || d.removeOptions.Force {
		t.Fatalf("retry: %v, left %v, removals %d", got, s.names(), d.removals)
	}
	// An empty state returns previous untouched and opens no client.
	opened := 0
	empty := &State{monDocker: func(context.Context) (monStoreDocker, error) { opened++; return d, nil }}
	previous := errors.New("earlier failure")
	if got := empty.RetryStores(t.Context(), previous); got != previous || opened != 0 {
		t.Fatalf("retry of an empty state: %v", got)
	}
	if err := empty.ReleaseStores(t.Context()); err != nil || opened != 0 {
		t.Fatalf("release of an empty state: %v, clients %d", err, opened)
	}
}

func TestMonStoreStateReleaseOrderIsByMONName(t *testing.T) {
	d := &monStoreDockerFake{existing: map[string]bool{}}
	s := &State{monDocker: func(context.Context) (monStoreDocker, error) { return d, nil }}
	for _, mon := range []string{"c", "a", "b"} {
		r, _ := s.Begin(mon)
		r.Attachment = &Attachment{ID: "ctr-" + mon}
		d.existing["ctr-"+mon] = true
	}
	d.users = nil
	err := s.ReleaseStores(t.Context())
	if err == nil {
		t.Fatal("live stores were released")
	}
	order := strings.Split(err.Error(), "\n")
	if len(order) != 3 || order[0][:5] != "MON a" || order[1][:5] != "MON b" || order[2][:5] != "MON c" {
		t.Fatalf("errors are not in MON name order: %q", order)
	}
}

// TestMonStoreStateSeamHasNoProductionWriter guards that nothing outside tests
// assigns the Docker seam.
func TestMonStoreStateSeamHasNoProductionWriter(t *testing.T) {
	var problems []string
	for file, parsed := range parseProductionFiles(t) {
		ast.Inspect(parsed, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.AssignStmt:
				for _, lhs := range x.Lhs {
					if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "monDocker" {
						problems = append(problems, file+": assigns the seam monDocker")
					}
				}
			case *ast.KeyValueExpr:
				if id, ok := x.Key.(*ast.Ident); ok && id.Name == "monDocker" {
					problems = append(problems, file+": sets the seam monDocker")
				}
			}
			return true
		})
	}
	if len(problems) != 0 {
		t.Fatalf("a production path writes a test seam: %v", problems)
	}
}

// TestMonStoreVolumeFieldsAreAssignedOnlyByTheirTransitions guards the state
// machine in the MON store files: each field of the record is assigned only by
// the methods listed, and the Docker calls sit in the one method that may make
// them, so no path other than create makes a volume and none other than
// release removes one. Other files of the package are not inspected: they have
// fields of the same names.
func TestMonStoreVolumeFieldsAreAssignedOnlyByTheirTransitions(t *testing.T) {
	allowed := map[string][]string{
		"Phase":           {"Create", "dispatch", "bind", "ready", "detach", "release"},
		"createRequested": {"Create"},
		"generation":      {"dispatch"},
		"Attachment":      {"dispatch", "confirmGone"},
		"identity":        {"bind", "detach"},
		"verified":        {"dispatch", "detach", "verify", "verifyStopped"},
		"createdAt":       {"Create"},
	}
	docker := map[string][]string{
		"VolumeCreate":     {"Create"},
		"VolumeRemove":     {"release"},
		"ContainerInspect": {"confirmGone"},
		"ContainerList":    {"confirmGone"},
	}
	var problems []string
	assigned := map[string]map[string]bool{}
	called := map[string]map[string]bool{}
	for file, parsed := range parseProductionFiles(t) {
		_ = file
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			where := receiverTypeName(fn) + "." + fn.Name.Name
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.AssignStmt:
					for _, lhs := range x.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && allowed[sel.Sel.Name] != nil {
							if assigned[sel.Sel.Name] == nil {
								assigned[sel.Sel.Name] = map[string]bool{}
							}
							assigned[sel.Sel.Name][where] = true
						}
					}
				case *ast.IncDecStmt:
					if sel, ok := x.X.(*ast.SelectorExpr); ok && allowed[sel.Sel.Name] != nil {
						if assigned[sel.Sel.Name] == nil {
							assigned[sel.Sel.Name] = map[string]bool{}
						}
						assigned[sel.Sel.Name][where] = true
					}
				case *ast.CallExpr:
					if sel, ok := x.Fun.(*ast.SelectorExpr); ok && docker[sel.Sel.Name] != nil {
						if called[sel.Sel.Name] == nil {
							called[sel.Sel.Name] = map[string]bool{}
						}
						called[sel.Sel.Name][where] = true
					}
				}
				return true
			})
		}
	}
	check := func(what string, found map[string]map[string]bool, table map[string][]string) {
		for key, methods := range table {
			for where := range found[key] {
				ok := false
				for _, m := range methods {
					ok = ok || where == "Volume."+m
				}
				if !ok {
					problems = append(problems, where+" "+what+" "+key)
				}
			}
			if len(found[key]) == 0 {
				problems = append(problems, "no use of "+key+" found: the guard is vacuous")
			}
		}
	}
	check("assigns", assigned, allowed)
	check("calls", called, docker)
	if len(problems) != 0 {
		t.Fatalf("the MON store state machine is bypassed: %v", problems)
	}
}

// Retire releases the record of a removed MON and forgets it only when the
// release succeeded, so a failed removal can be retried.
func TestMonStoreStateRetireForgetsOnlyAfterSuccess(t *testing.T) {
	s, r, d := newMonStoreFixtureFor("b")
	if err := r.Create(t.Context()); err != nil {
		t.Fatal(err)
	}
	busy := errors.New("volume is in use")
	d.removeErr = busy
	if err := s.Retire(t.Context(), "b"); !errors.Is(err, busy) || s.Store("b") != r {
		t.Fatalf("a failed release dropped the record: %v", err)
	}
	d.removeErr = nil
	if err := s.Retire(t.Context(), "b"); err != nil || s.Store("b") != nil || d.removals != 2 {
		t.Fatalf("retire: %v, removals %d", err, d.removals)
	}
	if err := s.Retire(t.Context(), "b"); err != nil {
		t.Fatalf("retiring a forgotten MON: %v", err)
	}
}
