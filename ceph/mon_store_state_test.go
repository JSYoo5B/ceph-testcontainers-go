package ceph

import (
	"context"
	"errors"
	"go/ast"
	"go/token"
	"go/types"
	"slices"
	"strings"
	"testing"
)

func TestMonStoreStateBeginPublishesOneRecordPerMON(t *testing.T) {
	var s monStoreState
	if _, err := s.begin(""); err == nil || s.count() != 0 {
		t.Fatalf("a record without a MON name was published: %v", err)
	}
	a, err := s.begin("a")
	if err != nil || s.store("a") != a || s.count() != 1 || s.volumeName("a") != a.name || a.phase != monStorePlanned {
		t.Fatalf("begin: %v", err)
	}
	if _, err := s.begin("a"); err == nil || s.store("a") != a || s.count() != 1 {
		t.Fatalf("a second store was allocated for the same MON: %v", err)
	}
	b, err := s.begin("b")
	if err != nil || b.name == a.name || b.owner == a.owner {
		t.Fatalf("two MONs share a volume or an owner token: %v", err)
	}
	if got := s.names(); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("names = %v", got)
	}
	if s.volumeName("c") != "" || s.store("c") != nil {
		t.Fatal("an unpublished MON has a volume")
	}
	if err := s.discardFresh(t.Context(), "c"); err == nil {
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
	if err := r.create(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.dispatch(t.Context()); err != nil {
		t.Fatal(err)
	}
	d.existing["ctr-1"] = true
	_ = r.recordContainer("ctr-1")
	if err := s.discardFresh(t.Context(), "a"); err == nil || s.store("a") != r {
		t.Fatalf("a live fresh store was forgotten: %v", err)
	}
	delete(d.existing, "ctr-1")
	busy := errors.New("volume is in use")
	d.removeErr = busy
	if err := s.discardFresh(t.Context(), "a"); !errors.Is(err, busy) || s.store("a") != r {
		t.Fatalf("a failed discard forgot the record: %v", err)
	}
	d.removeErr = nil
	if err := s.discardFresh(t.Context(), "a"); err != nil || s.store("a") != nil {
		t.Fatalf("discard: %v", err)
	}
	// The retry of a failed first start publishes a new record and volume.
	again, err := s.begin("a")
	if err != nil || again == r || again.name == r.name {
		t.Fatalf("the retry reused the discarded store: %v", err)
	}
}

func TestMonStoreStateReleaseStagesForgetOnlyAfterSuccessAndKeepTheOthers(t *testing.T) {
	d := &monStoreDockerFake{existing: map[string]bool{}}
	s := &monStoreState{monDocker: func(context.Context) (monStoreDocker, error) { return d, nil }}
	// Two unrequested records settle without Docker; one record with a live
	// container does not.
	for _, mon := range []string{"c", "a", "b"} {
		if _, err := s.begin(mon); err != nil {
			t.Fatal(err)
		}
	}
	live := s.store("b")
	live.attachment = &monStoreAttachment{generation: 1, id: "ctr-b"}
	live.phase = monStoreBound
	d.existing["ctr-b"] = true
	live.createRequested = true
	live.createdAt = "synthetic-created-identity"
	d.stored.Name, d.stored.Driver, d.stored.CreatedAt = live.name, "local", "synthetic-created-identity"
	d.stored.Labels = map[string]string{monStoreOwnerLabel: live.owner, monStoreMonLabel: "b"}

	err := s.releaseStores(t.Context())
	if err == nil || s.count() != 1 || s.store("b") != live || d.removals != 0 {
		t.Fatalf("stage 1: %v, left %v, removals %d", err, s.names(), d.removals)
	}
	// Stage 2 repeats only after a failure, and returns a clean previous as it is.
	if got := s.retryStores(t.Context(), nil); got != nil || s.count() != 1 {
		t.Fatalf("retry ran for a stage that had not failed: %v", got)
	}
	stage1 := err
	if got := s.retryStores(t.Context(), stage1); got == nil || s.count() != 1 {
		t.Fatalf("retry released a live store: %v", got)
	}
	delete(d.existing, "ctr-b")
	if got := s.retryStores(t.Context(), stage1); got != nil || s.count() != 0 || d.removals != 1 || d.removeOptions.Force {
		t.Fatalf("retry: %v, left %v, removals %d", got, s.names(), d.removals)
	}
	// An empty state returns previous untouched and opens no client.
	opened := 0
	empty := &monStoreState{monDocker: func(context.Context) (monStoreDocker, error) { opened++; return d, nil }}
	previous := errors.New("earlier failure")
	if got := empty.retryStores(t.Context(), previous); got != previous || opened != 0 {
		t.Fatalf("retry of an empty state: %v", got)
	}
	if err := empty.releaseStores(t.Context()); err != nil || opened != 0 {
		t.Fatalf("release of an empty state: %v, clients %d", err, opened)
	}
}

func TestMonStoreStateReleaseOrderIsByMONName(t *testing.T) {
	d := &monStoreDockerFake{existing: map[string]bool{}}
	s := &monStoreState{monDocker: func(context.Context) (monStoreDocker, error) { return d, nil }}
	for _, mon := range []string{"c", "a", "b"} {
		r, _ := s.begin(mon)
		r.attachment = &monStoreAttachment{id: "ctr-" + mon}
		d.existing["ctr-"+mon] = true
	}
	d.users = nil
	err := s.releaseStores(t.Context())
	if err == nil {
		t.Fatal("live stores were released")
	}
	order := strings.Split(err.Error(), "\n")
	if len(order) != 3 || order[0][:5] != "MON a" || order[1][:5] != "MON b" || order[2][:5] != "MON c" {
		t.Fatalf("errors are not in MON name order: %q", order)
	}
}

// A create reply that was lost for a container leaves a pending attachment and
// a user of the volume. Terminate removes the labeled container, and the second
// stage then releases the store in the same call.
func TestTerminateReleasesAMonStoreAfterTheLabeledContainers(t *testing.T) {
	d := &monStoreDockerFake{existing: map[string]bool{}, users: []string{"lost-mon"}}
	docker := &fakeOwnedDocker{containers: map[string]map[string]string{"lost-mon": {clusterOwnerLabel: "c"}}}
	docker.onRemove = func(id string) {
		if id == "lost-mon" {
			d.users = nil
		}
	}
	cluster := ownedCluster("c", docker)
	cluster.monStores.monDocker = func(context.Context) (monStoreDocker, error) { return d, nil }
	r, err := cluster.monStores.begin("a")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.create(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.dispatch(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Terminate(t.Context()); err != nil || cluster.monStores.count() != 0 || d.removals != 1 || d.removeOptions.Force {
		t.Fatalf("Terminate returned %v; stores %v, removals %d", err, cluster.monStores.names(), d.removals)
	}

	// Control: a user that survives the scan keeps the store owned and is reported.
	d = &monStoreDockerFake{existing: map[string]bool{}, users: []string{"survivor"}}
	cluster = ownedCluster("c", &fakeOwnedDocker{})
	cluster.monStores.monDocker = func(context.Context) (monStoreDocker, error) { return d, nil }
	r, _ = cluster.monStores.begin("a")
	_ = r.create(t.Context())
	_, _ = r.dispatch(t.Context())
	if err := cluster.Terminate(t.Context()); err == nil || cluster.monStores.count() != 1 || d.removals != 0 {
		t.Fatalf("a store with a surviving user was released: %v", err)
	}
}

// A cluster that publishes no store, which is every cluster in this slice,
// does not open a Docker client for the stores.
func TestTerminateWithoutMonStoresOpensNoStoreClient(t *testing.T) {
	cluster := ownedCluster("c", &fakeOwnedDocker{})
	cluster.monStores.monDocker = func(context.Context) (monStoreDocker, error) {
		return nil, errors.New("opened a MON store client without a store")
	}
	if err := cluster.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// monStoreMethods pins the component: the calls each method makes, receiver
// included, ignoring errors, fmt and slices helpers and the append builtin.
// None takes a lock or calls into Container, so the component stays below mu.
var monStoreMethods = map[string][]string{
	"begin":         {"newMonStoreVolume(mon)"},
	"store":         nil,
	"count":         {"len(s.records)"},
	"names":         {"yield(name)"},
	"volumeName":    nil,
	"discardFresh":  {"r.discardFresh(ctx)", "delete(s.records, mon)"},
	"releaseStores": {"s.releaseEach(ctx, false)"},
	"retryStores":   {"len(s.records)", "s.releaseEach(ctx, true)"},
	"releaseEach":   {"s.names()", "s.records[name].release(ctx, afterScan)", "delete(s.records, name)"},
}

// monStoreCallers lists every function outside the component that calls one
// of its methods in production, and how it holds Container.mu. Only terminate
// does, and only for the two release stages: no production code publishes a
// record in this slice.
var monStoreCallers = map[string]stateCaller{"terminate": {"takes", []string{"releaseStores", "retryStores"}}}

func monStoreUse(sel *ast.SelectorExpr) bool { return sel.Sel.Name == "monStores" }

func isMonStoreReceiver(fn *ast.FuncDecl) bool { return receiverTypeName(fn) == "monStoreState" }

func monStoreIgnoredCall(call *ast.CallExpr) bool {
	switch fun := call.Fun.(type) {
	case *ast.SelectorExpr:
		if pkg, ok := fun.X.(*ast.Ident); ok {
			return pkg.Name == "errors" || pkg.Name == "fmt" || pkg.Name == "slices"
		}
	case *ast.Ident:
		return fun.Name == "append"
	}
	return false
}

// TestMonStoreStateFieldsAreOnlyTouchedByItsMethods guards the ownership
// written on monStoreState: outside this type, Container.monStores is used only
// as the receiver of one of its methods, and the lock-free FactsOf source does
// not mention it.
func TestMonStoreStateFieldsAreOnlyTouchedByItsMethods(t *testing.T) {
	fieldAudit{label: "Container.monStores", use: monStoreUse, own: isMonStoreReceiver, methods: monStoreMethods, wholeFactsFile: true}.check(t)
}

// TestMonStoreStateMethodsMakeOnlyTheirPinnedCalls pins the method set and the
// calls of each method, and that only begin assigns the record map.
func TestMonStoreStateMethodsMakeOnlyTheirPinnedCalls(t *testing.T) {
	methodAudit{label: "monStoreState", own: isMonStoreReceiver, methods: monStoreMethods, writers: map[string][]string{"records": {"begin"}},
		render: func(call *ast.CallExpr) string {
			if monStoreIgnoredCall(call) {
				return ""
			}
			return types.ExprString(call)
		}}.check(t)
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

// TestMonStoreStateCallersHoldTheTopologyLock is the caller table guard: the
// set of production functions that use the component is exactly monStoreCallers,
// they call only the release stages, and each calls lockTopology before its
// first use. A later slice that adds a caller updates the table in its own diff.
func TestMonStoreStateCallersHoldTheTopologyLock(t *testing.T) {
	callerAudit{label: "the MON store state", use: monStoreUse, own: isMonStoreReceiver, callers: monStoreCallers}.check(t)
}

// TestTerminateReleasesMonStoresInTheirTwoStagesInOrder guards the Terminate
// order of the component: the first stage after the added and the primary MON
// containers are terminated and before the control plane, the second after the
// labeled-container removal and after the OSD retry stages, before the run
// volume.
func TestTerminateReleasesMonStoresInTheirTwoStagesInOrder(t *testing.T) {
	for _, parsed := range parseProductionFilesWithTerminateSteps(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isContainerTerminate(fn) {
				continue
			}
			at := map[string][]token.Pos{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					at[types.ExprString(call)] = append(at[types.ExprString(call)], call.Pos())
				}
				return true
			})
			order := []string{
				"c.lockTopology(ctx)",
				"c.fleet.stores.releaseVolume(ctx, c.fleet.osdHandles.count() == 0)",
				"c.Container.Terminate(ctx, opts...)",
				"c.monStores.releaseStores(ctx)",
				"c.controlPlane.terminate(ctx, opts...)",
				"c.startup.removeLabeledContainers(ctx)",
				"c.fleet.stores.retryVolume(ctx, volumeErr, c.fleet.osdHandles.count() == 0)",
				"c.monStores.retryStores(ctx, monStoreErr)",
				"c.runDir.release(ctx)",
			}
			var problems []string
			for _, name := range order {
				if len(at[name]) != 1 {
					problems = append(problems, name+" must be called exactly once in terminate")
				}
			}
			if len(problems) == 0 {
				for i := 1; i < len(order); i++ {
					if at[order[i-1]][0] >= at[order[i]][0] {
						problems = append(problems, order[i-1]+" must come before "+order[i])
					}
				}
			}
			if len(problems) != 0 {
				t.Fatalf("terminate changed the MON store release order: %v", problems)
			}
			return
		}
	}
	t.Fatal("Container.terminate not found")
}

// TestMonStoreVolumeFieldsAreAssignedOnlyByTheirTransitions guards the state
// machine in the MON store files: each field of the record is assigned only by
// the methods listed, and the Docker calls sit in the one method that may make
// them, so no path other than create makes a volume and none other than
// release removes one. Other files of the package are not inspected: they have
// fields of the same names.
func TestMonStoreVolumeFieldsAreAssignedOnlyByTheirTransitions(t *testing.T) {
	allowed := map[string][]string{
		"phase":           {"create", "dispatch", "bind", "ready", "detach", "release"},
		"createRequested": {"create"},
		"generation":      {"dispatch"},
		"attachment":      {"dispatch", "confirmGone"},
		"identity":        {"bind", "detach"},
		"verified":        {"dispatch", "detach", "verify", "verifyStopped"},
		"createdAt":       {"create"},
	}
	docker := map[string][]string{
		"VolumeCreate":     {"create"},
		"VolumeRemove":     {"release"},
		"ContainerInspect": {"confirmGone"},
		"ContainerList":    {"confirmGone"},
	}
	var problems []string
	assigned := map[string]map[string]bool{}
	called := map[string]map[string]bool{}
	for file, parsed := range parseProductionFiles(t) {
		if !strings.HasPrefix(file, "mon_store_") {
			continue
		}
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
					ok = ok || where == "monStoreVolume."+m
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
