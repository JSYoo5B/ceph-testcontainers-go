package ceph

import (
	"context"
	"errors"
	"go/ast"
	"go/token"
	"go/types"
	"testing"

	"github.com/moby/moby/api/types/volume"
)

func TestRunDirStateWithoutAVolumeIsInert(t *testing.T) {
	var d runDirState
	if d.published() || d.shared() || d.volumeName() != "" || d.mounts() != nil {
		t.Fatalf("an empty run directory reports a volume: %+v", d)
	}
	if err := d.release(t.Context()); err != nil {
		t.Fatalf("release of an empty run directory = %v", err)
	}
}

func TestRunDirStateReleaseMarksRemovedOnlyAfterSuccess(t *testing.T) {
	docker := &runVolumeClient{volumes: map[string]volume.Volume{}}
	v := newRunVolume()
	v.newClient = func(context.Context) (osdMemoryVolumeClient, error) { return docker, nil }
	if err := v.create(t.Context()); err != nil {
		t.Fatal(err)
	}
	d := runDirState{volume: v}
	if !d.published() || !d.shared() || d.volumeName() != v.name || len(d.mounts()) != 1 {
		t.Fatalf("a created run directory is not shared: %+v", d)
	}

	// A failed release keeps the volume shared and published, so Terminate
	// can retry it and an admin socket call is not refused early.
	boom := errors.New("synthetic client failure")
	v.newClient = func(context.Context) (osdMemoryVolumeClient, error) { return nil, boom }
	if err := d.release(t.Context()); !errors.Is(err, boom) || !d.shared() || !d.published() {
		t.Fatalf("failed release = %v, shared %v", err, d.shared())
	}

	v.newClient = func(context.Context) (osdMemoryVolumeClient, error) { return docker, nil }
	if err := d.release(t.Context()); err != nil || d.shared() || len(docker.volumes) != 0 {
		t.Fatalf("release = %v, shared %v", err, d.shared())
	}
	// A removed volume stays published (the stale-socket cleanup keys on that)
	// and keeps its name for the launch recipe, but is no longer shared, and
	// a second release does not reach Docker.
	if !d.published() || d.volumeName() != v.name {
		t.Fatal("a removed volume was forgotten")
	}
	v.newClient = func(context.Context) (osdMemoryVolumeClient, error) { return nil, boom }
	if err := d.release(t.Context()); err != nil {
		t.Fatalf("second release reached Docker: %v", err)
	}
}

// runDirMethods pins the component: the calls each method makes. None takes a
// lock or calls into Container.
var runDirMethods = map[string][]string{
	"create":     {"newRunVolume()", "d.volume.create(ctx)"},
	"published":  nil,
	"volumeName": nil,
	"mounts":     {"d.volume.mount()"},
	"shared":     nil,
	"release":    {"d.volume.remove(ctx)"},
}

// runDirCallers lists every function outside the component that calls one of
// its methods, with the method it calls and how it is guarded:
//
//	"init"     Run only, before the cluster is shared (create)
//	"free"     reads only the never-reassigned pointer or the immutable name
//	"takes"    calls lockTopology before its first mu-guarded use
//	"holds"    its doc comment requires the caller to hold Container.mu
var runDirCallers = map[string]stateCaller{
	"newCluster":          {"init", []string{"create"}},
	"runMonitor":          {"free", []string{"mounts"}},
	"startedContainer":    {"free", []string{"mounts"}},
	"ensureControlPlane":  {"free", []string{"mounts"}},
	"monitorJoinOptions":  {"free", []string{"mounts"}},
	"startManager":        {"free", []string{"mounts"}},
	"clearAdminSocket":    {"free", []string{"published"}},
	"clearStaleOSDSocket": {"free", []string{"published"}},
	"newOSDLaunchRecipe":  {"free", []string{"volumeName"}},
	"adminSocket":         {"takes", []string{"shared"}},
	"set":                 {"takes", []string{"shared"}},
	"terminate":           {"takes", []string{"release"}},
}

// runDirMuGuarded names the methods that read or write the removed mark and
// so need Container.mu: their callers must be "takes" rows.
var runDirMuGuarded = []string{"shared", "release"}

func runDirUse(sel *ast.SelectorExpr) bool { return sel.Sel.Name == "runDir" }

func isRunDirReceiver(fn *ast.FuncDecl) bool { return receiverTypeName(fn) == "runDirState" }

// TestRunDirStateFieldIsOnlyTouchedByItsMethods guards the ownership written
// on runDirState: outside this type, Container.runDir is used only as the
// receiver of one of its methods, and the lock-free FactsOf source never
// mentions it.
func TestRunDirStateFieldIsOnlyTouchedByItsMethods(t *testing.T) {
	fieldAudit{label: "Container.runDir", use: runDirUse, own: isRunDirReceiver, methods: runDirMethods, bound: stateBinding}.check(t)
}

// TestRunDirStateMethodsMakeOnlyTheirPinnedCalls guards the lock contract of
// the type: the method set is pinned, each method makes exactly the calls
// listed (none locks or calls into Container), and the volume field is
// assigned only by create, which is what makes the lock-free readers safe.
func TestRunDirStateMethodsMakeOnlyTheirPinnedCalls(t *testing.T) {
	methodAudit{label: "runDirState", own: isRunDirReceiver, methods: runDirMethods,
		render:  func(call *ast.CallExpr) string { return types.ExprString(call) },
		writers: map[string][]string{"volume": {"create"}}}.check(t)
}

// TestRunDirStateCreatePublishesBeforeTheDockerCreate guards the order that
// keeps a lost create reply owned: the record is assigned before the create
// call, and the create result is returned unchanged.
func TestRunDirStateCreatePublishesBeforeTheDockerCreate(t *testing.T) {
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isRunDirReceiver(fn) || fn.Name.Name != "create" {
				continue
			}
			var assignAt, createAt token.Pos
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.AssignStmt:
					if len(x.Lhs) == 1 && types.ExprString(x.Lhs[0]) == "d.volume" && assignAt == token.NoPos {
						assignAt = x.Pos()
					}
				case *ast.CallExpr:
					if types.ExprString(x) == "d.volume.create(ctx)" && createAt == token.NoPos {
						createAt = x.Pos()
					}
				}
				return true
			})
			if assignAt == token.NoPos || createAt == token.NoPos || assignAt >= createAt {
				t.Fatal("runDirState.create must publish the volume record before the Docker create")
			}
			return
		}
	}
	t.Fatal("runDirState.create not found")
}

// TestRunDirStateCallersAreAudited guards the callers: the set of functions
// that use the component and the method each one calls are pinned, the
// "takes" callers call lockTopology before their first use, create has the
// single caller Run, and a mu-guarded method has no caller that is not in
// the table.
func TestRunDirStateCallersAreAudited(t *testing.T) {
	callerAudit{label: "the run directory", use: runDirUse, own: isRunDirReceiver, callers: runDirCallers,
		only: map[string][]string{"init": {"newCluster"}}, guarded: runDirMuGuarded}.check(t)
}

// TestTerminateReleasesTheRunDirectoryAfterTheStorageRetryAndBeforeTheBridges
// guards the Terminate order: the run volume is removed once, after the
// labeled containers (and with them every container that mounts it) are gone
// and the storage retry ran, and before the bridges.
func TestTerminateReleasesTheRunDirectoryAfterTheStorageRetryAndBeforeTheBridges(t *testing.T) {
	order := []string{"lockTopology", "removeLabeledContainers", "retryVolume", "release", "removeBridges"}
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isContainerTerminate(fn) {
				continue
			}
			at := map[string][]token.Pos{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						key := sel.Sel.Name
						if key == "release" {
							if recv, ok := sel.X.(*ast.SelectorExpr); !ok || !runDirUse(recv) {
								return true
							}
						}
						at[key] = append(at[key], call.Pos())
					}
				}
				return true
			})
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
				t.Fatalf("terminate changed the run directory release order: %v", problems)
			}
			return
		}
	}
	t.Fatal("Container.terminate not found")
}
