package ceph

import (
	"context"
	"errors"
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"slices"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

func TestStartupIdentityZeroValueIsInert(t *testing.T) {
	var s startupIdentity
	if s.ownerID() != "" || s.supportsKeyType() || s.firstMonitorImage() != "" {
		t.Fatalf("a zero identity reports %+v", s)
	}
	// A cluster without an owner ID lists nothing, so a seam that fails the
	// test when it is reached is never reached.
	s.ownedContainerDocker = func(context.Context) (ownedContainerClient, error) {
		return nil, errors.New("listed the containers of a cluster without an owner")
	}
	if err := s.removeLabeledContainers(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestStartupIdentityRecordsEachFactOnce(t *testing.T) {
	s := newStartupIdentity("owner-a")
	if s.ownerID() != "owner-a" || s.supportsKeyType() || s.firstMonitorImage() != "" {
		t.Fatalf("a new identity reports %+v", s)
	}
	s.recordKeyTypeOption(true)
	if !s.supportsKeyType() || s.firstMonitorImage() != "" || s.ownerID() != "owner-a" {
		t.Fatalf("recordKeyTypeOption changed another fact: %+v", s)
	}
	s.recordFirstMonitorImage("sha256:essential")
	if s.firstMonitorImage() != "sha256:essential" || !s.supportsKeyType() || s.ownerID() != "owner-a" {
		t.Fatalf("recordFirstMonitorImage changed another fact: %+v", s)
	}
	s.recordKeyTypeOption(false)
	if s.supportsKeyType() {
		t.Fatal("the key-type fact was not replaced by a later recording")
	}
}

// The label carries the owner ID and nothing else, and two clusters never share
// one.
func TestStartupIdentityLabelCarriesTheOwner(t *testing.T) {
	label := func(owner string) map[string]string {
		req := &testcontainers.GenericContainerRequest{}
		id := newStartupIdentity(owner)
		if err := id.ownedLabel()(req); err != nil {
			t.Fatal(err)
		}
		return req.Labels
	}
	if got := label("a"); len(got) != 1 || got[clusterOwnerLabel] != "a" {
		t.Fatalf("labels = %v", got)
	}
	if label("a")[clusterOwnerLabel] == label("b")[clusterOwnerLabel] {
		t.Fatal("two owners produced the same label")
	}
}

// The removal lists by the owner's label only and reports a failed listing.
func TestStartupIdentityRemovalListsByItsOwnLabel(t *testing.T) {
	docker := &fakeOwnedDocker{containers: map[string]map[string]string{
		"mine":  {clusterOwnerLabel: "a"},
		"other": {clusterOwnerLabel: "b"},
	}}
	s := newStartupIdentity("a")
	s.ownedContainerDocker = func(context.Context) (ownedContainerClient, error) { return docker, nil }
	if err := s.removeLabeledContainers(t.Context()); err != nil || docker.containers["mine"] != nil || docker.containers["other"] == nil {
		t.Fatalf("removal returned %v and left %v", err, docker.containers)
	}
	boom := errors.New("synthetic client failure")
	s.ownedContainerDocker = func(context.Context) (ownedContainerClient, error) { return nil, boom }
	if err := s.removeLabeledContainers(t.Context()); !errors.Is(err, boom) {
		t.Fatalf("a client failure was not reported: %v", err)
	}
}

// startupIdentityMethods pins the component: the calls each method makes. None
// takes a lock or calls into Container; only removeLabeledContainers reaches
// Docker, through the seam or the default client.
var startupIdentityMethods = map[string][]string{
	"recordKeyTypeOption":     nil,
	"recordFirstMonitorImage": nil,
	"ownerID":                 nil,
	"supportsKeyType":         nil,
	"firstMonitorImage":       nil,
	"ownedLabel":              {"testcontainers.WithLabels"},
	"removeLabeledContainers": {"newClient(ctx)", "testcontainers.NewDockerClientWithOpts", "fmt.Errorf", "fmt.Errorf", "fmt.Errorf", "(func() literal)", "docker.Close", "docker.ContainerList", "make(mobycl.Filters).Add", "make", "docker.ContainerRemove", "onlyMissingHostResource", "append", "errors.Join"},
}

// startupIdentityWriters lists the only methods that assign a field, and the
// field each assigns.
var startupIdentityWriters = map[string]string{
	"recordKeyTypeOption":     "keyTypeOption",
	"recordFirstMonitorImage": "firstMonitorImageID",
}

// startupIdentityCallers lists every function outside the component that uses
// it, with the methods it calls. The type has no lock, so there is no lock
// column: every fact is written before the cluster is shared (Run) and read
// only afterwards. A new row needs a new review of that claim.
var startupIdentityCallers = map[string][]string{
	"Run":                     {"recordFirstMonitorImage", "recordKeyTypeOption"},
	"AddMonitor":              {"ownedLabel"},
	"OwnedLabel":              {"ownedLabel"},
	"TerminateOnCleanup":      {"ownerID"},
	"checkDualStackAddresses": {"firstMonitorImage"},
	"clientContainerContext":  {"ownedLabel"},
	"ensureControlPlane":      {"ownedLabel"},
	"ensureOSDMemory":         {"ownedLabel"},
	"ensureOSDVolume":         {"ownedLabel"},
	"generateKeyArgs":         {"firstMonitorImage", "supportsKeyType"},
	"newOSDLaunchRecipe":      {"ownerID"},
	"reserveTrackedHostPorts": {"ownedLabel"},
	"runMonitor":              {"ownedLabel"},
	"startManager":            {"ownedLabel"},
	"startService":            {"ownedLabel"},
	"startVerification":       {"ownerID"},
	"terminate":               {"ownerID", "removeLabeledContainers"},
}

var startupIdentityFields = map[string]bool{"owner": true, "keyTypeOption": true, "firstMonitorImageID": true, "ownedContainerDocker": true}

// startupUse2 reports a selector of a member of Container.startup.
func startupUse2(sel *ast.SelectorExpr) bool {
	inner, ok := sel.X.(*ast.SelectorExpr)
	return ok && startupUse(inner)
}

func startupUse(sel *ast.SelectorExpr) bool { return sel.Sel.Name == "startup" }

func isStartupReceiver(fn *ast.FuncDecl) bool { return receiverTypeName(fn) == "startupIdentity" }

// TestContainerDeclaresNoStartupIdentityFields pins that the moved fields and
// the test seam are gone from Container and that the component has exactly its
// four members and no lock.
func TestContainerDeclaresNoStartupIdentityFields(t *testing.T) {
	got := structFieldNames(t, "ceph.go", "Container")
	for _, gone := range []string{"firstMonitorImageID", "keyTypeOption", "ownedContainerDocker", "owner"} {
		if slices.Contains(got, gone) {
			t.Fatalf("Container still declares %s", gone)
		}
	}
	if !slices.Contains(got, "startup") {
		t.Fatal("Container does not declare the startup identity")
	}
	if fields := structFieldNames(t, "startup_identity.go", "startupIdentity"); !slices.Equal(fields, []string{"firstMonitorImageID", "keyTypeOption", "ownedContainerDocker", "owner"}) {
		t.Fatalf("startupIdentity fields = %v", fields)
	}
}

// TestStartupIdentityFieldIsOnlyTouchedByItsMethods guards the ownership
// written on startupIdentity: outside this type, Container.startup is only the
// receiver of one of its methods, so no code reads or assigns a member or takes
// the address of the component; the only production Container literal that sets
// it is Run's.
func TestStartupIdentityFieldIsOnlyTouchedByItsMethods(t *testing.T) {
	var problems []string
	uses := 0
	literals := 0
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || isStartupReceiver(fn) {
				continue
			}
			allowed := map[*ast.SelectorExpr]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					if method, ok := x.Fun.(*ast.SelectorExpr); ok {
						if _, known := startupIdentityMethods[method.Sel.Name]; known {
							if recv, ok := method.X.(*ast.SelectorExpr); ok && startupUse(recv) {
								allowed[recv] = true
							}
						}
					}
				case *ast.CompositeLit:
					if id, ok := x.Type.(*ast.Ident); ok && id.Name == "Container" {
						for _, elt := range x.Elts {
							if kv, ok := elt.(*ast.KeyValueExpr); ok {
								if key, ok := kv.Key.(*ast.Ident); ok {
									switch key.Name {
									case "startup":
										literals++
										if fn.Name.Name != "Run" || file != "ceph.go" {
											problems = append(problems, file+": "+fn.Name.Name+" sets startup in a Container literal")
										}
									case "owner", "keyTypeOption", "firstMonitorImageID", "ownedContainerDocker":
										problems = append(problems, file+": "+fn.Name.Name+" sets the removed field "+key.Name)
									}
								}
							}
						}
					}
				case *ast.UnaryExpr:
					if x.Op == token.AND {
						if sel, ok := x.X.(*ast.SelectorExpr); ok && (startupUse(sel) || startupUse2(sel)) {
							problems = append(problems, file+": takes the address of "+types.ExprString(x.X))
						}
					}
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && startupUse(sel) && isContainerReceiver(sel.X) {
					if allowed[sel] {
						uses++
					} else {
						problems = append(problems, file+": "+fn.Name.Name+" uses Container.startup outside a startupIdentity method call")
					}
				}
				return true
			})
		}
	}
	if uses == 0 || literals != 1 {
		problems = append(problems, "the guard is vacuous or Run's literal is missing: uses and literals found "+join([]string{itoa(uses), itoa(literals)}))
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("startup identity escapes its methods: %v", problems)
	}
}

// TestStartupIdentityMethodsMakeOnlyTheirPinnedCalls guards the lock contract
// of the type: the method set is pinned, each method makes exactly the calls
// listed, none names Container or a mutex, and a field is assigned only by its
// recorder.
func TestStartupIdentityMethodsMakeOnlyTheirPinnedCalls(t *testing.T) {
	seen := map[string]bool{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isStartupReceiver(fn) {
				continue
			}
			name := fn.Name.Name
			want, known := startupIdentityMethods[name]
			if !known {
				problems = append(problems, name+" is not pinned; review the lock contract and update startupIdentityMethods")
				continue
			}
			seen[name] = true
			var got []string
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					got = append(got, types.ExprString(x.Fun))
					if types.ExprString(x.Fun) == "newClient" {
						got[len(got)-1] = "newClient(ctx)"
					}
				case *ast.Ident:
					switch x.Name {
					case "Container", "closed", "controlPlane", "settings", "bootstrap":
						problems = append(problems, name+" names "+x.Name)
					}
				case *ast.SelectorExpr:
					switch x.Sel.Name {
					case "mu", "Lock", "Unlock", "TryLock", "lockTopology":
						problems = append(problems, name+" touches "+types.ExprString(x))
					}
				case *ast.AssignStmt:
					for _, lhs := range x.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok {
							if startupIdentityFields[sel.Sel.Name] && startupIdentityWriters[name] != sel.Sel.Name {
								problems = append(problems, name+" assigns "+sel.Sel.Name)
							}
						}
					}
				}
				return true
			})
			if !slices.Equal(slices.Sorted(slices.Values(got)), slices.Sorted(slices.Values(want))) {
				problems = append(problems, name+" calls "+join(got)+", pinned "+join(want))
			}
		}
	}
	for name := range startupIdentityMethods {
		if !seen[name] {
			problems = append(problems, name+" is pinned but missing")
		}
	}
	for name := range startupIdentityWriters {
		if _, ok := startupIdentityMethods[name]; !ok {
			problems = append(problems, name+" is a writer row without a pinned method")
		}
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("startupIdentity breaks its lock contract: %v", problems)
	}
}

// TestStartupIdentityCallersAreAudited is the caller-table guard: the functions
// that use the component and the methods each calls are pinned, the two
// recorders are called by Run only and exactly once each, and a stale row
// fails.
func TestStartupIdentityCallersAreAudited(t *testing.T) {
	found := map[string]map[string]bool{}
	recorded := map[string]int{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || isStartupReceiver(fn) {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				recv, ok := sel.X.(*ast.SelectorExpr)
				if !ok || !startupUse(recv) {
					return true
				}
				name := fn.Name.Name
				if found[name] == nil {
					found[name] = map[string]bool{}
				}
				found[name][sel.Sel.Name] = true
				if _, writer := startupIdentityWriters[sel.Sel.Name]; writer {
					recorded[sel.Sel.Name]++
					if name != "Run" {
						problems = append(problems, name+" calls the recorder "+sel.Sel.Name+"; only Run may")
					}
				}
				return true
			})
		}
	}
	for name, methods := range found {
		want, known := startupIdentityCallers[name]
		if !known {
			problems = append(problems, name+" uses the startup identity but is not in startupIdentityCallers")
			continue
		}
		got := slices.Sorted(func(yield func(string) bool) {
			for m := range methods {
				if !yield(m) {
					return
				}
			}
		})
		if !slices.Equal(got, want) {
			problems = append(problems, name+" calls "+join(got)+", pinned "+join(want))
		}
	}
	for name := range startupIdentityCallers {
		if found[name] == nil {
			problems = append(problems, name+" is listed but no longer uses the startup identity")
		}
	}
	for recorder := range startupIdentityWriters {
		if recorded[recorder] != 1 {
			problems = append(problems, recorder+" must be called exactly once, by Run")
		}
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("startup identity callers changed: %v", problems)
	}
}

// TestStartupIdentitySourceIsInTheRightFile pins the file of the component so a
// copy under another name cannot hide a second definition.
func TestStartupIdentitySourceIsInTheRightFile(t *testing.T) {
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			if gen, ok := decl.(*ast.GenDecl); ok && gen.Tok == token.TYPE {
				for _, spec := range gen.Specs {
					if ts, ok := spec.(*ast.TypeSpec); ok && ts.Name.Name == "startupIdentity" && filepath.Base(file) != "startup_identity.go" {
						t.Fatalf("startupIdentity is declared in %s", file)
					}
				}
			}
		}
	}
}
