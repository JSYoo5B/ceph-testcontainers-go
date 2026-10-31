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
// it, with the methods it calls. The type has no lock: every fact is written
// before the cluster is shared, by Run's "record" row, and only read
// afterwards, by the "fact" rows. A new row needs a new review of that claim.
var startupIdentityCallers = map[string]stateCaller{
	"bootstrapFirstMonitor":   {"record", []string{"recordFirstMonitorImage", "recordKeyTypeOption"}},
	"monitorJoinOptions":      {"fact", []string{"ownedLabel"}},
	"startJoiningMonitor":     {"fact", []string{"ownedLabel"}},
	"OwnedLabel":              {"fact", []string{"ownedLabel"}},
	"TerminateOnCleanup":      {"fact", []string{"ownerID"}},
	"checkDualStackAddresses": {"fact", []string{"firstMonitorImage"}},
	"startedContainer":        {"fact", []string{"ownedLabel"}},
	"ensureControlPlane":      {"fact", []string{"ownedLabel"}},
	"ensureOSDMemory":         {"fact", []string{"ownedLabel"}},
	"ensureOSDVolume":         {"fact", []string{"ownedLabel"}},
	"generateKeyArgs":         {"fact", []string{"firstMonitorImage", "supportsKeyType"}},
	"newOSDLaunchRecipe":      {"fact", []string{"ownerID"}},
	"reserveTrackedHostPorts": {"fact", []string{"ownedLabel"}},
	"runMonitor":              {"fact", []string{"ownedLabel"}},
	"Create":                  {"fact", []string{"ownedLabel"}},
	"monitorGenerations":      {"fact", []string{"ownedLabel"}},
	"startManager":            {"fact", []string{"ownedLabel"}},
	"startService":            {"fact", []string{"ownedLabel"}},
	"startVerification":       {"fact", []string{"ownerID"}},
	"terminate":               {"fact", []string{"ownerID", "removeLabeledContainers"}},
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
	literals := 0
	fieldAudit{label: "Container.startup", use: startupUse, own: isStartupReceiver, methods: startupIdentityMethods, bound: stateBinding,
		counted: func(fn *ast.FuncDecl, sel *ast.SelectorExpr) bool {
			return isContainerReceiver(sel.X) || receiverTypeName(fn) == "osdFleet" || receiverTypeName(fn) == "featureClient"
		},
		allow: func(file string, fn *ast.FuncDecl, n ast.Node, _ map[*ast.SelectorExpr]bool) []string {
			var problems []string
			switch x := n.(type) {
			case *ast.CompositeLit:
				if id, ok := x.Type.(*ast.Ident); !ok || id.Name != "Container" {
					return nil
				}
				for _, elt := range x.Elts {
					kv, ok := elt.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					key, ok := kv.Key.(*ast.Ident)
					if !ok {
						continue
					}
					switch key.Name {
					case "startup":
						literals++
						if fn.Name.Name != "newCluster" || file != "ceph.go" {
							problems = append(problems, file+": "+fn.Name.Name+" sets startup in a Container literal")
						}
					case "owner", "keyTypeOption", "firstMonitorImageID", "ownedContainerDocker":
						problems = append(problems, file+": "+fn.Name.Name+" sets the removed field "+key.Name)
					}
				}
			case *ast.UnaryExpr:
				if x.Op == token.AND {
					if sel, ok := x.X.(*ast.SelectorExpr); ok && (startupUse(sel) || startupUse2(sel)) && !stateBinding(fn) {
						problems = append(problems, file+": takes the address of "+types.ExprString(x.X))
					}
				}
			}
			return problems
		}}.check(t)
	if literals != 1 {
		t.Fatalf("Run's Container literal sets the startup identity %d times, want once", literals)
	}
}

// TestStartupIdentityMethodsMakeOnlyTheirPinnedCalls guards the lock contract
// of the type: the method set is pinned, each method makes exactly the calls
// listed, none names Container or a mutex, and a field is assigned only by its
// recorder.
func TestStartupIdentityMethodsMakeOnlyTheirPinnedCalls(t *testing.T) {
	writers := map[string][]string{}
	for field := range startupIdentityFields {
		writers[field] = nil
	}
	for method, field := range startupIdentityWriters {
		if _, ok := startupIdentityMethods[method]; !ok {
			t.Fatalf("%s is a writer row without a pinned method", method)
		}
		writers[field] = append(writers[field], method)
	}
	methodAudit{label: "startupIdentity", own: isStartupReceiver, methods: startupIdentityMethods, writers: writers,
		render: func(call *ast.CallExpr) string {
			if name := types.ExprString(call.Fun); name != "newClient" {
				return name
			}
			return "newClient(ctx)"
		},
		forbidden: []string{"Container", "closed", "controlPlane", "settings", "bootstrap"}, forbiddenMembers: []string{"mu", "Lock", "Unlock", "TryLock", "lockTopology"}}.check(t)
}

// TestStartupIdentityCallersAreAudited is the caller-table guard: the functions
// that use the component and the methods each calls are pinned, the two
// recorders are called by Run only and exactly once each, and a stale row
// fails.
func TestStartupIdentityCallersAreAudited(t *testing.T) {
	callerAudit{label: "the startup identity", use: startupUse, own: isStartupReceiver, callers: startupIdentityCallers,
		only: map[string][]string{"record": {"bootstrapFirstMonitor"}}}.check(t)
	// Each recorder is called exactly once, by Run's startup phase.
	recorded := map[string]int{}
	for _, parsed := range parseProductionFiles(t) {
		ast.Inspect(parsed, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					if recv, ok := sel.X.(*ast.SelectorExpr); ok && startupUse(recv) {
						recorded[sel.Sel.Name]++
					}
				}
			}
			return true
		})
	}
	for recorder := range startupIdentityWriters {
		if recorded[recorder] != 1 {
			t.Errorf("%s must be called exactly once, by Run", recorder)
		}
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

// startupPhaseCallers are the Run phases that write what a cluster records
// once at startup, each with its one caller. The recorder guards pin those
// writes to the phases; this keeps the phases themselves inside Run, before
// the cluster is returned.
var startupPhaseCallers = map[string]string{
	"newCluster":            "Run",
	"bootstrapFirstMonitor": "Run",
	"recordCephVersion":     "Container.bootstrapFirstMonitor",
}

// TestStartupPhasesHaveTheirSingleCaller guards that each startup phase is
// called exactly once, from its listed caller.
func TestStartupPhasesHaveTheirSingleCaller(t *testing.T) {
	calls := map[string][]string{}
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				name := ""
				switch f := call.Fun.(type) {
				case *ast.Ident:
					name = f.Name
				case *ast.SelectorExpr:
					name = f.Sel.Name
				}
				if _, phase := startupPhaseCallers[name]; phase {
					calls[name] = append(calls[name], funcKey(fn))
				}
				return true
			})
		}
	}
	for phase, caller := range startupPhaseCallers {
		if got := calls[phase]; len(got) != 1 || got[0] != caller {
			t.Errorf("%s is called from %v, want once from %s", phase, got, caller)
		}
	}
}
