package ceph

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestOSDRegistryZeroValueIsEmptyAndWritable(t *testing.T) {
	var r osdRegistry
	if r.get(0) != nil || r.count() != 0 || r.detachedOf(0) != nil || len(r.sorted()) != 0 {
		t.Fatalf("an empty registry reports a handle: %+v", r)
	}
	for range r.each() {
		t.Fatal("an empty registry iterated a handle")
	}
	r.forget(3) // deleting from a nil map is a no-op
	osd := &OSDContainer{ID: 3}
	r.register(3, osd)
	if r.get(3) != osd || r.count() != 1 {
		t.Fatalf("register did not record the handle: %+v", r)
	}
	r.forget(3)
	if r.get(3) != nil || r.count() != 0 {
		t.Fatalf("forget did not drop the handle: %+v", r)
	}
}

func TestOSDRegistrySortedListsByID(t *testing.T) {
	var r osdRegistry
	for _, id := range []int{5, 1, 3, 0, 4, 2} {
		r.register(id, &OSDContainer{ID: id})
	}
	var ids []int
	for _, osd := range r.sorted() {
		ids = append(ids, osd.ID)
	}
	if !slices.Equal(ids, []int{0, 1, 2, 3, 4, 5}) {
		t.Fatalf("sorted = %v", ids)
	}
}

func TestOSDRegistryEachAllowsForgettingTheVisitedKeyAndStopsEarly(t *testing.T) {
	var r osdRegistry
	for id := range 4 {
		r.register(id, &OSDContainer{ID: id})
	}
	visited := 0
	for id := range r.each() {
		visited++
		r.forget(id)
	}
	if visited != 4 || r.count() != 0 {
		t.Fatalf("visited %d, %d left", visited, r.count())
	}
	for id := range 4 {
		r.register(id, &OSDContainer{ID: id})
	}
	visited = 0
	for range r.each() {
		visited++
		break
	}
	if visited != 1 || r.count() != 4 {
		t.Fatalf("an early break visited %d and left %d", visited, r.count())
	}
}

// TestOSDRegistryParkAndUnparkMoveAHandleBetweenTheMaps pins the invariant that
// a handle is owned or detached, never both: park and unpark each change both
// maps in one call.
func TestOSDRegistryParkAndUnparkMoveAHandleBetweenTheMaps(t *testing.T) {
	var r osdRegistry
	original := &OSDContainer{ID: 7}
	r.register(7, original)
	r.register(8, &OSDContainer{ID: 8})
	detached := &DetachedOSD{id: 7}
	r.park(7, detached)
	if r.get(7) != nil || r.detachedOf(7) != detached || r.count() != 1 || r.get(8) == nil {
		t.Fatalf("park: owned=%v detached=%v", r.owned, r.detached)
	}
	reopened := &OSDContainer{ID: 7}
	r.unpark(7, reopened)
	if r.get(7) != reopened || r.detachedOf(7) != nil || r.count() != 2 {
		t.Fatalf("unpark: owned=%v detached=%v", r.owned, r.detached)
	}
	// An unpark on a registry that never parked anything still records the
	// owned descriptor.
	var empty osdRegistry
	empty.unpark(1, original)
	if empty.get(1) != original || empty.detachedOf(1) != nil {
		t.Fatalf("unpark on an empty registry: %+v", empty)
	}
}

func TestOSDRegistryReplaceSwapsTheOwnedDescriptorsOnly(t *testing.T) {
	var r osdRegistry
	r.register(0, &OSDContainer{ID: 0})
	detached := &DetachedOSD{id: 9}
	r.park(9, detached)
	next := map[int]*OSDContainer{4: {ID: 4}}
	r.replace(next)
	if r.get(0) != nil || r.get(4) == nil || r.count() != 1 || r.detachedOf(9) != detached {
		t.Fatalf("replace: owned=%v detached=%v", r.owned, r.detached)
	}
	r.replace(nil)
	if r.count() != 0 {
		t.Fatalf("replace(nil) left %d", r.count())
	}
	r.register(1, &OSDContainer{ID: 1}) // the registry stays writable after replace(nil)
	if r.count() != 1 {
		t.Fatal("register after replace(nil) did not record")
	}
}

// osdRegistryMethods pins the component: the calls each method makes. None
// takes a lock or calls into Container.
var osdRegistryMethods = map[string][]string{
	"get":        nil,
	"count":      {"len"},
	"each":       {"yield"},
	"sorted":     {"make", "len", "append", "slices.SortFunc"},
	"register":   {"make"},
	"forget":     {"delete"},
	"detachedOf": nil,
	"park":       {"make", "delete"},
	"unpark":     {"delete", "r.register"},
	"replace":    nil,
	// The lookups that need only the registry: the caller holds Container.mu,
	// as for every other method.
	"current":          {"r.get", "r.detachedOf", "invalid", "invalid", "invalid", "fmt.Errorf", "fmt.Errorf", "fmt.Errorf"},
	"ownedState":       {"resolveOwnedOSDState", "registrationOf", "r.get"},
	"placementDomains": {"make", "r.each", "osd.Placement", "strconv.Itoa", "len"},
}

// osdRegistryFieldWriters pins which methods assign each field.
var osdRegistryFieldWriters = map[string][]string{
	"owned":    {"register", "replace"},
	"detached": {"park"},
}

func osdRegistryUse(sel *ast.SelectorExpr) bool { return sel.Sel.Name == "osdHandles" }

func isOSDRegistryReceiver(fn *ast.FuncDecl) bool { return receiverTypeName(fn) == "osdRegistry" }

// TestOSDRegistryFieldIsOnlyTouchedByItsMethods guards the ownership written
// on osdRegistry: outside this type, Container.osdHandles is used only as the
// receiver of one of its methods (so no code reads or writes either map, or
// takes its address), and the lock-free FactsOf source never mentions it.
func TestOSDRegistryFieldIsOnlyTouchedByItsMethods(t *testing.T) {
	fieldAudit{label: "Container.osdHandles", use: osdRegistryUse, own: isOSDRegistryReceiver, methods: osdRegistryMethods, bound: osdFleetBinding}.check(t)
}

// TestOSDRegistryMethodsMakeOnlyTheirPinnedCalls guards the lock contract of
// the type: the method set is pinned, each method makes exactly the calls
// listed (none locks or calls into Container), and each map is assigned only
// by the methods that own the transition.
func TestOSDRegistryMethodsMakeOnlyTheirPinnedCalls(t *testing.T) {
	methodAudit{label: "osdRegistry", own: isOSDRegistryReceiver, methods: osdRegistryMethods, writers: osdRegistryFieldWriters}.check(t)
}

// osdRegistryCallers pins every function that uses the registry or sits
// between such a function and its lock (see stateCaller). "init" rows run
// before the cluster is shared: the composition planner and the unit-test
// cluster builder.
var osdRegistryCallers = map[string]stateCaller{
	// Run's composition planner: a throwaway Container no one else sees.
	"prepareInitialComposition": {"init", []string{"count", "each", "placementDomains", "register"}},
	"UnitCluster":               {"init", []string{"register"}},
	"UnitReplaceOSDs":           {"hook", []string{"replace"}},
	"UnitOSDCount":              {"hook", []string{"count"}},
	"OwnedOSD":                  {"hook", []string{"get"}},

	"registerOSD":                      {"holds", []string{"each", "register"}},
	"registerBlockDeviceOSD":           {"holds", nil},
	"removeOSD":                        {"takes", []string{"current", "forget"}},
	"checkOSDRemovable":                {"holds", []string{"each", "ownedState"}},
	"drainAndPurgeOSD":                 {"holds", nil},
	"terminate":                        {"takes", []string{"count"}},
	"terminateOSDs":                    {"holds", []string{"each", "forget"}},
	"detachOSD":                        {"takes", []string{"current", "park"}},
	"reopenOSD":                        {"takes", []string{"detachedOf", "get", "unpark"}},
	"ownedBy":                          {"holds", []string{"get"}},
	"ownsDaemon":                       {"holds", []string{"get"}},
	"daemonKeyrings":                   {"holds", []string{"get"}},
	"generationLocked":                 {"holds", []string{"get"}},
	"validateOSDPlacement":             {"holds", []string{"count", "each"}},
	"nativePoolDomains":                {"holds", []string{"get"}},
	"ownsNativeTarget":                 {"holds", []string{"each"}},
	"monitorConfigTargets":             {"holds", []string{"each"}},
	"osdStates":                        {"takes", []string{"count", "each"}},
	"WaitForClean":                     {"takes", []string{"count"}},
	"OSDs":                             {"takes", []string{"sorted"}},
	"CreatePool":                       {"takes", []string{"placementDomains"}},
	"SetIn":                            {"takes", []string{"current", "ownedState"}},
	"diagnosticOSDs":                   {"holds", []string{"each"}},
	"openDebugTarget":                  {"takes", []string{"get"}},
	"PoolPlacementDomains":             {"hook", []string{"placementDomains"}},
	"Owns":                             {"hook", nil},
	"refreshMonitorConfigAfterRemoval": {"holds", nil},
	"retireMonitor":                    {"holds", nil},
	"refreshMonitorConfig":             {"holds", nil},
	"removalOSDMutation":               {"holds", nil},
	"waitOSDRemoval":                   {"holds", nil},
	"removalOSDState":                  {"holds", []string{"ownedState"}},
}

// osdRegistryAcquirePos returns where fn first takes Container.mu: a
// lockTopology call, a mu.Lock or mu.TryLock call, or a mu.TryLock method
// value handed to a helper.
func osdRegistryAcquirePos(fn *ast.FuncDecl) token.Pos {
	at := token.NoPos
	note := func(p token.Pos) {
		if at == token.NoPos || p < at {
			at = p
		}
	}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			if isTopologyAcquire(x) {
				note(x.Pos())
			}
		case *ast.SelectorExpr:
			if inner, ok := x.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "mu" && x.Sel.Name == "TryLock" {
				note(x.Pos())
			}
		}
		return true
	})
	return at
}

// osdRegistryDocs maps each production function name to its lower-cased,
// whitespace-normalized doc comment. parseProductionFiles drops comments, so
// the files are parsed again with them.
func osdRegistryDocs(t *testing.T) map[string]string {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	docs := map[string]string{}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range parsed.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Doc != nil {
				docs[fn.Name.Name] += " " + strings.ToLower(strings.Join(strings.Fields(fn.Doc.Text()), " "))
			}
		}
	}
	return docs
}

// calleeName is the name a call goes by, for a plain or selector call.
func calleeName(call *ast.CallExpr) string {
	switch f := call.Fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return f.Sel.Name
	}
	return ""
}

// TestOSDRegistryCallersAreAudited guards the callers. The set of functions
// that use the registry, the methods each one calls and how each is guarded
// are pinned in osdRegistryCallers. A "takes" function acquires Container.mu
// before its first use. A "holds" function has a doc comment that says so, and
// every call to it, found by name over the production files, comes from a
// function that acquires the lock before the call or is itself a "holds",
// "hook" or "init" row of the table. This is the lock domain written on the
// type: Container.mu, with no lock of the registry's own.
func TestOSDRegistryCallersAreAudited(t *testing.T) {
	callerAudit{label: "the OSD registry", use: osdRegistryUse, own: isOSDRegistryReceiver, callers: osdRegistryCallers, only: map[string][]string{"init": {"prepareInitialComposition", "UnitCluster"}}}.check(t)
}

// TestOSDRegistryHasNoAddressTakenAndTestHooksExposeNoMap guards the finding
// of the audit: the unit-test hook used to publish &c.osds, a pointer to the
// map itself that any importing test could write without Container.mu. Nothing
// takes the address of the registry or of one of its maps, and the exported
// test state view has no member that points at OSD descriptors.
func TestOSDRegistryHasNoAddressTakenAndTestHooksExposeNoMap(t *testing.T) {
	var problems []string
	for file, parsed := range parseProductionFiles(t) {
		ast.Inspect(parsed, func(n ast.Node) bool {
			un, ok := n.(*ast.UnaryExpr)
			if !ok || un.Op != token.AND {
				return true
			}
			ast.Inspect(un.X, func(m ast.Node) bool {
				if sel, ok := m.(*ast.SelectorExpr); ok && (osdRegistryUse(sel) || sel.Sel.Name == "owned" || sel.Sel.Name == "detached") && !inOSDFleetBinding(parsed, un.Pos()) {
					problems = append(problems, file+": takes the address of "+types.ExprString(un.X))
				}
				return true
			})
			return true
		})
	}
	// The seam's state accessor must not return OSD descriptors (it fails the
	// test itself if it does).
	unitStateResults(t)
	if len(problems) != 0 {
		t.Fatalf("the OSD registry is exposed: %v", problems)
	}
}

// TestOSDRegistryReplaceHasTheSingleCallerTheTestHook guards that replace, the
// only method that swaps the whole owned map, is reached from production only
// through the unit-test hook.
func TestOSDRegistryReplaceHasTheSingleCallerTheTestHook(t *testing.T) {
	var callers []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || isOSDRegistryReceiver(fn) {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok && calleeName(call) == "replace" {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						if recv, ok := sel.X.(*ast.SelectorExpr); ok && osdRegistryUse(recv) {
							callers = append(callers, fn.Name.Name)
						}
					}
				}
				return true
			})
		}
	}
	if !slices.Equal(callers, []string{"UnitReplaceOSDs"}) {
		t.Fatalf("osdHandles.replace callers = %v, want only UnitReplaceOSDs", callers)
	}
}

// TestTerminateDropsTheOSDsBeforeTheStorageReleaseDecidesOnTheirCount guards
// the Terminate order the registry takes part in: the loop that terminates and
// forgets each OSD runs before the first storage release stage, which is told
// whether the registry is empty, and the second stage reads the count again
// after the labeled containers are gone.
func TestTerminateDropsTheOSDsBeforeTheStorageReleaseDecidesOnTheirCount(t *testing.T) {
	for _, parsed := range parseProductionFilesWithTerminateSteps(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isContainerTerminate(fn) {
				continue
			}
			var forgetAt, releaseAt, labeledAt, retryAt token.Pos
			counts := 0
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch calleeName(call) {
				case "forget":
					forgetAt = call.Pos()
				case "releaseMemory":
					releaseAt = call.Pos()
				case "removeLabeledContainers":
					labeledAt = call.Pos()
				case "retryMemory":
					retryAt = call.Pos()
				case "count":
					counts++
				}
				return true
			})
			if forgetAt == token.NoPos || releaseAt == token.NoPos || labeledAt == token.NoPos || retryAt == token.NoPos ||
				!(forgetAt < releaseAt && releaseAt < labeledAt && labeledAt < retryAt) || counts != 4 {
				t.Fatalf("terminate must forget each OSD, release the storage (count read for both release calls), remove labeled containers, then retry (count read for both retry calls): forget=%v release=%v labeled=%v retry=%v counts=%d", forgetAt, releaseAt, labeledAt, retryAt, counts)
			}
			return
		}
	}
	t.Fatal("Container.terminate not found")
}
