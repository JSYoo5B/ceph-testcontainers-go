package cephfs

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
)

func TestMDSRosterKeepsCreationOrderAndDescriptorIdentity(t *testing.T) {
	r := newMDSRoster(nil)
	if r.count() != 0 || r.issued() != 0 || len(r.snapshot()) != 0 || len(r.members()) != 0 {
		t.Fatalf("a new roster is not empty: %+v", r)
	}
	first, second := &MDS{ID: "a"}, &MDS{ID: "b"}
	twin := &MDS{ID: "a"} // same name, distinct descriptor
	r.publish(first)
	r.publish(second)
	r.publish(twin)
	if r.count() != 3 || r.at(0) != first || r.at(1) != second || r.at(2) != twin {
		t.Fatalf("creation order or descriptor identity lost: %+v", r.members())
	}
	r.remove(twin)
	if got := r.snapshot(); len(got) != 2 || got[0] != first || got[1] != second {
		t.Fatalf("remove dropped a descriptor that was not the argument: %+v", got)
	}
	r.remove(&MDS{ID: "a"}) // an equal but different descriptor is not owned
	if r.count() != 2 {
		t.Fatalf("remove matched by value instead of descriptor identity: %+v", r.members())
	}
}

func TestMDSRosterSnapshotAndCustomizersAreCopies(t *testing.T) {
	opt := testcontainers.WithEnv(map[string]string{"X": "1"})
	opts := []testcontainers.ContainerCustomizer{opt}
	r := newMDSRoster(opts)
	opts[0] = nil
	if got := r.customizers(); len(got) != 1 || got[0] == nil {
		t.Fatal("the roster aliases the caller's customizer slice")
	}
	got := r.customizers()
	got[0] = nil
	if r.customizers()[0] == nil {
		t.Fatal("customizers returned the stored slice")
	}
	d := &MDS{ID: "a"}
	r.publish(d)
	snap := r.snapshot()
	snap[0] = nil
	if r.at(0) != d {
		t.Fatal("snapshot aliases the owned slice")
	}
}

func TestMDSRosterNamesAreReservedInOrder(t *testing.T) {
	r := newMDSRoster(nil)
	if r.nextID("fs") != cephFSMDSID("fs", 0) || r.issued() != 0 {
		t.Fatalf("unexpected first name %q", r.nextID("fs"))
	}
	r.reserveIndex()
	r.reserveIndex()
	if r.issued() != 2 || r.nextID("fs") != cephFSMDSID("fs", 2) {
		t.Fatalf("index not consumed: %d %q", r.issued(), r.nextID("fs"))
	}
	r.remove(&MDS{})
	if r.issued() != 2 {
		t.Fatal("removing a daemon must not reuse its name index")
	}
}

// TestMDSRosterReadersUnderTheClusterLockAreRaceFree exercises the contract of
// the type: the roster has no lock, and the cluster mutex that every public
// reader and every writer takes is what keeps it race free.
func TestMDSRosterReadersUnderTheClusterLockAreRaceFree(t *testing.T) {
	fs, _, _, _ := cephFSContextAdmissionFixture()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				_ = fs.MDSs()
				if _, err := fs.mdsSnapshot(t.Context()); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 200 {
			d := &MDS{ID: "x"}
			servicehook.Lock(fs.cluster)
			fs.roster.publish(d)
			fs.roster.reserveIndex()
			servicehook.Unlock(fs.cluster)
			servicehook.Lock(fs.cluster)
			fs.roster.remove(d)
			servicehook.Unlock(fs.cluster)
		}
	}()
	wg.Wait()
}

// mdsRosterMethods pins the method set of mdsRoster with the calls each makes.
// A method that takes a lock or calls into Filesystem would change the lock
// contract written on the type, so it must be reviewed here first.
var mdsRosterMethods = map[string]struct{ calls []string }{
	"members":      {},
	"snapshot":     {calls: []string{"slices.Clone(r.daemons)"}},
	"count":        {calls: []string{"len(r.daemons)"}},
	"at":           {},
	"customizers":  {calls: []string{"slices.Clone(r.opts)"}},
	"issued":       {},
	"nextID":       {calls: []string{"cephFSMDSID(filesystem, r.nextIndex)"}},
	"reserveIndex": {},
	"publish":      {calls: []string{"append(r.daemons, daemon)"}},
	"remove": {calls: []string{
		"slices.DeleteFunc(r.daemons, (func(owned *MDS) bool literal))",
	}},
}

// mdsRosterWriters maps each mutating method to the only functions that may
// call it. All of them run with setupMu and the cluster mutex held.
var mdsRosterWriters = map[string][]string{
	"reserveIndex": {"admitMDSStart"},
	"publish":      {"publishStartedMDS"},
	"remove":       {"retireMDS", "removeFilesystem", "removeStoppedMDS"},
}

// mdsRosterSetupHeld are the functions that run with setupMu held by their
// callers and do not take it themselves. Every reference to one of them from
// outside this set must follow a setupMu acquisition in the same function.
var mdsRosterSetupHeld = map[string]bool{
	"startMDS": true, "startMDSWithService": true, "startMDSWithAdmission": true, "admitMDSStart": true, "createMDSCredentials": true, "publishStartedMDS": true, "retireMDS": true,
	"captureColdMDS": true, "scaleColdMDS": true, "verifyColdMDS": true, "checkColdMDSAuth": true,
}

// mdsRosterOwnerHeld are the functions documented "Owner held": they read the
// roster and rely on the caller for the cluster mutex. ConfigTargets is called
// by ceph.Container with its mutex held (the topology caller), through an
// interface the AST guard cannot follow.
var mdsRosterOwnerHeld = map[string]bool{
	"completedMDSReplacementCohort": true, "checkMDSReplacementOwner": true,
	"checkMDSReplacementPartial": true, "checkStoppedMDSOwner": true,
	"decodeStoppedMDSScope": true, "verifyStoppedMDSSurvivors": true, "readStoppedMDSScope": true, "ConfigTargets": true,
}

// mdsRosterSetupOnlyReaders read the roster under setupMu alone, between the
// two cluster-mutex sections of the cold-start flow. No writer can run then
// because every writer holds setupMu.
var mdsRosterSetupOnlyReaders = map[string]bool{"verifyColdMDS": true, "checkColdMDSAuth": true}

func parseCephFSProductionFiles(t *testing.T) map[string]*ast.File {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*ast.File{}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		out[file] = parsed
	}
	return out
}

func rosterReceiverName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return ""
	}
	typ := fn.Recv.List[0].Type
	if star, ok := typ.(*ast.StarExpr); ok {
		typ = star.X
	}
	if id, ok := typ.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}

func joinCalls(items []string) string { return "[" + strings.Join(items, "; ") + "]" }

// rosterCall describes "<x>.roster.<method>(...)".
func rosterCall(call *ast.CallExpr) (method string, recv *ast.SelectorExpr, ok bool) {
	sel, isSel := call.Fun.(*ast.SelectorExpr)
	if !isSel {
		return "", nil, false
	}
	inner, isInner := sel.X.(*ast.SelectorExpr)
	if !isInner || inner.Sel.Name != "roster" {
		return "", nil, false
	}
	return sel.Sel.Name, inner, true
}

func isClusterLockAcquire(call *ast.CallExpr) bool {
	if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "lockSetupThenTopology" {
		// The shared prefix takes the cluster mutex (after setupMu).
		return true
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "servicehook" && (sel.Sel.Name == "LockTopology" || sel.Sel.Name == "Lock")
}

func isSetupMuAcquire(call *ast.CallExpr) bool {
	if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "lockSetupThenTopology" {
		// The shared prefix takes setupMu first.
		return true
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "LockMutex" {
		return false
	}
	found := false
	for _, arg := range call.Args {
		ast.Inspect(arg, func(n ast.Node) bool {
			if s, ok := n.(*ast.SelectorExpr); ok && s.Sel.Name == "setupMu" {
				found = true
			}
			return true
		})
	}
	return found
}

// firstPos returns the first position in fn that satisfies match, or NoPos.
func firstPos(fn *ast.FuncDecl, match func(*ast.CallExpr) bool) token.Pos {
	at := token.NoPos
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && match(call) && (at == token.NoPos || call.Pos() < at) {
			at = call.Pos()
		}
		return true
	})
	return at
}

// TestMDSRosterFieldsAreOnlyReachedThroughItsMethods guards the ownership
// written on mdsRoster: outside the type, Filesystem.roster appears only as the
// receiver of one of its pinned methods, a mdsRoster literal appears only in
// newMDSRoster, and the Filesystem literal takes its roster from that
// constructor. So no code reads or writes daemons, opts or nextIndex without a
// method that the lock guards below cover.
func TestMDSRosterFieldsAreOnlyReachedThroughItsMethods(t *testing.T) {
	var problems []string
	uses := 0
	for file, parsed := range parseCephFSProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || rosterReceiverName(fn) == "mdsRoster" {
				continue
			}
			allowed := map[*ast.SelectorExpr]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if method, recv, ok := rosterCall(call); ok {
						if _, known := mdsRosterMethods[method]; known {
							allowed[recv] = true
						}
					}
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.SelectorExpr:
					if node.Sel.Name == "roster" {
						if allowed[node] {
							uses++
						} else {
							problems = append(problems, file+": "+fn.Name.Name+" uses Filesystem.roster outside a mdsRoster method call")
						}
					}
				case *ast.CompositeLit:
					if id, ok := node.Type.(*ast.Ident); ok && id.Name == "mdsRoster" && fn.Name.Name != "newMDSRoster" {
						problems = append(problems, file+": "+fn.Name.Name+" builds a mdsRoster literal; use newMDSRoster")
					}
				case *ast.KeyValueExpr:
					if key, ok := node.Key.(*ast.Ident); ok && key.Name == "roster" {
						if call, ok := node.Value.(*ast.CallExpr); !ok || types.ExprString(call.Fun) != "newMDSRoster" {
							problems = append(problems, file+": "+fn.Name.Name+" sets Filesystem.roster to something other than newMDSRoster(...)")
						}
					}
				}
				return true
			})
		}
	}
	if uses == 0 {
		problems = append(problems, "no use of Filesystem.roster found: the guard is vacuous")
	}
	if len(problems) != 0 {
		t.Fatalf("the MDS roster escapes its methods: %v", problems)
	}
}

// TestMDSRosterMethodsMakeOnlyTheirPinnedCalls guards the lock contract of the
// type: the method set is pinned, each method makes exactly the calls listed
// (no lock, no call into Filesystem or the cluster), and only the three writer
// methods assign a field.
func TestMDSRosterMethodsMakeOnlyTheirPinnedCalls(t *testing.T) {
	seen := map[string]bool{}
	var problems []string
	for _, parsed := range parseCephFSProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || rosterReceiverName(fn) != "mdsRoster" {
				continue
			}
			name := fn.Name.Name
			want, known := mdsRosterMethods[name]
			if !known {
				problems = append(problems, name+" is not pinned; review the lock contract and update mdsRosterMethods")
				continue
			}
			seen[name] = true
			var got []string
			writes := false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.CallExpr:
					got = append(got, strings.ReplaceAll(types.ExprString(node), "\t", ""))
				case *ast.AssignStmt:
					for _, lhs := range node.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name != "" {
							writes = true
						}
					}
				case *ast.IncDecStmt:
					writes = true
				}
				return true
			})
			norm := func(in []string) []string {
				out := make([]string, len(in))
				for i, s := range in {
					out[i] = strings.Join(strings.Fields(s), " ")
				}
				slices.Sort(out)
				return out
			}
			if !slices.Equal(norm(got), norm(want.calls)) {
				problems = append(problems, name+" calls "+joinCalls(got)+", pinned "+joinCalls(want.calls))
			}
			if _, writer := mdsRosterWriters[name]; writes != writer {
				problems = append(problems, name+": writes a field = "+map[bool]string{true: "true", false: "false"}[writes]+", but only the pinned writers may")
			}
		}
	}
	for name := range mdsRosterMethods {
		if !seen[name] {
			problems = append(problems, name+" is pinned but missing")
		}
	}
	if len(problems) != 0 {
		t.Fatalf("mdsRoster breaks its lock contract: %v", problems)
	}
}

// TestMDSRosterWritersAreTheFourPinnedFunctionsUnderBothLocks guards the writer
// rule written on the type: the mutating methods are called only by the pinned
// functions, each only after the cluster mutex is taken in the same function,
// and, except for the functions whose callers hold setupMu, after setupMu.
func TestMDSRosterWritersAreTheFourPinnedFunctionsUnderBothLocks(t *testing.T) {
	var problems []string
	found := map[string]bool{}
	for file, parsed := range parseCephFSProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || rosterReceiverName(fn) == "mdsRoster" {
				continue
			}
			clusterAt := firstPos(fn, isClusterLockAcquire)
			setupAt := firstPos(fn, isSetupMuAcquire)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				method, _, ok := rosterCall(call)
				if !ok {
					return true
				}
				callers, writer := mdsRosterWriters[method]
				if !writer {
					return true
				}
				if !slices.Contains(callers, fn.Name.Name) {
					problems = append(problems, file+": "+fn.Name.Name+" calls the writer roster."+method+"; pinned callers are "+joinCalls(callers))
					return true
				}
				found[method+"@"+fn.Name.Name] = true
				if clusterAt == token.NoPos || clusterAt > call.Pos() {
					problems = append(problems, fn.Name.Name+" calls roster."+method+" before it holds the cluster mutex")
				}
				if !mdsRosterSetupHeld[fn.Name.Name] && (setupAt == token.NoPos || setupAt > clusterAt) {
					problems = append(problems, fn.Name.Name+" calls roster."+method+" without taking setupMu before the cluster mutex")
				}
				return true
			})
		}
	}
	for method, callers := range mdsRosterWriters {
		for _, caller := range callers {
			if !found[method+"@"+caller] {
				problems = append(problems, "pinned writer call roster."+method+" in "+caller+" is missing: update mdsRosterWriters")
			}
		}
	}
	if len(problems) != 0 {
		t.Fatalf("MDS roster writer rule: %v", problems)
	}
}

// TestMDSRosterReadersHoldTheirLock guards the readers: a function that reads
// the roster takes the cluster mutex first, or is one of the pinned functions
// whose caller holds it (owner held) or whose reads are covered by setupMu
// alone. Every reference to a pinned function from outside the pinned sets must
// follow the lock it relies on.
func TestMDSRosterReadersHoldTheirLock(t *testing.T) {
	var problems []string
	readers := 0
	for file, parsed := range parseCephFSProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || rosterReceiverName(fn) == "mdsRoster" {
				continue
			}
			name := fn.Name.Name
			clusterAt := firstPos(fn, isClusterLockAcquire)
			setupAt := firstPos(fn, isSetupMuAcquire)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.CallExpr:
					method, _, ok := rosterCall(node)
					if !ok {
						return true
					}
					if _, writer := mdsRosterWriters[method]; writer {
						return true
					}
					readers++
					switch {
					case mdsRosterSetupOnlyReaders[name]:
						if !(method == "nextID") {
							problems = append(problems, file+": "+name+" reads roster."+method+" under setupMu alone; only nextID is covered")
						}
					case mdsRosterOwnerHeld[name]:
					case clusterAt != token.NoPos && clusterAt < node.Pos():
					default:
						problems = append(problems, file+": "+name+" reads roster."+method+" without the cluster mutex and is not a pinned owner-held function")
					}
				case *ast.SelectorExpr:
					ref := node.Sel.Name
					if mdsRosterOwnerHeld[ref] && ref != "ConfigTargets" && !mdsRosterOwnerHeld[name] && (clusterAt == token.NoPos || clusterAt > node.Pos()) {
						problems = append(problems, file+": "+name+" references the owner-held "+ref+" before it holds the cluster mutex")
					}
					if mdsRosterSetupHeld[ref] && !mdsRosterSetupHeld[name] && (setupAt == token.NoPos || setupAt > node.Pos()) {
						problems = append(problems, file+": "+name+" references "+ref+" before it holds setupMu")
					}
				}
				return true
			})
		}
	}
	if readers == 0 {
		problems = append(problems, "no roster reader found: the guard is vacuous")
	}
	if len(problems) != 0 {
		t.Fatalf("MDS roster reader rule: %v", problems)
	}
	for name := range mdsRosterSetupOnlyReaders {
		if !mdsRosterSetupHeld[name] {
			t.Fatalf("%s is a setupMu-only reader but not pinned as setup held", name)
		}
	}
}

// TestMDSRosterAddsNoMutex pins the number of mutexes of package cephfs. The
// roster split moved fields, not locks: setupMu, the schedule lease set mutex and
// the schedule lease mutex are the only three, and mdsRoster declares none.
func TestMDSRosterAddsNoMutex(t *testing.T) {
	const want = 3 // filesystemRegistry.setupMu, scheduleLeases.mu, SnapshotScheduleLease.mu
	got, inRoster := 0, 0
	for _, parsed := range parseCephFSProductionFiles(t) {
		for _, decl := range parsed.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range gen.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				st, ok := ts.Type.(*ast.StructType)
				if !ok {
					continue
				}
				for _, field := range st.Fields.List {
					if sel, ok := field.Type.(*ast.SelectorExpr); ok {
						if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "sync" {
							got += max(1, len(field.Names))
							if ts.Name.Name == "mdsRoster" {
								inRoster++
							}
						}
					}
				}
			}
		}
	}
	if inRoster != 0 {
		t.Fatalf("mdsRoster declares %d sync primitives; it must own no lock", inRoster)
	}
	if got != want {
		t.Fatalf("package cephfs declares %d sync fields, want %d; a new lock needs its own lock-order review", got, want)
	}
}
