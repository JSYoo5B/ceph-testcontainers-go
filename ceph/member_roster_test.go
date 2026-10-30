package ceph

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"slices"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

func TestMemberRosterZeroValueIsEmptyAndWritable(t *testing.T) {
	var r memberRoster
	if r.monitor("b") != nil || r.manager("a") != nil || r.service("rgw") != nil || r.initialManager() != nil ||
		r.monitorCount() != 0 || r.managerCount() != 0 || r.serviceCount() != 0 || r.hasService("rgw") ||
		len(r.sortedServices()) != 0 || len(r.serviceCopy()) != 0 {
		t.Fatalf("an empty roster reports a member: %+v", r)
	}
	for range r.eachMonitor() {
		t.Fatal("an empty roster iterated a monitor")
	}
	for range r.eachManager() {
		t.Fatal("an empty roster iterated a manager")
	}
	for range r.eachService() {
		t.Fatal("an empty roster iterated a service")
	}
	// Deleting from nil maps is a no-op, and every adder allocates its map.
	r.dropMonitor("b")
	r.dropManager("b")
	r.dropService("rgw")
	mon, mgr := &MonitorContainer{DaemonName: "b"}, &ManagerContainer{DaemonName: "b"}
	r.addMonitor("b", mon)
	r.addManager("b", mgr)
	r.setService("rgw", nil)
	if r.monitor("b") != mon || r.manager("b") != mgr || r.monitorCount() != 1 || r.managerCount() != 1 {
		t.Fatalf("an adder did not record: %+v", r)
	}
	if !r.hasService("rgw") || r.service("rgw") != nil || r.serviceCount() != 1 {
		t.Fatal("a service recorded without a container must still be present")
	}
	r.dropMonitor("b")
	r.dropManager("b")
	r.dropService("rgw")
	if r.monitorCount() != 0 || r.managerCount() != 0 || r.serviceCount() != 0 || r.hasService("rgw") {
		t.Fatalf("a drop left a member: %+v", r)
	}
}

// TestMemberRosterDropManagerClearsTheFirstManagerWithEntryA pins the one
// invariant the roster owns: the first MGR's container mirrors the entry "a",
// so forgetting the entry "a" forgets the mirror and any other entry does not.
func TestMemberRosterDropManagerClearsTheFirstManagerWithEntryA(t *testing.T) {
	first := &fixtureService{}
	var r memberRoster
	r.addManager("a", &ManagerContainer{DaemonName: "a"})
	r.addManager("b", &ManagerContainer{DaemonName: "b"})
	r.recordInitialManager(first)
	r.dropManager("b")
	if r.initialManager() != first || r.manager("a") == nil {
		t.Fatalf("dropping b changed a: %+v", r)
	}
	r.dropManager("a")
	if r.initialManager() != nil || r.manager("a") != nil {
		t.Fatalf("dropping a kept the mirror: %+v", r)
	}
	// clearInitialManager forgets the mirror only; the entry stays.
	r.addManager("a", &ManagerContainer{DaemonName: "a"})
	r.recordInitialManager(first)
	r.clearInitialManager()
	if r.initialManager() != nil || r.manager("a") == nil {
		t.Fatalf("clearInitialManager touched the entry: %+v", r)
	}
}

func TestMemberRosterEachAllowsDroppingTheVisitedKeyAndStopsEarly(t *testing.T) {
	var r memberRoster
	for _, name := range []string{"b", "c", "d", "e"} {
		r.addMonitor(name, &MonitorContainer{DaemonName: name})
		r.addManager(name, &ManagerContainer{DaemonName: name})
		r.setService(name, &fixtureService{})
	}
	visited := 0
	for name := range r.eachMonitor() {
		visited++
		r.dropMonitor(name)
	}
	for name := range r.eachManager() {
		visited++
		r.dropManager(name)
	}
	for name := range r.eachService() {
		visited++
		r.dropService(name)
	}
	if visited != 12 || r.monitorCount() != 0 || r.managerCount() != 0 || r.serviceCount() != 0 {
		t.Fatalf("visited %d: %+v", visited, r)
	}
	for _, name := range []string{"b", "c"} {
		r.addMonitor(name, &MonitorContainer{})
		r.addManager(name, &ManagerContainer{})
		r.setService(name, &fixtureService{})
	}
	visited = 0
	for range r.eachMonitor() {
		visited++
		break
	}
	for range r.eachManager() {
		visited++
		break
	}
	for range r.eachService() {
		visited++
		break
	}
	if visited != 3 || r.monitorCount() != 2 || r.managerCount() != 2 || r.serviceCount() != 2 {
		t.Fatalf("an early break visited %d: %+v", visited, r)
	}
}

func TestMemberRosterSortedServicesAndCopy(t *testing.T) {
	var r memberRoster
	a, b, c := &fixtureService{}, &fixtureService{}, &fixtureService{}
	r.setService("rgw", c)
	r.setService("mds.a", a)
	r.setService("mds.b", b)
	if got := r.sortedServices(); !slices.Equal(got, []testcontainers.Container{a, b, c}) {
		t.Fatalf("sortedServices = %v", got)
	}
	cp := r.serviceCopy()
	delete(cp, "rgw")
	cp["extra"] = a
	if r.serviceCount() != 3 || r.hasService("extra") {
		t.Fatal("a change to the copy reached the roster")
	}
}

// fixtureService is a distinct service container for the roster tests.
type fixtureService struct{ testcontainers.Container }

// TestServiceHooksReachTheRoster pins that the hooks for the service packages
// and the roster are the same state.
func TestServiceHooksReachTheRoster(t *testing.T) {
	c := &Container{}
	hooks := serviceHooks{}
	ctr := &fixtureService{}
	hooks.SetService(c, "rgw", ctr)
	if hooks.Service(c, "rgw") != ctr || c.members.roster.service("rgw") != ctr {
		t.Fatal("SetService did not reach the roster")
	}
	if got := hooks.Services(c); len(got) != 1 || got["rgw"] != ctr {
		t.Fatalf("Services = %v", got)
	}
	hooks.DeleteService(c, "rgw")
	if c.members.roster.hasService("rgw") {
		t.Fatal("DeleteService did not reach the roster")
	}
}

// TestContainerDeclaresNoRosterFields guards the move: Container holds the
// roster as one field and no longer declares the four fields it replaced, and
// the roster has exactly the four fields and no lock.
func TestContainerDeclaresNoRosterFields(t *testing.T) {
	fields := func(typeName string) []string {
		var names []string
		for _, parsed := range parseProductionFiles(t) {
			for _, decl := range parsed.Decls {
				gen, ok := decl.(*ast.GenDecl)
				if !ok {
					continue
				}
				for _, spec := range gen.Specs {
					ts, ok := spec.(*ast.TypeSpec)
					if !ok || ts.Name.Name != typeName {
						continue
					}
					for _, field := range ts.Type.(*ast.StructType).Fields.List {
						for _, name := range field.Names {
							names = append(names, name.Name)
						}
					}
				}
			}
		}
		slices.Sort(names)
		return names
	}
	container := fields("Container")
	for _, old := range []string{"manager", "monitors", "managers", "services", "roster"} {
		if slices.Contains(container, old) {
			t.Errorf("Container declares %s again; it belongs to memberRoster in membershipState", old)
		}
	}
	if got := fields("membershipState"); !slices.Equal(got, []string{"monitorTerminated", "roster"}) {
		t.Errorf("membershipState fields = %v, want the roster and the first-MON flag", got)
	}
	if got := fields("memberRoster"); !slices.Equal(got, []string{"firstMgr", "managers", "monitors", "services"}) {
		t.Errorf("memberRoster fields = %v", got)
	}
}

func memberRosterUse(sel *ast.SelectorExpr) bool { return sel.Sel.Name == "roster" }

func isMemberRosterReceiver(fn *ast.FuncDecl) bool { return receiverTypeName(fn) == "memberRoster" }

// memberRosterMethods pins the component: the calls each method makes. None
// takes a lock or calls into Container.
var memberRosterMethods = map[string][]string{
	"monitor":              nil,
	"monitorCount":         {"len"},
	"eachMonitor":          {"yield"},
	"addMonitor":           {"make"},
	"dropMonitor":          {"delete"},
	"manager":              nil,
	"managerCount":         {"len"},
	"eachManager":          {"yield"},
	"addManager":           {"make"},
	"dropManager":          {"delete"},
	"initialManager":       nil,
	"recordInitialManager": nil,
	"clearInitialManager":  nil,
	"service":              nil,
	"hasService":           nil,
	"serviceCount":         {"len"},
	"eachService":          {"yield"},
	"sortedServices":       {"slices.Sorted", "maps.Keys", "make", "len", "append"},
	"serviceCopy":          {"maps.Clone"},
	"setService":           {"make"},
	"dropService":          {"delete"},
	// currentManager needs only the roster; the caller holds Container.mu.
	"currentManager": {"r.manager", "invalid", "fmt.Errorf"},
}

// memberRosterFieldWriters pins which methods assign each field, directly or
// by index.
var memberRosterFieldWriters = map[string][]string{
	"firstMgr": {"dropManager", "recordInitialManager", "clearInitialManager"},
	"monitors": {"addMonitor"},
	"managers": {"addManager"},
	"services": {"setService"},
}

// TestMemberRosterFieldIsOnlyTouchedByItsMethods guards the ownership written
// on memberRoster: outside this type, Container.roster is used only as the
// receiver of one of its methods (so no code reads a map, writes one or takes
// an address), no production Container literal sets it, and the lock-free
// FactsOf source never mentions it.
func TestMemberRosterFieldIsOnlyTouchedByItsMethods(t *testing.T) {
	var problems []string
	uses := 0
	for file, parsed := range parseProductionFiles(t) {
		ast.Inspect(parsed, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok || types.ExprString(lit.Type) != "Container" {
				return true
			}
			for _, elt := range lit.Elts {
				if kv, ok := elt.(*ast.KeyValueExpr); ok && types.ExprString(kv.Key) == "roster" {
					problems = append(problems, file+": a Container literal sets roster")
				}
			}
			return true
		})
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || isMemberRosterReceiver(fn) {
				continue
			}
			allowed := map[*ast.SelectorExpr]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if method, ok := call.Fun.(*ast.SelectorExpr); ok {
						if _, known := memberRosterMethods[method.Sel.Name]; known {
							if recv, ok := method.X.(*ast.SelectorExpr); ok && memberRosterUse(recv) {
								allowed[recv] = true
							}
						}
					}
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && memberRosterUse(sel) {
					if allowed[sel] || monitorTopologyBinding(fn) {
						uses++
					} else {
						problems = append(problems, file+": "+fn.Name.Name+" uses Container.roster outside a memberRoster method call")
					}
				}
				return true
			})
			if file == "cluster_facts.go" && strings.Contains(fn.Name.Name, "Facts") {
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if sel, ok := n.(*ast.SelectorExpr); ok && memberRosterUse(sel) {
						problems = append(problems, "FactsOf source reads Container.roster")
					}
					return true
				})
			}
		}
	}
	if uses == 0 {
		problems = append(problems, "no use of Container.roster found: the guard is vacuous")
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("the roster escapes its methods: %v", problems)
	}
}

// TestMemberRosterMethodsMakeOnlyTheirPinnedCalls guards the lock contract of
// the type: the method set is pinned, each method makes exactly the calls
// listed (none locks or calls into Container), and each field is assigned only
// by the methods that own the transition.
func TestMemberRosterMethodsMakeOnlyTheirPinnedCalls(t *testing.T) {
	seen := map[string]bool{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isMemberRosterReceiver(fn) {
				continue
			}
			name := fn.Name.Name
			want, known := memberRosterMethods[name]
			if !known {
				problems = append(problems, name+" is not pinned; review the lock contract and update memberRosterMethods")
				continue
			}
			seen[name] = true
			var got []string
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					got = append(got, types.ExprString(x.Fun))
				case *ast.AssignStmt:
					for _, lhs := range x.Lhs {
						if index, ok := lhs.(*ast.IndexExpr); ok {
							lhs = index.X
						}
						sel, ok := lhs.(*ast.SelectorExpr)
						if !ok {
							continue
						}
						if writers, field := memberRosterFieldWriters[sel.Sel.Name]; field && !slices.Contains(writers, name) {
							problems = append(problems, name+" assigns "+sel.Sel.Name)
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
	for name := range memberRosterMethods {
		if !seen[name] {
			problems = append(problems, name+" is pinned but missing")
		}
	}
	// Every pinned writer really writes its field.
	for field, writers := range memberRosterFieldWriters {
		for _, writer := range writers {
			if _, ok := memberRosterMethods[writer]; !ok {
				problems = append(problems, field+" writer "+writer+" is not a pinned method")
			}
		}
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("memberRoster breaks its lock contract: %v", problems)
	}
}

// memberRosterCaller pins one function that uses the roster or sits between
// such a function and its lock.
//
//	mode "takes"  acquires Container.mu itself before its first roster use
//	mode "holds"  its doc comment requires the caller to hold Container.mu; every caller is itself an acquiring, "holds", "hook" or "init" function
//	mode "hook"   a hook for another package whose doc comment requires the caller to hold the mutex
//	mode "init"   runs before the cluster is shared
//
// methods is the roster methods the function calls directly, empty for an
// intermediate function that only calls a "holds" function.
type memberRosterCaller struct {
	mode    string
	methods []string
}

var memberRosterCallers = map[string]memberRosterCaller{
	"Service":       {"hook", []string{"service"}},
	"SetService":    {"hook", []string{"setService"}},
	"DeleteService": {"hook", []string{"dropService"}},
	"Services":      {"hook", []string{"serviceCopy"}},

	"ManagerContainer":   {"takes", []string{"initialManager"}},
	"ServiceContainers":  {"takes", []string{"sortedServices"}},
	"Managers":           {"takes", []string{"eachManager", "managerCount"}},
	"AddMonitor":         {"takes", []string{"addMonitor", "monitor"}},
	"retireMonitor":      {"holds", []string{"dropMonitor"}},
	"retireManager":      {"holds", []string{"currentManager", "dropManager", "eachManager", "managerCount"}},
	"startService":       {"takes", []string{"hasService", "setService"}},
	"terminate":          {"takes", []string{"clearInitialManager", "dropManager", "dropMonitor", "dropService", "eachManager", "eachMonitor", "eachService", "initialManager"}},
	"diagnosticDaemons":  {"holds", []string{"eachManager", "eachMonitor", "initialManager"}},
	"diagnosticServices": {"holds", []string{"eachService"}},

	"startManager":                     {"holds", []string{"addManager", "manager", "recordInitialManager"}},
	"currentMonitor":                   {"holds", []string{"monitor"}},
	"ownedBy":                          {"holds", []string{"manager"}},
	"ownsDaemon":                       {"holds", []string{"manager", "monitor"}},
	"daemonKeyrings":                   {"holds", []string{"manager"}},
	"monitorsLocked":                   {"holds", []string{"eachMonitor", "monitorCount"}},
	"monitorConfigTargets":             {"holds", []string{"eachManager", "eachMonitor", "eachService", "initialManager", "manager"}},
	"Owns":                             {"hook", nil},
	"refreshMonitorConfig":             {"holds", nil},
	"refreshMonitorConfigAfterRemoval": {"holds", nil},
	"ownsNativeTarget":                 {"holds", []string{"eachManager", "eachMonitor", "eachService", "initialManager"}},
}

// TestMemberRosterCallersAreAudited guards the callers. The set of functions
// that use the roster, the methods each one calls and how each is guarded are
// pinned in memberRosterCallers. A "takes" function acquires Container.mu
// before its first use. A "holds" function has a doc comment that says so, and
// every call to it, found by name over the production files, comes from a
// function that acquires the lock before the call or is itself a "holds",
// "hook" or "init" row of the table. This is the lock domain written on the
// type: Container.mu, with no lock of the roster's own.
func TestMemberRosterCallersAreAudited(t *testing.T) {
	type fnInfo struct {
		decl    *ast.FuncDecl
		methods map[string]bool
		acquire token.Pos
		calls   map[string][]token.Pos
	}
	// Functions are judged one declaration at a time: two declarations with
	// the same name (methods of different types) are never merged.
	infos := map[string][]*fnInfo{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			info := &fnInfo{decl: fn, methods: map[string]bool{}, acquire: osdRegistryAcquirePos(fn), calls: map[string][]token.Pos{}}
			firstUse := token.NoPos
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if name := calleeName(call); name != "" {
					info.calls[name] = append(info.calls[name], call.Pos())
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					if recv, ok := sel.X.(*ast.SelectorExpr); ok && memberRosterUse(recv) && !isMemberRosterReceiver(fn) {
						info.methods[sel.Sel.Name] = true
						if firstUse == token.NoPos || call.Pos() < firstUse {
							firstUse = call.Pos()
						}
					}
				}
				return true
			})
			name := fn.Name.Name
			infos[name] = append(infos[name], info)
			if len(info.methods) == 0 {
				continue
			}
			row, known := memberRosterCallers[name]
			switch {
			case !known:
				problems = append(problems, name+" uses the roster but is not in memberRosterCallers")
				continue
			case row.mode == "takes" && (info.acquire == token.NoPos || info.acquire > firstUse):
				problems = append(problems, name+" must take Container.mu before using the roster")
			}
			got := slices.Sorted(func(yield func(string) bool) {
				for m := range info.methods {
					if !yield(m) {
						return
					}
				}
			})
			if !slices.Equal(got, slices.Sorted(slices.Values(row.methods))) {
				problems = append(problems, name+" calls roster methods "+join(got)+", pinned "+join(row.methods))
			}
		}
	}
	docs := osdRegistryDocs(t)
	for name, row := range memberRosterCallers {
		decls := infos[name]
		if len(decls) == 0 {
			problems = append(problems, name+" is listed but does not exist")
			continue
		}
		using := 0
		for _, info := range decls {
			if len(info.methods) != 0 {
				using++
			}
		}
		if len(row.methods) != 0 && using == 0 {
			problems = append(problems, name+" is listed but no longer uses the roster")
		}
		if row.mode == "holds" || row.mode == "hook" {
			if doc := docs[name]; !strings.Contains(doc, "hold") && !strings.Contains(doc, " held") {
				problems = append(problems, name+" is a "+row.mode+" row but its doc comment does not say that the caller holds the lock")
			}
		}
	}
	// Every call to a "holds" function comes from a function that is guarded.
	for held, row := range memberRosterCallers {
		if row.mode != "holds" {
			continue
		}
		for caller, decls := range infos {
			for _, info := range decls {
				if caller == held && info.decl.Recv == nil {
					continue
				}
				for _, at := range info.calls[held] {
					callerRow, listed := memberRosterCallers[caller]
					acquired := info.acquire != token.NoPos && info.acquire < at
					ok := acquired || (listed && (callerRow.mode == "holds" || callerRow.mode == "hook" || callerRow.mode == "init"))
					if !ok {
						problems = append(problems, caller+" calls "+held+" without taking Container.mu and is not a pinned holds, hook or init function")
					}
				}
			}
		}
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("roster callers break the lock contract: %v", problems)
	}
}

// TestMemberRosterHasNoAddressTaken guards that nothing takes the address of
// the roster or of one of its maps, the way the old unit-test state view once
// published a pointer to a map.
func TestMemberRosterHasNoAddressTaken(t *testing.T) {
	var problems []string
	for file, parsed := range parseProductionFiles(t) {
		ast.Inspect(parsed, func(n ast.Node) bool {
			un, ok := n.(*ast.UnaryExpr)
			if !ok || un.Op != token.AND {
				return true
			}
			ast.Inspect(un.X, func(m ast.Node) bool {
				if sel, ok := m.(*ast.SelectorExpr); ok && (memberRosterUse(sel) || sel.Sel.Name == "firstMgr") && !inMonitorTopologyBinding(parsed, un.Pos()) {
					problems = append(problems, file+": takes the address of "+types.ExprString(un.X))
				}
				return true
			})
			return true
		})
	}
	if len(problems) != 0 {
		t.Fatalf("the roster is addressed: %v", problems)
	}
}

// TestTerminateStopsServicesBeforeOSDsAndMembersAfterThem guards the order of
// the roster's part of Terminate: the services are terminated first, while the
// OSDs they depend on are alive, then the first MGR (read once through the
// roster), the other MGRs and the added MONs, all after the storage release
// stage and before the first MON.
func TestTerminateStopsServicesBeforeOSDsAndMembersAfterThem(t *testing.T) {
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isContainerTerminate(fn) {
				continue
			}
			at := map[string][]token.Pos{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.RangeStmt:
					at["range "+types.ExprString(x.X)] = append(at["range "+types.ExprString(x.X)], x.Pos())
				case *ast.CallExpr:
					at[types.ExprString(x)] = append(at[types.ExprString(x)], x.Pos())
				}
				return true
			})
			order := []string{
				"range c.members.roster.eachService()", "range c.fleet.osdHandles.each()", "c.fleet.stores.releaseVolume(ctx, c.fleet.osdHandles.count() == 0)",
				"c.members.roster.initialManager()", "range c.members.roster.eachManager()", "range c.members.roster.eachMonitor()", "c.Container.Terminate(ctx, opts...)",
			}
			var problems []string
			for _, key := range order {
				if len(at[key]) == 0 {
					problems = append(problems, key+" is missing from terminate")
				}
			}
			if len(problems) == 0 {
				for i := 1; i < len(order); i++ {
					if at[order[i-1]][0] >= at[order[i]][0] {
						problems = append(problems, order[i-1]+" must come before "+order[i])
					}
				}
			}
			if got := len(at["c.members.roster.initialManager()"]); got != 2 {
				problems = append(problems, fmt.Sprintf("the first MGR must be read twice (to terminate it, then to decide whether entry a goes), got %d", got))
			}
			if len(problems) != 0 {
				t.Fatalf("terminate changed the roster order: %v", problems)
			}
			return
		}
	}
	t.Fatal("Container.terminate not found")
}
