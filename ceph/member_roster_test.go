package ceph

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"slices"
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
	fieldAudit{label: "Container.roster", use: memberRosterUse, own: isMemberRosterReceiver, methods: memberRosterMethods, bound: monitorTopologyBinding, literalField: "roster"}.check(t)
}

// TestMemberRosterMethodsMakeOnlyTheirPinnedCalls guards the lock contract of
// the type: the method set is pinned, each method makes exactly the calls
// listed (none locks or calls into Container), and each field is assigned only
// by the methods that own the transition.
func TestMemberRosterMethodsMakeOnlyTheirPinnedCalls(t *testing.T) {
	methodAudit{label: "memberRoster", own: isMemberRosterReceiver, methods: memberRosterMethods, writers: memberRosterFieldWriters}.check(t)
}

// memberRosterCallers pins every function that uses the roster or sits
// between such a function and its lock (see stateCaller).
var memberRosterCallers = map[string]stateCaller{
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
	"terminateServices":  {"holds", []string{"dropService", "eachService"}},
	"terminateMembers":   {"holds", []string{"clearInitialManager", "dropManager", "dropMonitor", "eachManager", "eachMonitor", "initialManager"}},
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
	callerAudit{label: "the roster", use: memberRosterUse, own: isMemberRosterReceiver, callers: memberRosterCallers}.check(t)
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
	for _, parsed := range parseProductionFilesWithTerminateSteps(t) {
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
