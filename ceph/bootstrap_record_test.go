package ceph

import (
	"bytes"
	"context"
	"errors"
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const bootstrapRecordConfig = "[global]\nfsid = 11111111-2222-3333-4444-555555555555\nmon_host = mon\n"

func newBootstrapRecord() *bootstrapRecord {
	r := &bootstrapRecord{}
	r.recordConfig([]byte(bootstrapRecordConfig))
	r.recordKeyring([]byte("[client.admin]\n\tkey = AQBsecretsecretsecretsecretsecretsecret==\n"))
	r.recordVersion("20.2.4")
	return r
}

func TestBootstrapRecordWithoutARecordIsInert(t *testing.T) {
	var r bootstrapRecord
	if r.hasConfig() || r.isComplete() || r.rawVersion() != "" || len(r.rawConfig()) != 0 || len(r.rawKeyring()) != 0 {
		t.Fatal("an empty record reports bootstrap data")
	}
	if config, keyring := r.clones(); len(config) != 0 || len(keyring) != 0 {
		t.Fatalf("clones = %q, %q", config, keyring)
	}
	if template, confirmed := r.templateAndConfirmed(); len(template) != 0 || confirmed {
		t.Fatalf("templateAndConfirmed = %q, %v", template, confirmed)
	}
	if got, err := r.identity(t.Context()); err != nil || got.confirmed || got.err == nil {
		t.Fatalf("identity of an empty record = %+v, %v: it must be unconfirmed with an identity error", got, err)
	}
	if _, _, complete, err := r.connection(t.Context()); complete || err != nil {
		t.Fatalf("connection of an empty record: complete=%v err=%v", complete, err)
	}
	if keyring, err := r.diagnosticKeyring(t.Context()); len(keyring) != 0 || err != nil {
		t.Fatalf("diagnosticKeyring = %q, %v", keyring, err)
	}
}

func TestBootstrapRecordCompletenessNeedsBothParts(t *testing.T) {
	var r bootstrapRecord
	r.recordConfig([]byte("c"))
	if !r.hasConfig() || r.isComplete() {
		t.Fatal("a record with only a configuration is complete")
	}
	r.recordKeyring([]byte("k"))
	if !r.isComplete() {
		t.Fatal("a record with both parts is incomplete")
	}
	r.recordConfig(nil)
	if r.hasConfig() || r.isComplete() {
		t.Fatal("a nil configuration still counts")
	}
}

// TestBootstrapRecordLockedReadersReturnCopies pins that a caller of a locked
// reader never aliases the stored bytes.
func TestBootstrapRecordLockedReadersReturnCopies(t *testing.T) {
	r := newBootstrapRecord()
	config, keyring := r.clones()
	config[0], keyring[0] = 'X', 'X'
	template := r.template()
	template[0] = 'X'
	templ2, confirmed := r.templateAndConfirmed()
	templ2[0] = 'X'
	connConfig, connKeyring, complete, err := r.connection(t.Context())
	if err != nil || !complete {
		t.Fatalf("connection = complete %v, %v", complete, err)
	}
	connConfig[0], connKeyring[0] = 'X', 'X'
	diag, err := r.diagnosticKeyring(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	diag[0] = 'X'
	if !confirmed || string(r.rawConfig()) != bootstrapRecordConfig || r.rawKeyring()[0] != '[' {
		t.Fatal("a returned copy changed the stored record")
	}
	launchConfig, launchAdmin := r.launchStrings()
	if launchConfig != bootstrapRecordConfig || !strings.HasPrefix(launchAdmin, "[client.admin]") {
		t.Fatalf("launchStrings = %q, %q", launchConfig, launchAdmin)
	}
}

func TestBootstrapRecordIdentityReadsTheFSIDAndConfirmation(t *testing.T) {
	r := newBootstrapRecord()
	got, err := r.identity(t.Context())
	if err != nil || got.err != nil || !got.confirmed || got.fsid != "11111111-2222-3333-4444-555555555555" {
		t.Fatalf("identity = %+v, %v", got, err)
	}
	r.recordKeyring(nil)
	if got, err := r.identity(t.Context()); err != nil || got.confirmed {
		t.Fatalf("identity without a keyring = %+v, %v", got, err)
	}
	r.recordKeyring([]byte("k"))
	r.recordConfig([]byte("[global]\nfsid = a\nfsid = b\n"))
	if got, err := r.identity(t.Context()); err != nil || got.err == nil {
		t.Fatalf("an ambiguous FSID produced %+v, %v", got, err)
	}
}

func TestBootstrapRecordApplyDefaultCRUSHRule(t *testing.T) {
	r := newBootstrapRecord()
	updated, err := r.applyDefaultCRUSHRule("7")
	if err != nil || !strings.Contains(string(updated), "osd pool default crush rule = 7") || !bytes.Equal(updated, r.rawConfig()) {
		t.Fatalf("applyDefaultCRUSHRule = %q, %v; stored %q", updated, err, r.rawConfig())
	}
	var bare bootstrapRecord
	bare.recordConfig([]byte("[client]\nkey = value\n"))
	if _, err := bare.applyDefaultCRUSHRule("7"); err == nil || string(bare.rawConfig()) != "[client]\nkey = value\n" {
		t.Fatalf("a failed rewrite changed the record: %v, %q", err, bare.rawConfig())
	}
}

// TestBootstrapRecordReadersHonorTheContextWhileTheLockIsHeld pins the three
// bounded readers: a held write lock ends in the context error and never blocks.
func TestBootstrapRecordReadersHonorTheContextWhileTheLockIsHeld(t *testing.T) {
	r := newBootstrapRecord()
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, read := range map[string]func(context.Context) error{
		"identity":   func(ctx context.Context) error { _, err := r.identity(ctx); return err },
		"connection": func(ctx context.Context) error { _, _, _, err := r.connection(ctx); return err },
		"diagnosticKeyring": func(ctx context.Context) error {
			_, err := r.diagnosticKeyring(ctx)
			return err
		},
	} {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
		done := make(chan error, 1)
		go func() { done <- read(ctx) }()
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("%s = %v, want the deadline", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s blocked past its context", name)
		}
		cancel()
	}
}

func TestBootstrapRecordConcurrentReadersAndWriters(t *testing.T) {
	r := newBootstrapRecord()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				_, _ = r.clones()
				_ = r.template()
				_, _ = r.identity(t.Context())
				_, _, _, _ = r.connection(t.Context())
				_, _ = r.diagnosticKeyring(t.Context())
				_, _ = r.launchStrings()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 200 {
			r.publishConfig([]byte(bootstrapRecordConfig))
			_, _ = r.applyDefaultCRUSHRule("3")
		}
	}()
	wg.Wait()
}

// TestTestStatePublishesTheBootstrapRecordLockIdentityAndHooksReachIt pins that
// ConfigMutex is the component's own lock, and that the three setters write the
// record the readers see.
func TestTestStatePublishesTheBootstrapRecordLockIdentityAndHooksReachIt(t *testing.T) {
	c := &Container{}
	if _, _, config, _ := (unitAccess{}).UnitState(c); config != &c.bootstrap.mu {
		t.Fatal("UnitState.ConfigMutex is not the bootstrap-record lock")
	}
	hooks := unitAccess{}
	hooks.UnitSetConfig(c, []byte(bootstrapRecordConfig))
	hooks.UnitSetKeyring(c, []byte("k"))
	hooks.UnitSetCephVersion(c, "20.2.4")
	if id, ok, err := (serviceHooks{}).ClusterIdentity(t.Context(), c); err != nil || !ok || id != "11111111-2222-3333-4444-555555555555" {
		t.Fatalf("ClusterIdentity = %q, %v, %v", id, ok, err)
	}
	if c.staticFacts().CephVersion != "20.2.4" || !c.bootstrap.isComplete() {
		t.Fatal("the setters did not reach the record")
	}
}

// bootstrapRecordMethods pins the component: the calls each method makes. Only
// the lock calls on its own mutex, the two bounded-lock helpers and the pure
// helpers appear; none calls into Container or takes another lock.
var bootstrapRecordMethods = map[string][]string{
	"recordConfig":          nil,
	"recordKeyring":         nil,
	"recordVersion":         nil,
	"rawConfig":             nil,
	"rawKeyring":            nil,
	"rawVersion":            nil,
	"hasConfig":             {"len(r.config)"},
	"isComplete":            {"len(r.config)", "len(r.keyring)"},
	"clones":                {"r.mu.RLock()", "r.mu.RUnlock()", "bytes.Clone(r.config)", "bytes.Clone(r.keyring)"},
	"templateAndConfirmed":  {"r.mu.RLock()", "r.mu.RUnlock()", "bytes.Clone(r.config)", "len(r.keyring)"},
	"template":              {"r.mu.RLock()", "r.mu.RUnlock()", "bytes.Clone(r.config)"},
	"launchStrings":         {"r.mu.RLock()", "r.mu.RUnlock()", "string(r.config)", "string(r.keyring)"},
	"identity":              {"lockTopologyReadMutex(ctx, &r.mu)", "r.mu.RUnlock()", "monitorConfigClusterIdentity(r.config)", "len(r.keyring)"},
	"connection":            {"lockTopologyReadMutex(ctx, &r.mu)", "r.mu.RUnlock()", "bytes.Clone(r.config)", "bytes.Clone(r.keyring)", "len(r.config)", "len(r.keyring)"},
	"diagnosticKeyring":     {"diagnosticLock(ctx, r.mu.TryRLock)", "r.mu.RUnlock()", "bytes.Clone(r.keyring)"},
	"applyDefaultCRUSHRule": {"r.mu.Lock()", "r.mu.Unlock()", "defaultCRUSHRuleConfig(r.config, value)"},
	"publishConfig":         {"r.mu.Lock()", "r.mu.Unlock()"},
	"mutex":                 nil,
}

// bootstrapRecordWriters is every production function that assigns a field of
// the record, with how it is guarded:
//
//	"startup" a plain write; Run builds the cluster before returning it
//	"takes"   takes the write lock around the write
//	"hook"    a TestSet* hook whose doc names the lock contract
var bootstrapRecordWriters = map[string]string{
	"recordConfig": "startup", "recordKeyring": "startup", "recordVersion": "startup",
	"applyDefaultCRUSHRule": "takes", "publishConfig": "takes",
	"UnitSetConfig": "hook", "UnitSetKeyring": "hook", "UnitSetCephVersion": "hook",
}

// bootstrapRecordCallers lists every function outside the component that calls
// one of its methods, with the method and how it is guarded:
//
//	"free"     the method takes the leaf lock itself; the caller may or may not
//	           hold Container.mu (the order is mu, then the record lock)
//	"takes"    a lock-free method; the caller takes the topology lock first
//	"startup"  a plain write made while Run builds the cluster
//	"lockfree" the lock-free FactsOf source; the version is written only during
//	           startup (see TestClusterFactsRecordedSourcesAreNotRewrittenOutsideStartup)
//	"identity" returns the lock identity for the test state view
var bootstrapRecordCallers = map[string]map[string]string{
	"createClient":                     {"hasConfig": "takes", "rawConfig": "takes"},
	"WithIdentity":                     {"rawConfig": "takes"},
	"startService":                     {"isComplete": "takes"},
	"AddMonitor":                       {"rawConfig": "takes"},
	"bootstrapFirstMonitor":            {"recordConfig": "startup", "recordKeyring": "startup"},
	"recordCephVersion":                {"recordVersion": "startup"},
	"staticFacts":                      {"rawVersion": "lockfree"},
	"adminClient":                      {"clones": "free"},
	"currentMonitorBootstrapAddresses": {"templateAndConfirmed": "free"},
	"newOSDLaunchRecipe":               {"launchStrings": "free"},
	"refreshMonitorConfigAfterRemoval": {"template": "free", "publishConfig": "free"},
	"configureDefaultCRUSHRoot":        {"applyDefaultCRUSHRule": "free"},
	"diagnosticsSnapshot":              {"diagnosticKeyring": "free"},
	"fullRatiosSession":                {"identity": "free"},
	"HealthDetails":                    {"identity": "free"},
	"PGs":                              {"identity": "free"},
	"Usage":                            {"identity": "free"},
	"ClusterIdentity":                  {"identity": "free"},
	"ConnectionConfig":                 {"connection": "free"},
	"UnitState":                        {"mutex": "identity"},
}

func isBootstrapReceiver(fn *ast.FuncDecl) bool { return receiverTypeName(fn) == "bootstrapRecord" }

func bootstrapUse(sel *ast.SelectorExpr) bool { return sel.Sel.Name == "bootstrap" }

var bootstrapFields = map[string]bool{"config": true, "keyring": true, "cephVersion": true, "mu": true}

// TestContainerDeclaresNoBootstrapFieldsOrLock pins that the moved fields and
// the former configMu are gone from Container, so every access goes through
// the component.
func TestContainerDeclaresNoBootstrapFieldsOrLock(t *testing.T) {
	got := structFieldNames(t, "ceph.go", "Container")
	for _, gone := range []string{"config", "keyring", "cephVersion", "configMu"} {
		if slices.Contains(got, gone) {
			t.Fatalf("Container still declares %s", gone)
		}
	}
	if !slices.Contains(got, "bootstrap") {
		t.Fatal("Container does not declare the bootstrap record")
	}
	if fields := structFieldNames(t, "bootstrap_record.go", "bootstrapRecord"); !slices.Equal(fields, []string{"cephVersion", "config", "keyring", "mu"}) {
		t.Fatalf("bootstrapRecord fields = %v", fields)
	}
}

// TestBootstrapRecordFieldIsOnlyTouchedByItsMethods guards the ownership
// written on bootstrapRecord: outside this type, Container.bootstrap is the
// receiver of one of its methods, or (the TestSet* hooks only) the owner of a
// field the hook assigns; the lock-free FactsOf source reads only the version
// through rawVersion; no production composite literal of Container sets it.
func TestBootstrapRecordFieldIsOnlyTouchedByItsMethods(t *testing.T) {
	var problems []string
	uses := 0
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || isBootstrapReceiver(fn) {
				continue
			}
			isHook := receiverTypeName(fn) == "unitAccess" && strings.HasPrefix(fn.Name.Name, "UnitSet")
			allowed := map[*ast.SelectorExpr]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					if method, ok := x.Fun.(*ast.SelectorExpr); ok {
						if _, known := bootstrapRecordMethods[method.Sel.Name]; known {
							if recv, ok := method.X.(*ast.SelectorExpr); ok && bootstrapUse(recv) {
								allowed[recv] = true
							}
						}
					}
				case *ast.AssignStmt:
					// c.bootstrap.config = x, in a TestSet hook only.
					for _, lhs := range x.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && bootstrapFields[sel.Sel.Name] && isHook {
							if recv, ok := sel.X.(*ast.SelectorExpr); ok && bootstrapUse(recv) {
								allowed[recv] = true
							}
						}
					}
				case *ast.CompositeLit:
					if id, ok := x.Type.(*ast.Ident); ok && id.Name == "Container" {
						for _, elt := range x.Elts {
							if kv, ok := elt.(*ast.KeyValueExpr); ok {
								if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "bootstrap" {
									problems = append(problems, file+": "+fn.Name.Name+" sets bootstrap in a Container literal")
								}
							}
						}
					}
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && bootstrapUse(sel) && (isContainerReceiver(sel.X) || receiverTypeName(fn) == "monitorTopology") {
					if allowed[sel] || monitorTopologyBinding(fn) {
						uses++
					} else {
						problems = append(problems, file+": "+fn.Name.Name+" uses Container.bootstrap outside a bootstrapRecord method call")
					}
				}
				return true
			})
		}
	}
	if uses == 0 {
		problems = append(problems, "no use of Container.bootstrap found: the guard is vacuous")
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("bootstrap record escapes its methods: %v", problems)
	}
}

// TestBootstrapRecordMethodsMakeOnlyTheirPinnedCalls guards the lock contract
// of the type: the method set is pinned, each method makes exactly the calls
// listed (no other lock, no Container call), and no method names Container,
// closed or another mutex.
func TestBootstrapRecordMethodsMakeOnlyTheirPinnedCalls(t *testing.T) {
	seen := map[string]bool{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isBootstrapReceiver(fn) {
				continue
			}
			name := fn.Name.Name
			want, known := bootstrapRecordMethods[name]
			if !known {
				problems = append(problems, name+" is not pinned; review the lock contract and update bootstrapRecordMethods")
				continue
			}
			seen[name] = true
			var got []string
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					got = append(got, types.ExprString(x))
				case *ast.Ident:
					switch x.Name {
					case "Container", "closed", "controlPlane", "settings":
						problems = append(problems, name+" names "+x.Name)
					}
				case *ast.SelectorExpr:
					switch x.Sel.Name {
					case "mu", "controlMu", "configMu", "extensionsMu":
						if id, ok := x.X.(*ast.Ident); !ok || id.Name != "r" {
							problems = append(problems, name+" reads "+types.ExprString(x))
						}
					}
				}
				return true
			})
			if !slices.Equal(slices.Sorted(slices.Values(got)), slices.Sorted(slices.Values(want))) {
				problems = append(problems, name+" calls "+strings.Join(got, ",")+", pinned "+strings.Join(want, ","))
			}
		}
	}
	for name := range bootstrapRecordMethods {
		if !seen[name] {
			problems = append(problems, name+" is pinned but missing")
		}
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("bootstrapRecord breaks its lock contract: %v", problems)
	}
}

// TestBootstrapRecordWritersAreAudited pins every production function that
// assigns config, keyring or cephVersion of the record (through the receiver r
// of its methods, or Container.bootstrap in a hook): the unlocked startup
// recorders, the two methods that take the write lock, and the three
// TestSet* hooks whose docs name the lock contract.
func TestBootstrapRecordWritersAreAudited(t *testing.T) {
	docs := osdRegistryDocs(t)
	got := map[string]bool{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			writes := false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if as, ok := n.(*ast.AssignStmt); ok {
					for _, lhs := range as.Lhs {
						sel, ok := lhs.(*ast.SelectorExpr)
						if !ok || !(sel.Sel.Name == "config" || sel.Sel.Name == "keyring" || sel.Sel.Name == "cephVersion") {
							continue
						}
						if id, ok := sel.X.(*ast.Ident); ok && id.Name == "r" && isBootstrapReceiver(fn) {
							writes = true
						}
						if inner, ok := sel.X.(*ast.SelectorExpr); ok && bootstrapUse(inner) && isContainerReceiver(inner.X) {
							writes = true
						}
					}
				}
				return true
			})
			if !writes {
				continue
			}
			name := fn.Name.Name
			got[name] = true
			mode, known := bootstrapRecordWriters[name]
			switch {
			case !known:
				problems = append(problems, name+" writes the bootstrap record but is not in the writer table")
			case mode == "takes" && !callsMethod(fn, "mu", "Lock"):
				problems = append(problems, name+" must take the write lock around its write")
			case mode == "startup" && callsMethod(fn, "mu", "Lock"):
				problems = append(problems, name+" is a startup row but takes a lock")
			case mode == "hook" && !(strings.Contains(docs[name], "holds") && strings.Contains(docs[name], "not shared")):
				problems = append(problems, name+" is a hook row but its doc comment does not name the lock contract")
			}
		}
	}
	for name := range bootstrapRecordWriters {
		if !got[name] {
			problems = append(problems, name+" is listed as a writer but no longer writes the record")
		}
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("bootstrap record writers changed: %v", problems)
	}
}

// TestBootstrapRecordCallersAreAudited is the caller-table guard: the set of
// functions that use the component and the method each calls are pinned, a
// "takes" caller takes the topology lock before its first use, a "startup"
// caller is Run or the one function Run calls, the lock-free FactsOf source is
// the single "lockfree" caller, the lock identity has the single caller
// UnitState, and a stale row fails.
func TestBootstrapRecordCallersAreAudited(t *testing.T) {
	found := map[string]map[string]bool{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || isBootstrapReceiver(fn) {
				continue
			}
			var lockAt token.Pos
			uses := map[string]token.Pos{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if isTopologyAcquire(call) && lockAt == token.NoPos {
					lockAt = call.Pos()
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					if recv, ok := sel.X.(*ast.SelectorExpr); ok && bootstrapUse(recv) {
						if _, seen := uses[sel.Sel.Name]; !seen {
							uses[sel.Sel.Name] = call.Pos()
						}
					}
				}
				return true
			})
			name := fn.Name.Name
			for method, at := range uses {
				if found[name] == nil {
					found[name] = map[string]bool{}
				}
				found[name][method] = true
				mode, known := bootstrapRecordCallers[name][method]
				switch {
				case !known:
					problems = append(problems, name+" calls bootstrap."+method+" but is not in bootstrapRecordCallers")
				case mode == "takes" && (lockAt == token.NoPos || lockAt > at):
					problems = append(problems, name+" must take the topology lock before bootstrap."+method)
				case mode == "startup" && name != "bootstrapFirstMonitor" && name != "recordCephVersion":
					problems = append(problems, name+" writes the bootstrap record outside startup")
				case mode == "lockfree" && name != "staticFacts":
					problems = append(problems, name+" reads the bootstrap record without a lock")
				case mode == "identity" && name != "UnitState":
					problems = append(problems, name+" publishes the bootstrap-record lock identity")
				}
			}
		}
	}
	for name, methods := range bootstrapRecordCallers {
		for method := range methods {
			if !found[name][method] {
				problems = append(problems, name+" is listed for bootstrap."+method+" but no longer calls it")
			}
		}
	}
	lockers := 0
	for _, methods := range found {
		if methods["mutex"] {
			lockers++
		}
	}
	if lockers != 1 || !found["UnitState"]["mutex"] {
		problems = append(problems, "bootstrap.mutex must have the single caller UnitState")
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("bootstrap record callers break the lock contract: %v", problems)
	}
}

// TestBootstrapRecordSourceIsInTheRightFile pins the file of the component so a
// copy under another name cannot hide a second definition.
func TestBootstrapRecordSourceIsInTheRightFile(t *testing.T) {
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			if gen, ok := decl.(*ast.GenDecl); ok {
				for _, spec := range gen.Specs {
					if ts, ok := spec.(*ast.TypeSpec); ok && ts.Name.Name == "bootstrapRecord" && filepath.Base(file) != "bootstrap_record.go" {
						t.Fatalf("bootstrapRecord is declared in %s", file)
					}
				}
			}
		}
	}
}
