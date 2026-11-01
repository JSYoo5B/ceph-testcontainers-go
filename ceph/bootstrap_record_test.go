package ceph

import (
	"bytes"
	"context"
	"errors"
	"go/ast"
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
var bootstrapRecordCallers = map[string]stateCaller{
	"createClient":                     {"takes", []string{"hasConfig", "rawConfig"}},
	"WithIdentity":                     {"takes", []string{"rawConfig"}},
	"startService":                     {"takes", []string{"isComplete"}},
	"AddMonitor":                       {"takes", []string{"rawConfig"}},
	"replaceMonitor":                   {"takes", []string{"rawConfig"}},
	"bootstrapFirstMonitor":            {"startup", []string{"recordConfig", "recordKeyring"}},
	"recordCephVersion":                {"startup", []string{"recordVersion"}},
	"staticFacts":                      {"lockfree", []string{"rawVersion"}},
	"adminClient":                      {"free", []string{"clones"}},
	"currentMonitorBootstrapAddresses": {"free", []string{"templateAndConfirmed"}},
	"newOSDLaunchRecipe":               {"free", []string{"launchStrings"}},
	"refreshMonitorConfigAfterRemoval": {"free", []string{"publishConfig", "template"}},
	"configureDefaultCRUSHRoot":        {"free", []string{"applyDefaultCRUSHRule"}},
	"evidenceInventory":                {"free", []string{"diagnosticKeyring"}},
	"fullRatiosSession":                {"free", []string{"identity"}},
	"HealthDetails":                    {"free", []string{"identity"}},
	"PGs":                              {"free", []string{"identity"}},
	"Usage":                            {"free", []string{"identity"}},
	"ClusterIdentity":                  {"free", []string{"identity"}},
	"ConnectionConfig":                 {"free", []string{"connection"}},
	"UnitState":                        {"identity", []string{"mutex"}},
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
	fieldAudit{label: "Container.bootstrap", use: bootstrapUse, own: isBootstrapReceiver, methods: bootstrapRecordMethods, bound: monitorTopologyBinding, literalField: "bootstrap", factsReads: []string{"rawVersion"},
		counted: func(fn *ast.FuncDecl, sel *ast.SelectorExpr) bool {
			return isContainerReceiver(sel.X) || receiverTypeName(fn) == "monitorTopology"
		},
		allow: func(_ string, fn *ast.FuncDecl, n ast.Node, allowed map[*ast.SelectorExpr]bool) []string {
			// c.bootstrap.config = x, in a UnitSet hook only.
			assign, ok := n.(*ast.AssignStmt)
			if !ok || receiverTypeName(fn) != "unitAccess" || !strings.HasPrefix(fn.Name.Name, "UnitSet") {
				return nil
			}
			for _, lhs := range assign.Lhs {
				if sel, ok := lhs.(*ast.SelectorExpr); ok && bootstrapFields[sel.Sel.Name] {
					if recv, ok := sel.X.(*ast.SelectorExpr); ok && bootstrapUse(recv) {
						allowed[recv] = true
					}
				}
			}
			return nil
		}}.check(t)
}

// TestBootstrapRecordMethodsMakeOnlyTheirPinnedCalls guards the lock contract
// of the type: the method set is pinned, each method makes exactly the calls
// listed (no other lock, no Container call), and no method names Container,
// closed or another mutex.
func TestBootstrapRecordMethodsMakeOnlyTheirPinnedCalls(t *testing.T) {
	methodAudit{label: "bootstrapRecord", own: isBootstrapReceiver, methods: bootstrapRecordMethods,
		render:    func(call *ast.CallExpr) string { return types.ExprString(call) },
		forbidden: []string{"Container", "closed", "controlPlane", "settings"}, forbiddenMembers: []string{"mu", "controlMu", "configMu", "extensionsMu"}, self: "r"}.check(t)
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
	callerAudit{label: "the bootstrap record", use: bootstrapUse, own: isBootstrapReceiver, callers: bootstrapRecordCallers,
		only: map[string][]string{"startup": {"bootstrapFirstMonitor", "recordCephVersion"}, "lockfree": {"staticFacts"}, "identity": {"UnitState"}}}.check(t)
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
