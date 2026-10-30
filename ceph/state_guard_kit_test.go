package ceph

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"slices"
	"strings"
	"testing"
)

// The state guards of this package share four checks, one per invariant a
// state component or an owner view states about itself. Each guard test
// declares the component's description and calls the check; what is
// particular to one component stays in its own test.
//
//	fieldAudit   outside the component's methods, its field is only the
//	             receiver of one of its methods (or bound by its owner's
//	             constructor), and the lock-free FactsOf source never reads it
//	methodAudit  the method set is pinned, each method makes exactly the calls
//	             listed, and each member is assigned only by its writers
//	callerAudit  every function that uses the component is pinned with the
//	             methods it calls and how it is guarded by Container.mu
//	bindingAudit an owner view is built by one literal in its constructor,
//	             which binds each of its states once by address

// fieldAudit describes how a component's field may be reached.
type fieldAudit struct {
	// label names the field in messages, e.g. "Container.osdHandles".
	label string
	// use reports whether sel reaches the component.
	use func(sel *ast.SelectorExpr) bool
	// own reports whether fn is one of the component's methods.
	own func(fn *ast.FuncDecl) bool
	// methods are the component's methods, the only calls the field may
	// receive.
	methods map[string][]string
	// bound reports whether fn is the owner constructor that binds the
	// field; nil when no owner binds it.
	bound func(fn *ast.FuncDecl) bool
	// counted, when set, limits the uses checked to those it accepts in fn
	// (the selectors of a Container rather than of another type).
	counted func(fn *ast.FuncDecl, sel *ast.SelectorExpr) bool
	// allow marks further selectors as legitimate uses and reports misuses
	// that only this component forbids; it sees every node of a function.
	allow func(file string, fn *ast.FuncDecl, n ast.Node, allowed map[*ast.SelectorExpr]bool) []string
	// literalField, when set, is the Container literal key no production
	// code may set.
	literalField string
	// factsReads are the methods through which the lock-free FactsOf source
	// may read the component; by default it may not reach it at all.
	factsReads []string
	// wholeFactsFile extends the FactsOf check from its Facts functions to
	// every function of cluster_facts.go.
	wholeFactsFile bool
}

func (a fieldAudit) check(t *testing.T) {
	t.Helper()
	var problems []string
	uses := 0
	for file, parsed := range parseProductionFiles(t) {
		if a.literalField != "" {
			ast.Inspect(parsed, func(n ast.Node) bool {
				lit, ok := n.(*ast.CompositeLit)
				if !ok || types.ExprString(lit.Type) != "Container" {
					return true
				}
				for _, elt := range lit.Elts {
					if kv, ok := elt.(*ast.KeyValueExpr); ok && types.ExprString(kv.Key) == a.literalField {
						problems = append(problems, file+": a Container literal sets "+a.literalField)
					}
				}
				return true
			})
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || a.own(fn) {
				continue
			}
			allowed := map[*ast.SelectorExpr]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if method, ok := call.Fun.(*ast.SelectorExpr); ok {
						if _, known := a.methods[method.Sel.Name]; known {
							if recv, ok := method.X.(*ast.SelectorExpr); ok && a.use(recv) {
								allowed[recv] = true
							}
						}
					}
				}
				if a.allow != nil {
					problems = append(problems, a.allow(file, fn, n, allowed)...)
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok || !a.use(sel) || (a.counted != nil && !a.counted(fn, sel)) {
					return true
				}
				if allowed[sel] || (a.bound != nil && a.bound(fn)) {
					uses++
				} else {
					problems = append(problems, file+": "+fn.Name.Name+" uses "+a.label+" outside a method call of its component")
				}
				return true
			})
			if file == "cluster_facts.go" && (a.wholeFactsFile || strings.Contains(fn.Name.Name, "Facts")) {
				permitted := map[*ast.SelectorExpr]bool{}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if call, ok := n.(*ast.CallExpr); ok {
						if method, ok := call.Fun.(*ast.SelectorExpr); ok && slices.Contains(a.factsReads, method.Sel.Name) {
							if recv, ok := method.X.(*ast.SelectorExpr); ok && a.use(recv) {
								permitted[recv] = true
							}
						}
					}
					if sel, ok := n.(*ast.SelectorExpr); ok && a.use(sel) && !permitted[sel] {
						problems = append(problems, "FactsOf source reads "+a.label)
					}
					return true
				})
			}
		}
	}
	if uses == 0 {
		problems = append(problems, "no use of "+a.label+" found: the guard is vacuous")
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("%s escapes its component: %v", a.label, problems)
	}
}

// methodAudit pins what a component's methods do.
type methodAudit struct {
	// label names the component in messages.
	label string
	// own reports whether fn is one of the component's methods.
	own func(fn *ast.FuncDecl) bool
	// methods pins each method's calls, as rendered by render.
	methods map[string][]string
	// render spells a call, or "" for a call the table ignores; nil spells
	// its function expression.
	render func(call *ast.CallExpr) string
	// writers pins the methods that may assign each member, directly or by
	// index.
	writers map[string][]string
	// forbidden are identifiers no method may name, and forbiddenMembers
	// the selectors no method may read (another lock, the closed flag),
	// except through the receiver named self, the component's own lock.
	forbidden        []string
	forbiddenMembers []string
	self             string
}

func (a methodAudit) check(t *testing.T) {
	t.Helper()
	render := a.render
	if render == nil {
		render = func(call *ast.CallExpr) string { return types.ExprString(call.Fun) }
	}
	seen := map[string]bool{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !a.own(fn) {
				continue
			}
			name := fn.Name.Name
			want, known := a.methods[name]
			if !known {
				problems = append(problems, name+" is not pinned; review the lock contract and update the method table of "+a.label)
				continue
			}
			seen[name] = true
			var got []string
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					if call := render(x); call != "" {
						got = append(got, call)
					}
				case *ast.AssignStmt:
					for _, lhs := range x.Lhs {
						if member := assignedMember(lhs); member != "" {
							if writers, field := a.writers[member]; field && !slices.Contains(writers, name) {
								problems = append(problems, name+" assigns "+member)
							}
						}
					}
				case *ast.Ident:
					if slices.Contains(a.forbidden, x.Name) {
						problems = append(problems, name+" names "+x.Name)
					}
				case *ast.SelectorExpr:
					if id, ok := x.X.(*ast.Ident); ok && a.self != "" && id.Name == a.self {
						break
					}
					if slices.Contains(a.forbiddenMembers, x.Sel.Name) {
						problems = append(problems, name+" reads "+types.ExprString(x))
					}
				}
				return true
			})
			if !slices.Equal(slices.Sorted(slices.Values(got)), slices.Sorted(slices.Values(want))) {
				problems = append(problems, name+" calls "+join(got)+", pinned "+join(want))
			}
		}
	}
	for name := range a.methods {
		if !seen[name] {
			problems = append(problems, name+" is pinned but missing")
		}
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("%s breaks its lock contract: %v", a.label, problems)
	}
}

// assignedMember is the member an assignment target writes, through a
// selector or an index of one, or "".
func assignedMember(lhs ast.Expr) string {
	for {
		switch x := lhs.(type) {
		case *ast.SelectorExpr:
			return x.Sel.Name
		case *ast.IndexExpr:
			lhs = x.X
		default:
			return ""
		}
	}
}

// stateCaller pins one function that uses a component, or that sits between
// such a function and the lock that guards it.
//
//	"takes"  acquires Container.mu itself before its first use
//	"holds"  its doc comment requires the caller to hold Container.mu, it
//	         does not take the lock itself, and every call to it comes from a
//	         function that acquired the lock before the call or is itself a
//	         "holds", "hook" or "init" row
//	"hook"   a hook for another package whose doc comment requires the
//	         caller to hold the lock
//	other    a mode particular to the component (for example "free" for a
//	         method that takes the component's own leaf lock); callerAudit.only
//	         may restrict it to named functions
//
// methods are the component methods it calls directly, empty for a function
// that only calls a "holds" row.
type stateCaller struct {
	mode    string
	methods []string
}

// callerAudit is the caller table of one component.
type callerAudit struct {
	// label names the component in messages.
	label string
	// use reports whether sel, the receiver of a call, reaches the
	// component.
	use func(sel *ast.SelectorExpr) bool
	// own reports whether fn is one of the component's methods.
	own func(fn *ast.FuncDecl) bool
	// callers is the table, keyed by function name, or by "Type.name" when
	// qualified is set.
	callers   map[string]stateCaller
	qualified bool
	// only restricts a mode to the functions named.
	only map[string][]string
	// guarded are methods that need Container.mu: a row that calls one must
	// be a "takes", "holds" or "hook" row.
	guarded []string
	// holdsDoc, when set, is the phrase the doc comment of a "holds" or
	// "hook" row must contain instead of any mention of holding the lock.
	holdsDoc string
}

func (a callerAudit) key(fn *ast.FuncDecl) string {
	if recv := receiverTypeName(fn); a.qualified && recv != "" {
		return recv + "." + fn.Name.Name
	}
	return fn.Name.Name
}

func (a callerAudit) check(t *testing.T) {
	t.Helper()
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
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && !a.own(fn) {
					if recv, ok := sel.X.(*ast.SelectorExpr); ok && a.use(recv) {
						info.methods[sel.Sel.Name] = true
						if firstUse == token.NoPos || call.Pos() < firstUse {
							firstUse = call.Pos()
						}
					}
				}
				return true
			})
			name := a.key(fn)
			infos[name] = append(infos[name], info)
			if len(info.methods) == 0 {
				continue
			}
			row, known := a.callers[name]
			if !known {
				problems = append(problems, name+" uses "+a.label+" but is not in its caller table")
				continue
			}
			if row.mode == "takes" && (info.acquire == token.NoPos || info.acquire > firstUse) {
				problems = append(problems, name+" must take Container.mu before using "+a.label)
			}
			if row.mode == "holds" && info.acquire != token.NoPos {
				problems = append(problems, name+" is a holds row and must not take Container.mu itself")
			}
			got := slices.Sorted(func(yield func(string) bool) {
				for m := range info.methods {
					if !yield(m) {
						return
					}
				}
			})
			if !slices.Equal(got, slices.Sorted(slices.Values(row.methods))) {
				problems = append(problems, name+" calls "+join(got)+" of "+a.label+", pinned "+join(row.methods))
			}
			for _, method := range got {
				if slices.Contains(a.guarded, method) && row.mode != "takes" && row.mode != "holds" && row.mode != "hook" {
					problems = append(problems, name+" calls the mu-guarded "+method+" of "+a.label+" without the topology lock")
				}
			}
		}
	}
	docs := guardDocs(t)
	for name, row := range a.callers {
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
			problems = append(problems, name+" is listed but no longer uses "+a.label)
		}
		if only, restricted := a.only[row.mode]; restricted && !slices.Contains(only, name) {
			problems = append(problems, name+" is a "+row.mode+" row, which only "+join(only)+" may be")
		}
		if row.mode == "holds" || row.mode == "hook" {
			doc := docs[name]
			if said := strings.Contains(doc, "hold") || strings.Contains(doc, " held"); !said || (a.holdsDoc != "" && !strings.Contains(doc, a.holdsDoc)) {
				problems = append(problems, name+" is a "+row.mode+" row but its doc comment does not say that the caller holds the lock")
			}
		}
	}
	for mode, names := range a.only {
		for _, name := range names {
			if row, listed := a.callers[name]; !listed || row.mode != mode {
				problems = append(problems, name+" must be the "+mode+" row of "+a.label)
			}
		}
	}
	// Every call to a "holds" row comes from a guarded function.
	for held, row := range a.callers {
		if row.mode != "holds" {
			continue
		}
		bare := held[strings.LastIndex(held, ".")+1:]
		for caller, decls := range infos {
			for _, info := range decls {
				if caller == held && info.decl.Recv == nil {
					continue
				}
				for _, at := range info.calls[bare] {
					callerRow, listed := a.callers[caller]
					acquired := info.acquire != token.NoPos && info.acquire < at
					if !acquired && !(listed && (callerRow.mode == "holds" || callerRow.mode == "hook" || callerRow.mode == "init")) {
						problems = append(problems, caller+" calls "+held+" without taking Container.mu and is not a pinned holds, hook or init function of "+a.label)
					}
				}
			}
		}
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("callers of %s break the lock contract: %v", a.label, problems)
	}
}

// guardDocs maps each production function to its lower-cased,
// whitespace-normalized doc comment, under its name and, for a method, also
// under "Type.name". Several declarations of one bare name share its entry.
func guardDocs(t *testing.T) map[string]string {
	t.Helper()
	docs := osdRegistryDocs(t)
	for file := range parseProductionFiles(t) {
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range parsed.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Doc != nil {
				if recv := receiverTypeName(fn); recv != "" {
					docs[recv+"."+fn.Name.Name] = strings.ToLower(strings.Join(strings.Fields(fn.Doc.Text()), " "))
				}
			}
		}
	}
	return docs
}

// bindingAudit pins how an owner view is built: one literal of typeName, only
// in its constructor, binding each state in bound exactly once as
// &c.<field>; with singleReturn the constructor is that one return. When
// recv is set, the owner's methods (receiver recv) use recv.owner only as the
// owner of a new handle literal of one of handles.
type bindingAudit struct {
	typeName     string
	constructor  func(fn *ast.FuncDecl) bool
	bound        []string
	singleReturn bool
	// skipEmpty leaves an empty literal of typeName (a zero value) uncounted.
	skipEmpty bool
	recv      string
	handles   []string
}

func (a bindingAudit) check(t *testing.T) {
	t.Helper()
	var problems []string
	literals := 0
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				lit, ok := n.(*ast.CompositeLit)
				if !ok {
					return true
				}
				if id, ok := lit.Type.(*ast.Ident); !ok || id.Name != a.typeName || (a.skipEmpty && len(lit.Elts) == 0) {
					return true
				}
				literals++
				if !a.constructor(fn) {
					problems = append(problems, file+": "+funcKey(fn)+" builds a "+a.typeName)
					return true
				}
				problems = append(problems, checkBoundLiteral(lit, a.typeName, a.bound)...)
				return true
			})
			if a.constructor(fn) && a.singleReturn {
				if len(fn.Body.List) != 1 {
					problems = append(problems, funcKey(fn)+" must be a single return")
				} else if ret, ok := fn.Body.List[0].(*ast.ReturnStmt); !ok || len(ret.Results) != 1 {
					problems = append(problems, funcKey(fn)+" must be a single return")
				} else if _, ok := ret.Results[0].(*ast.CompositeLit); !ok {
					problems = append(problems, funcKey(fn)+" must return a "+a.typeName+" literal")
				}
			}
			if a.recv != "" && receiverTypeName(fn) == a.typeName {
				problems = append(problems, checkOwnerUse(file, fn, a.recv, a.handles...)...)
			}
		}
	}
	if literals != 1 {
		problems = append(problems, fmt.Sprintf("want exactly one %s literal, got %d", a.typeName, literals))
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("the %s binding is not pinned: %v", a.typeName, problems)
	}
}

// checkBoundLiteral checks that lit, a keyed literal of typeName, binds each
// state in bound exactly once, as &c.<field>.
func checkBoundLiteral(lit *ast.CompositeLit, typeName string, bound []string) []string {
	var problems []string
	seen := map[string]int{}
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			problems = append(problems, typeName+" literal must be keyed")
			continue
		}
		key := kv.Key.(*ast.Ident).Name
		if !slices.Contains(bound, key) {
			continue
		}
		address, ok := kv.Value.(*ast.UnaryExpr)
		sel, selOK := (*ast.SelectorExpr)(nil), false
		if ok && address.Op == token.AND {
			sel, selOK = address.X.(*ast.SelectorExpr)
		}
		field := key
		if f, ok := ownerStateFields[key]; ok {
			field = f
		}
		if !selOK || sel.Sel.Name != field || !isContainerReceiver(sel.X) {
			problems = append(problems, typeName+" binds "+key+" to "+types.ExprString(kv.Value)+", want &c."+field)
		}
		seen[key]++
	}
	for _, field := range bound {
		if seen[field] != 1 {
			problems = append(problems, fmt.Sprintf("%s binds %s %d times, want once", typeName, field, seen[field]))
		}
	}
	return problems
}

// checkOwnerUse checks that the methods of a binding type, whose receiver is
// named recv, select recv.owner only as the owner of a new handle literal of
// one of handles.
func checkOwnerUse(file string, fn *ast.FuncDecl, recv string, handles ...string) []string {
	var problems []string
	allowed := map[*ast.SelectorExpr]bool{}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		if id, ok := lit.Type.(*ast.Ident); !ok || !slices.Contains(handles, id.Name) {
			return true
		}
		for _, elt := range lit.Elts {
			if kv, ok := elt.(*ast.KeyValueExpr); ok {
				if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "owner" {
					if sel, ok := kv.Value.(*ast.SelectorExpr); ok {
						allowed[sel] = true
					}
				}
			}
		}
		return true
	})
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "owner" {
			if id, ok := sel.X.(*ast.Ident); ok && id.Name == recv && !allowed[sel] {
				problems = append(problems, file+": "+funcKey(fn)+" uses the owner beyond a new handle's owner")
			}
		}
		return true
	})
	return problems
}

// ownerStateFields are the Container fields that hold an owner's state, by
// the state type: the owner view embeds the type, its constructor binds the
// field.
var ownerStateFields = map[string]string{
	"osdFleetState":   "fleet",
	"membershipState": "members",
	"authLedgerState": "ledger",
}
