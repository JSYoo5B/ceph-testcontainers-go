package ceph

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// chainOwnerFake is a small lease owner: it returns what the test gives it and
// can end the context while answering.
type chainOwnerFake struct {
	ev       osdChainEvidence
	err      error
	asked    []string
	endsCtx  context.CancelFunc
	answered bool
}

func (f *chainOwnerFake) chainEvidence(_ context.Context, lease string) (osdChainEvidence, error) {
	f.asked = append(f.asked, lease)
	f.answered = true
	if f.endsCtx != nil {
		f.endsCtx()
	}
	return f.ev, f.err
}

// quietChain is the evidence of a fully stopped, closed and scanned chain.
func quietChain(lease string, d osdBlockDevice) osdChainEvidence {
	return osdChainEvidence{lease: lease, device: d, daemonStopped: osdChainYes, borrowsClosed: osdChainYes, scanComplete: osdChainYes}
}

func judgeWith(t *testing.T, s *osdDeviceClaims, lease string, d osdBlockDevice, ev osdChainEvidence, evErr error) osdChainVerdict {
	t.Helper()
	return judgeChainRelease(s.exclusionOf(lease, d), ev, evErr, lease, d)
}

func TestJudgeChainReleaseIsClearOnlyWithBothReceipts(t *testing.T) {
	d := claimDevice("a", 1)
	var s osdDeviceClaims
	v := judgeWith(t, &s, "lease-a", d, quietChain("lease-a", d), nil)
	if v.outcome != osdChainClear || v.retained() {
		t.Fatalf("both receipts present: %+v", v)
	}
	if (osdChainVerdict{}).outcome != osdChainRetainUnknown || !(osdChainVerdict{}).retained() {
		t.Fatal("the zero verdict does not retain")
	}
}

func TestJudgeChainReleaseHeldClaimRetainsWhateverTheOwnerSays(t *testing.T) {
	d := claimDevice("a", 1)
	for name, other := range map[string]struct {
		lease string
		d     osdBlockDevice
	}{
		"same lease":     {"lease-a", claimDevice("z", 9)},
		"same node":      {"lease-z", claimDevice("a", 9)},
		"same major:min": {"lease-z", claimDevice("z", 1)},
	} {
		t.Run(name, func(t *testing.T) {
			var s osdDeviceClaims
			mustClaim(t, &s, 4, other.lease, other.d)
			v := judgeWith(t, &s, "lease-a", d, quietChain("lease-a", d), nil)
			if v.outcome != osdChainRetainClaim || !strings.Contains(v.detail, "osd.4") {
				t.Fatalf("verdict %+v", v)
			}
			// A claim wins over an owner failure too: it is reported as a claim.
			if v := judgeWith(t, &s, "lease-a", d, osdChainEvidence{}, errors.New("scan failed")); v.outcome != osdChainRetainClaim {
				t.Fatalf("verdict with owner error %+v", v)
			}
		})
	}
	t.Run("unrelated claim does not retain", func(t *testing.T) {
		var s osdDeviceClaims
		mustClaim(t, &s, 4, "lease-z", claimDevice("z", 9))
		if v := judgeWith(t, &s, "lease-a", d, quietChain("lease-a", d), nil); v.outcome != osdChainClear {
			t.Fatalf("verdict %+v", v)
		}
	})
}

// A claim is dropped only on container evidence, and only that drop turns the
// cluster receipt clear. Owner evidence alone cannot.
func TestJudgeChainReleaseFollowsTheClaimTablesOwnRules(t *testing.T) {
	d := claimDevice("a", 1)
	var s osdDeviceClaims
	creation := mustClaim(t, &s, 0, "lease-a", d)
	creation.Store(true) // creation attempted
	if s.release(0, false) {
		t.Fatal("attempted claim dropped without container evidence")
	}
	if v := judgeWith(t, &s, "lease-a", d, quietChain("lease-a", d), nil); v.outcome != osdChainRetainClaim {
		t.Fatalf("attempted claim, quiet owner: %+v", v)
	}
	if !s.release(0, true) {
		t.Fatal("confirmed removal did not drop the claim")
	}
	if v := judgeWith(t, &s, "lease-a", d, quietChain("lease-a", d), nil); v.outcome != osdChainClear {
		t.Fatalf("dropped claim, quiet owner: %+v", v)
	}
	// The cluster receipt alone is not clearance.
	if v := judgeWith(t, &s, "lease-a", d, osdChainEvidence{}, nil); v.outcome != osdChainRetainIdentity {
		t.Fatalf("dropped claim, empty evidence: %+v", v)
	}
}

func TestJudgeChainReleaseOwnerEvidenceTable(t *testing.T) {
	d := claimDevice("a", 1)
	other := func(f func(*osdChainEvidence)) osdChainEvidence {
		ev := quietChain("lease-a", d)
		f(&ev)
		return ev
	}
	for name, tc := range map[string]struct {
		ev   osdChainEvidence
		err  error
		want osdChainOutcome
	}{
		"owner error":               {quietChain("lease-a", d), errors.New("proc unreadable"), osdChainRetainUnknown},
		"zero evidence":             {osdChainEvidence{}, nil, osdChainRetainIdentity},
		"identity only":             {osdChainEvidence{lease: "lease-a", device: d}, nil, osdChainRetainUnknown},
		"other lease":               {other(func(e *osdChainEvidence) { e.lease = "lease-b" }), nil, osdChainRetainIdentity},
		"other node":                {other(func(e *osdChainEvidence) { e.device = claimDevice("b", 1) }), nil, osdChainRetainIdentity},
		"other minor":               {other(func(e *osdChainEvidence) { e.device.minor = 2 }), nil, osdChainRetainIdentity},
		"other size":                {other(func(e *osdChainEvidence) { e.device.sizeBytes += 512 }), nil, osdChainRetainIdentity},
		"daemon running":            {other(func(e *osdChainEvidence) { e.daemonStopped = osdChainNo }), nil, osdChainRetainBusy},
		"daemon unknown":            {other(func(e *osdChainEvidence) { e.daemonStopped = osdChainUnknown }), nil, osdChainRetainUnknown},
		"borrow open":               {other(func(e *osdChainEvidence) { e.borrowsClosed = osdChainNo }), nil, osdChainRetainBusy},
		"borrow unknown":            {other(func(e *osdChainEvidence) { e.borrowsClosed = osdChainUnknown }), nil, osdChainRetainUnknown},
		"scan incomplete":           {other(func(e *osdChainEvidence) { e.scanComplete = osdChainNo }), nil, osdChainRetainUnknown},
		"scan unknown":              {other(func(e *osdChainEvidence) { e.scanComplete = osdChainUnknown }), nil, osdChainRetainUnknown},
		"scan incomplete, no users": {other(func(e *osdChainEvidence) { e.scanComplete, e.openers = osdChainNo, 0 }), nil, osdChainRetainUnknown},
		"opener found":              {other(func(e *osdChainEvidence) { e.openers = 1 }), nil, osdChainRetainBusy},
		"negative opener count":     {other(func(e *osdChainEvidence) { e.openers = -1 }), nil, osdChainRetainUnknown},
		"invalid value outside":     {other(func(e *osdChainEvidence) { e.daemonStopped = osdChainFact(9) }), nil, osdChainRetainUnknown},
	} {
		t.Run(name, func(t *testing.T) {
			var s osdDeviceClaims
			v := judgeWith(t, &s, "lease-a", d, tc.ev, tc.err)
			if v.outcome != tc.want || !v.retained() || v.detail == "" {
				t.Fatalf("verdict %+v, want outcome %d retained", v, tc.want)
			}
		})
	}
}

func TestJudgeChainReleaseRefusesInadmissibleInputsAndMismatchedReceipts(t *testing.T) {
	d := claimDevice("a", 1)
	var s osdDeviceClaims
	good := quietChain("lease-a", d)
	for name, v := range map[string]osdChainVerdict{
		"bad lease":        judgeChainRelease(s.exclusionOf("a b", d), good, nil, "a b", d),
		"bad device":       judgeChainRelease(s.exclusionOf("lease-a", osdBlockDevice{}), good, nil, "lease-a", osdBlockDevice{}),
		"receipt lease":    judgeChainRelease(s.exclusionOf("lease-b", d), good, nil, "lease-a", d),
		"receipt device":   judgeChainRelease(s.exclusionOf("lease-a", claimDevice("b", 2)), good, nil, "lease-a", d),
		"zero receipt":     judgeChainRelease(osdClusterExclusion{}, good, nil, "lease-a", d),
		"stale held false": judgeChainRelease(osdClusterExclusion{lease: "lease-a", device: claimDevice("b", 2)}, good, nil, "lease-a", d),
	} {
		if v.outcome != osdChainRetainUnknown {
			t.Errorf("%s: %+v", name, v)
		}
	}
}

func TestExclusionOfReadsWithoutChangingTheTable(t *testing.T) {
	var s osdDeviceClaims
	mustClaim(t, &s, 1, "lease-a", claimDevice("a", 1))
	mustClaim(t, &s, 2, "lease-b", claimDevice("b", 2))
	before := map[int]osdDeviceClaimInfo{}
	for _, id := range s.osds() {
		before[id], _ = s.claimOf(id)
	}
	for i := 0; i < 2; i++ {
		_ = s.exclusionOf("lease-a", claimDevice("a", 1))
		_ = s.exclusionOf("lease-q", claimDevice("q", 7))
	}
	for id, want := range before {
		if got, _ := s.claimOf(id); got != want {
			t.Fatalf("osd.%d changed: %+v -> %+v", id, want, got)
		}
	}
	if s.count() != 2 {
		t.Fatalf("count %d", s.count())
	}
	if ex := s.exclusionOf("lease-q", claimDevice("q", 7)); ex.held {
		t.Fatalf("unrelated device reported held: %+v", ex)
	}
	var empty osdDeviceClaims
	if ex := empty.exclusionOf("lease-a", claimDevice("a", 1)); ex.held || empty.records != nil {
		t.Fatalf("empty table: %+v records %v", ex, empty.records)
	}
}

func TestCollectChainEvidence(t *testing.T) {
	d := claimDevice("a", 1)
	t.Run("returns the owner's copy and asks for the given lease", func(t *testing.T) {
		f := &chainOwnerFake{ev: quietChain("lease-a", d)}
		ev, err := collectChainEvidence(t.Context(), f, "lease-a")
		if err != nil || ev != f.ev || !slices.Equal(f.asked, []string{"lease-a"}) {
			t.Fatalf("ev %+v err %v asked %v", ev, err, f.asked)
		}
	})
	t.Run("nil owner", func(t *testing.T) {
		if _, err := collectChainEvidence(t.Context(), nil, "lease-a"); err == nil {
			t.Fatal("nil owner admitted")
		}
	})
	t.Run("typed nil owner", func(t *testing.T) {
		var f *chainOwnerFake
		if _, err := collectChainEvidence(t.Context(), f, "lease-a"); err == nil || !strings.Contains(err.Error(), "typed nil") {
			t.Fatalf("typed nil owner admitted: %v", err)
		}
	})
	t.Run("owner error is not evidence", func(t *testing.T) {
		f := &chainOwnerFake{ev: quietChain("lease-a", d), err: errors.New("proc unreadable")}
		ev, err := collectChainEvidence(t.Context(), f, "lease-a")
		if err == nil || ev != (osdChainEvidence{}) {
			t.Fatalf("ev %+v err %v", ev, err)
		}
	})
	t.Run("context ended before asking", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		f := &chainOwnerFake{ev: quietChain("lease-a", d)}
		if _, err := collectChainEvidence(ctx, f, "lease-a"); !errors.Is(err, context.Canceled) || f.answered {
			t.Fatalf("err %v, owner asked %v", err, f.answered)
		}
	})
	t.Run("answer after the deadline is not evidence", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		f := &chainOwnerFake{ev: quietChain("lease-a", d), endsCtx: cancel}
		ev, err := collectChainEvidence(ctx, f, "lease-a")
		if !errors.Is(err, context.Canceled) || ev != (osdChainEvidence{}) || !f.answered {
			t.Fatalf("ev %+v err %v", ev, err)
		}
		// The caller joins the error, so the verdict retains as unknown, not clear.
		var s osdDeviceClaims
		if v := judgeWith(t, &s, "lease-a", d, ev, err); v.outcome != osdChainRetainUnknown {
			t.Fatalf("verdict %+v", v)
		}
	})
}

func TestOSDChainLeaseOwnerCannotActOnTheChain(t *testing.T) {
	typ := reflect.TypeOf((*osdChainLeaseOwner)(nil)).Elem()
	if typ.NumMethod() != 1 || typ.Method(0).Name != "chainEvidence" {
		t.Fatalf("the owner interface has %d methods; it must offer evidence only", typ.NumMethod())
	}
}

// --- AST guards -------------------------------------------------------------

// osdChainReleaseCallers is the caller table of the three entry points of the
// gate. The single permitted production caller is none.
var osdChainReleaseCallers = map[string]bool{}

var osdChainGateNames = []string{"judgeChainRelease", "collectChainEvidence", "exclusionOf"}

var osdChainGateImports = []string{`"context"`, `"errors"`, `"fmt"`, `"reflect"`}

// osdChainGateProblems checks the production sources against the contract of
// the gate: no production caller, a closed import set, no lock, goroutine,
// defer or exported name, the judge's claim check before any owner evidence,
// and a claim-table read that assigns nothing.
func osdChainGateProblems(files map[string]*ast.File) []string {
	var problems []string
	judges := 0
	for file, parsed := range files {
		gate := file == "osd_device_release_gate.go"
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if !gate || !slices.Contains(osdChainGateNames, fn.Name.Name) {
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if id, ok := n.(*ast.Ident); ok && slices.Contains(osdChainGateNames, id.Name) && !osdChainReleaseCallers[fn.Name.Name] {
						if !(gate && fn.Name.Name == "collectChainEvidence") {
							problems = append(problems, file+": "+fn.Name.Name+" uses "+id.Name+" but is not in osdChainReleaseCallers")
						}
					}
					return true
				})
			}
			if gate {
				problems = append(problems, osdChainGateFuncProblems(fn, &judges)...)
			}
		}
		if !gate {
			continue
		}
		for _, imp := range parsed.Imports {
			if !slices.Contains(osdChainGateImports, imp.Path.Value) {
				problems = append(problems, "osd_device_release_gate.go imports "+imp.Path.Value)
			}
		}
		for _, decl := range parsed.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Name.IsExported() {
					problems = append(problems, "osd_device_release_gate.go declares exported "+d.Name.Name)
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					if ts, ok := spec.(*ast.TypeSpec); ok && ts.Name.IsExported() {
						problems = append(problems, "osd_device_release_gate.go declares exported type "+ts.Name.Name)
					}
				}
			}
		}
	}
	if judges != 1 {
		problems = append(problems, fmt.Sprintf("judgeChainRelease declared %d times, want once", judges))
	}
	return problems
}

func osdChainGateFuncProblems(fn *ast.FuncDecl, judges *int) []string {
	var problems []string
	firstHeld, firstErr := token.NoPos, token.NoPos
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.GoStmt:
			problems = append(problems, fn.Name.Name+" starts a goroutine")
		case *ast.DeferStmt:
			problems = append(problems, fn.Name.Name+" defers")
		case *ast.SelectorExpr:
			switch x.Sel.Name {
			case "Lock", "Unlock", "TryLock", "RLock", "RUnlock", "lockTopology", "mu":
				problems = append(problems, fn.Name.Name+" touches "+x.Sel.Name+": the caller holds Container.mu")
			case "held":
				if firstHeld == token.NoPos {
					firstHeld = x.Pos()
				}
			}
		case *ast.Ident:
			if x.Name == "evErr" && firstErr == token.NoPos {
				firstErr = x.Pos()
			}
		}
		return true
	})
	switch fn.Name.Name {
	case "judgeChainRelease":
		*judges++
		if firstHeld == token.NoPos || firstErr == token.NoPos || firstHeld > firstErr {
			problems = append(problems, "judgeChainRelease must test the claim receipt before it looks at the owner outcome")
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				switch types.ExprString(call.Fun) {
				case "osdDeviceLease.MatchString", "device.validate", "retain", "fmt.Sprintf", "ev.device.number":
				default:
					problems = append(problems, "judgeChainRelease calls "+types.ExprString(call.Fun)+": it is pure")
				}
			}
			return true
		})
	case "exclusionOf":
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if as, ok := n.(*ast.AssignStmt); ok {
				for _, l := range as.Lhs {
					if strings.HasPrefix(types.ExprString(l), "s.") {
						problems = append(problems, "exclusionOf assigns "+types.ExprString(l)+": it only reads the table")
					}
				}
			}
			return true
		})
	}
	return problems
}

func TestOSDChainGateHasNoProductionCallerAndKeepsItsContract(t *testing.T) {
	if problems := osdChainGateProblems(parseProductionFiles(t)); len(problems) != 0 {
		t.Fatalf("the OSD chain gate breaks its contract: %v", problems)
	}
	if len(osdChainReleaseCallers) != 0 {
		t.Fatalf("osdChainReleaseCallers = %v, want no production caller", osdChainReleaseCallers)
	}
}

func TestOSDChainGateGuardRejectsBrokenVariants(t *testing.T) {
	source, err := os.ReadFile("osd_device_release_gate.go")
	if err != nil {
		t.Fatal(err)
	}
	original := string(source)
	mutants := map[string][2]string{
		"owner outcome before the claim": {"\tif ex.held {", "\tif evErr == nil && ex.held {"},
		"judge takes a lock":             {"\tretain := func(", "\tc.mu.Lock()\n\tretain := func("},
		"judge defers":                   {"\tretain := func(", "\tdefer func() {}()\n\tretain := func("},
		"judge starts a goroutine":       {"\tretain := func(", "\tgo func() {}()\n\tretain := func("},
		"judge calls the owner":          {"\tretain := func(", "\t_, _ = collectChainEvidence(nil, nil, lease)\n\tretain := func("},
		"read assigns the table":         {"\tex := osdClusterExclusion{lease: lease, device: device}\n", "\tex := osdClusterExclusion{lease: lease, device: device}\n\ts.records = nil\n"},
		"imports a process package":      {"import (\n\t\"context\"", "import (\n\t\"os/exec\"\n\t\"context\""},
		"exported release helper":        {"// osdChainOutcome is why", "func ReleaseChain() {}\n\n// osdChainOutcome is why"},
	}
	if problems := osdChainGateProblems(map[string]*ast.File{"osd_device_release_gate.go": parseSourceString(t, original)}); len(problems) != 0 {
		t.Fatalf("control: the production source is rejected: %v", problems)
	}
	for name, m := range mutants {
		t.Run(name, func(t *testing.T) {
			if strings.Count(original, m[0]) != 1 {
				t.Fatalf("mutation anchor occurs %d times; update the mutant", strings.Count(original, m[0]))
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), "osd_device_release_gate.go", strings.Replace(original, m[0], m[1], 1), 0)
			if err != nil {
				t.Fatal(err)
			}
			if problems := osdChainGateProblems(map[string]*ast.File{"osd_device_release_gate.go": parsed}); len(problems) == 0 {
				t.Fatal("the guard accepted a broken gate")
			}
		})
	}
	for name, src := range map[string]string{
		"production caller of the judge":     "package ceph\nfunc (c *Container) wrapUp() { _ = judgeChainRelease(osdClusterExclusion{}, osdChainEvidence{}, nil, \"x\", osdBlockDevice{}) }\n",
		"production caller of the collector": "package ceph\nfunc (c *Container) wrapUp() { _, _ = collectChainEvidence(nil, nil, \"x\") }\n",
		"production caller of the receipt":   "package ceph\nfunc (c *Container) wrapUp() { _ = c.deviceClaims.exclusionOf(\"x\", osdBlockDevice{}) }\n",
	} {
		t.Run(name, func(t *testing.T) {
			if problems := osdChainGateProblems(map[string]*ast.File{"x.go": parseSourceString(t, src)}); len(problems) == 0 {
				t.Fatal("a production caller was accepted")
			}
		})
	}
}
